from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

from app.arr_clients import get_arr_client
from app.config import Settings
from app.jobs_store import JobStore
from app.models import map_rd_status, now_utc_iso
from app.rd_client import RealDebridClient
from app.staging import (
    add_extra_symlinks_to_staging,
    check_staging_ready,
    cleanup_staging_for_job,
    create_staging_symlink,
    episode_in_torrent_files,
    extract_episode_token,
    extract_expected_media_size,
    find_matching_media_file,
    find_sibling_media_files,
    get_last_scan_error,
)

_IMPORTED_RETAIN_SECONDS = 300  # keep imported jobs visible for 5 minutes then purge

logger = logging.getLogger(__name__)


def _rd_failure_reason(info: dict) -> str:
    rd_status = str(info.get("status") or "failed").strip() or "failed"
    detail = info.get("error") or info.get("message") or info.get("error_message") or info.get("status_message")
    if detail:
        return f"RD failure: {rd_status} - {detail}"
    return f"RD failure: {rd_status}"


def _maybe_finalize_scan(arr_client, command: dict | None) -> dict | None:
    if not isinstance(command, dict):
        return None
    command_id = command.get("id")
    if not command_id or not arr_client.is_configured():
        return None

    latest = arr_client.get_command(int(command_id))
    status = latest.get("status")
    result = latest.get("result")
    if status == "completed" and result == "successful":
        return {
            "status": "imported",
            "imported_at": now_utc_iso(),
            "last_error": None,
            "arr_scan_command": latest,
        }
    return {
        "arr_scan_command": latest,
        "status": "scan_pending",
        "last_error": None,
    }


class JobPoller:
    def __init__(self, store: JobStore, rd_client: RealDebridClient, settings: Settings):
        self.store = store
        self.rd_client = rd_client
        self.settings = settings
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if not self.settings.enable_poller:
            logger.info("POLL disabled")
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="rd-poller")
        self._thread.start()
        logger.info("POLL started interval=%ss", self.settings.poll_interval)

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.poll_once()
            except Exception:
                logger.exception("POLL cycle failed")
            self._stop.wait(self.settings.poll_interval)

    def poll_once(self) -> None:
        jobs = self.store.all()
        for job_id, job in jobs.items():
            if not isinstance(job, dict) or job.get("deleted_by_client") or job.get("polling_disabled"):
                continue

            if job.get("status") == "imported":
                # Auto-purge imported jobs after the retain window to keep the
                # jobs list clean. Staging symlinks are cleaned up at this point.
                imported_at = job.get("imported_at")
                if imported_at:
                    try:
                        from datetime import datetime, timezone
                        age = time.time() - datetime.fromisoformat(imported_at).timestamp()
                        if age >= _IMPORTED_RETAIN_SECONDS:
                            cleanup_staging_for_job(job_id, self.settings.staging_root, self.settings.visible_staging_root)
                            self.store.delete(job_id)
                            logger.info("PURGE imported job torrent_id=%s age=%.0fs", job_id, age)
                    except Exception:
                        pass
                continue

            rd_id = job.get("rd_torrent_id") or job.get("torrent_id")
            if not rd_id or not self.rd_client.is_configured():
                continue

            try:
                if job.get("status") == "scan_pending" and isinstance(job.get("arr_scan_command"), dict):
                    arr_client = get_arr_client(job.get("category"), self.settings)
                    command_id = job["arr_scan_command"].get("id")
                    if command_id and arr_client.is_configured():
                        command = arr_client.get_command(int(command_id))
                        cmd_status = command.get("status")
                        cmd_result = command.get("result")
                        if cmd_status == "completed":
                            if cmd_result == "successful":
                                # Sonarr reports the scan command as successful even when all
                                # files were rejected (e.g. FileNotFoundException).  Verify via
                                # history that an import record actually exists.
                                download_client_id = str(job.get("client_hash") or job_id).upper()
                                actually_imported = arr_client.check_history_for_import(download_client_id)
                                if actually_imported:
                                    self.store.merge(
                                        job_id,
                                        {
                                            "status": "imported",
                                            "imported_at": now_utc_iso(),
                                            "last_error": None,
                                            "scan_fail_count": 0,
                                            "arr_scan_command": command,
                                        },
                                    )
                                    logger.info("IMPORT success torrent_id=%s", job_id)
                                else:
                                    # Scan completed but no import record found — reset so the
                                    # file gets re-staged and re-triggered on the next cycle.
                                    logger.warning(
                                        "IMPORT scan_completed_no_history torrent_id=%s download_id=%s; resetting to ready_for_arr",
                                        job_id, download_client_id,
                                    )
                                    self.store.merge(
                                        job_id,
                                        {
                                            "status": "ready_for_arr",
                                            "arr_scan_command": None,
                                            "last_error": "scan completed but no import found in history; will retry",
                                        },
                                    )
                            else:
                                # Scan command failed — count failures and back off
                                fail_count = int(job.get("scan_fail_count") or 0) + 1
                                cmd_message = (command.get("body") or {}).get("message") or command.get("message") or ""
                                logger.warning(
                                    "IMPORT scan_failed torrent_id=%s result=%s fail_count=%d message=%r; resetting to ready_for_arr",
                                    job_id, cmd_result, fail_count, cmd_message,
                                )
                                if fail_count >= 5:
                                    # Repeated Sonarr scan failures almost always mean the ARR
                                    # pod cannot access the symlink target (missing
                                    # mountPropagation: HostToContainer on the Sonarr media
                                    # volume, or the FUSE mount is not propagating to the host).
                                    logger.error(
                                        "IMPORT giving_up torrent_id=%s after %d scan failures; "
                                        "check that Sonarr's media volume has mountPropagation: HostToContainer "
                                        "so it can see the FUSE mount at /data/downloads/torbox",
                                        job_id, fail_count,
                                    )
                                    self.store.merge(
                                        job_id,
                                        {
                                            "status": "ready_for_arr",
                                            "arr_scan_command": None,
                                            "scan_fail_count": fail_count,
                                            "polling_disabled": True,
                                            "last_error": (
                                                f"Sonarr scan failed {fail_count} times with result=unsuccessful. "
                                                "Sonarr cannot read the symlink target. "
                                                "Fix: add mountPropagation: HostToContainer to Sonarr's media volume mount "
                                                "so the FUSE mount at /data/downloads/torbox propagates into the Sonarr pod."
                                            ),
                                        },
                                    )
                                else:
                                    self.store.merge(
                                        job_id,
                                        {
                                            "status": "ready_for_arr",
                                            "arr_scan_command": None,
                                            "scan_fail_count": fail_count,
                                            "last_error": f"scan result={cmd_result} (attempt {fail_count}/5); will retry",
                                        },
                                    )
                        continue

                if job.get("status") == "ready_for_arr":
                    arr_client = get_arr_client(job.get("category"), self.settings)
                    arr_path = job.get("arr_path")
                    if arr_client.is_configured() and arr_path and not job.get("arr_scan_command"):
                        download_client_id = str(job.get("client_hash") or job_id).upper()
                        command = arr_client.trigger_scan(self.settings.visible_staging_root.__class__(arr_path), download_client_id)
                        patch = {
                            "arr_refresh_command": arr_client.refresh_monitored_downloads(),
                            "arr_scan_command": command,
                            "status": "scan_pending",
                            "last_error": None,
                        }
                        completed_patch = _maybe_finalize_scan(arr_client, command)
                        if completed_patch:
                            patch.update(completed_patch)
                        self.store.merge(job_id, patch)
                        logger.info("POLL queued import scan torrent_id=%s", job_id)
                    continue

                try:
                    info = self.rd_client.torrent_info(str(rd_id))
                except RuntimeError as exc:
                    message = str(exc)
                    cached_info = job.get("raw") if isinstance(job.get("raw"), dict) else None
                    if "TorBox API failed: 500" in message and cached_info and cached_info.get("status"):
                        info = dict(cached_info)
                        logger.warning(
                            "POLL using cached TorBox info torrent_id=%s rd_id=%s error=%s",
                            job_id,
                            rd_id,
                            message,
                        )
                    else:
                        raise

                patch = {
                    "rd_status": info.get("status"),
                    # Keep the original requested filename; don't overwrite with TorBox's
                    # provider filename which may point to a different deduplicated torrent.
                    "filename": job.get("requested_filename") or job.get("filename") or info.get("filename"),
                    "provider_filename": info.get("filename"),
                    "raw": info,
                    "last_checked_at": now_utc_iso(),
                    "last_error": None,
                }

                mapped = map_rd_status(info.get("status"))
                if mapped == "failed":
                    reason = _rd_failure_reason(info)
                    patch["status"] = mapped
                    patch["last_error"] = reason
                    patch["polling_disabled"] = True
                    self.store.merge(job_id, patch)
                    logger.warning(
                        "POLL failed torrent_id=%s rd_status=%s reason=%s",
                        job_id,
                        info.get("status"),
                        reason,
                    )
                    continue

                if mapped != "ready":
                    if mapped == "stalled":
                        # Track when stalling started; fail after 2 minutes.
                        stalled_since = job.get("stalled_since")
                        if not stalled_since:
                            patch["stalled_since"] = now_utc_iso()
                            patch["status"] = "stalled"
                            self.store.merge(job_id, patch)
                            logger.warning(
                                "POLL stalled torrent_id=%s rd_status=%s; will fail in 2 minutes",
                                job_id, info.get("status"),
                            )
                        else:
                            try:
                                from datetime import datetime, timezone as _tz
                                stalled_age = time.time() - datetime.fromisoformat(stalled_since).timestamp()
                            except Exception:
                                stalled_age = 0
                            if stalled_age >= 120:
                                reason = (
                                    f"TorBox stalled for {int(stalled_age)}s with no progress. "
                                    f"Release has no seeders or is unavailable. Sonarr should retry with a different release."
                                )
                                patch["status"] = "failed"
                                patch["last_error"] = reason
                                patch["polling_disabled"] = True
                                patch["stalled_since"] = None
                                self.store.merge(job_id, patch)
                                logger.error(
                                    "POLL stall_timeout torrent_id=%s stalled_for=%.0fs; failing job",
                                    job_id, stalled_age,
                                )
                            else:
                                patch["status"] = "stalled"
                                self.store.merge(job_id, patch)
                                logger.info(
                                    "POLL stalled torrent_id=%s stalled_for=%.0fs/120s",
                                    job_id, stalled_age,
                                )
                    else:
                        # Clear stalled_since if torrent recovered
                        if job.get("stalled_since"):
                            patch["stalled_since"] = None
                        patch["status"] = mapped
                        self.store.merge(job_id, patch)
                        logger.info("POLL updated torrent_id=%s status=%s rd_status=%s", job_id, mapped, info.get("status"))
                    continue

                source_file = find_matching_media_file(info, self.settings.debrid_all_dir)

                # Detect TorBox dedup mismatch: if the requested episode token
                # (from the Sonarr grab) doesn't match what's actually on disk,
                # fail fast instead of staging the wrong episode and looping.
                requested_fn = job.get("requested_filename") or job.get("filename") or ""
                if source_file and requested_fn:
                    req_ep = extract_episode_token(requested_fn)
                    got_ep = extract_episode_token(source_file.name)
                    if req_ep and got_ep and req_ep != got_ep and not episode_in_torrent_files(req_ep, info):
                        dedup_count = int(job.get("dedup_check_count") or 0) + 1
                        if dedup_count < 5:
                            # TorBox's file list may be stale — retry a few times
                            # before giving up to avoid false positives on multi-ep
                            # packs whose file list hasn't fully populated yet.
                            patch["dedup_check_count"] = dedup_count
                            patch["status"] = "ready"  # stay in ready, keep retrying
                            self.store.merge(job_id, patch)
                            logger.warning(
                                "STAGE dedup_mismatch torrent_id=%s requested=%s got=%s attempt=%d/5; retrying",
                                job_id, req_ep, got_ep, dedup_count,
                            )
                        else:
                            reason = (
                                f"TorBox dedup mismatch: requested {req_ep} but got {got_ep} "
                                f"({source_file.name}). The magnet was deduplicated to a "
                                f"different cached torrent. Sonarr should retry with a different release."
                            )
                            patch["status"] = "failed"
                            patch["last_error"] = reason
                            patch["polling_disabled"] = True
                            self.store.merge(job_id, patch)
                            logger.error(
                                "STAGE dedup_mismatch torrent_id=%s requested=%s got=%s giving up after %d attempts",
                                job_id, req_ep, got_ep, dedup_count,
                            )
                        continue

                if not source_file:
                    patch["status"] = "ready"
                    patch["arr_ready_reason"] = "source_not_found"
                    details = {
                        "wanted_filename": info.get("filename"),
                        "search_root": str(self.settings.debrid_all_dir),
                    }
                    mount_error = get_last_scan_error(self.settings.debrid_all_dir)
                    if mount_error:
                        details["mount_error"] = mount_error
                    patch["arr_ready_details"] = details
                    self.store.merge(job_id, patch)
                    logger.warning(
                        "STAGE source not found torrent_id=%s filename=%s root=%s",
                        job_id,
                        info.get("filename"),
                        self.settings.debrid_all_dir,
                    )
                    continue

                visible_source_file = source_file
                try:
                    candidate_visible_source = self.settings.visible_debrid_all_dir / source_file.relative_to(self.settings.debrid_all_dir)
                    if candidate_visible_source.exists() or candidate_visible_source.parent.exists():
                        visible_source_file = candidate_visible_source
                except ValueError:
                    visible_source_file = source_file

                staging_path, visible_dir, visible_file = create_staging_symlink(
                    job_id,
                    source_file,
                    self.settings.staging_root,
                    self.settings.visible_staging_root,
                    visible_source_file=visible_source_file,
                    category=job.get("category"),
                )

                # Season-pack support: symlink every sibling video file in the
                # same torrent directory so Sonarr imports all episodes at once.
                sibling_files = find_sibling_media_files(source_file)
                if sibling_files:
                    sibling_visible: list[Path] = []
                    for sib in sibling_files:
                        try:
                            vis_sib = self.settings.visible_debrid_all_dir / sib.relative_to(self.settings.debrid_all_dir)
                            sibling_visible.append(vis_sib if (vis_sib.exists() or vis_sib.parent.exists()) else sib)
                        except ValueError:
                            sibling_visible.append(sib)
                    add_extra_symlinks_to_staging(
                        job_id,
                        source_file,
                        sibling_files,
                        self.settings.staging_root,
                        self.settings.visible_staging_root,
                        sibling_visible,
                        category=job.get("category"),
                    )
                    logger.info(
                        "STAGE season_pack torrent_id=%s primary=%s extra_episodes=%d",
                        job_id, source_file.name, len(sibling_files),
                    )

                # Reset scan failure counter if the staging path changed (e.g. after a
                # pod restart, dedup fix, or corrected source file) so stale failure
                # counts from a previous bad staging don't cause premature give-up.
                prev_staging_path = job.get("staging_path") or ""
                if prev_staging_path and prev_staging_path != str(staging_path):
                    logger.info(
                        "STAGE path changed torrent_id=%s old=%s new=%s; resetting scan_fail_count",
                        job_id, prev_staging_path, staging_path,
                    )
                    patch["scan_fail_count"] = 0

                expected_media_size = extract_expected_media_size(info, source_file)
                host_ready, host_reason, host_details = check_staging_ready(
                    staging_path,
                    expected_media_size,
                    self.settings.import_stability_min_bytes,
                )
                visible_ready, visible_reason, visible_details = check_staging_ready(
                    visible_file,
                    expected_media_size,
                    self.settings.import_stability_min_bytes,
                )

                ready = host_ready and visible_ready
                reason = "ready" if ready else (visible_reason if not visible_ready else host_reason)
                details = {
                    "host": host_details,
                    "visible": visible_details,
                }

                patch.update(
                    {
                        "source_path": str(source_file),
                        "staging_path": str(staging_path),
                        "arr_path": str(visible_dir),
                        "arr_file_path": str(visible_file),
                        "arr_ready_reason": reason,
                        "arr_ready_details": details,
                        "status": "staged",
                    }
                )

                if ready:
                    patch["status"] = "ready_for_arr"
                    arr_client = get_arr_client(job.get("category"), self.settings)
                    if arr_client.is_configured() and not job.get("arr_scan_command"):
                        download_client_id = str(job.get("client_hash") or job_id).upper()
                        patch["arr_refresh_command"] = arr_client.refresh_monitored_downloads()
                        command = arr_client.trigger_scan(visible_dir, download_client_id)
                        patch["arr_scan_command"] = command
                        patch["status"] = "scan_pending"
                        completed_patch = _maybe_finalize_scan(arr_client, command)
                        if completed_patch:
                            patch.update(completed_patch)

                self.store.merge(job_id, patch)
                if patch["status"] == "staged":
                    logger.info(
                        "STAGE pending torrent_id=%s reason=%s details=%s",
                        job_id,
                        patch.get("arr_ready_reason"),
                        patch.get("arr_ready_details"),
                    )
                logger.info("POLL updated torrent_id=%s status=%s", job_id, patch['status'])
            except Exception as exc:
                message = str(exc)
                patch = {
                    "last_checked_at": now_utc_iso(),
                    "last_error": message,
                }
                if "unknown_ressource" in message or "404" in message:
                    patch["polling_disabled"] = True
                self.store.merge(job_id, patch)
                logger.exception("POLL error torrent_id=%s", job_id)
