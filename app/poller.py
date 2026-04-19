from __future__ import annotations

import logging
import threading

from app.arr_clients import get_arr_client
from app.config import Settings
from app.jobs_store import JobStore
from app.models import map_rd_status, now_utc_iso
from app.rd_client import RealDebridClient
from app.staging import (
    check_staging_ready,
    create_staging_symlink,
    extract_expected_media_size,
    find_matching_media_file,
)

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
                        status = command.get("status")
                        result = command.get("result")
                        if status == "completed" and result == "successful":
                            self.store.merge(
                                job_id,
                                {
                                    "status": "imported",
                                    "imported_at": now_utc_iso(),
                                    "last_error": None,
                                    "arr_scan_command": command,
                                },
                            )
                            logger.info("IMPORT success torrent_id=%s", job_id)
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

                info = self.rd_client.torrent_info(str(rd_id))
                patch = {
                    "rd_status": info.get("status"),
                    "filename": info.get("filename") or job.get("filename"),
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
                    patch["status"] = mapped
                    self.store.merge(job_id, patch)
                    logger.info("POLL updated torrent_id=%s status=%s rd_status=%s", job_id, mapped, info.get("status"))
                    continue

                source_file = find_matching_media_file(info, self.settings.debrid_all_dir)
                if not source_file:
                    patch["status"] = "ready"
                    patch["arr_ready_reason"] = "source_not_found"
                    patch["arr_ready_details"] = {
                        "wanted_filename": info.get("filename"),
                        "search_root": str(self.settings.debrid_all_dir),
                    }
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
