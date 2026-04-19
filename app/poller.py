from __future__ import annotations

import logging
import threading
from pathlib import Path

from app.arr_clients import get_arr_client
from app.config import Settings
from app.jobs_store import JobStore
from app.models import map_rd_status, now_utc_iso
from app.rd_client import RealDebridClient
from app.staging import (
    check_staging_ready,
    create_staging_download,
    create_staging_symlink,
    extract_expected_media_size,
    find_matching_media_entry,
    find_matching_media_file,
)

logger = logging.getLogger(__name__)


def _rd_failure_reason(info: dict) -> str:
    rd_status = str(info.get("status") or "failed").strip() or "failed"
    detail = info.get("error") or info.get("message") or info.get("error_message") or info.get("status_message")
    if detail:
        return f"RD failure: {rd_status} - {detail}"
    return f"RD failure: {rd_status}"


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
                        patch = {
                            "arr_refresh_command": arr_client.refresh_monitored_downloads(),
                            "arr_scan_command": arr_client.trigger_scan(self.settings.visible_staging_root.__class__(arr_path), download_client_id),
                            "status": "scan_pending",
                            "last_error": None,
                        }
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
                staging_path = None
                visible_dir = None
                visible_file = None

                if not source_file and self.settings.debrid_provider == "torbox":
                    remote_item = find_matching_media_entry(info)
                    if remote_item is not None:
                        remote_name = str(remote_item.get("path") or remote_item.get("name") or info.get("filename") or "")
                        try:
                            expected_remote_size = int(remote_item.get("bytes") or 0) or None
                        except Exception:
                            expected_remote_size = None
                        try:
                            download_url = self.rd_client.get_download_url(str(rd_id), str(remote_item.get("id") or "0"))
                            source_file, staging_path, visible_dir, visible_file = create_staging_download(
                                job_id,
                                remote_name,
                                download_url,
                                self.rd_client.download_file,
                                self.settings.staging_root,
                                self.settings.visible_staging_root,
                                expected_remote_size,
                            )
                            logger.info("STAGE downloaded remote source torrent_id=%s file=%s", job_id, Path(remote_name).name)
                        except Exception as exc:
                            logger.warning("STAGE remote download failed torrent_id=%s error=%s", job_id, exc)

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

                if staging_path is None or visible_dir is None or visible_file is None:
                    staging_path, visible_dir, visible_file = create_staging_symlink(
                        job_id,
                        source_file,
                        self.settings.staging_root,
                        self.settings.visible_staging_root,
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
                        patch["arr_scan_command"] = arr_client.trigger_scan(visible_dir, download_client_id)
                        patch["status"] = "scan_pending"

                self.store.merge(job_id, patch)
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
