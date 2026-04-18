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
            if not isinstance(job, dict) or job.get("deleted_by_client"):
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

                info = self.rd_client.torrent_info(str(rd_id))
                patch = {
                    "rd_status": info.get("status"),
                    "filename": info.get("filename") or job.get("filename"),
                    "raw": info,
                    "last_checked_at": now_utc_iso(),
                    "last_error": None,
                }

                mapped = map_rd_status(info.get("status"))
                if mapped != "ready":
                    patch["status"] = mapped
                    self.store.merge(job_id, patch)
                    logger.info("POLL updated torrent_id=%s status=%s", job_id, mapped)
                    continue

                source_file = find_matching_media_file(info, self.settings.debrid_all_dir)
                if not source_file:
                    patch["status"] = "ready"
                    patch["arr_ready_reason"] = "source_not_found"
                    self.store.merge(job_id, patch)
                    continue

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
                        download_client_id = str(job.get("client_hash") or job_id)
                        patch["arr_refresh_command"] = arr_client.refresh_monitored_downloads()
                        patch["arr_scan_command"] = arr_client.trigger_scan(visible_dir, download_client_id)
                        patch["status"] = "scan_pending"

                self.store.merge(job_id, patch)
                logger.info("POLL updated torrent_id=%s status=%s", job_id, patch['status'])
            except Exception as exc:
                self.store.merge(
                    job_id,
                    {
                        "last_checked_at": now_utc_iso(),
                        "last_error": str(exc),
                    },
                )
                logger.exception("POLL error torrent_id=%s", job_id)
