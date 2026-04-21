from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import requests

from app.config import Settings

logger = logging.getLogger(__name__)


class ArrClient:
    def __init__(self, name: str, base_url: str | None, api_key: str | None):
        self.name = name
        self.base_url = base_url.rstrip("/") if base_url else None
        self.api_key = api_key

    def is_configured(self) -> bool:
        return bool(self.base_url and self.api_key)

    def _headers(self) -> dict[str, str]:
        if not self.is_configured():
            raise RuntimeError(f"{self.name} is not configured")
        return {"X-Api-Key": str(self.api_key), "Content-Type": "application/json"}

    def refresh_monitored_downloads(self) -> dict[str, Any]:
        if not self.is_configured():
            return {"skipped": True, "reason": "not_configured"}
        logger.info("ARR refresh monitored downloads client=%s", self.name)
        if self.name != "sonarr":
            return {"skipped": True, "reason": "not_supported_for_category"}
        response = requests.post(
            f"{self.base_url}/api/v3/command",
            headers=self._headers(),
            json={"name": "RefreshMonitoredDownloads"},
            timeout=30,
        )
        logger.info("ARR refresh monitored downloads client=%s -> %s", self.name, response.status_code)
        response.raise_for_status()
        return response.json()

    def trigger_scan(self, folder: Path, download_id: str) -> dict[str, Any]:
        if not self.is_configured():
            return {"skipped": True, "reason": "not_configured"}
        logger.info("ARR trigger scan client=%s path=%s download_id=%s", self.name, folder, download_id)

        name = "DownloadedEpisodesScan" if self.name == "sonarr" else "DownloadedMoviesScan"
        payload = {
            "name": name,
            "path": str(folder),
            "downloadClientId": download_id,
            "importMode": "auto",
        }
        response = requests.post(
            f"{self.base_url}/api/v3/command",
            headers=self._headers(),
            json=payload,
            timeout=30,
        )
        logger.info("ARR trigger scan client=%s path=%s download_id=%s -> %s", self.name, folder, download_id, response.status_code)
        response.raise_for_status()
        return response.json()

    def get_command(self, command_id: int) -> dict[str, Any]:
        if not self.is_configured():
            return {"skipped": True, "reason": "not_configured"}
        response = requests.get(
            f"{self.base_url}/api/v3/command/{command_id}",
            headers={"X-Api-Key": str(self.api_key)},
            timeout=30,
        )
        logger.info("ARR get command client=%s command_id=%s -> %s", self.name, command_id, response.status_code)
        response.raise_for_status()
        return response.json()

    def check_history_for_import(self, download_id: str, since_seconds: int = 86400) -> bool:
        """Return True if *download_id* appears in the ARR history as a grabbed or imported item.

        Sonarr/Radarr mark the DownloadedEpisodesScan command as completed/successful even when no
        files were actually imported (e.g. FileNotFoundException, quality rejection).  Checking
        history gives us certainty that an import record exists before we mark the job as imported.

        The default window is 24 h because the 'grabbed' event is recorded when Sonarr first grabbed
        the release — which may have been hours before the scan command completed — so a short window
        like 5 minutes would miss it every time.
        """
        if not self.is_configured():
            return False
        try:
            import time as _time
            cutoff = _time.time() - since_seconds
            params = {"pageSize": 200, "sortKey": "date", "sortDir": "desc"}
            response = requests.get(
                f"{self.base_url}/api/v3/history",
                headers={"X-Api-Key": str(self.api_key)},
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            records = data.get("records") or data if isinstance(data, list) else []
            target = download_id.upper()
            for record in records:
                if str(record.get("downloadId") or "").upper() != target:
                    continue
                event_type = record.get("eventType") or ""
                if event_type not in {
                    "grabbed",
                    "downloadFolderImported",
                    "downloadImported",
                    "episodeFileImported",
                    "seriesFolderImported",
                }:
                    continue
                # Apply the time cutoff only to import events; 'grabbed' is always
                # recorded at grab-time (hours before the scan), so skip the cutoff for it.
                if event_type != "grabbed":
                    date_str = record.get("date") or ""
                    if date_str:
                        try:
                            from datetime import datetime, timezone
                            dt = datetime.fromisoformat(date_str.rstrip("Z")).replace(tzinfo=timezone.utc)
                            if dt.timestamp() < cutoff:
                                continue
                        except Exception:
                            pass
                logger.info(
                    "ARR history confirmed client=%s download_id=%s event=%s",
                    self.name, download_id, event_type,
                )
                return True
        except Exception as exc:
            logger.warning("ARR history check failed client=%s error=%s", self.name, exc)
        return False


    def attempt_manual_import(self, folder: Path, download_id: str) -> int:
        """Fallback: use Sonarr's /api/v3/manualimport to directly import files.

        Used when DownloadedEpisodesScan repeatedly completes without any import
        history (e.g. Sonarr rejects the file for an internal reason but the scan
        command still says 'successful').

        Returns the number of files queued for import (0 = nothing importable found).
        """
        if not self.is_configured():
            return 0
        try:
            # Step 1: ask Sonarr to parse the folder and return import candidates.
            resp = requests.get(
                f"{self.base_url}/api/v3/manualimport",
                headers={"X-Api-Key": str(self.api_key)},
                params={
                    "folder": str(folder),
                    "downloadId": download_id,
                    "filterExistingFiles": "false",
                },
                timeout=30,
            )
            resp.raise_for_status()
            candidates: list[dict] = resp.json() if isinstance(resp.json(), list) else []
            if not candidates:
                logger.info(
                    "ARR manual_import no_candidates client=%s folder=%s download_id=%s",
                    self.name, folder, download_id,
                )
                return 0

            importable: list[dict] = []
            for item in candidates:
                series = item.get("series") or {}
                episodes = item.get("episodes") or []
                if not series.get("id") or not episodes:
                    logger.warning(
                        "ARR manual_import skip_unresolved client=%s path=%s rejections=%s",
                        self.name, item.get("path"), item.get("rejections"),
                    )
                    continue
                importable.append({
                    "path": item["path"],
                    "seriesId": series["id"],
                    "episodeIds": [ep["id"] for ep in episodes],
                    "quality": item.get("quality"),
                    "languages": item.get("languages") or [],
                    "releaseGroup": item.get("releaseGroup") or "",
                    "downloadId": download_id,
                    "shouldReplace": False,
                })

            if not importable:
                logger.warning(
                    "ARR manual_import no_importable client=%s folder=%s candidates=%d",
                    self.name, folder, len(candidates),
                )
                return 0

            # Step 2: POST the import list to Sonarr.
            resp2 = requests.post(
                f"{self.base_url}/api/v3/manualimport",
                headers=self._headers(),
                json=importable,
                timeout=30,
            )
            resp2.raise_for_status()
            logger.info(
                "ARR manual_import submitted client=%s folder=%s count=%d",
                self.name, folder, len(importable),
            )
            return len(importable)
        except Exception as exc:
            logger.warning(
                "ARR manual_import failed client=%s folder=%s error=%s",
                self.name, folder, exc,
            )
            return 0


def get_arr_client(category: str | None, settings: Settings) -> ArrClient:
    normalized = (category or "sonarr").strip().lower()
    if normalized == "radarr":
        return ArrClient("radarr", settings.radarr_url, settings.radarr_api_key)
    return ArrClient("sonarr", settings.sonarr_url, settings.sonarr_api_key)
