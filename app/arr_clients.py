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


def get_arr_client(category: str | None, settings: Settings) -> ArrClient:
    normalized = (category or "sonarr").strip().lower()
    if normalized == "radarr":
        return ArrClient("radarr", settings.radarr_url, settings.radarr_api_key)
    return ArrClient("sonarr", settings.sonarr_url, settings.sonarr_api_key)
