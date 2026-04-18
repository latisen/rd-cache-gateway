from __future__ import annotations

from typing import Any

import requests


class RealDebridClient:
    BASE_URL = "https://api.real-debrid.com/rest/1.0"

    def __init__(self, token: str | None, timeout: int = 60):
        self.token = token
        self.timeout = timeout

    def is_configured(self) -> bool:
        return bool(self.token)

    def _headers(self) -> dict[str, str]:
        if not self.token:
            raise RuntimeError("RD_TOKEN is not set")
        return {"Authorization": f"Bearer {self.token}"}

    def user(self) -> dict[str, Any]:
        response = requests.get(
            f"{self.BASE_URL}/user",
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def add_magnet(self, magnet_uri: str) -> str:
        response = requests.post(
            f"{self.BASE_URL}/torrents/addMagnet",
            headers=self._headers(),
            data={"magnet": magnet_uri},
            timeout=self.timeout,
        )
        if response.status_code not in {200, 201}:
            raise RuntimeError(f"RD addMagnet failed: {response.status_code} {response.text}")
        payload = response.json()
        torrent_id = payload.get("id")
        if not torrent_id:
            raise RuntimeError(f"RD addMagnet returned no id: {payload}")
        return str(torrent_id)

    def add_torrent_file(self, content: bytes, filename: str | None = None) -> str:
        response = requests.post(
            f"{self.BASE_URL}/torrents/addTorrent",
            headers=self._headers(),
            files={"file": (filename or "upload.torrent", content, "application/x-bittorrent")},
            timeout=self.timeout,
        )
        if response.status_code not in {200, 201}:
            raise RuntimeError(f"RD addTorrent failed: {response.status_code} {response.text}")
        payload = response.json()
        torrent_id = payload.get("id")
        if not torrent_id:
            raise RuntimeError(f"RD addTorrent returned no id: {payload}")
        return str(torrent_id)

    def select_all_files(self, torrent_id: str) -> None:
        response = requests.post(
            f"{self.BASE_URL}/torrents/selectFiles/{torrent_id}",
            headers=self._headers(),
            data={"files": "all"},
            timeout=self.timeout,
        )
        if response.status_code not in {200, 204}:
            raise RuntimeError(f"RD selectFiles failed: {response.status_code} {response.text}")

    def torrent_info(self, torrent_id: str) -> dict[str, Any]:
        response = requests.get(
            f"{self.BASE_URL}/torrents/info/{torrent_id}",
            headers=self._headers(),
            timeout=self.timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(f"RD info failed for {torrent_id}: {response.status_code} {response.text}")
        return response.json()

    def delete_torrent(self, torrent_id: str) -> None:
        response = requests.delete(
            f"{self.BASE_URL}/torrents/delete/{torrent_id}",
            headers=self._headers(),
            timeout=self.timeout,
        )
        if response.status_code not in {200, 204}:
            raise RuntimeError(f"RD delete failed for {torrent_id}: {response.status_code} {response.text}")
