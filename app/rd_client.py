from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)


class RealDebridClient:
    RD_BASE_URL = "https://api.real-debrid.com/rest/1.0"
    TORBOX_BASE_URL = "https://api.torbox.app/v1/api"

    def __init__(self, token: str | None, timeout: int = 60, provider: str = "realdebrid"):
        self.token = token
        self.timeout = timeout
        self.provider = (provider or "realdebrid").strip().lower()

    def is_configured(self) -> bool:
        return bool(self.token)

    def _label(self) -> str:
        return "TB" if self.provider == "torbox" else "RD"

    def _headers(self) -> dict[str, str]:
        if not self.token:
            raise RuntimeError("Debrid API token is not set")
        return {"Authorization": f"Bearer {self.token}"}

    def _torbox_payload(self, response: requests.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"TorBox API returned invalid JSON: {response.text[:300]}") from exc

        if response.status_code not in {200, 201} or payload.get("success") is False:
            detail = payload.get("detail") or payload.get("error") or response.text
            raise RuntimeError(f"TorBox API failed: {response.status_code} {detail}")
        return payload

    def _torbox_extract_id(self, payload: dict[str, Any]) -> str | None:
        candidates: list[Any] = []
        data = payload.get("data")
        if isinstance(data, dict):
            candidates.extend([data.get("torrent_id"), data.get("id"), data.get("hash")])
        elif isinstance(data, list) and data:
            item = data[0]
            if isinstance(item, dict):
                candidates.extend([item.get("torrent_id"), item.get("id"), item.get("hash")])
        candidates.extend([payload.get("torrent_id"), payload.get("id"), payload.get("hash")])
        for value in candidates:
            if value not in {None, ""}:
                return str(value)
        return None

    def _normalize_torbox_item(self, item: dict[str, Any]) -> dict[str, Any]:
        progress_raw = item.get("progress") or 0
        try:
            progress = float(progress_raw)
        except Exception:
            progress = 0.0
        if 0.0 <= progress <= 1.0:
            progress *= 100.0

        files = []
        for entry in item.get("files") or []:
            if not isinstance(entry, dict):
                continue
            files.append(
                {
                    "id": entry.get("id"),
                    "path": entry.get("short_name") or entry.get("name") or entry.get("absolute_path") or "",
                    "bytes": int(entry.get("size") or 0),
                    "selected": 1,
                }
            )

        if item.get("download_finished") or item.get("download_present"):
            provider_status = "completed"
        else:
            raw_state = str(item.get("download_state") or "queued").lower()
            # Normalise all TorBox stalled variants to a single canonical value
            # so the poller's stall-timeout logic only needs to check one string.
            if "stall" in raw_state or "no_seed" in raw_state or "no seed" in raw_state:
                provider_status = "stalled"
            else:
                provider_status = raw_state
        return {
            "id": str(item.get("id") or item.get("hash") or ""),
            "status": provider_status,
            "filename": item.get("name") or item.get("hash") or "torrent",
            "bytes": int(item.get("size") or 0),
            "progress": int(progress),
            "speed": int(item.get("download_speed") or 0),
            "seeders": int(item.get("seeds") or 0),
            "peers": int(item.get("peers") or 0),
            "files": files,
            "hash": item.get("hash"),
            "error": item.get("tracker_message") or item.get("error"),
            "download_finished": item.get("download_finished"),
            "download_present": item.get("download_present"),
            "raw": item,
        }

    def _torbox_list_items(self, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        query = params or {"bypass_cache": False, "limit": 1000}
        last_error: RuntimeError | None = None

        for attempt in range(1, 6):
            response = requests.get(
                f"{self.TORBOX_BASE_URL}/torrents/mylist",
                headers=self._headers(),
                params=query,
                timeout=self.timeout,
            )
            try:
                payload = self._torbox_payload(response)
                data = payload.get("data")
                if isinstance(data, list):
                    return [item for item in data if isinstance(item, dict)]
                if isinstance(data, dict):
                    return [data]
                return []
            except RuntimeError as exc:
                last_error = exc
                if response.status_code >= 500 and attempt < 5:
                    logger.warning(
                        "%s mylist transient error attempt=%s status=%s detail=%s",
                        self._label(),
                        attempt,
                        response.status_code,
                        response.text[:200],
                    )
                    time.sleep(0.25 * attempt)
                    continue
                raise

        raise last_error or RuntimeError("TorBox API failed: unknown error")

    def _torbox_find_item(self, torrent_id: str, bypass_cache: bool = False) -> dict[str, Any] | None:
        params: dict[str, Any] = {"bypass_cache": bypass_cache}
        if str(torrent_id).isdigit():
            params["id"] = int(torrent_id)

        data = self._torbox_list_items(params)
        wanted = str(torrent_id).lower()
        for item in data:
            if str(item.get("id") or "").lower() == wanted:
                return item
            if str(item.get("hash") or "").lower() == wanted:
                return item
        if len(data) == 1:
            return data[0]
        return None

    def list_webdav_entries(self) -> list[dict[str, Any]]:
        if self.provider != "torbox" or not self.is_configured():
            return []

        entries: list[dict[str, Any]] = []
        seen_names: set[str] = set()
        for item in self._torbox_list_items({"bypass_cache": True, "limit": 1000}):
            torrent_id = str(item.get("id") or item.get("hash") or "")
            modified = item.get("updated_at") or item.get("created_at")
            for file_item in item.get("files") or []:
                if not isinstance(file_item, dict):
                    continue
                name = Path(
                    str(file_item.get("short_name") or file_item.get("name") or file_item.get("absolute_path") or "")
                ).name
                if not name:
                    continue
                if name in seen_names:
                    stem = Path(name).stem
                    suffix = Path(name).suffix
                    name = f"{stem}-{torrent_id}{suffix}"
                seen_names.add(name)
                entries.append(
                    {
                        "href": f"/dav/__all__/{quote(name)}",
                        "name": name,
                        "is_dir": False,
                        "size": int(file_item.get("size") or 0),
                        "torrent_id": torrent_id,
                        "file_id": str(file_item.get("id") or 0),
                        "modified": modified,
                    }
                )
        return sorted(entries, key=lambda entry: str(entry.get("name") or "").lower())

    def get_download_url(self, torrent_id: str, file_id: str | int | None = None) -> str:
        if self.provider != "torbox":
            raise RuntimeError("WebDAV download URLs are only supported for TorBox")

        if str(torrent_id).isdigit():
            resolved_torrent_id = int(torrent_id)
        else:
            item = self._torbox_find_item(str(torrent_id))
            if item is None or item.get("id") in {None, ""}:
                raise RuntimeError(f"TorBox requestdl could not resolve torrent id for {torrent_id}")
            resolved_torrent_id = int(item.get("id"))

        params = {
            "token": str(self.token or ""),
            "torrent_id": resolved_torrent_id,
            "file_id": int(file_id or 0),
            "zip_link": "false",
            "redirect": "false",
        }
        response = requests.get(
            f"{self.TORBOX_BASE_URL}/torrents/requestdl",
            params=params,
            timeout=self.timeout,
        )
        payload = self._torbox_payload(response)
        data = payload.get("data")
        if isinstance(data, str) and data:
            return data
        raise RuntimeError(f"TorBox requestdl returned no download URL for torrent {torrent_id} file {file_id}")

    def download_file(self, download_url: str, destination: str | Path, expected_size: int | None = None) -> Path:
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists() and target.is_file():
            actual_size = target.stat().st_size
            if not expected_size or actual_size == expected_size:
                logger.info("%s reuse staged file path=%s size=%s", self._label(), target, actual_size)
                return target

        temp_target = target.with_suffix(target.suffix + ".part")
        logger.info("%s download file url=%s path=%s", self._label(), download_url, target)
        with requests.get(download_url, stream=True, timeout=self.timeout) as response:
            response.raise_for_status()
            with temp_target.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)

        actual_size = temp_target.stat().st_size if temp_target.exists() else 0
        if expected_size and actual_size != expected_size:
            temp_target.unlink(missing_ok=True)
            raise RuntimeError(
                f"Downloaded file size mismatch for {target.name}: expected {expected_size}, got {actual_size}"
            )

        temp_target.replace(target)
        logger.info("%s download complete path=%s size=%s", self._label(), target, actual_size)
        return target

    def user(self) -> dict[str, Any]:
        if self.provider == "torbox":
            response = requests.get(
                f"{self.TORBOX_BASE_URL}/user/me",
                headers=self._headers(),
                timeout=self.timeout,
            )
            payload = self._torbox_payload(response)
            return payload.get("data") or {}

        response = requests.get(
            f"{self.RD_BASE_URL}/user",
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def add_magnet(self, magnet_uri: str) -> str:
        logger.info("%s add magnet", self._label())

        if self.provider == "torbox":
            response = requests.post(
                f"{self.TORBOX_BASE_URL}/torrents/createtorrent",
                headers=self._headers(),
                data={"magnet": magnet_uri, "allow_zip": "false", "as_queued": "false"},
                timeout=self.timeout,
            )
            logger.info("%s add magnet -> %s", self._label(), response.status_code)
            payload = self._torbox_payload(response)
            torrent_id = self._torbox_extract_id(payload)
            if not torrent_id:
                raise RuntimeError(f"TorBox create torrent returned no id: {payload}")
            return str(torrent_id)

        response = requests.post(
            f"{self.RD_BASE_URL}/torrents/addMagnet",
            headers=self._headers(),
            data={"magnet": magnet_uri},
            timeout=self.timeout,
        )
        logger.info("%s add magnet -> %s", self._label(), response.status_code)
        if response.status_code not in {200, 201}:
            raise RuntimeError(f"RD addMagnet failed: {response.status_code} {response.text}")
        payload = response.json()
        torrent_id = payload.get("id")
        if not torrent_id:
            raise RuntimeError(f"RD addMagnet returned no id: {payload}")
        return str(torrent_id)

    def add_torrent_file(self, content: bytes, filename: str | None = None) -> str:
        logger.info("%s add torrent file filename=%s bytes=%s", self._label(), filename or "upload.torrent", len(content))

        if self.provider == "torbox":
            response = requests.post(
                f"{self.TORBOX_BASE_URL}/torrents/createtorrent",
                headers=self._headers(),
                data={"allow_zip": "false", "as_queued": "false", "name": filename or "upload.torrent"},
                files={"file": (filename or "upload.torrent", content, "application/x-bittorrent")},
                timeout=self.timeout,
            )
            logger.info("%s add torrent file -> %s", self._label(), response.status_code)
            payload = self._torbox_payload(response)
            torrent_id = self._torbox_extract_id(payload)
            if not torrent_id:
                raise RuntimeError(f"TorBox create torrent returned no id: {payload}")
            return str(torrent_id)

        response = requests.post(
            f"{self.RD_BASE_URL}/torrents/addTorrent",
            headers=self._headers(),
            files={"file": (filename or "upload.torrent", content, "application/x-bittorrent")},
            timeout=self.timeout,
        )
        logger.info("%s add torrent file -> %s", self._label(), response.status_code)
        if response.status_code not in {200, 201}:
            raise RuntimeError(f"RD addTorrent failed: {response.status_code} {response.text}")
        payload = response.json()
        torrent_id = payload.get("id")
        if not torrent_id:
            raise RuntimeError(f"RD addTorrent returned no id: {payload}")
        return str(torrent_id)

    def select_all_files(self, torrent_id: str) -> None:
        if self.provider == "torbox":
            logger.info("%s select all files skipped torrent_id=%s", self._label(), torrent_id)
            return

        logger.info("%s select all files torrent_id=%s", self._label(), torrent_id)
        response = requests.post(
            f"{self.RD_BASE_URL}/torrents/selectFiles/{torrent_id}",
            headers=self._headers(),
            data={"files": "all"},
            timeout=self.timeout,
        )
        logger.info("%s select all files torrent_id=%s -> %s", self._label(), torrent_id, response.status_code)
        if response.status_code not in {200, 204}:
            raise RuntimeError(f"RD selectFiles failed: {response.status_code} {response.text}")

    def torrent_info(self, torrent_id: str) -> dict[str, Any]:
        logger.info("%s poll torrent_id=%s", self._label(), torrent_id)

        if self.provider == "torbox":
            item = self._torbox_find_item(torrent_id, bypass_cache=True)
            if item is None:
                raise RuntimeError(f"TorBox info failed for {torrent_id}: torrent not found")
            payload = self._normalize_torbox_item(item)
            logger.info(
                "%s poll torrent_id=%s -> 200 provider_status=%s progress=%s seeders=%s speed=%s error=%s",
                self._label(),
                torrent_id,
                payload.get("status"),
                payload.get("progress"),
                payload.get("seeders"),
                payload.get("speed"),
                payload.get("error") or "",
            )
            return payload

        response = requests.get(
            f"{self.RD_BASE_URL}/torrents/info/{torrent_id}",
            headers=self._headers(),
            timeout=self.timeout,
        )
        if response.status_code != 200:
            logger.info("%s poll torrent_id=%s -> %s", self._label(), torrent_id, response.status_code)
            raise RuntimeError(f"RD info failed for {torrent_id}: {response.status_code} {response.text}")
        payload = response.json()
        logger.info(
            "%s poll torrent_id=%s -> %s rd_status=%s progress=%s seeders=%s speed=%s error=%s",
            self._label(),
            torrent_id,
            response.status_code,
            payload.get("status"),
            payload.get("progress"),
            payload.get("seeders"),
            payload.get("speed"),
            payload.get("error") or payload.get("message") or "",
        )
        return payload

    def delete_torrent(self, torrent_id: str) -> None:
        logger.info("%s delete torrent_id=%s", self._label(), torrent_id)

        if self.provider == "torbox":
            if str(torrent_id).isdigit():
                resolved_id = int(torrent_id)
            else:
                item = self._torbox_find_item(torrent_id)
                if item is None:
                    raise RuntimeError(f"TorBox delete failed for {torrent_id}: torrent not found")
                resolved_id = int(item.get("id"))
            response = requests.post(
                f"{self.TORBOX_BASE_URL}/torrents/controltorrent",
                headers={**self._headers(), "Content-Type": "application/json"},
                json={"torrent_id": resolved_id, "operation": "delete"},
                timeout=self.timeout,
            )
            logger.info("%s delete torrent_id=%s -> %s", self._label(), resolved_id, response.status_code)
            self._torbox_payload(response)
            return

        response = requests.delete(
            f"{self.RD_BASE_URL}/torrents/delete/{torrent_id}",
            headers=self._headers(),
            timeout=self.timeout,
        )
        logger.info("%s delete torrent_id=%s -> %s", self._label(), torrent_id, response.status_code)
        if response.status_code not in {200, 204}:
            raise RuntimeError(f"RD delete failed for {torrent_id}: {response.status_code} {response.text}")
