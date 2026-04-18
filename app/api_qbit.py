from __future__ import annotations

import base64
import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import unquote

from app.models import map_job_to_qbit_state, safe_int, safe_progress


def extract_urls_from_add_request(urls: str | None, url: str | None) -> list[str]:
    values: list[str] = []
    for raw in (urls, url):
        if not raw:
            continue
        for part in raw.splitlines():
            part = part.strip()
            if part:
                values.append(part)
    return values


def is_magnet_link(value: str) -> bool:
    return value.strip().lower().startswith("magnet:?")


def magnet_display_name(magnet_uri: str) -> str:
    match = re.search(r"[?&]dn=([^&]+)", magnet_uri, re.IGNORECASE)
    if not match:
        return "magnet"
    try:
        return unquote(match.group(1))
    except Exception:
        return match.group(1)


def magnet_info_hash(magnet_uri: str) -> str | None:
    match = re.search(r"[?&]xt=urn:btih:([A-Za-z0-9]+)", magnet_uri, re.IGNORECASE)
    if not match:
        return None

    raw = match.group(1).strip()
    if re.fullmatch(r"[0-9A-Fa-f]{40}|[0-9A-Fa-f]{64}", raw):
        return raw.lower()

    if re.fullmatch(r"[A-Z2-7]{32}", raw.upper()):
        try:
            return base64.b32decode(raw.upper()).hex()
        except Exception:
            return None

    return raw.lower() if raw else None


def _consume_bencode_value(data: bytes, index: int) -> int:
    token = data[index:index + 1]
    if not token:
        raise ValueError("Unexpected end of torrent data")

    if token == b"i":
        return data.index(b"e", index + 1) + 1

    if token == b"l":
        index += 1
        while data[index:index + 1] != b"e":
            index = _consume_bencode_value(data, index)
        return index + 1

    if token == b"d":
        index += 1
        while data[index:index + 1] != b"e":
            colon = data.index(b":", index)
            key_length = int(data[index:colon])
            key_end = colon + 1 + key_length
            index = _consume_bencode_value(data, key_end)
        return index + 1

    if token.isdigit():
        colon = data.index(b":", index)
        length = int(data[index:colon])
        return colon + 1 + length

    raise ValueError("Invalid bencode token")


def torrent_file_info_hash(content: bytes) -> str | None:
    try:
        if not content.startswith(b"d"):
            return None

        index = 1
        while content[index:index + 1] != b"e":
            colon = content.index(b":", index)
            key_length = int(content[index:colon])
            key_start = colon + 1
            key_end = key_start + key_length
            key = content[key_start:key_end]
            value_start = key_end
            value_end = _consume_bencode_value(content, value_start)
            if key == b"info":
                return hashlib.sha1(content[value_start:value_end]).hexdigest()
            index = value_end
    except Exception:
        return None

    return None


def temporary_job_id_from_text(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]


def build_qbit_torrent_list(
    jobs: dict[str, dict],
    save_path: str,
    category_filter: str | None = None,
) -> list[dict]:
    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    items: list[dict] = []

    for torrent_id, job in jobs.items():
        if not isinstance(job, dict):
            continue
        if job.get("deleted_by_client"):
            continue

        category = job.get("category") or "sonarr"
        if category_filter and category != category_filter:
            continue

        raw = job.get("raw") or {}
        job_status = str(job.get("status") or "queued")
        state = map_job_to_qbit_state(job_status)
        display_hash = str(job.get("client_hash") or torrent_id).lower()
        save_dir = str(job.get("arr_path") or f"{save_path}/{display_hash}")
        content_path = str(job.get("arr_file_path") or (f"{save_dir}/{job.get('filename')}" if job.get("filename") else save_dir))
        progress = 0.0
        eta = 8640000
        completion_on = 0
        dlspeed = safe_int(raw.get("speed"), 0)

        if state == "pausedUP":
            progress = 1.0
            eta = 0
            completion_on = now_ts
        elif job_status == "staged":
            progress = 0.95
            eta = 60
        elif job_status == "ready":
            progress = 0.99
            eta = 30
        elif job_status == "downloading":
            progress = safe_progress(safe_int(raw.get("progress"), 0) / 100.0, 0.0)
        elif job_status == "queued":
            progress = 0.0
        elif job_status == "failed":
            progress = 0.0

        total_size = safe_int(raw.get("bytes"), 0)
        items.append(
            {
                "hash": display_hash,
                "name": job.get("filename") or torrent_id,
                "state": state,
                "progress": progress,
                "size": total_size,
                "total_size": total_size,
                "dlspeed": dlspeed,
                "upspeed": 0,
                "priority": 1,
                "num_seeds": 0,
                "num_leechs": 0,
                "ratio": 0,
                "eta": eta,
                "category": category,
                "label": category,
                "save_path": save_dir,
                "content_path": content_path,
                "completion_on": completion_on,
            }
        )

    return items


def build_preferences(save_path: str) -> dict:
    return {
        "locale": "en",
        "create_subfolder_enabled": True,
        "start_paused_enabled": False,
        "auto_delete_mode": 0,
        "preallocate_all": False,
        "incomplete_files_ext": False,
        "auto_tmm_enabled": False,
        "torrent_changed_tmm_enabled": False,
        "save_path_changed_tmm_enabled": False,
        "category_changed_tmm_enabled": False,
        "save_path": save_path,
        "temp_path_enabled": False,
        "temp_path": "",
        "scan_dirs": {},
        "web_ui_address": "*",
        "web_ui_port": 8000,
        "use_https": False,
        "upnp": False,
        "random_port": False,
        "dl_limit": 0,
        "up_limit": 0,
        "max_connec": -1,
        "max_connec_per_torrent": -1,
        "max_uploads": -1,
        "max_uploads_per_torrent": -1,
    }


def build_categories(save_path: str) -> dict:
    return {
        "sonarr": {"name": "sonarr", "savePath": save_path},
        "radarr": {"name": "radarr", "savePath": save_path},
    }
