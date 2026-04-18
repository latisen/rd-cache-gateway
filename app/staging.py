from __future__ import annotations

import os
import re
from difflib import SequenceMatcher
from pathlib import Path

VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".mov", ".m4v"}


def stage_folder_name(torrent_id: str, source_file: Path) -> str:
    base = re.sub(r"[^A-Za-z0-9._ -]+", ".", source_file.stem).strip(" .")
    base = re.sub(r"\s+", ".", base)
    base = re.sub(r"\.+", ".", base)
    if not base:
        base = "release"
    suffix = re.sub(r"[^A-Za-z0-9]+", "", torrent_id).lower()[:12] or "job"
    return f"{base}-{suffix}"


def normalize_name(value: str) -> str:
    value = Path(value).name.lower().strip()
    value = re.sub(r"\.[a-z0-9]{2,4}$", "", value)
    value = re.sub(r"[^a-z0-9]+", "", value)
    return value


def extract_episode_token(value: str) -> str | None:
    match = re.search(r"(s\d{2}e\d{2})", value, re.IGNORECASE)
    return match.group(1).lower() if match else None


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def find_matching_media_file(info: dict, root: Path) -> Path | None:
    if not root.exists():
        return None

    filename = info.get("filename") or info.get("original_filename")
    if not filename:
        return None

    wanted_name = Path(filename).name
    wanted_norm = normalize_name(wanted_name)
    wanted_ep = extract_episode_token(wanted_name)

    candidates: list[Path] = []
    try:
        for candidate in root.rglob("*"):
            if candidate.is_file() and candidate.suffix.lower() in VIDEO_EXTENSIONS:
                candidates.append(candidate)
    except Exception:
        return None

    exact = [path for path in candidates if normalize_name(path.name) == wanted_norm]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        return sorted(exact, key=lambda path: len(str(path)))[0]

    if wanted_ep:
        scored = [path for path in candidates if extract_episode_token(path.name) == wanted_ep]
        if scored:
            scored.sort(key=lambda path: similarity(normalize_name(path.name), wanted_norm), reverse=True)
            return scored[0]

    if candidates:
        candidates.sort(key=lambda path: similarity(normalize_name(path.name), wanted_norm), reverse=True)
        best = candidates[0]
        if similarity(normalize_name(best.name), wanted_norm) >= 0.9:
            return best

    return None


def extract_expected_media_size(info: dict, source_file: Path) -> int | None:
    files = info.get("files") or []
    wanted_name = source_file.name
    wanted_norm = normalize_name(wanted_name)

    for item in files:
        item_path = str(item.get("path") or item.get("name") or "")
        item_name = Path(item_path).name
        if not item_name:
            continue
        if item_name == wanted_name or normalize_name(item_name) == wanted_norm:
            try:
                value = int(item.get("bytes") or 0)
            except Exception:
                value = 0
            if value > 0:
                return value

    return None


def _refresh_symlink(link_path: Path, source_file: Path) -> Path:
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if link_path.exists() or link_path.is_symlink():
        link_path.unlink()
    link_target = os.path.relpath(str(source_file), start=str(link_path.parent))
    link_path.symlink_to(link_target, target_is_directory=False)
    return link_path



def create_staging_symlink(
    torrent_id: str,
    source_file: Path,
    staging_root: Path,
    visible_root: Path,
) -> tuple[Path, Path, Path]:
    folder_name = stage_folder_name(torrent_id, source_file)
    host_dir = staging_root / folder_name
    visible_dir = visible_root / folder_name

    link_path = _refresh_symlink(host_dir / source_file.name, source_file)
    visible_file = _refresh_symlink(visible_dir / source_file.name, source_file)

    return link_path, visible_dir, visible_file


def cleanup_staging_for_job(torrent_id: str, staging_root: Path, visible_root: Path | None = None) -> None:
    roots = [staging_root]
    if visible_root is not None:
        roots.append(visible_root)

    seen: set[Path] = set()
    for root in roots:
        candidates = [root / torrent_id, *root.glob(f"*-{torrent_id}")]
        for job_dir in candidates:
            if job_dir in seen or not job_dir.exists():
                continue
            seen.add(job_dir)
            for child in job_dir.iterdir():
                if child.is_symlink() or child.is_file():
                    child.unlink(missing_ok=True)
            try:
                job_dir.rmdir()
            except OSError:
                pass


def check_staging_ready(
    staging_path: Path,
    expected_size: int | None = None,
    min_bytes: int = 1,
) -> tuple[bool, str, dict]:
    try:
        if not staging_path.exists() and not staging_path.is_symlink():
            return False, "staging_missing", {}
        if not staging_path.is_symlink():
            return False, "staging_not_symlink", {}
        target = staging_path.resolve(strict=True)
        if not target.exists() or not target.is_file():
            return False, "target_missing", {}
        actual_size = target.stat().st_size
        if actual_size < min_bytes:
            return False, "target_too_small", {"actual_size": actual_size, "min_size": min_bytes}
        if expected_size and actual_size != expected_size:
            return False, "size_mismatch", {"actual_size": actual_size, "expected_size": expected_size}
        return True, "ready", {"target": str(target), "actual_size": actual_size}
    except Exception as exc:
        return False, "staging_error", {"error": str(exc)}
