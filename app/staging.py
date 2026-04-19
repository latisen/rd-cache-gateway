from __future__ import annotations

import logging
import os
import re
import threading
import time
from difflib import SequenceMatcher
from pathlib import Path

logger = logging.getLogger(__name__)

VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".mov", ".m4v"}
_MEDIA_INDEX_TTL = 15
_MEDIA_INDEX: dict[str, tuple[float, list[Path]]] = {}
_MEDIA_INDEX_LOCK = threading.Lock()


def stage_folder_name(torrent_id: str, source_file: Path) -> str:
    base = re.sub(r"[^A-Za-z0-9._ -]+", ".", source_file.stem).strip(" .")
    base = re.sub(r"\s+", ".", base)
    base = re.sub(r"\.+", ".", base)
    if not base:
        base = "release"
    suffix = re.sub(r"[^A-Za-z0-9]+", "", torrent_id).lower()[:12] or "job"
    return f"{base}-{suffix}"


def _canonicalize_episode_patterns(value: str) -> str:
    def repl(match: re.Match[str]) -> str:
        season = int(match.group(1))
        episode = int(match.group(2))
        return f"s{season:02d}e{episode:02d}"

    normalized = re.sub(r"(?i)\bseason\W*0*(\d{1,2})\W*episode\W*0*(\d{1,2})\b", repl, value)
    normalized = re.sub(r"(?i)\bs\W*0*(\d{1,2})\W*e\W*0*(\d{1,2})\b", repl, normalized)
    normalized = re.sub(r"(?i)\b0*(\d{1,2})x0*(\d{1,2})\b", repl, normalized)
    return normalized


def normalize_name(value: str) -> str:
    value = _canonicalize_episode_patterns(Path(value).name.lower().strip())
    value = re.sub(r"\.[a-z0-9]{2,4}$", "", value)
    value = re.sub(r"[^a-z0-9]+", "", value)
    return value


def extract_name_words(value: str) -> set[str]:
    stem = _canonicalize_episode_patterns(Path(value).stem.lower())
    stem = re.sub(r"s\d{2}e\d{2}", " ", stem, flags=re.IGNORECASE)
    words = re.split(r"[^a-z0-9]+", stem)
    ignored = {"1080p", "720p", "2160p", "web", "dl", "webrip", "amzn", "ddp2", "ddp5", "h", "264", "265", "x264", "x265", "ntb", "kitsune", "mkv", "season", "episode"}
    return {word for word in words if len(word) >= 3 and word not in ignored and not word.isdigit()}


def _word_overlap(a: set[str], b: set[str]) -> int:
    return len(a & b)


def _get_media_candidates(root: Path) -> list[Path]:
    cache_key = str(root)
    now = time.time()
    with _MEDIA_INDEX_LOCK:
        cached = _MEDIA_INDEX.get(cache_key)
        if cached and now - cached[0] < _MEDIA_INDEX_TTL:
            return cached[1]

    candidates: list[Path] = []
    try:
        for candidate in root.rglob("*"):
            if candidate.is_file() and candidate.suffix.lower() in VIDEO_EXTENSIONS:
                candidates.append(candidate)
    except Exception as exc:
        logger.warning("STAGE scan failed root=%s error=%s", root, exc)
        return []

    with _MEDIA_INDEX_LOCK:
        _MEDIA_INDEX[cache_key] = (now, candidates)
    return candidates


def extract_episode_token(value: str) -> str | None:
    normalized = _canonicalize_episode_patterns(str(value))
    match = re.search(r"(s\d{2}e\d{2})", normalized, re.IGNORECASE)
    return match.group(1).lower() if match else None


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def find_matching_media_file(info: dict, root: Path) -> Path | None:
    if not root.exists():
        logger.warning("STAGE source root missing root=%s", root)
        return None

    filename = info.get("filename") or info.get("original_filename")
    if not filename:
        return None

    wanted_name = Path(filename).name
    wanted_norm = normalize_name(wanted_name)
    wanted_ep = extract_episode_token(wanted_name)
    wanted_words = extract_name_words(wanted_name)

    direct = root / wanted_name
    if direct.is_file():
        return direct

    for item in info.get("files") or []:
        item_name = Path(str(item.get("path") or item.get("name") or "")).name
        if not item_name:
            continue
        direct_item = root / item_name
        if direct_item.is_file():
            return direct_item

    candidates = _get_media_candidates(root)
    if not candidates:
        return None

    exact = [path for path in candidates if normalize_name(path.name) == wanted_norm]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        return sorted(exact, key=lambda path: len(str(path)))[0]

    if wanted_ep:
        scored: list[tuple[int, float, Path]] = []
        for path in candidates:
            if extract_episode_token(path.name) != wanted_ep:
                continue
            overlap = _word_overlap(wanted_words, extract_name_words(path.name))
            sim = similarity(normalize_name(path.name), wanted_norm)
            if overlap >= 2 or sim >= 0.92:
                scored.append((overlap, sim, path))
        if scored:
            scored.sort(key=lambda item: (item[0], item[1], -len(str(item[2]))), reverse=True)
            return scored[0][2]

    ranked: list[tuple[int, float, Path]] = []
    for path in candidates:
        overlap = _word_overlap(wanted_words, extract_name_words(path.name))
        sim = similarity(normalize_name(path.name), wanted_norm)
        ranked.append((overlap, sim, path))

    ranked.sort(key=lambda item: (item[0], item[1], -len(str(item[2]))), reverse=True)
    best_overlap, best_sim, best_path = ranked[0]
    if best_overlap >= 2 and best_sim >= 0.75:
        return best_path
    if best_sim >= 0.95:
        return best_path

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
