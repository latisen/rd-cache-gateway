from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel


JobStatus = Literal[
    "queued",
    "downloading",
    "ready",
    "staged",
    "ready_for_arr",
    "scan_pending",
    "imported",
    "failed",
]

RD_TO_JOB_STATUS: dict[str, JobStatus] = {
    "queued": "queued",
    "magnet_conversion": "queued",
    "waiting_files_selection": "queued",
    "downloading": "downloading",
    "downloaded": "ready",
    "error": "failed",
    "dead": "failed",
    "virus": "failed",
    "magnet_error": "failed",
}

JOB_TO_QBIT_STATE: dict[str, str] = {
    "queued": "downloading",
    "downloading": "downloading",
    "ready": "downloading",
    "staged": "downloading",
    "ready_for_arr": "pausedUP",
    "scan_pending": "pausedUP",
    "imported": "pausedUP",
    "failed": "error",
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def map_rd_status(rd_status: str | None) -> JobStatus:
    if not rd_status:
        return "queued"
    return RD_TO_JOB_STATUS.get(str(rd_status).strip().lower(), "downloading")


def map_job_to_qbit_state(job_status: str | None) -> str:
    if not job_status:
        return "downloading"
    return JOB_TO_QBIT_STATE.get(job_status, "downloading")


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def safe_progress(value: Any, default: float = 0.0) -> float:
    try:
        numeric = float(value)
    except Exception:
        return default
    if numeric < 0:
        return 0.0
    if numeric > 1:
        return 1.0
    return numeric


class HealthResponse(BaseModel):
    status: str
    hostname: str
    version: str


class RDUserResponse(BaseModel):
    status: str
    username: str | None = None
    email: str | None = None
    points: int | None = None


class CreateJobRequest(BaseModel):
    magnet: str
    category: str | None = "sonarr"


class CreateJobResponse(BaseModel):
    status: str
    torrent_id: str
    rd_status: str | None = None
    filename: str | None = None


class JobStatusResponse(BaseModel):
    torrent_id: str
    status: str
    rd_status: str | None = None
    filename: str | None = None
    saved_at: str | None = None
    last_checked_at: str | None = None
    source_path: str | None = None
    staging_path: str | None = None
    arr_path: str | None = None
    raw: dict[str, Any] | None = None
    last_error: str | None = None
    imported_at: str | None = None
