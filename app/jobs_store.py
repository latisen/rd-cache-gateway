from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from threading import RLock
from typing import Callable

logger = logging.getLogger(__name__)


class JobStore:
    def __init__(self, jobs_file: Path):
        self.jobs_file = jobs_file
        self._lock = RLock()
        self.ensure_ready()

    def ensure_ready(self) -> None:
        with self._lock:
            try:
                self.jobs_file.parent.mkdir(parents=True, exist_ok=True)
                if not self.jobs_file.exists():
                    self._write_unlocked({})
            except PermissionError:
                fallback_dir = Path(tempfile.gettempdir()) / "rd-cache-gateway-data"
                fallback_dir.mkdir(parents=True, exist_ok=True)
                self.jobs_file = fallback_dir / self.jobs_file.name
                logger.warning("DATA_DIR not writable, falling back to %s", self.jobs_file)
                if not self.jobs_file.exists():
                    self._write_unlocked({})

    def _read_unlocked(self) -> dict[str, dict]:
        if not self.jobs_file.exists():
            return {}
        try:
            return json.loads(self.jobs_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _write_unlocked(self, jobs: dict[str, dict]) -> None:
        self.jobs_file.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix="jobs-",
            suffix=".json.tmp",
            dir=str(self.jobs_file.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(jobs, handle, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self.jobs_file)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def all(self) -> dict[str, dict]:
        with self._lock:
            return self._read_unlocked()

    def get(self, job_id: str) -> dict | None:
        with self._lock:
            return self._read_unlocked().get(job_id)

    def replace_all(self, jobs: dict[str, dict]) -> None:
        with self._lock:
            self._write_unlocked(jobs)

    def merge(self, job_id: str, patch: dict) -> dict:
        with self._lock:
            jobs = self._read_unlocked()
            job = jobs.get(job_id, {"torrent_id": job_id})
            job.update(patch)
            jobs[job_id] = job
            self._write_unlocked(jobs)
            return dict(job)

    def update(self, job_id: str, updater: Callable[[dict], None]) -> dict:
        with self._lock:
            jobs = self._read_unlocked()
            job = jobs.get(job_id, {"torrent_id": job_id})
            updater(job)
            jobs[job_id] = job
            self._write_unlocked(jobs)
            return dict(job)

    def replace_key(self, old_id: str, new_id: str) -> dict | None:
        with self._lock:
            jobs = self._read_unlocked()
            if old_id not in jobs:
                return None
            if old_id == new_id:
                return jobs[old_id]
            job = jobs.pop(old_id)
            job["torrent_id"] = new_id
            jobs[new_id] = job
            self._write_unlocked(jobs)
            return dict(job)

    def delete(self, job_id: str) -> bool:
        with self._lock:
            jobs = self._read_unlocked()
            if job_id not in jobs:
                return False
            del jobs[job_id]
            self._write_unlocked(jobs)
            return True
