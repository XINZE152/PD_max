"""Durable state for asynchronous model training jobs."""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


_STATUSES = {"QUEUED", "RUNNING", "COMPLETED", "FAILED", "INTERRUPTED"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class TrainingJobStore:
    def __init__(self, path: str | Path):
        self.path = Path(path).resolve()
        self._lock = threading.RLock()

    def _read(self) -> Dict[str, Any]:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"jobs": []}
        return data if isinstance(data, dict) and isinstance(data.get("jobs"), list) else {"jobs": []}

    def _write(self, data: Dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_name(f".{self.path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temp.open("w", encoding="utf-8") as stream:
                json.dump(data, stream, ensure_ascii=False, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, self.path)
        finally:
            temp.unlink(missing_ok=True)

    def create(self, *, actor: str) -> Dict[str, Any]:
        with self._lock:
            data = self._read()
            job = {
                "job_id": str(uuid.uuid4()),
                "status": "QUEUED",
                "progress": 0.0,
                "queue_reason": "等待当前图片检测结束",
                "created_at": _now_iso(),
                "created_by": str(actor or "unknown"),
            }
            data["jobs"].append(job)
            self._write(data)
            return dict(job)

    def update(self, job_id: str, **changes: Any) -> Dict[str, Any]:
        with self._lock:
            data = self._read()
            for job in data["jobs"]:
                if job.get("job_id") != job_id:
                    continue
                if "status" in changes and changes["status"] not in _STATUSES:
                    raise ValueError("无效训练任务状态")
                job.update(changes)
                job["updated_at"] = _now_iso()
                self._write(data)
                return dict(job)
        raise KeyError(job_id)

    def get(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            for job in self._read()["jobs"]:
                if job.get("job_id") == job_id:
                    return dict(job)
        return None

    def list(self, *, limit: int = 100) -> list[Dict[str, Any]]:
        with self._lock:
            rows = [dict(job) for job in self._read()["jobs"]]
        rows.sort(key=lambda item: item.get("created_at", ""), reverse=True)
        return rows[: max(1, min(500, int(limit)))]

    def interrupt_stale(self) -> int:
        with self._lock:
            data = self._read()
            count = 0
            for job in data["jobs"]:
                if job.get("status") in {"QUEUED", "RUNNING"}:
                    job.update(
                        {
                            "status": "INTERRUPTED",
                            "error": "服务进程重启，训练任务已中断",
                            "updated_at": _now_iso(),
                        }
                    )
                    count += 1
            if count:
                self._write(data)
            return count
