"""Persistent candidate and active-model registry."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import joblib


class ModelRegistryError(RuntimeError):
    code = "MODEL_REGISTRY_ERROR"


class ModelActivationError(ModelRegistryError):
    code = "MODEL_ACTIVATION_BLOCKED"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class ModelRegistry:
    def __init__(self, path: str | Path, *, fallback_model_path: str | Path):
        self.path = Path(path).resolve()
        self.fallback_model_path = Path(fallback_model_path).resolve()
        self._lock = threading.RLock()

    def _read(self) -> Dict[str, Any]:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"schema_version": 2, "active_version": None, "versions": []}
        if not isinstance(data, dict):
            return {"schema_version": 2, "active_version": None, "versions": []}
        versions = data.get("versions") if isinstance(data.get("versions"), list) else []
        normalized = []
        for item in versions:
            if not isinstance(item, dict):
                continue
            entry = dict(item)
            if "version" not in entry and entry.get("timestamp"):
                entry["version"] = entry["timestamp"]
            entry.setdefault("status", "ACTIVE" if data.get("active_version") == entry.get("version") else "CANDIDATE")
            normalized.append(entry)
        data["schema_version"] = 2
        data["versions"] = normalized
        data.setdefault("active_version", None)
        return data

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

    @staticmethod
    def _entry_version(entry: Dict[str, Any]) -> str:
        value = str(entry.get("version") or entry.get("timestamp") or "").strip()
        if not value:
            raise ModelRegistryError("候选模型缺少版本号")
        return value

    @staticmethod
    def _resolve_artifact(path: Any, registry_dir: Path) -> Path:
        artifact = Path(str(path or ""))
        if not artifact.is_absolute():
            artifact = registry_dir / artifact
        return artifact.resolve()

    def register_candidate(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            data = self._read()
            version = self._entry_version(entry)
            if any(self._entry_version(item) == version for item in data["versions"]):
                raise ModelRegistryError(f"模型版本已存在: {version}")
            candidate = dict(entry)
            candidate.update(
                {
                    "version": version,
                    "timestamp": candidate.get("timestamp") or version,
                    "status": "CANDIDATE",
                    "created_at": candidate.get("created_at") or _now_iso(),
                }
            )
            model_path = self._resolve_artifact(candidate.get("model_path"), self.path.parent)
            if not model_path.is_file():
                raise ModelRegistryError("候选模型文件不存在")
            candidate["model_path"] = str(model_path)
            candidate["model_sha256"] = candidate.get("model_sha256") or file_sha256(model_path)
            data["versions"].append(candidate)
            self._write(data)
            return candidate

    def bootstrap_fallback(self) -> Dict[str, Any]:
        """Register the legacy configured model as the first rollback baseline."""
        if not self.fallback_model_path.is_file():
            return self.resolve_active()
        with self._lock:
            data = self._read()
            if data.get("active_version"):
                return self.resolve_active()
            digest = file_sha256(self.fallback_model_path)
            version = f"legacy-{digest[:12]}"
            entry = next(
                (item for item in data["versions"] if self._entry_version(item) == version),
                None,
            )
            if entry is None:
                entry = {
                    "version": version,
                    "timestamp": version,
                    "status": "ACTIVE",
                    "model_path": str(self.fallback_model_path),
                    "model_sha256": digest,
                    "gates": {"passed": True, "legacy_baseline": True},
                    "created_at": _now_iso(),
                    "activated_at": _now_iso(),
                    "activated_by": "system-bootstrap",
                }
                data["versions"].append(entry)
            else:
                entry["status"] = "ACTIVE"
            data["active_version"] = version
            data["updated_at"] = _now_iso()
            self._write(data)
            return dict(entry)

    def update_candidate(self, version: str, **changes: Any) -> Dict[str, Any]:
        with self._lock:
            data = self._read()
            for entry in data["versions"]:
                if self._entry_version(entry) != str(version):
                    continue
                entry.update(changes)
                entry["updated_at"] = _now_iso()
                self._write(data)
                return dict(entry)
        raise ModelRegistryError(f"模型版本不存在: {version}")

    def list_models(self) -> Dict[str, Any]:
        with self._lock:
            data = self._read()
        active = self.resolve_active()
        data["current_model"] = active["model_path"]
        data["active_model"] = active
        return data

    def get(self, version: str) -> Optional[Dict[str, Any]]:
        wanted = str(version or "").strip()
        with self._lock:
            for entry in self._read()["versions"]:
                if self._entry_version(entry) == wanted:
                    return dict(entry)
        return None

    def resolve_active(self) -> Dict[str, Any]:
        with self._lock:
            data = self._read()
            active_version = data.get("active_version")
            for entry in data["versions"]:
                if self._entry_version(entry) == active_version:
                    return dict(entry)
        return {
            "version": None,
            "status": "LEGACY",
            "model_path": str(self.fallback_model_path),
        }

    def validate_loadable(self, version: str) -> tuple[Any, Dict[str, Any]]:
        entry = self.get(version)
        if entry is None:
            raise ModelActivationError("模型版本不存在")
        path = self._resolve_artifact(entry.get("model_path"), self.path.parent)
        if not path.is_file():
            raise ModelActivationError("模型文件不存在")
        expected = str(entry.get("model_sha256") or "")
        if expected and file_sha256(path) != expected:
            raise ModelActivationError("模型文件校验和不一致")
        try:
            model = joblib.load(path)
        except Exception as exc:
            raise ModelActivationError("候选模型无法加载") from exc
        if not callable(getattr(model, "predict_proba", None)):
            raise ModelActivationError("候选模型缺少 predict_proba")
        entry["model_path"] = str(path)
        return model, entry

    def activate(
        self,
        version: str,
        *,
        actor: str,
        force: bool = False,
        reason: str = "",
    ) -> Dict[str, Any]:
        _model, validated = self.validate_loadable(version)
        gates = validated.get("gates") if isinstance(validated.get("gates"), dict) else {}
        if not gates.get("passed", False) and not force:
            raise ModelActivationError("候选模型未通过评估门槛")
        if force and not str(reason or "").strip():
            raise ModelActivationError("强制启用必须填写原因")

        with self._lock:
            data = self._read()
            previous = data.get("active_version")
            now = _now_iso()
            selected = None
            for entry in data["versions"]:
                current_version = self._entry_version(entry)
                if current_version == version:
                    entry.update(
                        {
                            "status": "ACTIVE",
                            "activated_at": now,
                            "activated_by": str(actor or "unknown"),
                            "force_activated": bool(force),
                            "force_reason": str(reason or ""),
                        }
                    )
                    selected = entry
                elif current_version == previous and entry.get("status") == "ACTIVE":
                    entry["status"] = "INACTIVE"
            if selected is None:
                raise ModelActivationError("模型版本不存在")
            data["active_version"] = version
            data["updated_at"] = now
            self._write(data)
            return dict(selected)
