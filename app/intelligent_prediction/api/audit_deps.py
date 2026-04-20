"""智能预测接口：审计主体（可选 Bearer，不强制登录）。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Optional

from fastapi import Header, Request
from jose import JWTError, jwt

from app import config as app_config


@dataclass
class AuditActor:
    user_id: Optional[int]
    user_label: str
    client_ip: Optional[str]


def _decode_payload(authorization: Optional[str]) -> Optional[dict]:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.split(" ", 1)[1].strip()
    try:
        return jwt.decode(token, app_config.JWT_SECRET_KEY, algorithms=[app_config.JWT_ALGORITHM])
    except JWTError:
        return None


def try_decode_uid(authorization: Optional[str]) -> Optional[int]:
    payload = _decode_payload(authorization)
    if not payload:
        return None
    uid = payload.get("uid") or payload.get("sub")
    if uid is None:
        return None
    try:
        return int(uid)
    except (TypeError, ValueError):
        return None


def get_user_identity_from_authorization(authorization: Optional[str]) -> str:
    payload = _decode_payload(authorization)
    if not payload:
        return "-"
    uid = payload.get("uid") or payload.get("sub")
    role = payload.get("role")
    username = payload.get("username")
    if uid is not None and role:
        return f"uid={uid} role={role}"
    if uid is not None:
        return f"uid={uid}"
    if username:
        return str(username)
    return "-"


def get_audit_actor(
    request: Request,
    authorization: Annotated[Optional[str], Header()] = None,
) -> AuditActor:
    return AuditActor(
        user_id=try_decode_uid(authorization),
        user_label=get_user_identity_from_authorization(authorization),
        client_ip=request.client.host if request.client else None,
    )
