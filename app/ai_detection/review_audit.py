"""Database audit trail for image-detection review and model management."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from app.database import get_conn


def actor_fields(actor: Optional[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    data = actor or {}
    user_id = data.get("sub", data.get("uid"))
    return {
        "actor_user_id": str(user_id) if user_id is not None else None,
        "actor_username": str(data.get("username") or "") or None,
        "actor_role": str(data.get("role") or "") or None,
    }


def insert_review_audit(
    *,
    action: str,
    actor: Optional[Dict[str, Any]],
    feedback_folder: Optional[str] = None,
    sample_id: Optional[str] = None,
    old_label: Optional[int] = None,
    new_label: Optional[int] = None,
    note: str = "",
    details: Optional[Dict[str, Any]] = None,
) -> None:
    fields = actor_fields(actor)
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ai_detection_review_audit (
                    action, actor_user_id, actor_username, actor_role,
                    feedback_folder, sample_id, old_label, new_label, note, details_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(action),
                    fields["actor_user_id"],
                    fields["actor_username"],
                    fields["actor_role"],
                    feedback_folder,
                    sample_id,
                    old_label,
                    new_label,
                    str(note or ""),
                    json.dumps(details or {}, ensure_ascii=False),
                ),
            )
