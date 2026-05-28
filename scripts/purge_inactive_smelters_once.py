"""一次性硬删除 dict_factories 中 is_active=0 的冶炼厂（级联清理关联子表）。"""
from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.database import get_conn
from app.services.tl_service import TLService


def main() -> None:
    service = TLService()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name FROM dict_factories WHERE is_active = 0 ORDER BY id"
            )
            rows = cur.fetchall()

    if not rows:
        print("未发现 is_active=0 的冶炼厂，无需清理。")
        return

    print(f"待清理停用冶炼厂 {len(rows)} 条：")
    for sid, name in rows:
        print(f"  - id={sid} name={name}")

    ok: list[dict] = []
    failed: list[dict] = []

    for sid, name in rows:
        smelter_id = int(sid)
        try:
            res = service.purge_smelter(smelter_id, cascade=True)
            ok.append(
                {
                    "id": smelter_id,
                    "name": str(name),
                    "deleted_counts": res.get("deleted_counts") or {},
                }
            )
            print(f"已删除 id={smelter_id} ({name})")
        except Exception as e:
            failed.append({"id": smelter_id, "name": str(name), "error": str(e)})
            print(f"删除失败 id={smelter_id} ({name}): {e}", file=sys.stderr)

    summary = {
        "success_count": len(ok),
        "failed_count": len(failed),
        "success": ok,
        "failed": failed,
    }
    print("\n清理汇总：")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
