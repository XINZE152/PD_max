"""
从「合作库房清单」类 Excel 将库房名称与地址直接写入 dict_warehouses（不经由 TL add_warehouse）。

若需与线上一致的库房类型校验、天地图完整落库，请使用 HTTP：
``POST /tl/import_partner_warehouses_excel``（multipart 上传同一 xlsx）。

用法：

  uv run python scripts/import_partner_warehouses_excel.py ^
    --file "C:\\Users\\zhang carry\\Desktop\\3.合作库房清单.xlsx"

  uv run python scripts/import_partner_warehouses_excel.py --file ... --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.database import get_conn  # noqa: E402
from app.services.partner_warehouse_excel import (  # noqa: E402
    PartnerWarehouseExcelError,
    parse_partner_warehouse_rows,
)


def main() -> None:
    p = argparse.ArgumentParser(description="从 Excel 导入合作库房名称与地址到 dict_warehouses（直连 SQL）")
    p.add_argument("--file", "-f", required=True, type=Path, help="Excel 文件路径")
    p.add_argument("--sheet", type=str, default=None, help="工作表名称")
    p.add_argument("--sheet-index", type=int, default=None, help="工作表索引（0 起）")
    p.add_argument("--name-col", type=str, default=None, help="库房名称列名")
    p.add_argument("--address-col", type=str, default=None, help="库房地址列名")
    p.add_argument("--dry-run", action="store_true", help="只预览不写库")
    p.add_argument(
        "--no-upsert",
        action="store_true",
        help="若名称已存在则跳过（默认：存在则更新 address）",
    )
    args = p.parse_args()

    path = args.file.expanduser().resolve()
    if not path.is_file():
        raise SystemExit(f"文件不存在: {path}")

    try:
        sheet, name_col, addr_col, rows = parse_partner_warehouse_rows(
            path,
            sheet=args.sheet,
            sheet_index=args.sheet_index,
            name_col=args.name_col,
            address_col=args.address_col,
        )
    except PartnerWarehouseExcelError as e:
        raise SystemExit(str(e)) from e

    print(f"工作表: {sheet!r}，列: 名称={name_col!r}，地址={addr_col!r}，有效行数: {len(rows)}")
    if args.dry_run:
        for name, addr in rows[:20]:
            print(f"  - {name!r} -> {addr!r}")
        if len(rows) > 20:
            print(f"  ... 另有 {len(rows) - 20} 行")
        print("--dry-run，未写数据库")
        return

    inserted = updated = unchanged = 0
    sql_insert = (
        "INSERT INTO dict_warehouses (name, address, is_active) VALUES (%s, %s, 1)"
    )
    sql_upsert = sql_insert + " ON DUPLICATE KEY UPDATE address = VALUES(address)"

    with get_conn() as conn:
        conn.autocommit(False)
        try:
            with conn.cursor() as cur:
                for name, addr in rows:
                    if args.no_upsert:
                        cur.execute(
                            "SELECT id FROM dict_warehouses WHERE name = %s",
                            (name,),
                        )
                        if cur.fetchone():
                            unchanged += 1
                            continue
                        cur.execute(sql_insert, (name, addr))
                        inserted += 1
                    else:
                        cur.execute(sql_upsert, (name, addr))
                        rc = cur.rowcount
                        if rc == 2:
                            updated += 1
                        elif rc == 1:
                            inserted += 1
                        else:
                            unchanged += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    mode = "仅插入已存在则跳过" if args.no_upsert else "插入或按名称更新地址"
    print(
        f"完成（{mode}）：插入 {inserted} 行，更新 {updated} 行，"
        f"未变更/跳过 {unchanged} 行"
    )


if __name__ == "__main__":
    main()
