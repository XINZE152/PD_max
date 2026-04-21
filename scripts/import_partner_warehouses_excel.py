"""
从「合作库房清单」类 Excel 将库房名称与库房地址写入主库 dict_warehouses。

用法（在项目根目录、已配置 .env 的 MySQL 环境下）：

  uv run python scripts/import_partner_warehouses_excel.py ^
    --file "C:\\Users\\zhang carry\\Desktop\\3.合作库房清单.xlsx"

  # 仅预览不写库
  uv run python scripts/import_partner_warehouses_excel.py --file ... --dry-run

  # 指定工作表名或索引（索引从 0 开始）
  uv run python scripts/import_partner_warehouses_excel.py --file ... --sheet "合作库房清单"
  uv run python scripts/import_partner_warehouses_excel.py --file ... --sheet-index 1

默认列名：库房名称 -> name，库房地址 -> address；可用 --name-col / --address-col 覆盖。
已存在同名仓库时默认更新 address（可用 --no-upsert 改为跳过已存在行）。
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.database import get_conn  # noqa: E402 — 需在 sys.path 注入之后


DEFAULT_NAME_CANDIDATES = ("库房名称", "仓库名称", "名称", "name", "Name", "库房名")
DEFAULT_ADDRESS_CANDIDATES = ("库房地址", "仓库地址", "地址", "address", "Address", "详细地址")


def _norm_col(s: object) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    t = str(s).strip()
    t = re.sub(r"\s+", "", t)
    return t


def _resolve_column(df: pd.DataFrame, explicit: str | None, candidates: tuple[str, ...]) -> str:
    if explicit and explicit.strip():
        c = explicit.strip()
        if c not in df.columns:
            raise SystemExit(f"列不存在: {c!r}，当前列: {list(df.columns)}")
        return c
    norm_map = {_norm_col(c): c for c in df.columns}
    for cand in candidates:
        key = _norm_col(cand)
        if key in norm_map:
            return norm_map[key]
    raise SystemExit(
        "无法自动识别列，请用 --name-col / --address-col 指定。"
        f" 当前列: {list(df.columns)}"
    )


def _cell_str(v: object) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    return s


def main() -> None:
    p = argparse.ArgumentParser(description="从 Excel 导入合作库房名称与地址到 dict_warehouses")
    p.add_argument("--file", "-f", required=True, type=Path, help="Excel 文件路径")
    p.add_argument("--sheet", type=str, default=None, help="工作表名称（默认取第一个表或常见「合作库房清单」）")
    p.add_argument("--sheet-index", type=int, default=None, help="工作表索引（与 --sheet 二选一，0 起）")
    p.add_argument("--name-col", type=str, default=None, help="库房名称列名")
    p.add_argument("--address-col", type=str, default=None, help="库房地址列名")
    p.add_argument("--dry-run", action="store_true", help="只打印将写入的行，不写数据库")
    p.add_argument(
        "--no-upsert",
        action="store_true",
        help="若名称已存在则跳过（默认：存在则更新 address）",
    )
    args = p.parse_args()

    path = args.file.expanduser().resolve()
    if not path.is_file():
        raise SystemExit(f"文件不存在: {path}")

    xl = pd.ExcelFile(path)
    if args.sheet_index is not None:
        sheet = xl.sheet_names[args.sheet_index]
    elif args.sheet:
        if args.sheet not in xl.sheet_names:
            raise SystemExit(f"工作表不存在: {args.sheet!r}，可选: {xl.sheet_names}")
        sheet = args.sheet
    else:
        preferred = "合作库房清单"
        sheet = preferred if preferred in xl.sheet_names else xl.sheet_names[0]

    df = pd.read_excel(path, sheet_name=sheet, dtype=object)
    name_col = _resolve_column(df, args.name_col, DEFAULT_NAME_CANDIDATES)
    addr_col = _resolve_column(df, args.address_col, DEFAULT_ADDRESS_CANDIDATES)

    rows: list[tuple[str, str]] = []
    for _, r in df.iterrows():
        name = _cell_str(r.get(name_col))
        addr = _cell_str(r.get(addr_col))
        if not name and not addr:
            continue
        if not name:
            print(f"[跳过] 无库房名称: 地址={addr[:40]!r}...")
            continue
        if not addr:
            print(f"[跳过] 无库房地址: 名称={name!r}")
            continue
        if len(name) > 100:
            print(f"[跳过] 名称超过 100 字: {name[:30]!r}...")
            continue
        if len(addr) > 500:
            print(f"[警告] 地址超过 500 字将截断: {name!r}")
            addr = addr[:500]
        rows.append((name, addr))

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
                        # MySQL: 1=新插入；2=已有行且发生更新；0=键冲突但值未变
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
