"""
将 Excel 中的库房联系人、电话、危废经营许可数量、月均收货(吨)、运费参考(元) 写入 dict_warehouses。

适用于「循融宝库房档案」等含扩展列的表；也兼容仅有联系人/电话的「合作库房清单」类表。

规则：
- 仅更新数据库中已存在的库房（按名称匹配）；Excel 中在库里不存在的名称直接忽略。
- 默认仅当数据库对应列为 NULL 时才写入（避免覆盖已有手工数据）；加 --overwrite 则用 Excel 非空值覆盖。
- Excel 单元格为空则不修改该字段。
- 「运费」列若含多个数字（如 100-120、80/110、100/100/170），取算术平均写入 freight_amount（元/吨参考）。

用法：

  uv run python scripts/sync_warehouse_profile_from_excel.py \\
    --file "/path/to/循融宝库房档案(1).xlsx"

  uv run python scripts/sync_warehouse_profile_from_excel.py \\
    --file "3.合作库房清单.xlsx" --sheet "合作库房清单"

  uv run python scripts/sync_warehouse_profile_from_excel.py -f ./a.xlsx --dry-run

循融宝库房档案：总表「库房列表」为联系人/电话；各省分表含「危废经营许可审批数量」「历史月均收货量/吨」时，请加：

  uv run python scripts/sync_warehouse_profile_from_excel.py -f "./循融宝库房档案(1).xlsx" --all-sheets
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.database import get_conn  # noqa: E402

# ---------- 列名候选（与 Excel 表头去空白后比对） ----------
_NAME_CANDIDATES = (
    "库房名称",
    "仓库名称",
    "仓库名",
    "名称",
    "name",
    "Name",
    "库房名",
)
_CONTACT_CANDIDATES = ("联系人", "库房联系人", "对接人", "负责人")
_PHONE_CANDIDATES = ("电话", "联系电话", "手机号", "手机", "联系方式")
_LICENSE_CANDIDATES = (
    "危废经营许可审批数量",
    "危废经营许可证数量",
    "危废经营许可数量",
    "危废许可证数量",
    "许可证数量",
    "危废许可数量",
)
_MONTHLY_CANDIDATES = (
    "历史月均收货量/吨",
    "历史月均收货量",
    "月均收货",
    "月均收货吨",
    "月均收货(吨)",
    "月收货",
    "月均收货量",
)
_FREIGHT_CANDIDATES = (
    "运费",
    "运费参考",
    "运费元",
    "参考运费",
    "运价",
)


def _norm_header(s: object) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return re.sub(r"\s+", "", str(s).strip())


def _resolve_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    m = {_norm_header(c): c for c in df.columns}
    for cand in candidates:
        k = _norm_header(cand)
        if k in m:
            return m[k]
    return None


def _cell_str(v: object) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return str(v).strip()


def _cell_decimal(v: object) -> Optional[Decimal]:
    s = _cell_str(v)
    if not s:
        return None
    s = s.replace(",", "").replace("，", "").replace(" ", "")
    if s in ("-", "—", "无", "暂无", "NA", "N/A", "null", "None"):
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _parse_waste_license_ton_wan_per_year(v: object) -> Optional[Decimal]:
    """
    循融宝分表「危废经营许可审批数量」常见写法：0.2万吨/年、4 万吨/年、1.02万吨 / 年。
    解析为「万吨/年」的数值写入 hazardous_waste_license_qty（与 Excel 量级一致，非证书张数）。
    """
    s = _cell_str(v)
    if not s:
        return None
    s2 = s.replace(" ", "").replace("／", "/").replace("Ｗ", "万")
    if s2 in ("-", "—", "无", "暂无", "NA", "N/A"):
        return None
    m = re.search(r"([\d.]+)\s*万\s*吨", s2)
    if m:
        try:
            return Decimal(m.group(1))
        except InvalidOperation:
            return None
    return _cell_decimal(v)


def _parse_license_field(v: object) -> Optional[Decimal]:
    s = _cell_str(v)
    if not s:
        return None
    if "万" in s or "吨" in s:
        return _parse_waste_license_ton_wan_per_year(v)
    return _cell_decimal(v)


def _parse_freight_reference(v: object) -> Optional[Decimal]:
    """
    运费列常见写法：80、100-120、80/110、100/100/170（元/吨或区间）。
    提取串内所有数字；多个数字时取算术平均，作为 freight_amount 单值参考。
    """
    s = _cell_str(v)
    if not s:
        return None
    if s in ("-", "—", "无", "暂无", "NA", "N/A", "/", "\\", "无运费"):
        return None
    s = s.replace("－", "-").replace("~", "-").replace("至", "-")
    s = re.sub(r"[元吨]", "", s, flags=re.I)
    s = re.sub(r"(?<=\d)\s*-\s*(?=\d)", " ", s)
    for sep in ("/", "|", "、", "\\"):
        s = s.replace(sep, " ")
    s = s.replace("，", " ").replace(",", " ")
    raw_nums = re.findall(r"\d+\.?\d*|\d*\.\d+", s)
    nums: List[Decimal] = []
    for p in raw_nums:
        try:
            d = Decimal(p)
        except InvalidOperation:
            continue
        nums.append(d)
    if not nums:
        return None
    if len(nums) == 1:
        return nums[0]
    mean = sum(nums) / Decimal(len(nums))
    try:
        return mean.quantize(Decimal("0.0001"))
    except InvalidOperation:
        return mean


def _canonical_lookup_key(name: str) -> str:
    """弱化「⻢/马」等异体与空白差异，用于二次匹配。"""
    s = unicodedata.normalize("NFKC", name.strip())
    s = s.replace("\u2ee2", "\u9a6c").replace("\u2f8f", "\u9a6c")
    s = re.sub(r"\s+", "", s)
    return s


@dataclass
class ProfileRow:
    name: str
    contact: str
    phone: str
    license_qty: Optional[Decimal]
    monthly_ton: Optional[Decimal]
    freight_amount: Optional[Decimal]


def _pick_sheet(xl: pd.ExcelFile, sheet: str | None, sheet_index: int | None) -> str:
    if sheet_index is not None:
        if sheet_index < 0 or sheet_index >= len(xl.sheet_names):
            raise SystemExit(f"sheet_index 越界: {sheet_index}，共 {len(xl.sheet_names)} 张表")
        return xl.sheet_names[sheet_index]
    if sheet:
        if sheet not in xl.sheet_names:
            raise SystemExit(f"工作表不存在: {sheet!r}，可选: {xl.sheet_names}")
        return sheet
    preferred = ("循融宝库房档案", "合作库房清单", "库房档案")
    for p in preferred:
        if p in xl.sheet_names:
            return p
    for sn in xl.sheet_names:
        if "循融" in sn and "库房" in sn:
            return sn
    return xl.sheet_names[0]


def load_profile_rows_from_df(
    df: pd.DataFrame,
    *,
    name_col: str | None,
    contact_col: str | None,
    phone_col: str | None,
    license_col: str | None,
    monthly_col: str | None,
    freight_col: str | None,
) -> Tuple[List[ProfileRow], Dict[str, Optional[str]]]:
    nc = name_col.strip() if name_col else None
    if nc and nc not in df.columns:
        raise ValueError(f"名称列不存在: {nc!r}，当前列: {list(df.columns)}")
    if not nc:
        nc = _resolve_col(df, _NAME_CANDIDATES)
    if not nc:
        raise ValueError(f"无法识别库房名称列，当前列: {list(df.columns)}")

    cc = contact_col.strip() if contact_col else _resolve_col(df, _CONTACT_CANDIDATES)
    pc = phone_col.strip() if phone_col else _resolve_col(df, _PHONE_CANDIDATES)
    lc = license_col.strip() if license_col else _resolve_col(df, _LICENSE_CANDIDATES)
    mc = monthly_col.strip() if monthly_col else _resolve_col(df, _MONTHLY_CANDIDATES)
    fc = freight_col.strip() if freight_col else _resolve_col(df, _FREIGHT_CANDIDATES)

    col_used: Dict[str, Optional[str]] = {
        "库房名称": nc,
        "联系人": cc,
        "电话": pc,
        "危废经营许可数量": lc,
        "月均收货": mc,
        "运费": fc,
    }

    out: List[ProfileRow] = []
    for _, r in df.iterrows():
        name = _cell_str(r.get(nc))
        if not name or len(name) > 100:
            continue
        contact = _cell_str(r.get(cc)) if cc else ""
        phone = _cell_str(r.get(pc)) if pc else ""
        lic = _parse_license_field(r.get(lc)) if lc else None
        mon = _cell_decimal(r.get(mc)) if mc else None
        frt = _parse_freight_reference(r.get(fc)) if fc else None
        # 个别分表把「x万吨/年」误填在月均列：若月均格为产能文案且危废为空，则写入危废字段
        if mc and mon is None:
            raw_m = r.get(mc)
            s_m = _cell_str(raw_m)
            if s_m and ("万" in s_m or "吨" in s_m) and lic is None:
                alt = _parse_waste_license_ton_wan_per_year(raw_m)
                if alt is not None:
                    lic = alt
        if not contact and not phone and lic is None and mon is None and frt is None:
            continue
        if phone and len(phone) > 32:
            phone = phone[:32]
        if contact and len(contact) > 64:
            contact = contact[:64]
        out.append(
            ProfileRow(
                name=name,
                contact=contact,
                phone=phone,
                license_qty=lic,
                monthly_ton=mon,
                freight_amount=frt,
            )
        )
    return out, col_used


def _merge_profile_rows(target: Dict[str, ProfileRow], rows: List[ProfileRow]) -> None:
    """同名库房：后读入的工作表覆盖先读入的非空字段（便于总表+分表组合）。"""
    for pr in rows:
        key = pr.name.strip()
        if not key:
            continue
        if key not in target:
            target[key] = ProfileRow(
                name=key,
                contact=pr.contact,
                phone=pr.phone,
                license_qty=pr.license_qty,
                monthly_ton=pr.monthly_ton,
                freight_amount=pr.freight_amount,
            )
            continue
        o = target[key]
        target[key] = ProfileRow(
            name=key,
            contact=pr.contact or o.contact,
            phone=pr.phone or o.phone,
            license_qty=pr.license_qty if pr.license_qty is not None else o.license_qty,
            monthly_ton=pr.monthly_ton if pr.monthly_ton is not None else o.monthly_ton,
            freight_amount=pr.freight_amount if pr.freight_amount is not None else o.freight_amount,
        )


def load_profile_rows(
    path: Path,
    *,
    sheet: str | None,
    sheet_index: int | None,
    name_col: str | None,
    contact_col: str | None,
    phone_col: str | None,
    license_col: str | None,
    monthly_col: str | None,
    freight_col: str | None,
) -> Tuple[str, List[ProfileRow], Dict[str, Optional[str]]]:
    xl = pd.ExcelFile(path)
    sheet_name = _pick_sheet(xl, sheet, sheet_index)
    df = pd.read_excel(path, sheet_name=sheet_name, dtype=object)
    rows, col_used = load_profile_rows_from_df(
        df,
        name_col=name_col,
        contact_col=contact_col,
        phone_col=phone_col,
        license_col=license_col,
        monthly_col=monthly_col,
        freight_col=freight_col,
    )
    return sheet_name, rows, col_used


def load_all_sheets_profile_rows(
    path: Path,
    *,
    name_col: str | None,
    contact_col: str | None,
    phone_col: str | None,
    license_col: str | None,
    monthly_col: str | None,
    freight_col: str | None,
) -> Tuple[List[ProfileRow], List[Tuple[str, int, Dict[str, Optional[str]]]]]:
    """
    遍历工作簿全部工作表，跳过无法识别库房名称列的表；合并同名行。
    返回 (合并后的行列表, [(表名, 有效行数, 列映射), ...])。
    """
    xl = pd.ExcelFile(path)
    merged: Dict[str, ProfileRow] = {}
    sheet_infos: List[Tuple[str, int, Dict[str, Optional[str]]]] = []

    for sn in xl.sheet_names:
        df = pd.read_excel(path, sheet_name=sn, dtype=object)
        try:
            part, col_used = load_profile_rows_from_df(
                df,
                name_col=name_col,
                contact_col=contact_col,
                phone_col=phone_col,
                license_col=license_col,
                monthly_col=monthly_col,
                freight_col=freight_col,
            )
        except ValueError:
            print(f"  [跳过工作表] {sn!r}：无库房名称列或表结构不适用")
            continue
        if not part:
            sheet_infos.append((sn, 0, col_used))
            continue
        _merge_profile_rows(merged, part)
        sheet_infos.append((sn, len(part), col_used))

    return list(merged.values()), sheet_infos


def _load_warehouse_indexes(cur) -> Tuple[Dict[str, int], Dict[str, List[int]]]:
    """exact_name -> id；canonical_key -> [ids]（碰撞时保留列表便于告警）。"""
    cur.execute("SELECT id, name FROM dict_warehouses")
    exact: Dict[str, int] = {}
    canon: Dict[str, List[int]] = {}
    for wid, name in cur.fetchall():
        n = (name or "").strip()
        if not n:
            continue
        if n not in exact:
            exact[n] = wid
        ck = _canonical_lookup_key(n)
        canon.setdefault(ck, []).append(wid)
    return exact, canon


def _find_warehouse_id(
    excel_name: str,
    exact: Dict[str, int],
    canon: Dict[str, List[int]],
) -> Tuple[Optional[int], str]:
    s = excel_name.strip()
    if s in exact:
        return exact[s], "exact"
    ck = _canonical_lookup_key(s)
    ids = canon.get(ck) or []
    if len(ids) == 1:
        return ids[0], "canonical"
    if len(ids) > 1:
        return None, "ambiguous"
    return None, "missing"


def main() -> None:
    ap = argparse.ArgumentParser(description="从 Excel 补全 dict_warehouses 联系人/电话/许可量/月均收货/运费")
    ap.add_argument("--file", "-f", type=Path, required=True, help="Excel 路径")
    ap.add_argument("--sheet", type=str, default=None, help="工作表名（默认识别循融宝库房档案/合作库房清单等）")
    ap.add_argument("--sheet-index", type=int, default=None, help="工作表索引（0 起），优先于 --sheet")
    ap.add_argument("--name-col", type=str, default=None)
    ap.add_argument("--contact-col", type=str, default=None)
    ap.add_argument("--phone-col", type=str, default=None)
    ap.add_argument("--license-col", type=str, default=None)
    ap.add_argument("--monthly-col", type=str, default=None)
    ap.add_argument("--freight-col", type=str, default=None)
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Excel 非空时覆盖数据库已有值（默认仅填补 NULL）",
    )
    ap.add_argument("--dry-run", action="store_true", help="只打印统计与样例，不写库")
    ap.add_argument(
        "--all-sheets",
        action="store_true",
        help="遍历工作簿内全部工作表并合并（循融宝：库房列表 + 各省分表）",
    )
    args = ap.parse_args()

    path = args.file.expanduser().resolve()
    if not path.is_file():
        raise SystemExit(f"文件不存在: {path}")

    if args.all_sheets and (args.sheet is not None or args.sheet_index is not None):
        raise SystemExit("不可同时使用 --all-sheets 与 --sheet / --sheet-index")

    if args.all_sheets:
        rows, sheet_infos = load_all_sheets_profile_rows(
            path,
            name_col=args.name_col,
            contact_col=args.contact_col,
            phone_col=args.phone_col,
            license_col=args.license_col,
            monthly_col=args.monthly_col,
            freight_col=args.freight_col,
        )
        print("模式: 多工作表合并（--all-sheets）")
        for sn, cnt, cu in sheet_infos:
            lic = cu.get("危废经营许可数量")
            mon = cu.get("月均收货")
            frt = cu.get("运费")
            print(f"  {sn!r}: 有效行 {cnt} | 危废列={lic!r} | 月均列={mon!r} | 运费列={frt!r}")
        print(f"合并后库房数（去重）: {len(rows)}")
    else:
        sheet_name, rows, col_used = load_profile_rows(
            path,
            sheet=args.sheet,
            sheet_index=args.sheet_index,
            name_col=args.name_col,
            contact_col=args.contact_col,
            phone_col=args.phone_col,
            license_col=args.license_col,
            monthly_col=args.monthly_col,
            freight_col=args.freight_col,
        )
        print(f"工作表: {sheet_name!r}")
        for k, v in col_used.items():
            print(f"  列映射 {k}: {v!r}")
        print(f"有效数据行: {len(rows)}")

    only_null = not args.overwrite
    mode = "仅填补 NULL" if only_null else "非空则覆盖"
    print(f"写入策略: {mode}" + ("（预览）" if args.dry_run else ""))

    stats = {
        "matched": 0,
        "updated_rows": 0,
        "skipped_missing_wh": 0,
        "skipped_ambiguous": 0,
        "skipped_no_op": 0,
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
            exact, canon = _load_warehouse_indexes(cur)

            pending: List[Tuple[int, ProfileRow, Dict[str, Any]]] = []
            for pr in rows:
                wid, how = _find_warehouse_id(pr.name, exact, canon)
                if how == "missing":
                    stats["skipped_missing_wh"] += 1
                    continue
                if how == "ambiguous":
                    stats["skipped_ambiguous"] += 1
                    ck = _canonical_lookup_key(pr.name)
                    print(f"  [歧义跳过] {pr.name!r} 对应多 id: {canon.get(ck, [])}")
                    continue
                assert wid is not None
                stats["matched"] += 1

                cur.execute(
                    "SELECT contact_name, contact_phone, hazardous_waste_license_qty, monthly_avg_receipt_ton, "
                    "freight_amount FROM dict_warehouses WHERE id = %s",
                    (wid,),
                )
                db = cur.fetchone()
                if not db:
                    continue
                d_contact, d_phone, d_lic, d_mon, d_frt = db

                upd: Dict[str, Any] = {}
                if pr.contact:
                    if not only_null or d_contact is None or str(d_contact).strip() == "":
                        upd["contact_name"] = pr.contact
                if pr.phone:
                    if not only_null or d_phone is None or str(d_phone).strip() == "":
                        upd["contact_phone"] = pr.phone
                if pr.license_qty is not None:
                    if not only_null or d_lic is None:
                        upd["hazardous_waste_license_qty"] = pr.license_qty
                if pr.monthly_ton is not None:
                    if not only_null or d_mon is None:
                        upd["monthly_avg_receipt_ton"] = pr.monthly_ton
                if pr.freight_amount is not None:
                    if not only_null or d_frt is None:
                        upd["freight_amount"] = pr.freight_amount

                if not upd:
                    stats["skipped_no_op"] += 1
                    continue
                pending.append((wid, pr, upd))

            if args.dry_run:
                for wid, pr, upd in pending[:15]:
                    print(f"  [dry-run] id={wid} name={pr.name!r} -> {upd}")
                if len(pending) > 15:
                    print(f"  ... 另有 {len(pending) - 15} 条待更新")
                print(
                    f"统计: 匹配 {stats['matched']}, 将更新 {len(pending)}, "
                    f"库中无此名 {stats['skipped_missing_wh']}, "
                    f"名称歧义 {stats['skipped_ambiguous']}, 无需变更 {stats['skipped_no_op']}"
                )
                return

            for wid, pr, upd in pending:
                cols = ", ".join(f"`{k}`=%s" for k in upd.keys())
                vals = list(upd.values()) + [wid]
                cur.execute(f"UPDATE dict_warehouses SET {cols} WHERE id = %s", vals)
                if cur.rowcount:
                    stats["updated_rows"] += 1

    print(
        f"完成。匹配 {stats['matched']} 行 Excel 库房名；"
        f"执行更新 {stats['updated_rows']} 条；"
        f"库中不存在名称已忽略 {stats['skipped_missing_wh']} 行；"
        f"名称歧义跳过 {stats['skipped_ambiguous']}；"
        f"无变更跳过 {stats['skipped_no_op']}。"
    )


if __name__ == "__main__":
    main()
