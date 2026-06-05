# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import time
from datetime import date, datetime
from typing import Any, Dict, List, Tuple

import pandas as pd
import psycopg2.extras
from sqlalchemy import text
from sqlalchemy.engine import Engine

from device_collector_helpers import normalize_device_name


PLACEMENT_PARSER_VERSION = "placement_v20260605_da_raw_ssa1"
PLACEMENT_TABLE = "fact_adgroup_placement_daily"

DATE_HEADER_CANDIDATES = ["일별", "날짜", "date", "dt", "statdt"]
DEVICE_HEADER_CANDIDATES = ["PC/모바일 매체", "PC/모바일", "pc/mobile", "pc mobile", "device"]
CAMPAIGN_TYPE_HEADER_CANDIDATES = ["캠페인유형", "campaign type", "campaigntp"]
CAMPAIGN_HEADER_CANDIDATES = ["캠페인", "campaign", "campaign name", "campaignname"]
ADGROUP_HEADER_CANDIDATES = ["광고그룹", "adgroup", "ad group", "adgroup name", "adgroupname"]
PLACEMENT_HEADER_CANDIDATES = ["검색/콘텐츠 매체", "검색콘텐츠매체", "검색/콘텐츠", "placement", "media"]
IMP_HEADER_CANDIDATES = ["노출수", "impression", "impressions", "imp"]
CLK_HEADER_CANDIDATES = ["클릭수", "click", "clicks", "clk"]
COST_HEADER_CANDIDATES = ["총비용", "비용", "cost"]
TOTAL_CONV_HEADER_CANDIDATES = ["총 전환수", "총전환수", "전환수", "total conversion", "conversions"]
TOTAL_SALES_HEADER_CANDIDATES = ["총 전환매출액(원)", "총전환매출액", "전환매출액", "conversion sales"]
PURCHASE_CONV_HEADER_CANDIDATES = ["구매완료 전환수", "구매완료전환수", "purchase conversion"]
PURCHASE_SALES_HEADER_CANDIDATES = ["구매완료 전환매출액(원)", "구매완료전환매출액", "purchase sales"]
TOTAL_ROAS_HEADER_CANDIDATES = ["총 광고수익률(%)", "총광고수익률", "roas"]
PURCHASE_ROAS_HEADER_CANDIDATES = ["구매완료 광고수익률(%)", "구매완료광고수익률", "purchase roas"]


def _normalize_header(value: Any) -> str:
    return re.sub(r"[\s_\-\"']", "", str(value or "").lower())


def _get_col_idx(headers: List[str], candidates: List[str]) -> int:
    norm_headers = [_normalize_header(h) for h in headers]
    norm_candidates = [_normalize_header(c) for c in candidates]
    for cand in norm_candidates:
        for idx, header in enumerate(norm_headers):
            if cand and cand == header:
                return idx
    for cand in norm_candidates:
        for idx, header in enumerate(norm_headers):
            if cand and cand in header:
                return idx
    return -1


def _detect_header_idx(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return -1
    max_scan = min(20, len(df.index))
    for idx in range(max_scan):
        row = [str(x or "") for x in df.iloc[idx].fillna("").tolist()]
        if _get_col_idx(row, PLACEMENT_HEADER_CANDIDATES) != -1 and _get_col_idx(row, IMP_HEADER_CANDIDATES) != -1:
            return idx
    return -1


def _safe_float(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    raw = str(value).replace(",", "").replace("%", "").strip()
    if not raw or raw == "-":
        return 0.0
    try:
        return float(raw)
    except Exception:
        return 0.0


def _safe_int(value: Any) -> int:
    return int(round(_safe_float(value)))


def _cell(row, idx: int) -> str:
    if idx < 0 or idx >= len(row):
        return ""
    value = row.iloc[idx]
    if pd.isna(value):
        return ""
    return str(value).strip()


def _parse_dt(value: Any, fallback: date) -> date:
    raw = str(value or "").strip()
    if not raw:
        return fallback
    cleaned = raw.rstrip(".")
    for fmt in ("%Y.%m.%d", "%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except Exception:
            continue
    return fallback


def normalize_placement_type(value: Any) -> str:
    raw = str(value or "").strip()
    lowered = raw.lower()
    if "콘텐츠" in raw or "content" in lowered:
        return "CONTENT"
    if "검색" in raw or "search" in lowered:
        return "SEARCH"
    return raw.upper() if raw else "UNKNOWN"


def _normalize_name(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def ensure_placement_tables(engine: Engine):
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS fact_adgroup_placement_daily (
                dt DATE,
                customer_id TEXT,
                campaign_id TEXT,
                adgroup_id TEXT,
                campaign_type TEXT,
                device_name TEXT,
                placement_type TEXT,
                imp BIGINT,
                clk BIGINT,
                cost BIGINT,
                conv DOUBLE PRECISION,
                sales BIGINT DEFAULT 0,
                purchase_conv DOUBLE PRECISION DEFAULT 0,
                purchase_sales BIGINT DEFAULT 0,
                roas DOUBLE PRECISION DEFAULT 0,
                purchase_roas DOUBLE PRECISION DEFAULT 0,
                data_source TEXT,
                source_report TEXT,
                PRIMARY KEY(dt, customer_id, adgroup_id, device_name, placement_type)
            )
            """
        ))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_adgroup_placement_customer_dt ON fact_adgroup_placement_daily (customer_id, dt)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_adgroup_placement_campaign ON fact_adgroup_placement_daily (customer_id, campaign_id, dt)"))


def build_adgroup_name_lookup(engine: Engine, customer_id: str) -> Dict[Tuple[str, str], Dict[str, str] | None]:
    sql = """
    SELECT
        COALESCE(c.campaign_name, '') AS campaign_name,
        COALESCE(a.adgroup_name, '') AS adgroup_name,
        COALESCE(c.campaign_id, a.campaign_id, '') AS campaign_id,
        COALESCE(a.adgroup_id, '') AS adgroup_id,
        COALESCE(c.campaign_tp, '') AS campaign_type
    FROM dim_adgroup a
    LEFT JOIN dim_campaign c
      ON a.customer_id::text = c.customer_id::text
     AND a.campaign_id::text = c.campaign_id::text
    WHERE a.customer_id::text = :cid
    """
    lookup: Dict[Tuple[str, str], Dict[str, str] | None] = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {"cid": str(customer_id)}).fetchall()
    except Exception:
        return lookup

    for row in rows or []:
        key = (_normalize_name(row[0]), _normalize_name(row[1]))
        if not key[0] or not key[1]:
            continue
        payload = {
            "campaign_id": str(row[2] or "").strip(),
            "adgroup_id": str(row[3] or "").strip(),
            "campaign_type": str(row[4] or "").strip(),
        }
        if lookup.get(key) is not None and lookup.get(key) != payload:
            lookup[key] = None
        else:
            lookup[key] = payload
    return lookup


def parse_da_raw_ssa_placement_report(
    df: pd.DataFrame | None,
    *,
    customer_id: str,
    target_date: date,
    adgroup_lookup: Dict[Tuple[str, str], Dict[str, str] | None] | None = None,
    allowed_campaign_ids: set[str] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "status": "empty",
        "parser": PLACEMENT_PARSER_VERSION,
        "source_report": "DA_RAW_SSA",
        "raw_rows": 0,
        "parsed_rows": 0,
        "skipped_rows": 0,
        "missing_mapping_rows": 0,
        "ambiguous_mapping_rows": 0,
    }
    if df is None or getattr(df, "empty", False):
        return [], meta

    raw_df = df.reset_index(drop=True).copy()
    header_idx = _detect_header_idx(raw_df)
    if header_idx == -1:
        meta["status"] = "header_missing"
        return [], meta

    headers = [str(x or "") for x in raw_df.iloc[header_idx].fillna("").tolist()]
    data_df = raw_df.iloc[header_idx + 1:].reset_index(drop=True)
    meta["raw_rows"] = int(len(data_df.index))

    idx = {
        "dt": _get_col_idx(headers, DATE_HEADER_CANDIDATES),
        "device": _get_col_idx(headers, DEVICE_HEADER_CANDIDATES),
        "campaign_type": _get_col_idx(headers, CAMPAIGN_TYPE_HEADER_CANDIDATES),
        "campaign": _get_col_idx(headers, CAMPAIGN_HEADER_CANDIDATES),
        "adgroup": _get_col_idx(headers, ADGROUP_HEADER_CANDIDATES),
        "placement": _get_col_idx(headers, PLACEMENT_HEADER_CANDIDATES),
        "imp": _get_col_idx(headers, IMP_HEADER_CANDIDATES),
        "clk": _get_col_idx(headers, CLK_HEADER_CANDIDATES),
        "cost": _get_col_idx(headers, COST_HEADER_CANDIDATES),
        "conv": _get_col_idx(headers, TOTAL_CONV_HEADER_CANDIDATES),
        "sales": _get_col_idx(headers, TOTAL_SALES_HEADER_CANDIDATES),
        "purchase_conv": _get_col_idx(headers, PURCHASE_CONV_HEADER_CANDIDATES),
        "purchase_sales": _get_col_idx(headers, PURCHASE_SALES_HEADER_CANDIDATES),
        "roas": _get_col_idx(headers, TOTAL_ROAS_HEADER_CANDIDATES),
        "purchase_roas": _get_col_idx(headers, PURCHASE_ROAS_HEADER_CANDIDATES),
    }
    required = ["dt", "device", "campaign", "adgroup", "placement", "imp", "clk", "cost"]
    if any(idx[name] == -1 for name in required):
        meta["status"] = "required_columns_missing"
        meta["missing_columns"] = [name for name in required if idx[name] == -1]
        return [], meta

    adgroup_lookup = adgroup_lookup or {}
    allowed = {str(x).strip() for x in (allowed_campaign_ids or set()) if str(x).strip()} or None
    grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    for _, row in data_df.iterrows():
        campaign_name = _normalize_name(_cell(row, idx["campaign"]))
        adgroup_name = _normalize_name(_cell(row, idx["adgroup"]))
        placement_type = normalize_placement_type(_cell(row, idx["placement"]))
        if not campaign_name or not adgroup_name or placement_type == "UNKNOWN":
            meta["skipped_rows"] += 1
            continue

        mapping = adgroup_lookup.get((campaign_name, adgroup_name))
        if mapping is None and (campaign_name, adgroup_name) in adgroup_lookup:
            meta["ambiguous_mapping_rows"] += 1
            continue
        if not mapping:
            meta["missing_mapping_rows"] += 1
            continue

        campaign_id = str(mapping.get("campaign_id") or "").strip()
        adgroup_id = str(mapping.get("adgroup_id") or "").strip()
        if not campaign_id or not adgroup_id:
            meta["missing_mapping_rows"] += 1
            continue
        if allowed and campaign_id not in allowed:
            meta["skipped_rows"] += 1
            continue

        row_dt = _parse_dt(_cell(row, idx["dt"]), target_date)
        device_name = normalize_device_name(_cell(row, idx["device"]))
        campaign_type = str(mapping.get("campaign_type") or "").strip() or _cell(row, idx["campaign_type"])
        key = (row_dt, str(customer_id), campaign_id, adgroup_id, device_name, placement_type)
        rec = grouped.setdefault(key, {
            "dt": row_dt,
            "customer_id": str(customer_id),
            "campaign_id": campaign_id,
            "adgroup_id": adgroup_id,
            "campaign_type": campaign_type,
            "device_name": device_name,
            "placement_type": placement_type,
            "imp": 0,
            "clk": 0,
            "cost": 0,
            "conv": 0.0,
            "sales": 0,
            "purchase_conv": 0.0,
            "purchase_sales": 0,
            "roas": 0.0,
            "purchase_roas": 0.0,
            "data_source": "REPORT",
            "source_report": "DA_RAW_SSA",
        })
        rec["imp"] += _safe_int(_cell(row, idx["imp"]))
        rec["clk"] += _safe_int(_cell(row, idx["clk"]))
        rec["cost"] += _safe_int(_cell(row, idx["cost"]))
        rec["conv"] += _safe_float(_cell(row, idx["conv"])) if idx["conv"] != -1 else 0.0
        rec["sales"] += _safe_int(_cell(row, idx["sales"])) if idx["sales"] != -1 else 0
        rec["purchase_conv"] += _safe_float(_cell(row, idx["purchase_conv"])) if idx["purchase_conv"] != -1 else 0.0
        rec["purchase_sales"] += _safe_int(_cell(row, idx["purchase_sales"])) if idx["purchase_sales"] != -1 else 0

    rows = list(grouped.values())
    for rec in rows:
        rec["roas"] = round((float(rec["sales"] or 0) / float(rec["cost"] or 0)) * 100, 4) if rec.get("cost") else 0.0
        rec["purchase_roas"] = round((float(rec["purchase_sales"] or 0) / float(rec["cost"] or 0)) * 100, 4) if rec.get("cost") else 0.0

    meta["parsed_rows"] = int(len(rows))
    meta["status"] = "ok" if rows else "no_mapped_rows"
    return rows, meta


def replace_placement_fact_range(engine: Engine, rows: List[Dict[str, Any]], customer_id: str, target_date: date):
    table = PLACEMENT_TABLE
    if not rows:
        for _ in range(3):
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt=:dt"), {"cid": str(customer_id), "dt": target_date})
                return
            except Exception:
                time.sleep(2)
        return

    pk_cols = ["dt", "customer_id", "adgroup_id", "device_name", "placement_type"]
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep="last").sort_values(by=pk_cols).astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    conflict_clause = "ON CONFLICT ({}) DO UPDATE SET {}".format(
        pk_str,
        ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols]),
    )
    sql = f"INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}"
    tuples = list(df.itertuples(index=False, name=None))

    for _ in range(3):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            return
        except Exception:
            if raw_conn:
                try:
                    raw_conn.rollback()
                except Exception:
                    pass
            time.sleep(2)
        finally:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
            if raw_conn:
                try:
                    raw_conn.close()
                except Exception:
                    pass
