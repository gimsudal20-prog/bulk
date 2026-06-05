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
from targeting_collector_helpers import _flatten_stat_rows


PLACEMENT_PARSER_VERSION = "placement_v20260605_report_media1"
PLACEMENT_TABLE = "fact_adgroup_placement_daily"
PLACEMENT_BREAKDOWN_CANDIDATES = [
    "mediaTp",
    "networkTp",
    "network",
    "media",
    "mediaType",
    "adNetworkTp",
    "deliveryMedia",
    "placement",
]

DATE_HEADER_CANDIDATES = ["일별", "날짜", "date", "dt", "statdt"]
DEVICE_HEADER_CANDIDATES = [
    "PC/모바일 매체", "PC/모바일", "PC Mobile Type", "pc/mobile", "pc mobile",
    "pc mobile type", "pc_mobile_type", "device", "device name", "platform",
]
CAMPAIGN_TYPE_HEADER_CANDIDATES = ["캠페인유형", "campaign type", "campaigntp"]
CAMPAIGN_HEADER_CANDIDATES = ["캠페인", "campaign", "campaign name", "campaignname"]
ADGROUP_HEADER_CANDIDATES = ["광고그룹", "adgroup", "ad group", "adgroup name", "adgroupname"]
PLACEMENT_HEADER_CANDIDATES = ["검색/콘텐츠 매체", "검색콘텐츠매체", "검색/콘텐츠", "placement", "media"]
REPORT_PLACEMENT_HEADER_CANDIDATES = [
    *PLACEMENT_HEADER_CANDIDATES,
    "매체이름", "매체명", "매체", "노출매체", "지면", "노출지면", "네트워크", "광고매체",
    "media name", "media type", "network", "ad network", "publisher",
]
AD_ID_HEADER_CANDIDATES = ["광고id", "소재id", "adid", "ad id", "nccadid"]
ADGROUP_ID_HEADER_CANDIDATES = ["광고그룹id", "adgroupid", "ad group id", "nccadgroupid"]
CAMPAIGN_ID_HEADER_CANDIDATES = ["캠페인id", "campaignid", "campaign id", "ncccampaignid"]
IMP_HEADER_CANDIDATES = ["노출수", "impression", "impressions", "imp", "impcnt"]
CLK_HEADER_CANDIDATES = ["클릭수", "click", "clicks", "clk", "clkcnt"]
COST_HEADER_CANDIDATES = ["총비용", "비용", "광고비", "cost", "salesamt"]
TOTAL_CONV_HEADER_CANDIDATES = ["총 전환수", "총전환수", "전환수", "total conversion", "conversions", "ccnt"]
TOTAL_SALES_HEADER_CANDIDATES = ["총 전환매출액(원)", "총전환매출액", "전환매출액", "conversion sales", "convamt", "sales"]
PURCHASE_CONV_HEADER_CANDIDATES = ["구매완료 전환수", "구매완료전환수", "purchase conversion"]
PURCHASE_SALES_HEADER_CANDIDATES = ["구매완료 전환매출액(원)", "구매완료전환매출액", "purchase sales"]
TOTAL_ROAS_HEADER_CANDIDATES = ["총 광고수익률(%)", "총광고수익률", "roas"]
PURCHASE_ROAS_HEADER_CANDIDATES = ["구매완료 광고수익률(%)", "구매완료광고수익률", "purchase roas"]
STAT_ID_ALIASES = ["id", "nccAdgroupId", "adgroupId", "adgroup_id", "광고그룹ID", "광고그룹id"]
STAT_METRIC_ALIASES = {
    "imp": ["impCnt", "impressions", "impression", "imp", "노출수"],
    "clk": ["clkCnt", "clicks", "click", "clk", "클릭수"],
    "cost": ["salesAmt", "cost", "spend", "광고비", "비용", "총비용"],
    "conv": ["ccnt", "convCnt", "conversionCount", "conversions", "전환수"],
    "sales": ["convAmt", "conversionValue", "salesByConversion", "conversionSales", "전환매출", "전환매출액"],
}
PLACEMENT_BREAKDOWN_ALIASES = [
    "mediaTp", "mediaType", "media", "networkTp", "network", "adNetworkTp", "deliveryMedia", "placement",
    "검색/콘텐츠", "검색/콘텐츠 매체", "검색콘텐츠매체", "매체", "네트워크", "지면",
]


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


def _get_case_insensitive(row: dict, names: List[str]) -> Any:
    if not isinstance(row, dict):
        return None
    for name in names:
        if name in row:
            return row.get(name)
    norm_map = {_normalize_header(k): k for k in row.keys()}
    for name in names:
        key = norm_map.get(_normalize_header(name))
        if key is not None:
            return row.get(key)
    return None


def _extract_stat_id(row: dict) -> str:
    return str(_get_case_insensitive(row, STAT_ID_ALIASES) or "").strip()


def _extract_stat_metric(row: dict, metric_key: str) -> float:
    val = _get_case_insensitive(row, STAT_METRIC_ALIASES.get(metric_key, []))
    if val is not None:
        return _safe_float(val)
    metrics = row.get("metrics") if isinstance(row, dict) else None
    if isinstance(metrics, dict):
        val = _get_case_insensitive(metrics, STAT_METRIC_ALIASES.get(metric_key, []))
        if val is not None:
            return _safe_float(val)
    if isinstance(metrics, list):
        order = {"imp": 0, "clk": 1, "cost": 2, "conv": 3, "sales": 4}
        idx = order.get(metric_key, -1)
        if 0 <= idx < len(metrics):
            return _safe_float(metrics[idx])
    return 0.0


def _extract_placement_breakdown_value(row: dict, requested_breakdown: str = "") -> str:
    aliases = list(dict.fromkeys([requested_breakdown, *PLACEMENT_BREAKDOWN_ALIASES]))
    val = _get_case_insensitive(row, aliases)
    if val not in (None, ""):
        return str(val).strip()

    for container_key in ["breakdown", "breakdowns", "dimension", "dimensions", "segment", "segments"]:
        obj = row.get(container_key) if isinstance(row, dict) else None
        if isinstance(obj, dict):
            val = _get_case_insensitive(obj, aliases + ["value", "name"])
            if val not in (None, ""):
                return str(val).strip()
        elif isinstance(obj, list):
            for item in obj:
                if not isinstance(item, dict):
                    continue
                key_name = _get_case_insensitive(item, ["key", "type", "field", "breakdown"])
                val = _get_case_insensitive(item, aliases + ["value", "name"])
                if val in (None, ""):
                    continue
                if not key_name or _normalize_header(str(key_name)) in {_normalize_header(a) for a in aliases}:
                    return str(val).strip()

    name_val = _get_case_insensitive(row, ["name"])
    return str(name_val or "").strip()


def _debug_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        raw = value
    elif isinstance(value, (dict, list)):
        raw = value
    else:
        raw = str(value)
    text = str(raw)
    if len(text) > 500:
        return text[:500] + "...<truncated>"
    return raw


def _sample_stat_rows(rows: List[dict], breakdown: str, limit: int = 3) -> List[Dict[str, Any]]:
    samples: List[Dict[str, Any]] = []
    aliases = list(dict.fromkeys([breakdown, *PLACEMENT_BREAKDOWN_ALIASES, "name"]))
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        candidate_values = {}
        for alias in aliases:
            val = _get_case_insensitive(row, [alias])
            if val not in (None, ""):
                candidate_values[alias] = _debug_value(val)
        containers = {}
        for key in ["breakdown", "breakdowns", "dimension", "dimensions", "segment", "segments", "metrics"]:
            if key in row:
                containers[key] = _debug_value(row.get(key))
        samples.append({
            "id": _extract_stat_id(row),
            "requested_breakdown": breakdown,
            "extracted": _extract_placement_breakdown_value(row, breakdown),
            "normalized": normalize_placement_type(_extract_placement_breakdown_value(row, breakdown)),
            "keys": list(row.keys())[:30],
            "candidate_values": candidate_values,
            "containers": containers,
            "first_values": {k: _debug_value(row.get(k)) for k in list(row.keys())[:12]},
        })
        if len(samples) >= limit:
            break
    return samples


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
    compact = _normalize_header(raw)
    if raw in {"2", "02"} or compact in {"c", "contentnetwork", "contentsnetwork", "contentad", "contentsad"}:
        return "CONTENT"
    if "콘텐츠" in raw or "컨텐츠" in raw or "content" in lowered or "contents" in lowered:
        return "CONTENT"
    if raw in {"1", "01"} or compact in {"s", "searchnetwork", "searchad", "searchmedia", "powerlink"}:
        return "SEARCH"
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


def build_adgroup_id_lookup(engine: Engine, customer_id: str) -> Dict[str, Dict[str, str]]:
    sql = """
    SELECT
        COALESCE(a.adgroup_id, '') AS adgroup_id,
        COALESCE(c.campaign_id, a.campaign_id, '') AS campaign_id,
        COALESCE(c.campaign_tp, '') AS campaign_type
    FROM dim_adgroup a
    LEFT JOIN dim_campaign c
      ON a.customer_id::text = c.customer_id::text
     AND a.campaign_id::text = c.campaign_id::text
    WHERE a.customer_id::text = :cid
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {"cid": str(customer_id)}).fetchall()
    except Exception:
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for row in rows or []:
        adgroup_id = str(row[0] or "").strip()
        if not adgroup_id:
            continue
        out[adgroup_id] = {
            "campaign_id": str(row[1] or "").strip(),
            "campaign_type": str(row[2] or "").strip(),
        }
    return out


def _table_columns(engine: Engine, table_name: str) -> set[str]:
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=:table
            """), {"table": str(table_name)}).fetchall()
        return {str(r[0]) for r in rows or []}
    except Exception:
        return set()


def _coalesce_existing(cols: set[str], candidates: List[str]) -> str:
    existing = [f"f.{c}" for c in candidates if c in cols]
    if not existing:
        return "0"
    return f"COALESCE({', '.join(existing)}, 0)"


def build_ad_id_lookup(engine: Engine, customer_id: str) -> Dict[str, Dict[str, str]]:
    required = {"customer_id", "ad_id", "adgroup_id"}
    if not required.issubset(_table_columns(engine, "dim_ad")):
        return {}
    sql = """
    SELECT
        COALESCE(d.ad_id, '') AS ad_id,
        COALESCE(d.adgroup_id, '') AS adgroup_id,
        COALESCE(c.campaign_id, a.campaign_id, '') AS campaign_id,
        COALESCE(c.campaign_tp, '') AS campaign_type
    FROM dim_ad d
    LEFT JOIN dim_adgroup a
      ON d.customer_id::text = a.customer_id::text
     AND d.adgroup_id::text = a.adgroup_id::text
    LEFT JOIN dim_campaign c
      ON a.customer_id::text = c.customer_id::text
     AND a.campaign_id::text = c.campaign_id::text
    WHERE d.customer_id::text = :cid
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {"cid": str(customer_id)}).fetchall()
    except Exception:
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for row in rows or []:
        ad_id = str(row[0] or "").strip()
        adgroup_id = str(row[1] or "").strip()
        if not ad_id or not adgroup_id:
            continue
        out[ad_id] = {
            "adgroup_id": adgroup_id,
            "campaign_id": str(row[2] or "").strip(),
            "campaign_type": str(row[3] or "").strip(),
        }
    return out


def read_adgroup_purchase_split_lookup(engine: Engine, customer_id: str, target_date: date) -> Dict[str, Dict[str, float]]:
    fact_cols = _table_columns(engine, "fact_ad_daily")
    dim_cols = _table_columns(engine, "dim_ad")
    if not {"customer_id", "ad_id", "dt"}.issubset(fact_cols) or not {"customer_id", "ad_id", "adgroup_id"}.issubset(dim_cols):
        return {}
    conv_expr = _coalesce_existing(fact_cols, ["purchase_conv", "primary_conv"])
    sales_expr = _coalesce_existing(fact_cols, ["purchase_sales", "primary_sales"])
    if conv_expr == "0" and sales_expr == "0":
        return {}
    sql = """
    SELECT
        d.adgroup_id,
        SUM({conv_expr}) AS purchase_conv,
        SUM({sales_expr}) AS purchase_sales
    FROM fact_ad_daily f
    JOIN dim_ad d
      ON f.customer_id::text = d.customer_id::text
     AND f.ad_id::text = d.ad_id::text
    WHERE f.customer_id::text = :cid
      AND f.dt = :dt
    GROUP BY d.adgroup_id
    """.format(conv_expr=conv_expr, sales_expr=sales_expr)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {"cid": str(customer_id), "dt": target_date}).fetchall()
    except Exception:
        return {}
    out: Dict[str, Dict[str, float]] = {}
    for row in rows or []:
        adgroup_id = str(row[0] or "").strip()
        if not adgroup_id:
            continue
        out[adgroup_id] = {
            "purchase_conv": float(row[1] or 0.0),
            "purchase_sales": float(row[2] or 0.0),
        }
    return out


def _is_shopping_campaign_type(value: Any) -> bool:
    raw = str(value or "").strip()
    return "쇼핑" in raw or raw.upper() in {"SHOPPING", "SSA", "SHOPPING_SEARCH"}


def _read_grouped_fact_rows(
    engine: Engine,
    *,
    table_name: str,
    id_join_sql: str,
    id_alias: str,
    customer_id: str,
    target_date: date,
) -> List[Dict[str, Any]]:
    fact_cols = _table_columns(engine, table_name)
    if not {"customer_id", "dt"}.issubset(fact_cols):
        return []
    imp_expr = _coalesce_existing(fact_cols, ["imp"])
    clk_expr = _coalesce_existing(fact_cols, ["clk"])
    cost_expr = _coalesce_existing(fact_cols, ["cost"])
    conv_expr = _coalesce_existing(fact_cols, ["conv", "total_conv"])
    sales_expr = _coalesce_existing(fact_cols, ["sales", "total_sales"])
    purchase_conv_expr = _coalesce_existing(fact_cols, ["purchase_conv", "primary_conv"])
    purchase_sales_expr = _coalesce_existing(fact_cols, ["purchase_sales", "primary_sales"])
    avg_rnk_expr = _coalesce_existing(fact_cols, ["avg_rnk"])
    sql = f"""
    SELECT
        COALESCE({id_alias}.adgroup_id, '') AS adgroup_id,
        COALESCE(c.campaign_id, a.campaign_id, '') AS campaign_id,
        COALESCE(c.campaign_tp, '') AS campaign_type,
        SUM({imp_expr}) AS imp,
        SUM({clk_expr}) AS clk,
        SUM({cost_expr}) AS cost,
        SUM({conv_expr}) AS conv,
        SUM({sales_expr}) AS sales,
        SUM({purchase_conv_expr}) AS purchase_conv,
        SUM({purchase_sales_expr}) AS purchase_sales,
        AVG(NULLIF({avg_rnk_expr}, 0)) AS avg_rnk
    FROM {table_name} f
    {id_join_sql}
    LEFT JOIN dim_adgroup a
      ON a.customer_id::text = f.customer_id::text
     AND a.adgroup_id::text = {id_alias}.adgroup_id::text
    LEFT JOIN dim_campaign c
      ON c.customer_id::text = f.customer_id::text
     AND c.campaign_id::text = a.campaign_id::text
    WHERE f.customer_id::text = :cid
      AND f.dt = :dt
    GROUP BY
        COALESCE({id_alias}.adgroup_id, ''),
        COALESCE(c.campaign_id, a.campaign_id, ''),
        COALESCE(c.campaign_tp, '')
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {"cid": str(customer_id), "dt": target_date}).mappings().all()
    except Exception:
        return []
    return [dict(row) for row in rows or []]


def build_placement_rows_from_existing_facts(
    engine: Engine,
    customer_id: str,
    target_date: date,
    *,
    allowed_campaign_ids: set[str] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Fallback placement rows from already-collected daily facts.

    The SearchAd API spec does not expose a search/content placement breakdown.
    Keep the dashboard populated by mapping existing adgroup-level totals to
    SEARCH, while marking the source clearly for later audit.
    """
    allowed = {str(x).strip() for x in (allowed_campaign_ids or set()) if str(x).strip()} or None
    grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    source_counts = {"fact_adgroup_daily": 0, "fact_keyword_daily": 0, "fact_ad_daily": 0}

    def add_row(row: Dict[str, Any], source: str, *, shopping_only: bool | None = None, fill_only: bool = False) -> None:
        adgroup_id = str(row.get("adgroup_id") or "").strip()
        campaign_id = str(row.get("campaign_id") or "").strip()
        campaign_type = str(row.get("campaign_type") or "").strip()
        if not adgroup_id or not campaign_id:
            return
        if allowed and campaign_id not in allowed:
            return
        is_shop = _is_shopping_campaign_type(campaign_type)
        if shopping_only is True and not is_shop:
            return
        if shopping_only is False and is_shop:
            return
        imp = int(float(row.get("imp") or 0))
        clk = int(float(row.get("clk") or 0))
        cost = int(float(row.get("cost") or 0))
        conv = float(row.get("conv") or 0.0)
        sales = int(float(row.get("sales") or 0))
        purchase_conv = float(row.get("purchase_conv") or 0.0)
        purchase_sales = int(float(row.get("purchase_sales") or 0))
        if imp == 0 and clk == 0 and cost == 0 and conv == 0 and sales == 0 and purchase_conv == 0 and purchase_sales == 0:
            return
        key = (target_date, str(customer_id), campaign_id, adgroup_id, "UNSEGMENTED", "SEARCH")
        if fill_only and key in grouped:
            return
        rec = grouped.setdefault(key, {
            "dt": target_date,
            "customer_id": str(customer_id),
            "campaign_id": campaign_id,
            "adgroup_id": adgroup_id,
            "campaign_type": campaign_type,
            "device_name": "UNSEGMENTED",
            "placement_type": "SEARCH",
            "imp": 0,
            "clk": 0,
            "cost": 0,
            "conv": 0.0,
            "sales": 0,
            "purchase_conv": 0.0,
            "purchase_sales": 0,
            "roas": 0.0,
            "purchase_roas": 0.0,
            "data_source": f"FALLBACK_{source}_SEARCH",
            "source_report": "EXISTING_FACTS",
        })
        rec["imp"] += imp
        rec["clk"] += clk
        rec["cost"] += cost
        rec["conv"] += conv
        rec["sales"] += sales
        rec["purchase_conv"] += purchase_conv
        rec["purchase_sales"] += purchase_sales
        source_counts[source] = source_counts.get(source, 0) + 1

    adgroup_cols = _table_columns(engine, "fact_adgroup_daily")
    if {"customer_id", "dt", "adgroup_id"}.issubset(adgroup_cols):
        for row in _read_grouped_fact_rows(
            engine,
            table_name="fact_adgroup_daily",
            id_join_sql="",
            id_alias="f",
            customer_id=customer_id,
            target_date=target_date,
        ):
            add_row(row, "fact_adgroup_daily")

    kw_cols = _table_columns(engine, "fact_keyword_daily")
    dim_kw_cols = _table_columns(engine, "dim_keyword")
    if {"customer_id", "dt", "keyword_id"}.issubset(kw_cols) and {"customer_id", "keyword_id", "adgroup_id"}.issubset(dim_kw_cols):
        id_join = "JOIN dim_keyword k ON k.customer_id::text = f.customer_id::text AND k.keyword_id::text = f.keyword_id::text"
        for row in _read_grouped_fact_rows(
            engine,
            table_name="fact_keyword_daily",
            id_join_sql=id_join,
            id_alias="k",
            customer_id=customer_id,
            target_date=target_date,
        ):
            add_row(row, "fact_keyword_daily", shopping_only=False, fill_only=True)

    ad_cols = _table_columns(engine, "fact_ad_daily")
    dim_ad_cols = _table_columns(engine, "dim_ad")
    if {"customer_id", "dt", "ad_id"}.issubset(ad_cols) and {"customer_id", "ad_id", "adgroup_id"}.issubset(dim_ad_cols):
        id_join = "JOIN dim_ad d ON d.customer_id::text = f.customer_id::text AND d.ad_id::text = f.ad_id::text"
        for row in _read_grouped_fact_rows(
            engine,
            table_name="fact_ad_daily",
            id_join_sql=id_join,
            id_alias="d",
            customer_id=customer_id,
            target_date=target_date,
        ):
            add_row(row, "fact_ad_daily", shopping_only=True, fill_only=True)
            add_row(row, "fact_ad_daily", shopping_only=False, fill_only=True)

    rows = list(grouped.values())
    for rec in rows:
        rec["roas"] = round((float(rec["sales"] or 0) / float(rec["cost"] or 0)) * 100, 4) if rec.get("cost") else 0.0
        rec["purchase_roas"] = round((float(rec["purchase_sales"] or 0) / float(rec["cost"] or 0)) * 100, 4) if rec.get("cost") else 0.0

    return rows, {
        "status": "fallback_existing_facts" if rows else "fallback_no_rows",
        "parser": PLACEMENT_PARSER_VERSION,
        "source_report": "EXISTING_FACTS",
        "parsed_rows": len(rows),
        "placement_type": "SEARCH",
        "source_counts": source_counts,
    }


def fetch_stats_placement_breakdown_rows(
    customer_id: str,
    adgroup_ids: List[str],
    target_date: date,
    *,
    request_json_fn,
    log_fn=None,
) -> Tuple[List[dict], Dict[str, Any]]:
    clean_ids = [str(x).strip() for x in adgroup_ids if str(x or "").strip()]
    meta: Dict[str, Any] = {
        "status": "not_requested",
        "attempted_breakdowns": [],
        "selected_breakdown": "",
        "raw_rows": 0,
        "errors": [],
        "debug_samples": {},
    }
    if not clean_ids:
        meta["status"] = "no_adgroups"
        return [], meta

    import json

    fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt"], separators=(",", ":"))
    d_str = target_date.strftime("%Y-%m-%d")
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(",", ":"))
    for breakdown in PLACEMENT_BREAKDOWN_CANDIDATES:
        rows: List[dict] = []
        errors: List[str] = []
        meta["attempted_breakdowns"].append(breakdown)
        for i in range(0, len(clean_ids), 50):
            chunk = clean_ids[i:i + 50]
            params = {
                "ids": ",".join(chunk),
                "fields": fields,
                "timeRange": time_range,
                "breakdown": breakdown,
            }
            status, data = request_json_fn("GET", "/stats", customer_id, params=params, raise_error=False)
            if status == 200:
                rows.extend(_flatten_stat_rows(data))
            else:
                errors.append(f"HTTP {status} - {data}")
        parsed_candidate_count = sum(1 for r in rows if normalize_placement_type(_extract_placement_breakdown_value(r, breakdown)) in {"SEARCH", "CONTENT"})
        if parsed_candidate_count > 0:
            meta.update({
                "status": "ok",
                "selected_breakdown": breakdown,
                "raw_rows": len(rows),
                "parsed_candidate_rows": parsed_candidate_count,
            })
            if log_fn:
                log_fn(f"   ↪ /stats 지면 breakdown 선택: {breakdown} | raw={len(rows)} parsed_candidates={parsed_candidate_count}")
            return rows, meta
        if errors:
            meta["errors"].append({"breakdown": breakdown, "sample": errors[:2]})
        elif rows:
            samples = _sample_stat_rows(rows, breakdown)
            meta["debug_samples"][breakdown] = samples
            if log_fn:
                first = samples[0] if samples else {}
                log_fn(
                    f"   /stats breakdown={breakdown} raw={len(rows)} no search/content "
                    f"sample_keys={first.get('keys', [])} extracted={first.get('extracted', '')} "
                    f"candidate_values={first.get('candidate_values', {})}"
                )

    meta["status"] = "no_supported_breakdown"
    return [], meta


def build_placement_rows_from_stats(
    raw_rows: List[dict],
    *,
    customer_id: str,
    target_date: date,
    adgroup_lookup: Dict[str, Dict[str, str]],
    purchase_split_lookup: Dict[str, Dict[str, float]] | None = None,
    selected_breakdown: str = "",
    allowed_campaign_ids: set[str] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    allowed = {str(x).strip() for x in (allowed_campaign_ids or set()) if str(x).strip()} or None
    purchase_split_lookup = purchase_split_lookup or {}
    grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    rejected = {"missing_id": 0, "unknown_adgroup": 0, "out_of_scope": 0, "missing_placement": 0, "zero_metric": 0}
    samples = {k: [] for k in rejected}

    for row in raw_rows or []:
        if not isinstance(row, dict):
            continue
        adgroup_id = _extract_stat_id(row)
        if not adgroup_id:
            rejected["missing_id"] += 1
            if len(samples["missing_id"]) < 3:
                samples["missing_id"].append({k: row.get(k) for k in list(row.keys())[:12]})
            continue
        mapping = adgroup_lookup.get(adgroup_id)
        if not mapping:
            rejected["unknown_adgroup"] += 1
            continue
        campaign_id = str(mapping.get("campaign_id") or "").strip()
        if allowed and campaign_id not in allowed:
            rejected["out_of_scope"] += 1
            continue
        placement_type = normalize_placement_type(_extract_placement_breakdown_value(row, selected_breakdown))
        if placement_type not in {"SEARCH", "CONTENT"}:
            rejected["missing_placement"] += 1
            if len(samples["missing_placement"]) < 3:
                samples["missing_placement"].append({k: row.get(k) for k in list(row.keys())[:12]})
            continue
        imp = int(_extract_stat_metric(row, "imp") or 0)
        clk = int(_extract_stat_metric(row, "clk") or 0)
        cost = int(_extract_stat_metric(row, "cost") or 0)
        conv = float(_extract_stat_metric(row, "conv") or 0.0)
        sales = int(_extract_stat_metric(row, "sales") or 0)
        if imp == 0 and clk == 0 and cost == 0 and conv == 0 and sales == 0:
            rejected["zero_metric"] += 1
            continue
        key = (target_date, str(customer_id), campaign_id, adgroup_id, "UNSEGMENTED", placement_type)
        rec = grouped.setdefault(key, {
            "dt": target_date,
            "customer_id": str(customer_id),
            "campaign_id": campaign_id,
            "adgroup_id": adgroup_id,
            "campaign_type": str(mapping.get("campaign_type") or "").strip(),
            "device_name": "UNSEGMENTED",
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
            "data_source": f"STATS_BREAKDOWN_{selected_breakdown}" if selected_breakdown else "STATS_BREAKDOWN",
            "source_report": "STATS",
        })
        rec["imp"] += imp
        rec["clk"] += clk
        rec["cost"] += cost
        rec["conv"] += conv
        rec["sales"] += sales

    rows = list(grouped.values())
    cost_by_adgroup: Dict[str, int] = {}
    for rec in rows:
        cost_by_adgroup[rec["adgroup_id"]] = cost_by_adgroup.get(rec["adgroup_id"], 0) + int(rec.get("cost") or 0)
    for rec in rows:
        split = purchase_split_lookup.get(str(rec.get("adgroup_id") or "").strip()) or {}
        total_cost = cost_by_adgroup.get(str(rec.get("adgroup_id") or "").strip(), 0)
        ratio = (float(rec.get("cost") or 0) / float(total_cost)) if total_cost > 0 else 0.0
        rec["purchase_conv"] = round(float(split.get("purchase_conv", 0.0) or 0.0) * ratio, 6)
        rec["purchase_sales"] = int(round(float(split.get("purchase_sales", 0.0) or 0.0) * ratio))
        rec["roas"] = round((float(rec["sales"] or 0) / float(rec["cost"] or 0)) * 100, 4) if rec.get("cost") else 0.0
        rec["purchase_roas"] = round((float(rec["purchase_sales"] or 0) / float(rec["cost"] or 0)) * 100, 4) if rec.get("cost") else 0.0

    return rows, {
        "status": "ok" if rows else "no_rows",
        "parser": PLACEMENT_PARSER_VERSION,
        "source_report": "STATS",
        "breakdown": selected_breakdown,
        "raw_rows": len(raw_rows or []),
        "parsed_rows": len(rows),
        "rejected": rejected,
        "samples": samples,
        "purchase_split_rows": len(purchase_split_lookup),
    }


def _is_device_header(header: Any) -> bool:
    raw = str(header or "").strip()
    if not raw:
        return False
    return _get_col_idx([raw], DEVICE_HEADER_CANDIDATES) != -1


def _get_report_placement_col_idx(headers: List[str]) -> int:
    norm_headers = [_normalize_header(h) for h in headers]
    norm_candidates = [_normalize_header(c) for c in REPORT_PLACEMENT_HEADER_CANDIDATES]

    for cand in norm_candidates:
        for idx, header in enumerate(norm_headers):
            if cand and cand == header and not _is_device_header(headers[idx]):
                return idx
    for cand in norm_candidates:
        for idx, header in enumerate(norm_headers):
            if cand and cand in header and not _is_device_header(headers[idx]):
                return idx
    return -1


def _score_report_header_row(row_vals: List[str]) -> int:
    score = 0
    if (
        _get_col_idx(row_vals, AD_ID_HEADER_CANDIDATES) != -1
        or _get_col_idx(row_vals, ADGROUP_ID_HEADER_CANDIDATES) != -1
        or _get_col_idx(row_vals, CAMPAIGN_ID_HEADER_CANDIDATES) != -1
    ):
        score += 2
    if _get_report_placement_col_idx(row_vals) != -1:
        score += 3
    metric_hits = sum(
        1
        for candidates in [IMP_HEADER_CANDIDATES, CLK_HEADER_CANDIDATES, COST_HEADER_CANDIDATES, TOTAL_CONV_HEADER_CANDIDATES, TOTAL_SALES_HEADER_CANDIDATES]
        if _get_col_idx(row_vals, candidates) != -1
    )
    score += min(metric_hits, 3)
    return score


def _detect_report_header_idx(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return -1
    best_idx = -1
    best_score = -1
    scan_limit = min(60, len(df.index))
    for idx in range(scan_limit):
        row_vals = [str(x or "") for x in df.iloc[idx].fillna("").tolist()]
        score = _score_report_header_row(row_vals)
        if score > best_score:
            best_idx = idx
            best_score = score
        if score >= 6:
            return idx
    return best_idx if best_score >= 4 else -1


def _sample_report_rows(data_df: pd.DataFrame, idx: Dict[str, int], limit: int = 5) -> List[Dict[str, Any]]:
    samples: List[Dict[str, Any]] = []
    if data_df is None or data_df.empty:
        return samples
    for row_no, (_, row) in enumerate(data_df.head(100).iterrows(), start=1):
        raw_placement = _cell(row, idx.get("placement", -1))
        if not raw_placement and len(samples) >= limit:
            continue
        samples.append({
            "row_no": row_no,
            "ad_id": _cell(row, idx.get("ad_id", -1)),
            "adgroup_id": _cell(row, idx.get("adgroup_id", -1)),
            "campaign_id": _cell(row, idx.get("campaign_id", -1)),
            "device": _cell(row, idx.get("device", -1)),
            "placement_raw": raw_placement,
            "placement_type": normalize_placement_type(raw_placement),
            "imp": _cell(row, idx.get("imp", -1)),
            "clk": _cell(row, idx.get("clk", -1)),
            "cost": _cell(row, idx.get("cost", -1)),
        })
        if len(samples) >= limit:
            break
    return samples


def _placement_type_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows or []:
        key = str(row.get("placement_type") or "UNKNOWN").strip() or "UNKNOWN"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _top_values(counter: Dict[str, int], limit: int = 20) -> List[Dict[str, Any]]:
    return [
        {"value": value, "count": count}
        for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]
    ]


def build_placement_rows_from_report(
    df: pd.DataFrame | None,
    *,
    customer_id: str,
    target_date: date,
    source_report: str,
    ad_id_lookup: Dict[str, Dict[str, str]] | None = None,
    adgroup_lookup: Dict[str, Dict[str, str]] | None = None,
    adgroup_name_lookup: Dict[Tuple[str, str], Dict[str, str] | None] | None = None,
    purchase_split_lookup: Dict[str, Dict[str, float]] | None = None,
    allowed_campaign_ids: set[str] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "status": "empty",
        "parser": PLACEMENT_PARSER_VERSION,
        "source_report": source_report,
        "raw_rows": 0,
        "parsed_rows": 0,
        "placement_type_counts": {},
        "rejected": {
            "missing_mapping": 0,
            "ambiguous_mapping": 0,
            "out_of_scope": 0,
            "missing_placement": 0,
            "zero_metric": 0,
            "short_row": 0,
        },
        "samples": [],
        "placement_values": [],
    }
    if df is None or getattr(df, "empty", False):
        return [], meta

    raw_df = df.reset_index(drop=True).copy()
    header_idx = _detect_report_header_idx(raw_df)
    if header_idx == -1:
        meta["status"] = "header_missing"
        meta["sample_rows"] = [
            [str(x) for x in raw_df.iloc[i].fillna("").tolist()[:20]]
            for i in range(min(5, len(raw_df.index)))
        ]
        return [], meta

    headers = [str(x or "") for x in raw_df.iloc[header_idx].fillna("").tolist()]
    data_df = raw_df.iloc[header_idx + 1:].reset_index(drop=True)
    meta["raw_rows"] = int(len(data_df.index))
    meta["header_idx"] = int(header_idx)
    meta["headers"] = headers[:40]

    idx = {
        "dt": _get_col_idx(headers, DATE_HEADER_CANDIDATES),
        "ad_id": _get_col_idx(headers, AD_ID_HEADER_CANDIDATES),
        "adgroup_id": _get_col_idx(headers, ADGROUP_ID_HEADER_CANDIDATES),
        "campaign_id": _get_col_idx(headers, CAMPAIGN_ID_HEADER_CANDIDATES),
        "campaign": _get_col_idx(headers, CAMPAIGN_HEADER_CANDIDATES),
        "adgroup": _get_col_idx(headers, ADGROUP_HEADER_CANDIDATES),
        "device": _get_col_idx(headers, DEVICE_HEADER_CANDIDATES),
        "placement": _get_report_placement_col_idx(headers),
        "imp": _get_col_idx(headers, IMP_HEADER_CANDIDATES),
        "clk": _get_col_idx(headers, CLK_HEADER_CANDIDATES),
        "cost": _get_col_idx(headers, COST_HEADER_CANDIDATES),
        "conv": _get_col_idx(headers, TOTAL_CONV_HEADER_CANDIDATES),
        "sales": _get_col_idx(headers, TOTAL_SALES_HEADER_CANDIDATES),
        "purchase_conv": _get_col_idx(headers, PURCHASE_CONV_HEADER_CANDIDATES),
        "purchase_sales": _get_col_idx(headers, PURCHASE_SALES_HEADER_CANDIDATES),
    }
    meta["column_indices"] = idx
    meta["samples"] = _sample_report_rows(data_df, idx)

    if idx["placement"] == -1:
        meta["status"] = "placement_column_missing"
        return [], meta
    if idx["ad_id"] == -1 and idx["adgroup_id"] == -1 and (idx["campaign"] == -1 or idx["adgroup"] == -1):
        meta["status"] = "id_columns_missing"
        return [], meta
    if idx["imp"] == -1 and idx["clk"] == -1 and idx["cost"] == -1 and idx["conv"] == -1 and idx["sales"] == -1:
        meta["status"] = "metric_columns_missing"
        return [], meta

    ad_id_lookup = ad_id_lookup or {}
    adgroup_lookup = adgroup_lookup or {}
    adgroup_name_lookup = adgroup_name_lookup or {}
    purchase_split_lookup = purchase_split_lookup or {}
    allowed = {str(x).strip() for x in (allowed_campaign_ids or set()) if str(x).strip()} or None
    grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    raw_placement_counts: Dict[str, int] = {}

    max_idx = max([x for x in idx.values() if x != -1], default=0)
    for _, row in data_df.iterrows():
        if len(row) <= max_idx:
            meta["rejected"]["short_row"] += 1
            continue

        raw_placement = _cell(row, idx["placement"])
        if raw_placement:
            raw_placement_counts[raw_placement] = raw_placement_counts.get(raw_placement, 0) + 1
        placement_type = normalize_placement_type(raw_placement)
        if placement_type not in {"SEARCH", "CONTENT"}:
            meta["rejected"]["missing_placement"] += 1
            continue

        ad_id = _cell(row, idx["ad_id"])
        adgroup_id = _cell(row, idx["adgroup_id"])
        campaign_id = _cell(row, idx["campaign_id"])
        campaign_type = ""

        mapping = ad_id_lookup.get(ad_id) if ad_id else None
        if mapping:
            adgroup_id = adgroup_id or str(mapping.get("adgroup_id") or "").strip()
            campaign_id = campaign_id or str(mapping.get("campaign_id") or "").strip()
            campaign_type = str(mapping.get("campaign_type") or "").strip()

        if adgroup_id:
            ag_mapping = adgroup_lookup.get(adgroup_id)
            if ag_mapping:
                campaign_id = campaign_id or str(ag_mapping.get("campaign_id") or "").strip()
                campaign_type = campaign_type or str(ag_mapping.get("campaign_type") or "").strip()

        if (not adgroup_id or not campaign_id) and idx["campaign"] != -1 and idx["adgroup"] != -1:
            name_key = (_normalize_name(_cell(row, idx["campaign"])), _normalize_name(_cell(row, idx["adgroup"])))
            name_mapping = adgroup_name_lookup.get(name_key)
            if name_mapping is None and name_key in adgroup_name_lookup:
                meta["rejected"]["ambiguous_mapping"] += 1
                continue
            if name_mapping:
                adgroup_id = adgroup_id or str(name_mapping.get("adgroup_id") or "").strip()
                campaign_id = campaign_id or str(name_mapping.get("campaign_id") or "").strip()
                campaign_type = campaign_type or str(name_mapping.get("campaign_type") or "").strip()

        if not adgroup_id or not campaign_id:
            meta["rejected"]["missing_mapping"] += 1
            continue
        if allowed and campaign_id not in allowed:
            meta["rejected"]["out_of_scope"] += 1
            continue

        imp = _safe_int(_cell(row, idx["imp"])) if idx["imp"] != -1 else 0
        clk = _safe_int(_cell(row, idx["clk"])) if idx["clk"] != -1 else 0
        cost = _safe_int(_cell(row, idx["cost"])) if idx["cost"] != -1 else 0
        conv = _safe_float(_cell(row, idx["conv"])) if idx["conv"] != -1 else 0.0
        sales = _safe_int(_cell(row, idx["sales"])) if idx["sales"] != -1 else 0
        purchase_conv = _safe_float(_cell(row, idx["purchase_conv"])) if idx["purchase_conv"] != -1 else 0.0
        purchase_sales = _safe_int(_cell(row, idx["purchase_sales"])) if idx["purchase_sales"] != -1 else 0
        if imp == 0 and clk == 0 and cost == 0 and conv == 0 and sales == 0 and purchase_conv == 0 and purchase_sales == 0:
            meta["rejected"]["zero_metric"] += 1
            continue

        row_dt = _parse_dt(_cell(row, idx["dt"]), target_date) if idx["dt"] != -1 else target_date
        device_name = normalize_device_name(_cell(row, idx["device"])) if idx["device"] != -1 else ""
        device_name = device_name or "UNSEGMENTED"
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
            "data_source": f"REPORT_{source_report}_PLACEMENT",
            "source_report": source_report,
        })
        rec["imp"] += imp
        rec["clk"] += clk
        rec["cost"] += cost
        rec["conv"] += conv
        rec["sales"] += sales
        rec["purchase_conv"] += purchase_conv
        rec["purchase_sales"] += purchase_sales

    rows = list(grouped.values())
    direct_purchase = any(float(r.get("purchase_conv") or 0.0) or int(r.get("purchase_sales") or 0) for r in rows)
    cost_by_adgroup: Dict[str, int] = {}
    for rec in rows:
        cost_by_adgroup[rec["adgroup_id"]] = cost_by_adgroup.get(rec["adgroup_id"], 0) + int(rec.get("cost") or 0)
    for rec in rows:
        if not direct_purchase:
            split = purchase_split_lookup.get(str(rec.get("adgroup_id") or "").strip()) or {}
            total_cost = cost_by_adgroup.get(str(rec.get("adgroup_id") or "").strip(), 0)
            ratio = (float(rec.get("cost") or 0) / float(total_cost)) if total_cost > 0 else 0.0
            rec["purchase_conv"] = round(float(split.get("purchase_conv", 0.0) or 0.0) * ratio, 6)
            rec["purchase_sales"] = int(round(float(split.get("purchase_sales", 0.0) or 0.0) * ratio))
        rec["roas"] = round((float(rec["sales"] or 0) / float(rec["cost"] or 0)) * 100, 4) if rec.get("cost") else 0.0
        rec["purchase_roas"] = round((float(rec["purchase_sales"] or 0) / float(rec["cost"] or 0)) * 100, 4) if rec.get("cost") else 0.0

    counts = _placement_type_counts(rows)
    meta["parsed_rows"] = int(len(rows))
    meta["placement_type_counts"] = counts
    meta["placement_values"] = _top_values(raw_placement_counts)
    meta["purchase_split_rows"] = len(purchase_split_lookup)
    if rows and int(counts.get("CONTENT", 0) or 0) > 0:
        meta["status"] = "ok"
    elif rows:
        meta["status"] = "no_content_rows"
    else:
        meta["status"] = "no_mapped_rows"
    return rows, meta


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
                return 0
            except Exception as exc:
                last_exc = exc
                time.sleep(2)
        raise RuntimeError(f"{table} empty replace failed: {type(last_exc).__name__}: {last_exc}")

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

    last_exc: Exception | None = None
    for _ in range(3):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            cur.execute(f"DELETE FROM {table} WHERE customer_id=%s AND dt=%s", (str(customer_id), target_date))
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            return len(df)
        except Exception as exc:
            last_exc = exc
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
    raise RuntimeError(f"{table} replace failed: {type(last_exc).__name__}: {last_exc}")
