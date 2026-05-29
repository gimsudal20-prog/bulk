# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import re
import time
from datetime import date
from typing import Any, Callable, Dict, Iterable, List, Tuple

import pandas as pd
import psycopg2.extras
from sqlalchemy import text
from sqlalchemy.engine import Engine


TARGETING_PARSER_VERSION = "targeting_v20260528_stats_breakdown1"

FIELDS = ["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt"]
METRIC_ALIASES = {
    "imp": ["impCnt", "impressions", "impression", "imp", "노출수"],
    "clk": ["clkCnt", "clicks", "click", "clk", "클릭수"],
    "cost": ["salesAmt", "cost", "spend", "광고비", "비용", "총비용"],
    "conv": ["ccnt", "convCnt", "conversionCount", "conversions", "전환수"],
    "sales": ["convAmt", "conversionValue", "salesByConversion", "conversionSales", "전환매출", "전환매출액"],
}
ID_ALIASES = ["id", "nccCampaignId", "campaignId", "campaign_id", "캠페인ID", "캠페인id"]
BREAKDOWN_ALIASES = {
    "hh24": ["hh24", "hour", "hourOfDay", "hour_of_day", "시간대", "시간"],
    "ageRangeNm": ["ageRangeNm", "age_range_nm", "ageRange", "age", "연령대", "연령"],
}


def _safe_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        if pd.isna(v):
            return 0.0
    except Exception:
        pass
    s = str(v).replace(",", "").strip()
    if not s or s == "-":
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _safe_int(v: Any) -> int:
    return int(round(_safe_float(v)))


def _norm_key(v: str) -> str:
    return str(v or "").lower().replace(" ", "").replace("_", "").replace("-", "").replace("/", "")


def _get_case_insensitive(row: dict, names: Iterable[str]) -> Any:
    if not isinstance(row, dict):
        return None
    for name in names:
        if name in row:
            return row.get(name)
    norm_map = {_norm_key(k): k for k in row.keys()}
    for name in names:
        key = norm_map.get(_norm_key(name))
        if key is not None:
            return row.get(key)
    return None


def _extract_id(row: dict) -> str:
    val = _get_case_insensitive(row, ID_ALIASES)
    return str(val or "").strip()


def _extract_metric(row: dict, metric_key: str) -> float:
    val = _get_case_insensitive(row, METRIC_ALIASES.get(metric_key, []))
    if val is not None:
        return _safe_float(val)

    # 일부 /stats breakdown 응답은 metrics 배열로 내려오는 경우가 있어 방어적으로 처리합니다.
    metrics = row.get("metrics") if isinstance(row, dict) else None
    if isinstance(metrics, dict):
        val = _get_case_insensitive(metrics, METRIC_ALIASES.get(metric_key, []))
        if val is not None:
            return _safe_float(val)
    if isinstance(metrics, list):
        # fields 순서와 동일하게 내려온 응답 방어: [impCnt, clkCnt, salesAmt, ccnt, convAmt]
        order = {"imp": 0, "clk": 1, "cost": 2, "conv": 3, "sales": 4}
        idx = order.get(metric_key, -1)
        if 0 <= idx < len(metrics):
            return _safe_float(metrics[idx])
    return 0.0


def _extract_breakdown_value(row: dict, breakdown_key: str) -> str:
    aliases = BREAKDOWN_ALIASES.get(breakdown_key, [breakdown_key])
    val = _get_case_insensitive(row, aliases)
    if val not in (None, ""):
        return str(val).strip()

    bd = row.get("breakdown") or row.get("breakdowns") if isinstance(row, dict) else None
    if isinstance(bd, dict):
        val = _get_case_insensitive(bd, aliases)
        if val not in (None, ""):
            return str(val).strip()
    elif isinstance(bd, list):
        for item in bd:
            if not isinstance(item, dict):
                continue
            val = _get_case_insensitive(item, aliases + ["value", "name"])
            key_name = _get_case_insensitive(item, ["key", "type", "field", "breakdown"])
            if val not in (None, "") and (not key_name or _norm_key(str(key_name)) in {_norm_key(a) for a in aliases}):
                return str(val).strip()

    # 기타 중첩 응답 방어
    for container_key in ["dimension", "dimensions", "segment", "segments"]:
        obj = row.get(container_key) if isinstance(row, dict) else None
        if isinstance(obj, dict):
            val = _get_case_insensitive(obj, aliases)
            if val not in (None, ""):
                return str(val).strip()
    return ""


def _flatten_stat_rows(obj: Any, inherited_id: str = "") -> List[dict]:
    rows: List[dict] = []
    if isinstance(obj, dict):
        current_id = _extract_id(obj) or str(inherited_id or "").strip()
        has_metric = any(_get_case_insensitive(obj, vals) is not None for vals in METRIC_ALIASES.values())
        has_metrics_arr = isinstance(obj.get("metrics"), (list, dict))
        if current_id and (has_metric or has_metrics_arr):
            row = dict(obj)
            if not _extract_id(row):
                row["id"] = current_id
            rows.append(row)

        # 일부 breakdown 응답은 {id, breakdown:[{hh24, metrics...}]} 형태입니다.
        # 하위 row에 id가 없으면 부모 id를 승계합니다.
        for key in ["data", "rows", "result", "results", "items", "stat", "stats", "breakdown", "breakdowns"]:
            if key in obj and obj[key] is not obj:
                rows.extend(_flatten_stat_rows(obj[key], current_id))
    elif isinstance(obj, list):
        for item in obj:
            rows.extend(_flatten_stat_rows(item, inherited_id))
    return rows


def normalize_hour_value(v: Any) -> int | None:
    raw = str(v or "").strip()
    if not raw:
        return None

    if raw.isdigit():
        digits = raw
    else:
        matches = re.findall(r"\d{1,2}", raw)
        if matches:
            digits = matches[0]
        else:
            digits = "".join(ch for ch in raw if ch.isdigit())
            if len(digits) >= 2:
                digits = digits[:2]
    if not digits:
        return None
    try:
        hour = int(digits[:2] if len(digits) >= 2 else digits)
    except Exception:
        return None
    if 0 <= hour <= 23:
        return hour
    return None


def normalize_age_range(v: Any) -> str:
    raw = str(v or "").strip()
    if not raw or raw in {"-", "None", "nan", "NaN"}:
        return "미분류"
    return raw.replace(" ", "")


def ensure_targeting_tables(engine: Engine):
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS fact_campaign_hourly_daily (
                dt DATE,
                customer_id TEXT,
                campaign_id TEXT,
                hour_of_day INTEGER,
                imp BIGINT DEFAULT 0,
                clk BIGINT DEFAULT 0,
                cost BIGINT DEFAULT 0,
                conv DOUBLE PRECISION DEFAULT 0,
                sales BIGINT DEFAULT 0,
                roas DOUBLE PRECISION DEFAULT 0,
                data_source TEXT,
                parser_version TEXT,
                PRIMARY KEY(dt, customer_id, campaign_id, hour_of_day)
            )
            """
        ))
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS fact_campaign_age_daily (
                dt DATE,
                customer_id TEXT,
                campaign_id TEXT,
                age_range TEXT,
                imp BIGINT DEFAULT 0,
                clk BIGINT DEFAULT 0,
                cost BIGINT DEFAULT 0,
                conv DOUBLE PRECISION DEFAULT 0,
                sales BIGINT DEFAULT 0,
                roas DOUBLE PRECISION DEFAULT 0,
                data_source TEXT,
                parser_version TEXT,
                PRIMARY KEY(dt, customer_id, campaign_id, age_range)
            )
            """
        ))


def _replace_campaign_hourly_rows(engine: Engine, customer_id: str, target_date: date, rows: List[Dict[str, Any]], scoped_ids: List[str] | None = None) -> int:
    ensure_targeting_tables(engine)
    target_ids = [str(x) for x in (scoped_ids or []) if str(x or "").strip()]
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            if target_ids:
                cur.execute(
                    "DELETE FROM fact_campaign_hourly_daily WHERE dt=%s AND customer_id=%s AND campaign_id = ANY(%s)",
                    (target_date, str(customer_id), target_ids),
                )
            else:
                cur.execute("DELETE FROM fact_campaign_hourly_daily WHERE dt=%s AND customer_id=%s", (target_date, str(customer_id)))
            if rows:
                values = [(
                    r["dt"], r["customer_id"], r["campaign_id"], int(r["hour_of_day"]),
                    int(r.get("imp", 0) or 0), int(r.get("clk", 0) or 0), int(r.get("cost", 0) or 0),
                    float(r.get("conv", 0) or 0), int(r.get("sales", 0) or 0), float(r.get("roas", 0) or 0),
                    str(r.get("data_source") or "stats_breakdown_hh24"), str(r.get("parser_version") or TARGETING_PARSER_VERSION),
                ) for r in rows]
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO fact_campaign_hourly_daily
                    (dt, customer_id, campaign_id, hour_of_day, imp, clk, cost, conv, sales, roas, data_source, parser_version)
                    VALUES %s
                    ON CONFLICT (dt, customer_id, campaign_id, hour_of_day) DO UPDATE SET
                        imp=EXCLUDED.imp, clk=EXCLUDED.clk, cost=EXCLUDED.cost, conv=EXCLUDED.conv,
                        sales=EXCLUDED.sales, roas=EXCLUDED.roas, data_source=EXCLUDED.data_source,
                        parser_version=EXCLUDED.parser_version
                """, values, page_size=1000)
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()
    return len(rows)


def _replace_campaign_age_rows(engine: Engine, customer_id: str, target_date: date, rows: List[Dict[str, Any]], scoped_ids: List[str] | None = None) -> int:
    ensure_targeting_tables(engine)
    target_ids = [str(x) for x in (scoped_ids or []) if str(x or "").strip()]
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            if target_ids:
                cur.execute(
                    "DELETE FROM fact_campaign_age_daily WHERE dt=%s AND customer_id=%s AND campaign_id = ANY(%s)",
                    (target_date, str(customer_id), target_ids),
                )
            else:
                cur.execute("DELETE FROM fact_campaign_age_daily WHERE dt=%s AND customer_id=%s", (target_date, str(customer_id)))
            if rows:
                values = [(
                    r["dt"], r["customer_id"], r["campaign_id"], str(r["age_range"]),
                    int(r.get("imp", 0) or 0), int(r.get("clk", 0) or 0), int(r.get("cost", 0) or 0),
                    float(r.get("conv", 0) or 0), int(r.get("sales", 0) or 0), float(r.get("roas", 0) or 0),
                    str(r.get("data_source") or "stats_breakdown_ageRangeNm"), str(r.get("parser_version") or TARGETING_PARSER_VERSION),
                ) for r in rows]
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO fact_campaign_age_daily
                    (dt, customer_id, campaign_id, age_range, imp, clk, cost, conv, sales, roas, data_source, parser_version)
                    VALUES %s
                    ON CONFLICT (dt, customer_id, campaign_id, age_range) DO UPDATE SET
                        imp=EXCLUDED.imp, clk=EXCLUDED.clk, cost=EXCLUDED.cost, conv=EXCLUDED.conv,
                        sales=EXCLUDED.sales, roas=EXCLUDED.roas, data_source=EXCLUDED.data_source,
                        parser_version=EXCLUDED.parser_version
                """, values, page_size=1000)
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()
    return len(rows)


def get_stats_breakdown_range(
    customer_id: str,
    ids: List[str],
    target_date: date,
    breakdown: str,
    *,
    request_json_fn: Callable[..., Tuple[int, Any]],
    chunk_size: int = 50,
    log_fn: Callable[[str], None] | None = None,
) -> List[dict]:
    if not ids:
        return []
    d_str = target_date.strftime("%Y-%m-%d")
    fields = json.dumps(FIELDS, separators=(",", ":"))
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(",", ":"))
    out: List[dict] = []

    def fetch(chunk: List[str]) -> Tuple[bool, List[dict], str]:
        params = {
            "ids": ",".join(chunk),
            "fields": fields,
            "timeRange": time_range,
            "breakdown": breakdown,
        }
        status, data = request_json_fn("GET", "/stats", customer_id, params=params, raise_error=False)
        if status == 200:
            return True, _flatten_stat_rows(data), ""
        return False, [], f"HTTP {status} - {data}"

    clean_ids = [str(x).strip() for x in ids if str(x or "").strip()]
    for i in range(0, len(clean_ids), max(1, int(chunk_size))):
        chunk = clean_ids[i:i + max(1, int(chunk_size))]
        ok, rows, err = fetch(chunk)
        if ok:
            out.extend(rows)
            continue

        # breakdown 조회는 동일 캠페인 유형만 묶어야 하는 경우가 있어 실패 시 단건으로 재시도합니다.
        if log_fn:
            log_fn(f"   ⚠️ /stats breakdown={breakdown} 묶음 조회 실패 → 단건 재시도 ({len(chunk)}개) | {err}")
        for one in chunk:
            ok_one, rows_one, err_one = fetch([one])
            if ok_one:
                out.extend(rows_one)
            elif log_fn:
                log_fn(f"   ⚠️ /stats breakdown={breakdown} 단건 실패 id={one} | {err_one}")
            time.sleep(0.05)
    return out


def _build_rows_from_breakdown(raw_rows: List[dict], customer_id: str, target_date: date, breakdown: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    agg: Dict[Tuple[str, Any], Dict[str, Any]] = {}
    rejected = {"missing_id": 0, "missing_breakdown": 0, "bad_hour": 0, "zero_metric": 0}

    for row in raw_rows or []:
        cid = _extract_id(row)
        if not cid:
            rejected["missing_id"] += 1
            continue
        bd_value = _extract_breakdown_value(row, breakdown)
        if not bd_value:
            rejected["missing_breakdown"] += 1
            continue
        if breakdown == "hh24":
            bucket_value = normalize_hour_value(bd_value)
            if bucket_value is None:
                rejected["bad_hour"] += 1
                continue
        else:
            bucket_value = normalize_age_range(bd_value)

        imp = int(_extract_metric(row, "imp") or 0)
        clk = int(_extract_metric(row, "clk") or 0)
        cost = int(_extract_metric(row, "cost") or 0)
        conv = float(_extract_metric(row, "conv") or 0)
        sales = int(_extract_metric(row, "sales") or 0)
        if imp == 0 and clk == 0 and cost == 0 and conv == 0 and sales == 0:
            rejected["zero_metric"] += 1
            continue

        key = (cid, bucket_value)
        b = agg.setdefault(key, {
            "dt": target_date,
            "customer_id": str(customer_id),
            "campaign_id": cid,
            "imp": 0,
            "clk": 0,
            "cost": 0,
            "conv": 0.0,
            "sales": 0,
            "roas": 0.0,
            "parser_version": TARGETING_PARSER_VERSION,
        })
        b["imp"] += imp
        b["clk"] += clk
        b["cost"] += cost
        b["conv"] += conv
        b["sales"] += sales

    rows = []
    for (cid, bucket_value), b in agg.items():
        b["roas"] = (float(b["sales"]) / float(b["cost"]) * 100.0) if float(b["cost"] or 0) > 0 else 0.0
        if breakdown == "hh24":
            b["hour_of_day"] = int(bucket_value)
            b["data_source"] = "stats_breakdown_hh24"
        else:
            b["age_range"] = str(bucket_value)
            b["data_source"] = "stats_breakdown_ageRangeNm"
        rows.append(b)

    meta = {
        "raw_rows": len(raw_rows or []),
        "parsed_rows": len(rows),
        **rejected,
    }
    return rows, meta


def collect_campaign_time_age_stats(
    engine: Engine,
    customer_id: str,
    target_date: date,
    *,
    campaign_ids: List[str],
    shopping_campaign_ids: Iterable[str] | None = None,
    campaign_type_map: Dict[str, str] | None = None,
    shopping_only: bool = False,
    get_stats_breakdown_range_fn: Callable[..., List[dict]],
    log_fn: Callable[[str], None] | None = None,
) -> Dict[str, Any]:
    ensure_targeting_tables(engine)
    all_campaign_ids = [str(x).strip() for x in campaign_ids if str(x or "").strip()]
    if shopping_only:
        shopping_set = {str(x).strip() for x in (shopping_campaign_ids or []) if str(x or "").strip()}
        hour_ids = [x for x in all_campaign_ids if x in shopping_set]
    else:
        hour_ids = all_campaign_ids

    type_map = {str(k): str(v or "") for k, v in (campaign_type_map or {}).items()}
    buckets: Dict[str, List[str]] = {}
    for cid in hour_ids:
        buckets.setdefault(type_map.get(cid, "UNKNOWN") or "UNKNOWN", []).append(cid)

    hour_raw: List[dict] = []
    for _, ids in buckets.items():
        hour_raw.extend(get_stats_breakdown_range_fn(customer_id, ids, target_date, "hh24", log_fn=log_fn))
    hour_rows, hour_meta = _build_rows_from_breakdown(hour_raw, customer_id, target_date, "hh24")
    hour_saved = _replace_campaign_hourly_rows(engine, customer_id, target_date, hour_rows, scoped_ids=hour_ids)

    # 연령대 데이터는 과거 쇼핑검색 중심으로 안내된 문서가 있었지만,
    # 실제 운영/타게팅 리포트 흐름에서는 파워링크 캠페인도 ageRangeNm 조회가 가능한 계정이 있습니다.
    # 캠페인 유형이 섞인 ids 요청은 일부 계정에서 실패할 수 있어 유형별 bucket 단위로 먼저 요청하고,
    # 실패 시 get_stats_breakdown_range 내부에서 단건 재시도합니다.
    if shopping_only:
        shopping_set = {str(x).strip() for x in (shopping_campaign_ids or []) if str(x or "").strip()}
        age_ids = [x for x in all_campaign_ids if x in shopping_set]
    else:
        age_ids = all_campaign_ids

    age_buckets: Dict[str, List[str]] = {}
    for cid in age_ids:
        age_buckets.setdefault(type_map.get(cid, "UNKNOWN") or "UNKNOWN", []).append(cid)

    age_raw: List[dict] = []
    for _, ids in age_buckets.items():
        age_raw.extend(get_stats_breakdown_range_fn(customer_id, ids, target_date, "ageRangeNm", log_fn=log_fn))
    age_rows, age_meta = _build_rows_from_breakdown(age_raw, customer_id, target_date, "ageRangeNm")
    age_saved = _replace_campaign_age_rows(engine, customer_id, target_date, age_rows, scoped_ids=age_ids)

    return {
        "hour_ids": len(hour_ids),
        "hour_rows_saved": int(hour_saved),
        "hour_raw_rows": int(hour_meta.get("raw_rows", 0)),
        "hour_meta": hour_meta,
        "age_ids": len(age_ids),
        "age_rows_saved": int(age_saved),
        "age_raw_rows": int(age_meta.get("raw_rows", 0)),
        "age_meta": age_meta,
        "parser_version": TARGETING_PARSER_VERSION,
    }
