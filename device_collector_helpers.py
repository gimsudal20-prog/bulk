# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from datetime import date
from typing import Any, Dict, List, Tuple

import pandas as pd
import psycopg2.extras
from sqlalchemy import text
from sqlalchemy.engine import Engine


DEVICE_PARSER_VERSION = "pcm_v20260328_final2"

DEVICE_HEADER_CANDIDATES = [
    "pc mobile type", "pc_mobile_type", "pc/mobile type", "pcmobiletype",
    "device", "device_name", "devicename", "platform", "platform type",
    "기기", "디바이스", "노출기기", "노출 기기", "단말기", "플랫폼",
]
AD_HEADER_CANDIDATES = ["광고id", "소재id", "adid"]
CAMPAIGN_HEADER_CANDIDATES = ["캠페인id", "campaignid"]
IMP_HEADER_CANDIDATES = ["노출수", "impressions", "impcnt"]
CLK_HEADER_CANDIDATES = ["클릭수", "clicks", "clkcnt"]
COST_HEADER_CANDIDATES = ["총비용", "cost", "salesamt"]
CONV_HEADER_CANDIDATES = ["전환수", "conversions", "ccnt"]
SALES_HEADER_CANDIDATES = ["전환매출액", "conversionvalue", "sales", "convamt"]
RANK_HEADER_CANDIDATES = ["평균노출순위", "averageposition", "avgrnk"]

DEFAULT_AD_IDX = 5
DEFAULT_CAMP_IDX = 1
DEFAULT_DEVICE_IDX = 7
DEFAULT_IMP_IDX = 8
DEFAULT_CLK_IDX = 9
DEFAULT_COST_IDX = 10
DEFAULT_CONV_IDX = 11
DEFAULT_SALES_IDX = 12
DEFAULT_RANK_IDX = 14


def _normalize_header(v: str) -> str:
    return str(v).lower().replace(" ", "").replace("_", "").replace("-", "").replace('"', '').replace("'", "")


def _get_col_idx(headers: List[str], candidates: List[str]) -> int:
    norm_headers = [_normalize_header(h) for h in headers]
    norm_candidates = [_normalize_header(c) for c in candidates]
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c == h:
                return i
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c and c in h:
                return i
    return -1


def _safe_float(v) -> float:
    if pd.isna(v):
        return 0.0
    s = str(v).replace(",", "").strip()
    if not s or s == "-":
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def normalize_device_name(v: Any) -> str:
    raw = str(v or "").strip()
    if not raw or raw == "-":
        return ""
    raw_upper = raw.upper().strip()
    norm = _normalize_header(raw)

    if raw_upper in {"P", "PC"} or norm in {"p", "pc", "desktop"} or "desktop" in norm:
        return "PC"
    if raw_upper in {"M", "MO", "MOBILE"} or norm in {"m", "mo", "mobile"}:
        return "MOBILE"
    if any(k in raw for k in ["모바일", "휴대폰"]) or any(k in norm for k in ["mobile", "phone", "app", "mobileweb"]):
        return "MOBILE"
    if "pc" in norm:
        return "PC"
    return ""


def _looks_like_ad_id(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s.startswith("nad-")


def _looks_like_campaign_id(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s.startswith("cmp-")


def _looks_like_metric_value(v: Any) -> bool:
    s = str(v or "").replace(",", "").strip()
    if not s or s == "-":
        return False
    try:
        float(s)
        return True
    except Exception:
        return False


def _nonempty_headers(row: pd.Series) -> List[str]:
    return [str(x) for x in row.fillna("").tolist() if str(x).strip()]


def _score_header_row(row_vals: List[str]) -> int:
    pk_score = 2 if any(x in row_vals for x in [_normalize_header(x) for x in AD_HEADER_CANDIDATES]) else 0
    device_score = 2 if any(x in row_vals for x in [_normalize_header(x) for x in DEVICE_HEADER_CANDIDATES]) else 0
    metric_hits = sum(1 for x in [_normalize_header(x) for x in IMP_HEADER_CANDIDATES + CLK_HEADER_CANDIDATES + COST_HEADER_CANDIDATES] if x in row_vals)
    metric_score = min(metric_hits, 3)
    return pk_score + device_score + metric_score


def _detect_header_idx(df: pd.DataFrame) -> int:
    best_idx = -1
    best_score = -1
    scan_limit = min(60, len(df))
    for i in range(scan_limit):
        row_vals = [_normalize_header(str(x)) for x in df.iloc[i].fillna("")]
        score = _score_header_row(row_vals)
        if score > best_score:
            best_score = score
            best_idx = i
        if score >= 4:
            return i
    return best_idx if best_score >= 2 else -1


def _infer_value_based_indices(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {}
    sample = df.head(min(120, len(df))).copy()
    ncols = len(sample.columns)
    ad_idx = -1
    ad_hits = -1
    device_idx = -1
    device_hits = -1
    camp_idx = -1
    camp_hits = -1

    for idx in range(ncols):
        col = sample.iloc[:, idx].fillna("")
        a_hits = int(sum(1 for v in col if _looks_like_ad_id(v)))
        d_hits = int(sum(1 for v in col if normalize_device_name(v)))
        c_hits = int(sum(1 for v in col if _looks_like_campaign_id(v)))
        if a_hits > ad_hits:
            ad_hits, ad_idx = a_hits, idx
        if d_hits > device_hits:
            device_hits, device_idx = d_hits, idx
        if c_hits > camp_hits:
            camp_hits, camp_idx = c_hits, idx

    metrics = {
        "imp_idx": DEFAULT_IMP_IDX if ncols > DEFAULT_IMP_IDX else -1,
        "clk_idx": DEFAULT_CLK_IDX if ncols > DEFAULT_CLK_IDX else -1,
        "cost_idx": DEFAULT_COST_IDX if ncols > DEFAULT_COST_IDX else -1,
        "conv_idx": DEFAULT_CONV_IDX if ncols > DEFAULT_CONV_IDX else -1,
        "sales_idx": DEFAULT_SALES_IDX if ncols > DEFAULT_SALES_IDX else -1,
        "rank_idx": DEFAULT_RANK_IDX if ncols > DEFAULT_RANK_IDX else -1,
    }

    if ad_hits <= 0:
        ad_idx = DEFAULT_AD_IDX if ncols > DEFAULT_AD_IDX else -1
    if device_hits <= 0:
        device_idx = DEFAULT_DEVICE_IDX if ncols > DEFAULT_DEVICE_IDX else -1
    if camp_hits <= 0:
        camp_idx = DEFAULT_CAMP_IDX if ncols > DEFAULT_CAMP_IDX else -1

    return {
        "ad_idx": ad_idx,
        "device_idx": device_idx,
        "camp_idx": camp_idx,
        "ad_hits": ad_hits,
        "device_hits": device_hits,
        "camp_hits": camp_hits,
        **metrics,
    }


def _ensure_column(engine: Engine, table: str, column: str, datatype: str):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {datatype}"))
    except Exception:
        pass


def ensure_device_tables(engine: Engine):
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS fact_campaign_device_daily (
                dt DATE,
                customer_id TEXT,
                campaign_id TEXT,
                device_name TEXT,
                imp BIGINT,
                clk BIGINT,
                cost BIGINT,
                conv DOUBLE PRECISION,
                sales BIGINT DEFAULT 0,
                roas DOUBLE PRECISION DEFAULT 0,
                avg_rnk DOUBLE PRECISION DEFAULT 0,
                data_source TEXT,
                source_report TEXT,
                PRIMARY KEY(dt, customer_id, campaign_id, device_name)
            )
            """
        ))
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS fact_ad_device_daily (
                dt DATE,
                customer_id TEXT,
                ad_id TEXT,
                device_name TEXT,
                imp BIGINT,
                clk BIGINT,
                cost BIGINT,
                conv DOUBLE PRECISION,
                sales BIGINT DEFAULT 0,
                roas DOUBLE PRECISION DEFAULT 0,
                avg_rnk DOUBLE PRECISION DEFAULT 0,
                data_source TEXT,
                source_report TEXT,
                PRIMARY KEY(dt, customer_id, ad_id, device_name)
            )
            """
        ))

    for table in ["fact_campaign_device_daily", "fact_ad_device_daily"]:
        _ensure_column(engine, table, "roas", "DOUBLE PRECISION DEFAULT 0")
        _ensure_column(engine, table, "avg_rnk", "DOUBLE PRECISION DEFAULT 0")
        _ensure_column(engine, table, "data_source", "TEXT")
        _ensure_column(engine, table, "source_report", "TEXT")


def build_ad_to_campaign_map(engine: Engine, customer_id: str) -> Dict[str, str]:
    sql = """
    SELECT da.ad_id::text AS ad_id,
           COALESCE(dag.campaign_id::text, '') AS campaign_id
    FROM dim_ad da
    LEFT JOIN dim_adgroup dag
      ON da.customer_id::text = dag.customer_id::text
     AND da.adgroup_id::text = dag.adgroup_id::text
    WHERE da.customer_id::text = :cid
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {"cid": str(customer_id)}).fetchall()
        return {str(r[0]).strip(): str(r[1]).strip() for r in rows if str(r[0]).strip()}
    except Exception:
        return {}


def _fetch_existing_device_scope_pairs(engine: Engine, table: str, customer_id: str, d1: date, pk_name: str, ids: List[str]) -> List[Tuple[str, str]]:
    ids = [str(x).strip() for x in (ids or []) if str(x).strip()]
    if not ids:
        return []
    sql = text(
        f"SELECT {pk_name}::text AS scope_id, COALESCE(device_name::text, '') AS device_name "
        f"FROM {table} WHERE customer_id=:cid AND dt=:dt AND {pk_name} = ANY(:ids)"
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"cid": str(customer_id), "dt": d1, "ids": ids}).fetchall()
    seen = set()
    out: List[Tuple[str, str]] = []
    for row in rows or []:
        scope_id = str((row[0] if row else '') or '').strip()
        device_name = str((row[1] if row else '') or '').strip()
        key = (scope_id, device_name)
        if scope_id and key not in seen:
            seen.add(key)
            out.append(key)
    return out


def _clear_stale_device_scope_pairs(engine: Engine, table: str, customer_id: str, d1: date, pk_name: str, pairs: List[Tuple[str, str]]):
    stale_pairs = [(str(pk).strip(), str(device).strip()) for pk, device in (pairs or []) if str(pk).strip() and str(device).strip()]
    if not stale_pairs:
        return
    ids = [pk for pk, _ in stale_pairs]
    devices = [device for _, device in stale_pairs]
    sql = text(
        f"DELETE FROM {table} t USING (SELECT * FROM unnest(:ids, :devices)) AS s(scope_id, device_name) "
        f"WHERE t.customer_id=:cid AND t.dt=:dt AND t.{pk_name}::text = s.scope_id::text AND COALESCE(t.device_name::text, '') = s.device_name::text"
    )
    for _ in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(sql, {"cid": str(customer_id), "dt": d1, "ids": ids, "devices": devices})
            return
        except Exception:
            time.sleep(2)


def replace_device_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1: date, pk_name: str):
    pk_cols = ["dt", "customer_id", pk_name, "device_name"]
    scope_ids = []
    seen = set()
    for row in rows or []:
        value = str((row or {}).get(pk_name) or '').strip()
        if value and value not in seen:
            seen.add(value)
            scope_ids.append(value)

    if not scope_ids:
        if not rows:
            for _ in range(3):
                try:
                    with engine.begin() as conn:
                        conn.execute(text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt=:dt"), {"cid": str(customer_id), "dt": d1})
                    break
                except Exception:
                    time.sleep(2)
            return
    else:
        if not rows:
            for _ in range(3):
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt=:dt AND {pk_name} = ANY(:ids)"),
                            {"cid": str(customer_id), "dt": d1, "ids": scope_ids},
                        )
                    break
                except Exception:
                    time.sleep(2)
            return

        current_pairs = {(str((row or {}).get(pk_name) or '').strip(), str((row or {}).get('device_name') or '').strip()) for row in rows or []}
        current_pairs = {pair for pair in current_pairs if pair[0] and pair[1]}
        existing_pairs = _fetch_existing_device_scope_pairs(engine, table, customer_id, d1, pk_name, scope_ids)
        stale_pairs = [pair for pair in existing_pairs if pair not in current_pairs]
        if stale_pairs:
            _clear_stale_device_scope_pairs(engine, table, customer_id, d1, pk_name, stale_pairs)

    if not rows:
        return

    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep="last").sort_values(by=pk_cols).astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    conflict_clause = (
        f"ON CONFLICT ({pk_str}) DO UPDATE SET " + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
        if update_cols else
        f"ON CONFLICT ({pk_str}) DO NOTHING"
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
            break
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

def parse_ad_device_report(
    df: pd.DataFrame,
    ad_to_campaign: Dict[str, str] | None = None,
) -> Tuple[Dict[Tuple[str, str], dict], Dict[Tuple[str, str], dict], dict]:
    if df is None or df.empty:
        return {}, {}, {"status": "empty", "parser": DEVICE_PARSER_VERSION}

    ad_to_campaign = ad_to_campaign or {}
    raw_df = df.reset_index(drop=True).copy()
    header_idx = _detect_header_idx(raw_df)
    raw_headers: List[str] = []

    if header_idx != -1:
        raw_headers = _nonempty_headers(raw_df.iloc[header_idx])
        headers = [_normalize_header(str(x)) for x in raw_df.iloc[header_idx].fillna("")]
        data_df = raw_df.iloc[header_idx + 1:].reset_index(drop=True)
        ad_idx = _get_col_idx(headers, AD_HEADER_CANDIDATES)
        camp_idx = _get_col_idx(headers, CAMPAIGN_HEADER_CANDIDATES)
        device_idx = _get_col_idx(headers, DEVICE_HEADER_CANDIDATES)
        imp_idx = _get_col_idx(headers, IMP_HEADER_CANDIDATES)
        clk_idx = _get_col_idx(headers, CLK_HEADER_CANDIDATES)
        cost_idx = _get_col_idx(headers, COST_HEADER_CANDIDATES)
        conv_idx = _get_col_idx(headers, CONV_HEADER_CANDIDATES)
        sales_idx = _get_col_idx(headers, SALES_HEADER_CANDIDATES)
        rank_idx = _get_col_idx(headers, RANK_HEADER_CANDIDATES)
    else:
        data_df = raw_df
        ad_idx = camp_idx = device_idx = imp_idx = clk_idx = cost_idx = conv_idx = sales_idx = rank_idx = -1

    inferred = _infer_value_based_indices(data_df if not data_df.empty else raw_df)
    if ad_idx == -1:
        ad_idx = inferred.get("ad_idx", -1)
    if device_idx == -1:
        device_idx = inferred.get("device_idx", -1)
    if camp_idx == -1:
        camp_idx = inferred.get("camp_idx", -1)
    if imp_idx == -1:
        imp_idx = inferred.get("imp_idx", -1)
    if clk_idx == -1:
        clk_idx = inferred.get("clk_idx", -1)
    if cost_idx == -1:
        cost_idx = inferred.get("cost_idx", -1)
    if conv_idx == -1:
        conv_idx = inferred.get("conv_idx", -1)
    if sales_idx == -1:
        sales_idx = inferred.get("sales_idx", -1)
    if rank_idx == -1:
        rank_idx = inferred.get("rank_idx", -1)

    if ad_idx == -1 or device_idx == -1:
        return {}, {}, {
            "status": "missing_required_columns" if header_idx != -1 else "no_header",
            "parser": DEVICE_PARSER_VERSION,
            "header_idx": header_idx,
            "ad_idx": ad_idx,
            "device_idx": device_idx,
            "camp_idx": camp_idx,
            "sample_headers": raw_headers[:16],
            "infer_ad_hits": inferred.get("ad_hits"),
            "infer_device_hits": inferred.get("device_hits"),
        }

    ad_stats: Dict[Tuple[str, str], dict] = {}
    scan_rows = 0
    reject_short = 0
    reject_empty_ad = 0
    reject_empty_device = 0
    reject_zero_metrics = 0
    preview_rows = []
    max_idx = max([x for x in [ad_idx, device_idx, camp_idx, imp_idx, clk_idx, cost_idx, conv_idx, sales_idx, rank_idx] if x != -1], default=0)

    for row_no, (_, row) in enumerate(data_df.iterrows(), start=1):
        scan_rows += 1
        if len(row) <= max_idx:
            reject_short += 1
            continue
        raw_ad_id = row.iloc[ad_idx] if ad_idx != -1 and len(row) > ad_idx else ""
        raw_device = row.iloc[device_idx] if device_idx != -1 and len(row) > device_idx else ""
        ad_id = str(raw_ad_id).strip()
        if not _looks_like_ad_id(ad_id):
            reject_empty_ad += 1
            if len(preview_rows) < 3 and (str(raw_ad_id).strip() or str(raw_device).strip()):
                preview_rows.append({
                    "row_no": row_no,
                    "raw_ad": str(raw_ad_id),
                    "raw_device": str(raw_device),
                    "reason": "empty_ad",
                })
            continue
        device_name = normalize_device_name(raw_device)
        if not device_name:
            reject_empty_device += 1
            if len(preview_rows) < 3:
                preview_rows.append({
                    "row_no": row_no,
                    "raw_ad": str(raw_ad_id),
                    "raw_device": str(raw_device),
                    "reason": "empty_device",
                })
            continue

        imp = int(_safe_float(row.iloc[imp_idx])) if imp_idx != -1 and len(row) > imp_idx else 0
        clk = int(_safe_float(row.iloc[clk_idx])) if clk_idx != -1 and len(row) > clk_idx else 0
        cost = int(_safe_float(row.iloc[cost_idx])) if cost_idx != -1 and len(row) > cost_idx else 0
        conv = _safe_float(row.iloc[conv_idx]) if conv_idx != -1 and len(row) > conv_idx else 0.0
        sales = int(_safe_float(row.iloc[sales_idx])) if sales_idx != -1 and len(row) > sales_idx else 0
        if imp == 0 and clk == 0 and cost == 0 and conv == 0 and sales == 0:
            reject_zero_metrics += 1
            if len(preview_rows) < 3:
                preview_rows.append({
                    "row_no": row_no,
                    "raw_ad": str(raw_ad_id),
                    "raw_device": str(raw_device),
                    "device_name": device_name,
                    "reason": "zero_metrics",
                })
            continue

        key = (ad_id, device_name)
        bucket = ad_stats.setdefault(key, {
            "campaign_id": "",
            "imp": 0,
            "clk": 0,
            "cost": 0,
            "conv": 0.0,
            "sales": 0,
            "rank_sum": 0.0,
            "rank_cnt": 0,
        })
        campaign_id = str(row.iloc[camp_idx]).strip() if camp_idx != -1 and len(row) > camp_idx else ""
        bucket["campaign_id"] = bucket.get("campaign_id") or campaign_id or ad_to_campaign.get(ad_id, "")
        bucket["imp"] += imp
        bucket["clk"] += clk
        bucket["cost"] += cost
        bucket["conv"] += conv
        bucket["sales"] += sales

        if rank_idx != -1 and len(row) > rank_idx:
            rnk = _safe_float(row.iloc[rank_idx])
            if rnk > 0 and imp > 0:
                bucket["rank_sum"] += (rnk * imp)
                bucket["rank_cnt"] += imp

    if not ad_stats:
        return {}, {}, {
            "status": "no_rows",
            "parser": DEVICE_PARSER_VERSION,
            "header_idx": header_idx,
            "sample_headers": raw_headers[:16],
            "ad_idx": ad_idx,
            "camp_idx": camp_idx,
            "device_idx": device_idx,
            "imp_idx": imp_idx,
            "clk_idx": clk_idx,
            "cost_idx": cost_idx,
            "conv_idx": conv_idx,
            "sales_idx": sales_idx,
            "rank_idx": rank_idx,
            "scan_rows": scan_rows,
            "reject_short": reject_short,
            "reject_empty_ad": reject_empty_ad,
            "reject_empty_device": reject_empty_device,
            "reject_zero_metrics": reject_zero_metrics,
            "preview_rows": preview_rows,
            "infer_ad_hits": inferred.get("ad_hits"),
            "infer_device_hits": inferred.get("device_hits"),
            "infer_camp_hits": inferred.get("camp_hits"),
        }

    campaign_stats: Dict[Tuple[str, str], dict] = {}
    missing_campaign_count = 0
    for (ad_id, device_name), bucket in ad_stats.items():
        campaign_id = str(bucket.get("campaign_id") or ad_to_campaign.get(ad_id, "")).strip()
        if not campaign_id:
            missing_campaign_count += 1
            continue
        key = (campaign_id, device_name)
        cb = campaign_stats.setdefault(key, {
            "imp": 0,
            "clk": 0,
            "cost": 0,
            "conv": 0.0,
            "sales": 0,
            "rank_sum": 0.0,
            "rank_cnt": 0,
        })
        cb["imp"] += int(bucket.get("imp", 0) or 0)
        cb["clk"] += int(bucket.get("clk", 0) or 0)
        cb["cost"] += int(bucket.get("cost", 0) or 0)
        cb["conv"] += float(bucket.get("conv", 0.0) or 0.0)
        cb["sales"] += int(bucket.get("sales", 0) or 0)
        cb["rank_sum"] += float(bucket.get("rank_sum", 0.0) or 0.0)
        cb["rank_cnt"] += int(bucket.get("rank_cnt", 0) or 0)

    return ad_stats, campaign_stats, {
        "status": "ok",
        "parser": DEVICE_PARSER_VERSION,
        "ad_rows": len(ad_stats),
        "campaign_rows": len(campaign_stats),
        "missing_campaign_rows": missing_campaign_count,
        "header_idx": header_idx,
    }


def save_device_stats(
    engine: Engine,
    customer_id: str,
    target_date: date,
    table_name: str,
    pk_name: str,
    stat_res: Dict[Tuple[str, str], dict],
    data_source: str = "report_device_total_only",
    source_report: str = "AD",
) -> int:
    if not stat_res:
        return 0

    rows = []
    for (entity_id, device_name), s in stat_res.items():
        if not entity_id or not device_name:
            continue
        cost = int(s.get("cost", 0) or 0)
        sales = int(s.get("sales", 0) or 0)
        roas = (sales / cost * 100.0) if cost > 0 else 0.0
        avg_rnk = (float(s.get("rank_sum", 0.0) or 0.0) / int(s.get("rank_cnt", 1) or 1)) if int(s.get("rank_cnt", 0) or 0) > 0 else 0.0
        rows.append({
            "dt": target_date,
            "customer_id": str(customer_id),
            pk_name: str(entity_id),
            "device_name": str(device_name),
            "imp": int(s.get("imp", 0) or 0),
            "clk": int(s.get("clk", 0) or 0),
            "cost": cost,
            "conv": float(s.get("conv", 0.0) or 0.0),
            "sales": sales,
            "roas": roas,
            "avg_rnk": round(avg_rnk, 2),
            "data_source": data_source,
            "source_report": source_report,
        })

    replace_device_fact_range(engine, table_name, rows, customer_id, target_date, pk_name)
    return len(rows)


def summarize_stat_res(stat_res: Dict[Tuple[str, str], dict]) -> dict:
    out = {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0}
    for s in (stat_res or {}).values():
        out["imp"] += int(s.get("imp", 0) or 0)
        out["clk"] += int(s.get("clk", 0) or 0)
        out["cost"] += int(s.get("cost", 0) or 0)
        out["conv"] += float(s.get("conv", 0.0) or 0.0)
        out["sales"] += int(s.get("sales", 0) or 0)
    return out
