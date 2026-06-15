# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import pandas as pd
import psycopg2.extras
from sqlalchemy import text

import collector_db
from placement_collector_helpers import (
    ADGROUP_HEADER_CANDIDATES,
    CAMPAIGN_HEADER_CANDIDATES,
    CAMPAIGN_TYPE_HEADER_CANDIDATES,
    CLK_HEADER_CANDIDATES,
    COST_HEADER_CANDIDATES,
    IMP_HEADER_CANDIDATES,
    PLACEMENT_HEADER_CANDIDATES,
    PURCHASE_CONV_HEADER_CANDIDATES,
    PURCHASE_SALES_HEADER_CANDIDATES,
    TOTAL_CONV_HEADER_CANDIDATES,
    TOTAL_SALES_HEADER_CANDIDATES,
    build_adgroup_name_lookup,
    build_placement_rows_from_report,
    ensure_placement_tables,
    _detect_report_header_idx,
    _get_col_idx,
    _normalize_name,
)


METRIC_FIELDS = ["imp", "clk", "cost", "conv", "sales", "purchase_conv", "purchase_sales"]


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def die(message: str) -> None:
    log(f"FATAL: {message}")
    sys.exit(1)


def _parse_date(value: str, label: str) -> date:
    try:
        return datetime.strptime(str(value or "").strip(), "%Y-%m-%d").date()
    except Exception as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD: {value}") from exc


def _read_report(path: Path) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, encoding=encoding, header=None, dtype=str, keep_default_na=False)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(
        path,
        encoding="utf-8-sig",
        encoding_errors="replace",
        header=None,
        dtype=str,
        keep_default_na=False,
    )


def _read_first_line(path: Path) -> str:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        try:
            first = next(csv.reader(fh))
        except StopIteration:
            return ""
    return str(first[0] if first else "").strip()


def _parse_report_title(path: Path) -> Dict[str, str]:
    title = _read_first_line(path)
    match = re.search(
        r"DA_RAW_SSA\((\d{4})\.(\d{2})\.(\d{2})\.~(\d{4})\.(\d{2})\.(\d{2})\.\),\s*([0-9]+)",
        title,
    )
    if not match:
        return {"title": title}
    y1, m1, d1, y2, m2, d2, raw_customer_id = match.groups()
    return {
        "title": title,
        "start": f"{y1}-{m1}-{d1}",
        "end": f"{y2}-{m2}-{d2}",
        "raw_customer_id": raw_customer_id,
    }


def _safe_float(value: Any) -> float:
    text_value = str(value or "").replace(",", "").replace("%", "").strip()
    if not text_value:
        return 0.0
    try:
        return float(text_value)
    except Exception:
        return 0.0


def _raw_totals(df: pd.DataFrame) -> Tuple[Dict[str, float], int]:
    header_idx = _detect_report_header_idx(df)
    if header_idx == -1:
        die("CSV header row was not found.")
    headers = [str(x or "") for x in df.iloc[header_idx].fillna("").tolist()]
    data_df = df.iloc[header_idx + 1:].reset_index(drop=True)
    idx = {
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
    }
    required = ["campaign", "adgroup", "placement", "imp", "clk", "cost"]
    missing = [name for name in required if idx[name] == -1]
    if missing:
        die(f"CSV required columns missing: {missing}")

    totals = {field: 0.0 for field in METRIC_FIELDS}
    keys = set()
    for _, row in data_df.iterrows():
        campaign = _normalize_name(row.iloc[idx["campaign"]])
        adgroup = _normalize_name(row.iloc[idx["adgroup"]])
        placement = str(row.iloc[idx["placement"]] or "").strip()
        if campaign and adgroup and placement:
            keys.add((campaign, adgroup, placement))
        for field in METRIC_FIELDS:
            if idx.get(field, -1) != -1:
                totals[field] += _safe_float(row.iloc[idx[field]])
    return totals, len(keys)


def _stable_id(prefix: str, *parts: Any) -> str:
    raw = "\x1f".join(str(part or "") for part in parts)
    return f"{prefix}_{hashlib.sha1(raw.encode('utf-8')).hexdigest()[:20]}"


def _augment_lookup_with_csv_dims(engine, customer_id: str, df: pd.DataFrame, lookup: Dict[Tuple[str, str], Dict[str, str] | None]) -> Tuple[Dict[Tuple[str, str], Dict[str, str] | None], int, int]:
    header_idx = _detect_report_header_idx(df)
    if header_idx == -1:
        die("CSV header row was not found.")
    headers = [str(x or "") for x in df.iloc[header_idx].fillna("").tolist()]
    data_df = df.iloc[header_idx + 1:].reset_index(drop=True)
    idx = {
        "campaign_type": _get_col_idx(headers, CAMPAIGN_TYPE_HEADER_CANDIDATES),
        "campaign": _get_col_idx(headers, CAMPAIGN_HEADER_CANDIDATES),
        "adgroup": _get_col_idx(headers, ADGROUP_HEADER_CANDIDATES),
    }
    if idx["campaign"] == -1 or idx["adgroup"] == -1:
        die("CSV campaign/adgroup columns missing.")

    out = dict(lookup or {})
    campaign_rows_by_id: Dict[str, Dict[str, Any]] = {}
    adgroup_rows_by_id: Dict[str, Dict[str, Any]] = {}
    for _, row in data_df.iterrows():
        campaign_name_raw = str(row.iloc[idx["campaign"]] or "").strip()
        adgroup_name_raw = str(row.iloc[idx["adgroup"]] or "").strip()
        campaign_name = _normalize_name(campaign_name_raw)
        adgroup_name = _normalize_name(adgroup_name_raw)
        if not campaign_name or not adgroup_name:
            continue
        key = (campaign_name, adgroup_name)
        if out.get(key):
            continue

        campaign_type = str(row.iloc[idx["campaign_type"]] if idx["campaign_type"] != -1 else "").strip()
        campaign_id = _stable_id("csvcmp", customer_id, campaign_name)
        adgroup_id = _stable_id("csvag", customer_id, campaign_name, adgroup_name)
        out[key] = {
            "campaign_id": campaign_id,
            "adgroup_id": adgroup_id,
            "campaign_type": campaign_type,
        }
        campaign_rows_by_id[campaign_id] = {
            "customer_id": str(customer_id),
            "campaign_id": campaign_id,
            "campaign_name": campaign_name_raw,
            "campaign_tp": campaign_type,
            "status": "CSV_IMPORT",
        }
        adgroup_rows_by_id[adgroup_id] = {
            "customer_id": str(customer_id),
            "adgroup_id": adgroup_id,
            "adgroup_name": adgroup_name_raw,
            "campaign_id": campaign_id,
            "status": "CSV_IMPORT",
        }

    campaign_rows = list(campaign_rows_by_id.values())
    adgroup_rows = list(adgroup_rows_by_id.values())
    if campaign_rows:
        collector_db.upsert_many(engine, "dim_campaign", campaign_rows, ["customer_id", "campaign_id"])
    if adgroup_rows:
        collector_db.upsert_many(engine, "dim_adgroup", adgroup_rows, ["customer_id", "adgroup_id"])
    return out, len(campaign_rows), len(adgroup_rows)


def _summarize_rows(rows: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    totals = {field: 0.0 for field in METRIC_FIELDS}
    count = 0
    for row in rows:
        count += 1
        for field in METRIC_FIELDS:
            totals[field] += float(row.get(field) or 0.0)
    totals["rows"] = float(count)
    return totals


def _db_totals(engine, customer_id: str, start: date, end: date) -> Dict[str, float]:
    sql = text(
        """
        SELECT
            COUNT(*) AS rows,
            COALESCE(SUM(imp), 0) AS imp,
            COALESCE(SUM(clk), 0) AS clk,
            COALESCE(SUM(cost), 0) AS cost,
            COALESCE(SUM(conv), 0) AS conv,
            COALESCE(SUM(sales), 0) AS sales,
            COALESCE(SUM(purchase_conv), 0) AS purchase_conv,
            COALESCE(SUM(purchase_sales), 0) AS purchase_sales
        FROM fact_adgroup_placement_daily
        WHERE customer_id = :customer_id
          AND dt BETWEEN :start AND :end
          AND UPPER(COALESCE(NULLIF(TRIM(source_report), ''), 'UNKNOWN')) = 'DA_RAW_SSA'
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"customer_id": customer_id, "start": start, "end": end}).mappings().first()
    return {field: float((row or {}).get(field) or 0.0) for field in ["rows", *METRIC_FIELDS]}


def _diffs(expected: Dict[str, float], actual: Dict[str, float]) -> list[str]:
    out = []
    for field in METRIC_FIELDS:
        exp = round(float(expected.get(field) or 0.0), 6)
        got = round(float(actual.get(field) or 0.0), 6)
        if abs(exp - got) > 0.000001:
            out.append(f"{field}: raw={exp} db={got}")
    return out


def _replace_period(engine, rows: list[Dict[str, Any]], customer_id: str, start: date, end: date) -> Tuple[int, int]:
    if not rows:
        die("No parsed rows to import.")
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
    insert_sql = f"INSERT INTO fact_adgroup_placement_daily ({col_names}) VALUES %s {conflict_clause}"
    tuples = list(df.itertuples(index=False, name=None))

    last_exc: Exception | None = None
    for _ in range(3):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            cur.execute(
                """
                DELETE FROM fact_adgroup_placement_daily
                WHERE customer_id=%s AND dt BETWEEN %s AND %s
                """,
                (str(customer_id), start, end),
            )
            deleted = cur.rowcount
            psycopg2.extras.execute_values(cur, insert_sql, tuples, page_size=1000)
            raw_conn.commit()
            return int(deleted or 0), len(df)
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
    raise RuntimeError(f"CSV import failed: {type(last_exc).__name__}: {last_exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import a downloaded DA_RAW_SSA placement CSV into dashboard placement facts.")
    parser.add_argument("--csv", required=True, help="Path to DA_RAW_SSA CSV")
    parser.add_argument("--customer_id", required=True, help="Dashboard/DB customer_id to replace")
    parser.add_argument("--start", default="", help="Report start date. Defaults to CSV title.")
    parser.add_argument("--end", default="", help="Report end date. Defaults to CSV title.")
    parser.add_argument("--dt", default="", help="Storage date for aggregate rows. Defaults to report end date.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        die(f"CSV not found: {csv_path}")
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        die("DATABASE_URL is required.")

    title_meta = _parse_report_title(csv_path)
    start = _parse_date(args.start or title_meta.get("start", ""), "start")
    end = _parse_date(args.end or title_meta.get("end", ""), "end")
    storage_dt = _parse_date(args.dt or str(end), "dt")
    customer_id = str(args.customer_id).strip()
    raw_customer_id = str(title_meta.get("raw_customer_id") or "").strip()

    report_df = _read_report(csv_path)
    raw_totals, raw_group_rows = _raw_totals(report_df)

    engine = collector_db.get_engine(db_url)
    ensure_placement_tables(engine)
    lookup = build_adgroup_name_lookup(engine, customer_id)
    if not lookup:
        die(f"No dim_adgroup mapping for customer_id={customer_id}")
    lookup, synthetic_campaigns, synthetic_adgroups = _augment_lookup_with_csv_dims(engine, customer_id, report_df, lookup)
    if synthetic_campaigns or synthetic_adgroups:
        log(f"CSV dim backfill: campaigns={synthetic_campaigns} adgroups={synthetic_adgroups}")

    rows, meta = build_placement_rows_from_report(
        report_df,
        customer_id=customer_id,
        target_date=storage_dt,
        source_report="DA_RAW_SSA",
        adgroup_name_lookup=lookup,
    )
    parsed_totals = _summarize_rows(rows)
    parse_diffs = _diffs(raw_totals, parsed_totals)
    if parse_diffs:
        die("Parsed rows do not match raw totals: " + "; ".join(parse_diffs))
    if int(parsed_totals.get("rows") or 0) != raw_group_rows:
        die(f"Parsed row count mismatch: raw_group_rows={raw_group_rows} parsed_rows={int(parsed_totals.get('rows') or 0)} meta={meta}")

    deleted, inserted = _replace_period(engine, rows, customer_id, start, end)
    db_totals = _db_totals(engine, customer_id, start, end)
    db_diffs = _diffs(raw_totals, db_totals)
    if db_diffs:
        die("Imported DB totals do not match raw totals: " + "; ".join(db_diffs))
    if int(db_totals.get("rows") or 0) != raw_group_rows:
        die(f"Imported row count mismatch: raw_group_rows={raw_group_rows} db_rows={int(db_totals.get('rows') or 0)}")

    log(
        "CSV placement import verified "
        f"| raw_customer_id={raw_customer_id or '-'} target_customer_id={customer_id} "
        f"| range={start}~{end} storage_dt={storage_dt} "
        f"| raw_rows={raw_group_rows} inserted={inserted} deleted={deleted} "
        f"| synthetic_campaigns={synthetic_campaigns} synthetic_adgroups={synthetic_adgroups} "
        f"| imp={int(raw_totals['imp'])} clk={int(raw_totals['clk'])} "
        f"| cost={int(raw_totals['cost'])} sales={int(raw_totals['sales'])} "
        f"| purchase_sales={int(raw_totals['purchase_sales'])}"
    )


if __name__ == "__main__":
    main()
