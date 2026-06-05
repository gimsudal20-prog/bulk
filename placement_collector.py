# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import base64
import concurrent.futures
import csv
import hashlib
import hmac
import io
import json
import os
import random
import sys
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import text

import collector_api
import collector_db
from placement_collector_helpers import (
    build_ad_id_lookup,
    build_adgroup_id_lookup,
    build_adgroup_name_lookup,
    build_placement_rows_from_existing_facts,
    build_placement_rows_from_report,
    ensure_placement_tables,
    build_placement_rows_from_stats,
    fetch_stats_placement_breakdown_rows,
    read_adgroup_purchase_split_lookup,
    replace_placement_fact_range,
)

try:
    from account_master import load_naver_accounts
except Exception:
    load_naver_accounts = None


load_dotenv(override=False)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60
DEBUG_DIR = Path(os.getenv("DEBUG_REPORT_DIR", "debug_reports"))


def resolve_placement_report_types() -> List[str]:
    raw = (os.getenv("PLACEMENT_REPORT_TYPES") or "AD").strip()
    out = []
    for item in raw.replace(";", ",").split(","):
        report_type = item.strip().upper()
        if report_type and report_type not in out:
            out.append(report_type)
    return out or ["AD"]


PLACEMENT_REPORT_TYPES = resolve_placement_report_types()

thread_local = threading.local()


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def die(message: str) -> None:
    log(f"FATAL: {message}")
    sys.exit(1)


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def now_millis() -> str:
    return str(int(time.time() * 1000))


def sign_path_only(method: str, path: str, timestamp: str, secret: str) -> str:
    msg = f"{timestamp}.{method}.{path}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def make_headers(method: str, path: str, customer_id: str) -> Dict[str, str]:
    ts = now_millis()
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": sign_path_only(method.upper(), path, ts, API_SECRET),
    }


def request_json(method: str, path: str, customer_id: str, params: dict | None = None, json_data: dict | None = None, raise_error: bool = True) -> Tuple[int, Any]:
    url = BASE_URL + path
    session = get_session()
    for attempt in range(8):
        try:
            response = session.request(
                method,
                url,
                headers=make_headers(method, path, customer_id),
                params=params,
                json=json_data,
                timeout=TIMEOUT,
            )
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(2 + attempt + random.uniform(0.1, 1.5))
                continue
            try:
                data = response.json()
            except ValueError:
                data = response.text
            if raise_error and response.status_code >= 400:
                raise requests.HTTPError(f"{response.status_code} Error: {data}", response=response)
            return response.status_code, data
        except requests.exceptions.RequestException:
            time.sleep(2 + attempt)
    if raise_error:
        raise RuntimeError(f"최대 재시도 초과: {url}")
    return 0, None


def safe_call(method: str, path: str, customer_id: str, params: dict | None = None) -> Tuple[bool, Any]:
    try:
        _, data = request_json(method, path, customer_id, params=params, raise_error=True)
        return True, data
    except Exception as exc:
        log(f"safe_call 실패: {method} {path} customer_id={customer_id} | {type(exc).__name__}: {exc}")
        return False, None


def save_debug_report(report_type: str, customer_id: str, job_id: str, content: str) -> None:
    if str(os.getenv("DEBUG_REPORTS", "1")).lower() not in {"1", "true", "yes", "y"}:
        return
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        (DEBUG_DIR / f"{ts}_{customer_id}_{report_type}_{job_id}.txt").write_text(content or "", encoding="utf-8")
    except Exception as exc:
        log(f"debug report 저장 실패 무시: {type(exc).__name__}: {exc}")


def save_debug_json(report_type: str, customer_id: str, payload: Dict[str, Any]) -> None:
    if str(os.getenv("DEBUG_REPORTS", "1")).lower() not in {"1", "true", "yes", "y"}:
        return
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = DEBUG_DIR / f"{ts}_{customer_id}_{report_type}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
        log(f"debug json 저장: {path}")
    except Exception as exc:
        log(f"debug json 저장 실패 무시: {type(exc).__name__}: {exc}")


def parse_variable_width_report_text(txt: str) -> pd.DataFrame:
    txt = (txt or "").strip()
    if not txt:
        return pd.DataFrame()
    delimiter = "\t" if "\t" in txt else ","
    rows = list(csv.reader(io.StringIO(txt), delimiter=delimiter))
    if not rows:
        return pd.DataFrame()
    width = max(len(row) for row in rows)
    normalized = [row + [""] * (width - len(row)) for row in rows]
    return pd.DataFrame(normalized, dtype=str)


def download_report_dataframe(customer_id: str, report_type: str, job_id: str, initial_url: str) -> pd.DataFrame | None:
    return collector_api.download_report_dataframe(
        customer_id,
        report_type,
        job_id,
        initial_url,
        get_session=get_session,
        base_url=BASE_URL,
        make_headers=make_headers,
        request_json=request_json,
        save_debug_report=save_debug_report,
        parse_report_text_to_df_fn=parse_variable_width_report_text,
        log_fn=log,
    )


def cleanup_ghost_reports(customer_id: str) -> None:
    return collector_api.cleanup_ghost_reports(customer_id, request_json, safe_call)


def fetch_placement_report(customer_id: str, target_date: date, report_type: str) -> pd.DataFrame | None:
    dfs = collector_api.fetch_multiple_stat_reports(
        customer_id,
        [report_type],
        target_date,
        cleanup_ghost_reports_fn=cleanup_ghost_reports,
        request_json=request_json,
        download_report_dataframe_fn=download_report_dataframe,
        safe_call=safe_call,
        fast_mode=False,
        log_fn=log,
    )
    return dfs.get(report_type)


def placement_type_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows or []:
        key = str(row.get("placement_type") or "UNKNOWN").strip() or "UNKNOWN"
        counts[key] = counts.get(key, 0) + 1
    return counts


def fetch_placement_rows_from_sources(
    engine,
    customer_id: str,
    target_date: date,
    *,
    allowed_campaign_ids: set[str] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    adgroup_lookup = build_adgroup_id_lookup(engine, customer_id)
    ad_id_lookup = build_ad_id_lookup(engine, customer_id)
    adgroup_name_lookup = build_adgroup_name_lookup(engine, customer_id)
    purchase_lookup = read_adgroup_purchase_split_lookup(engine, customer_id, target_date)
    report_attempts: List[Dict[str, Any]] = []
    best_rows: List[Dict[str, Any]] = []
    best_meta: Dict[str, Any] = {}

    for report_type in PLACEMENT_REPORT_TYPES:
        df = fetch_placement_report(customer_id, target_date, report_type)
        report_rows, report_meta = build_placement_rows_from_report(
            df,
            customer_id=customer_id,
            target_date=target_date,
            source_report=report_type,
            ad_id_lookup=ad_id_lookup,
            adgroup_lookup=adgroup_lookup,
            adgroup_name_lookup=adgroup_name_lookup,
            purchase_split_lookup=purchase_lookup,
            allowed_campaign_ids=allowed_campaign_ids,
        )
        report_attempts.append(report_meta)
        counts = placement_type_counts(report_rows)
        log(f"   [{report_type}] 지면 리포트 파싱: status={report_meta.get('status')} rows={len(report_rows)} placements={counts}")
        if report_rows and int(counts.get("CONTENT", 0) or 0) > 0:
            return report_rows, {
                "status": "ok",
                "source": f"report_{report_type}",
                "report": report_meta,
                "report_attempts": report_attempts,
                "placement_type_counts": counts,
            }
        if report_rows and not best_rows:
            best_rows = report_rows
            best_meta = {
                "status": "no_content_rows",
                "source": f"report_{report_type}",
                "report": report_meta,
                "report_attempts": list(report_attempts),
                "placement_type_counts": counts,
            }

    adgroup_ids = sorted(adgroup_lookup.keys())
    if allowed_campaign_ids:
        allowed = {str(x).strip() for x in allowed_campaign_ids if str(x).strip()}
        adgroup_ids = [
            gid for gid in adgroup_ids
            if str((adgroup_lookup.get(gid) or {}).get("campaign_id") or "").strip() in allowed
        ]
    raw_rows, fetch_meta = fetch_stats_placement_breakdown_rows(
        customer_id,
        adgroup_ids,
        target_date,
        request_json_fn=request_json,
        log_fn=log,
    )
    stats_rows, parse_meta = build_placement_rows_from_stats(
        raw_rows,
        customer_id=customer_id,
        target_date=target_date,
        adgroup_lookup=adgroup_lookup,
        purchase_split_lookup=purchase_lookup,
        selected_breakdown=str(fetch_meta.get("selected_breakdown") or ""),
        allowed_campaign_ids=allowed_campaign_ids,
    )
    stats_counts = placement_type_counts(stats_rows)
    stats_meta = {
        "status": "ok" if stats_rows and int(stats_counts.get("CONTENT", 0) or 0) > 0 else str(fetch_meta.get("status") or parse_meta.get("status") or "no_rows"),
        "source": "stats_breakdown",
        "fetch": fetch_meta,
        "parse": parse_meta,
        "adgroup_scope": len(adgroup_ids),
        "placement_type_counts": stats_counts,
    }
    if stats_rows and int(stats_counts.get("CONTENT", 0) or 0) > 0:
        stats_meta["report_attempts"] = report_attempts
        return stats_rows, stats_meta
    if stats_rows and not best_rows:
        best_rows = stats_rows
        best_meta = {
            "status": "no_content_rows",
            "source": "stats_breakdown",
            "stats": stats_meta,
            "report_attempts": report_attempts,
            "placement_type_counts": stats_counts,
        }

    fallback_rows, fallback_meta = build_placement_rows_from_existing_facts(
        engine,
        customer_id,
        target_date,
        allowed_campaign_ids=allowed_campaign_ids,
    )
    fallback_counts = placement_type_counts(fallback_rows)
    if fallback_rows and not best_rows:
        best_rows = fallback_rows
        best_meta = {
            "status": "fallback_existing_facts_no_content",
            "source": "existing_facts",
            "fallback": fallback_meta,
            "stats": stats_meta,
            "report_attempts": report_attempts,
            "placement_type_counts": fallback_counts,
        }

    if best_rows:
        best_meta.setdefault("stats", stats_meta)
        best_meta.setdefault("fallback", fallback_meta)
        best_meta.setdefault("report_attempts", report_attempts)
        return best_rows, best_meta

    return [], {
        "status": str(fetch_meta.get("status") or "no_rows"),
        "source": "none",
        "stats": stats_meta,
        "fallback": fallback_meta,
        "report_attempts": report_attempts,
        "placement_type_counts": {},
    }


def list_campaigns(customer_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/campaigns", customer_id)
    return data if ok and isinstance(data, list) else []


def list_adgroups(customer_id: str, campaign_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/adgroups", customer_id, {"nccCampaignId": campaign_id})
    return data if ok and isinstance(data, list) else []


def sync_campaign_adgroup_dims(engine, customer_id: str) -> Tuple[int, int, set[str]]:
    campaign_rows: List[Dict[str, Any]] = []
    adgroup_rows: List[Dict[str, Any]] = []
    campaign_ids: set[str] = set()
    for campaign in list_campaigns(customer_id):
        campaign_id = str(campaign.get("nccCampaignId") or "").strip()
        if not campaign_id:
            continue
        campaign_ids.add(campaign_id)
        campaign_rows.append({
            "customer_id": str(customer_id),
            "campaign_id": campaign_id,
            "campaign_name": str(campaign.get("name") or ""),
            "campaign_tp": str(campaign.get("campaignTp") or ""),
            "status": str(campaign.get("status") or ""),
        })
        for adgroup in list_adgroups(customer_id, campaign_id):
            adgroup_id = str(adgroup.get("nccAdgroupId") or "").strip()
            if not adgroup_id:
                continue
            adgroup_rows.append({
                "customer_id": str(customer_id),
                "adgroup_id": adgroup_id,
                "adgroup_name": str(adgroup.get("name") or ""),
                "campaign_id": campaign_id,
                "status": str(adgroup.get("status") or ""),
            })
    collector_db.upsert_many(engine, "dim_campaign", campaign_rows, ["customer_id", "campaign_id"])
    collector_db.upsert_many(engine, "dim_adgroup", adgroup_rows, ["customer_id", "adgroup_id"])
    return len(campaign_rows), len(adgroup_rows), campaign_ids


def ensure_required_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_campaign (customer_id TEXT, campaign_id TEXT, campaign_name TEXT, campaign_tp TEXT, status TEXT, PRIMARY KEY(customer_id, campaign_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_adgroup (customer_id TEXT, adgroup_id TEXT, adgroup_name TEXT, campaign_id TEXT, status TEXT, PRIMARY KEY(customer_id, adgroup_id))"))
    ensure_placement_tables(engine)


def resolve_target_date(raw: str | None) -> date:
    if raw:
        return datetime.strptime(str(raw).strip(), "%Y-%m-%d").date()
    return (datetime.utcnow() + timedelta(hours=9)).date() - timedelta(days=1)


def resolve_accounts(args: argparse.Namespace) -> List[Dict[str, str]]:
    accounts: List[Dict[str, str]] = []
    if args.customer_id:
        accounts.append({"id": str(args.customer_id).strip(), "name": str(args.account_name or args.customer_id).strip()})
    elif load_naver_accounts is not None:
        accounts = load_naver_accounts(media_types=["sa"])
    if args.account_name:
        needle = str(args.account_name).strip().lower()
        accounts = [a for a in accounts if needle in str(a.get("name") or "").lower()]
    seen = set()
    out = []
    for account in accounts:
        cid = str(account.get("id") or "").strip()
        if cid and cid not in seen:
            seen.add(cid)
            out.append(account)
    return out


def read_saved_placement_summary(engine, target_date: date, customer_ids: List[str]) -> Dict[str, Dict[str, int]]:
    cids = [str(x).strip() for x in customer_ids if str(x or "").strip()]
    if not cids:
        return {}
    sql = text(
        """
        SELECT
            COALESCE(placement_type, 'UNKNOWN') AS placement_type,
            COUNT(*) AS rows,
            COALESCE(SUM(imp), 0) AS imp,
            COALESCE(SUM(clk), 0) AS clk,
            COALESCE(SUM(cost), 0) AS cost
        FROM fact_adgroup_placement_daily
        WHERE dt = :dt
          AND customer_id = ANY(:cids)
        GROUP BY COALESCE(placement_type, 'UNKNOWN')
        ORDER BY COALESCE(placement_type, 'UNKNOWN')
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"dt": target_date, "cids": cids}).mappings().all()
    return {
        str(row.get("placement_type") or "UNKNOWN"): {
            "rows": int(row.get("rows") or 0),
            "imp": int(row.get("imp") or 0),
            "clk": int(row.get("clk") or 0),
            "cost": int(row.get("cost") or 0),
        }
        for row in rows or []
    }


def collect_account(engine, account: Dict[str, str], target_date: date, skip_dim: bool = False) -> Dict[str, Any]:
    customer_id = str(account.get("id") or "").strip()
    account_name = str(account.get("name") or customer_id).strip()
    result: Dict[str, Any] = {
        "customer_id": customer_id,
        "account_name": account_name,
        "target_date": str(target_date),
        "status": "pending",
        "campaign_dim_rows": 0,
        "adgroup_dim_rows": 0,
        "placement_rows_saved": 0,
        "placement_type_counts": {},
        "report_status": "not_requested",
        "error": "",
    }
    try:
        log(f"[{account_name}] 검색/콘텐츠 지면 수집 시작: {target_date}")
        campaign_ids: set[str] | None = None
        if not skip_dim:
            c_cnt, g_cnt, campaign_ids = sync_campaign_adgroup_dims(engine, customer_id)
            result["campaign_dim_rows"] = c_cnt
            result["adgroup_dim_rows"] = g_cnt
            log(f"[{account_name}] 구조 동기화 완료: 캠페인 {c_cnt} / 광고그룹 {g_cnt}")

        rows, meta = fetch_placement_rows_from_sources(
            engine,
            customer_id,
            target_date,
            allowed_campaign_ids=campaign_ids,
        )
        counts = placement_type_counts(rows)
        if rows:
            saved_rows = replace_placement_fact_range(engine, rows, customer_id, target_date)
            if int(counts.get("CONTENT", 0) or 0) == 0:
                save_debug_json(
                    "placement_no_content",
                    customer_id,
                    {
                        "customer_id": customer_id,
                        "account_name": account_name,
                        "target_date": str(target_date),
                        "row_count": len(rows),
                        "placement_type_counts": counts,
                        "meta": meta,
                    },
                )
        else:
            saved_rows = 0
            save_debug_json(
                "placement_stats_no_rows",
                customer_id,
                {
                    "customer_id": customer_id,
                    "account_name": account_name,
                    "target_date": str(target_date),
                    "meta": meta,
                },
            )
            log(f"[{account_name}] 지면 원천 미확정/0건으로 기존 저장 데이터 삭제는 건너뜀")
        result["status"] = "ok"
        result["report_status"] = str(meta.get("status") or "unknown")
        result["placement_rows_saved"] = int(saved_rows or 0)
        result["placement_type_counts"] = counts
        result["parser_meta"] = meta
        log(
            f"[{account_name}] 검색/콘텐츠 지면 저장 완료: {saved_rows}건 "
            f"| status={meta.get('status')} | placements={counts}"
        )
        return result
    except Exception as exc:
        result["status"] = "error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        log(f"[{account_name}] 수집 실패: {result['error']}")
        return result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DA_RAW_SSA 검색/콘텐츠 지면별 별도 수집기")
    parser.add_argument("--date", default="", help="수집일(YYYY-MM-DD). 비우면 KST 기준 어제")
    parser.add_argument("--customer_id", default="", help="단일 네이버 고객 ID")
    parser.add_argument("--account_name", default="", help="계정명 일부 필터")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--skip_dim", action="store_true", help="캠페인/광고그룹 구조 동기화를 건너뜁니다")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    if not API_KEY or not API_SECRET:
        die("NAVER_ADS_API_KEY / NAVER_ADS_SECRET 값이 필요합니다.")
    if not DB_URL:
        die("DATABASE_URL 값이 필요합니다.")

    target_date = resolve_target_date(args.date)
    engine = collector_db.get_engine(DB_URL)
    ensure_required_tables(engine)

    accounts = resolve_accounts(args)
    if not accounts:
        die("수집할 네이버 검색광고 계정이 없습니다.")

    log(f"검색/콘텐츠 지면 수집 대상: {len(accounts)}개 / 날짜={target_date} / workers={args.workers}")
    results: List[Dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, int(args.workers or 1))) as executor:
        futures = [executor.submit(collect_account, engine, account, target_date, args.skip_dim) for account in accounts]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    ok = sum(1 for r in results if r.get("status") == "ok")
    errors = [r for r in results if r.get("status") not in {"ok"}]
    rows_saved = sum(int(r.get("placement_rows_saved") or 0) for r in results)
    result_counts: Dict[str, int] = {}
    for result in results:
        for placement_type, count in (result.get("placement_type_counts") or {}).items():
            result_counts[str(placement_type)] = result_counts.get(str(placement_type), 0) + int(count or 0)
    saved_summary = read_saved_placement_summary(
        engine,
        target_date,
        [str(account.get("id") or "").strip() for account in accounts],
    )
    log(f"검색/콘텐츠 지면 수집 완료: ok={ok}/{len(results)} rows={rows_saved} placements={result_counts} saved={saved_summary} errors={len(errors)}")
    for item in errors[:10]:
        log(f"오류/누락: {item.get('account_name')} status={item.get('status')} report={item.get('report_status')} error={item.get('error')}")
    if ok == 0 and errors:
        die("검색/콘텐츠 지면 수집이 전 계정에서 실패했습니다.")
    if rows_saved <= 0:
        die("검색/콘텐츠 지면 저장 행이 0건입니다.")
    if int((saved_summary.get("CONTENT") or {}).get("rows") or 0) <= 0:
        die("CONTENT 지면 저장 행이 0건입니다. debug_reports의 placement_no_content 아티팩트를 확인하세요.")


if __name__ == "__main__":
    main()
