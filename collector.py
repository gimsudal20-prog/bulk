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
import re
import sys
import threading
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Engine

from device_collector_helpers import (
    DEVICE_PARSER_VERSION,
    build_ad_to_campaign_map,
    parse_ad_device_report,
    save_device_stats,
    summarize_stat_res,
)

import collector_api as collector_api_mod
import collector_db as collector_db_mod
import collector_media as collector_media_mod
import collector_parsers as collector_parsers_mod
import collector_runner as collector_runner_mod

try:
    from account_master import load_naver_accounts
except Exception:
    load_naver_accounts = None

load_dotenv(override=False)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
CUSTOMER_ID = (os.getenv("CUSTOMER_ID") or "").strip()

BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60

SKIP_KEYWORD_DIM = False
SKIP_AD_DIM = False
SKIP_KEYWORD_STATS = False
SKIP_AD_STATS = False
FAST_MODE = False

CART_ENABLE_DATE = date(2026, 3, 11)
SHOPPING_HINT_KEYS = ('shopping', '쇼핑', 'product', 'productcatalog', 'catalog', 'shop')

COLLECT_MODE_ALIASES = {
    "sa_only": "sa_only",
    "검색광고 전체만": "sa_only",
    "검색광고전체만": "sa_only",
    "device_only": "device_only",
    "기기만": "device_only",
    "sa_with_device": "sa_with_device",
    "검색광고 전체+기기": "sa_with_device",
    "검색광고전체+기기": "sa_with_device",
    "검색광고 전체 + 기기": "sa_with_device",
}


def normalize_collect_mode(value: str | None) -> str:
    raw = (value or "sa_with_device").strip()
    if not raw:
        return "sa_with_device"
    if raw in COLLECT_MODE_ALIASES:
        return COLLECT_MODE_ALIASES[raw]
    lowered = raw.lower()
    if lowered in COLLECT_MODE_ALIASES:
        return COLLECT_MODE_ALIASES[lowered]
    raise ValueError(
        f"collect_mode 값이 올바르지 않습니다: {value} (허용: sa_only, device_only, sa_with_device, 검색광고 전체만, 기기만, 검색광고 전체+기기)"
    )


def label_collect_mode(value: str | None) -> str:
    normalized = normalize_collect_mode(value)
    return {
        "sa_only": "검색광고 전체만",
        "device_only": "기기만",
        "sa_with_device": "검색광고 전체+기기",
    }.get(normalized, normalized)


def normalize_sa_scope(value: str | None) -> str:
    raw = str(value or "full").strip()
    if not raw:
        return "full"
    lowered = raw.lower()
    alias = {
        "full": "full",
        "전체": "full",
        "ad_only": "ad_only",
        "소재만": "ad_only",
    }
    if raw in alias:
        return alias[raw]
    if lowered in alias:
        return alias[lowered]
    raise ValueError(f"sa_scope 값이 올바르지 않습니다: {value} (허용: full, ad_only, 전체, 소재만)")


def label_sa_scope(value: str | None) -> str:
    normalized = normalize_sa_scope(value)
    return {
        "full": "전체",
        "ad_only": "소재만",
    }.get(normalized, normalized)


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _exc_label(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _traceback_tail(exc: Exception, limit: int = 3) -> str:
    try:
        lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    except Exception:
        return ""
    flat = [ln.strip() for part in lines for ln in str(part).splitlines() if ln.strip()]
    return " | ".join(flat[-limit:])


def _safe_rollback(raw_conn, *, ctx: str = ""):
    if not raw_conn:
        return
    try:
        if getattr(raw_conn, "closed", False):
            return
        raw_conn.rollback()
    except Exception as rollback_exc:
        if "closed" in str(rollback_exc).lower():
            return
        extra = f" | {ctx}" if ctx else ""
        log(f"⚠️ 롤백 실패{extra} | {_exc_label(rollback_exc)}")


def _safe_close(resource, *, label: str, ctx: str = ""):
    if not resource:
        return
    try:
        resource.close()
    except Exception as close_exc:
        if "closed" in str(close_exc).lower():
            return
        extra = f" | {ctx}" if ctx else ""
        log(f"⚠️ {label} close 실패{extra} | {_exc_label(close_exc)}")


def _log_retry_failure(action: str, attempt: int, total: int, exc: Exception, *, ctx: str = ""):
    extra = f" | {ctx}" if ctx else ""
    log(f"⚠️ {action} 실패 {attempt}/{total}{extra} | {_exc_label(exc)}")


def _log_best_effort_failure(action: str, exc: Exception, *, ctx: str = ""):
    extra = f" | {ctx}" if ctx else ""
    log(f"⚠️ {action} 무시됨{extra} | {_exc_label(exc)}")


def _raise_retry_failure(action: str, exc: Exception | None, *, ctx: str = ""):
    msg = f"{action} 최종 실패"
    if ctx:
        msg += f" | {ctx}"
    if exc is not None:
        msg += f" | {_exc_label(exc)}"
        raise RuntimeError(msg) from exc
    raise RuntimeError(msg)


DEBUG_DIR = Path(os.getenv("DEBUG_REPORT_DIR", "debug_reports"))


def save_debug_report(tp: str, customer_id: str, job_id: str, content: str):
    try:
        if FAST_MODE:
            return
        if not os.getenv("DEBUG_REPORTS", "1") in ["1", "true", "TRUE", "yes", "YES"]:
            return
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = DEBUG_DIR / f"{ts}_{customer_id}_{tp}_{job_id}.txt"
        fname.write_text(content or "", encoding="utf-8")
    except Exception as e:
        _log_best_effort_failure("debug report 저장", e, ctx=f"tp={tp} customer_id={customer_id}")


def _df_state(df: pd.DataFrame | None) -> tuple[str, int]:
    if df is None:
        return "missing", 0
    try:
        rows = int(len(df.index))
    except Exception:
        rows = 0
    return ("empty" if rows == 0 else "ok"), rows


def _new_account_collect_result(customer_id: str, account_name: str, target_date: date, collect_mode: str, sa_scope: str, skip_dim: bool, fast_mode: bool, shopping_only: bool) -> Dict[str, Any]:
    return {
        "customer_id": str(customer_id),
        "account_name": str(account_name),
        "target_date": str(target_date),
        "status": "pending",
        "error": "",
        "collect_mode": str(collect_mode or "sa_with_device"),
        "collect_mode_label": label_collect_mode(collect_mode),
        "sa_scope": str(sa_scope or "full"),
        "sa_scope_label": label_sa_scope(sa_scope),
        "collect_sa": False,
        "collect_device": False,
        "skip_dim": bool(skip_dim),
        "fast_mode": bool(fast_mode),
        "shopping_only": bool(shopping_only),
        "campaign_targets": 0,
        "keyword_targets": 0,
        "ad_targets": 0,
        "shopping_campaign_targets": 0,
        "dim_campaigns": 0,
        "dim_adgroups": 0,
        "dim_keywords": 0,
        "dim_ads": 0,
        "used_realtime_fallback": False,
        "realtime_reason": "",
        "ad_report_status": "not_requested",
        "ad_report_rows": 0,
        "ad_conversion_status": "not_requested",
        "ad_conversion_rows": 0,
        "shopping_keyword_conversion_status": "not_requested",
        "shopping_keyword_conversion_rows": 0,
        "split_attempted": False,
        "split_report_ok": False,
        "split_source": "none",
        "campaign_rows_saved": 0,
        "keyword_rows_saved": 0,
        "ad_rows_saved": 0,
        "device_campaign_rows_saved": 0,
        "device_ad_rows_saved": 0,
        "media_rows_saved": 0,
        "media_source": "not_requested",
        "media_detail_rows": 0,
        "media_summary_rows": 0,
        "shopping_query_rows_saved": 0,
        "device_status": "not_requested",
        "device_missing_campaign_rows": 0,
        "zero_data": False,
        "notes": [],
    }


def _summary_icon(status: str) -> str:
    return {
        "ok": "✅",
        "zero_data": "⚪",
        "error": "❌",
        "skipped": "⏭️",
        "pending": "…",
    }.get(str(status or ""), "•")


def _markdown_escape(value: Any) -> str:
    s = str(value if value is not None else "")
    return s.replace("|", "\\|").replace("\n", " ").strip()


def emit_collection_run_summary(results: List[Dict[str, Any]], target_date: date, collect_mode: str, shopping_only: bool = False, sa_scope: str = "full"):
    rows = [r for r in (results or []) if isinstance(r, dict)]
    if not rows:
        log("📊 실행 요약을 생성할 결과가 없습니다.")
        return

    total = len(rows)
    ok_cnt = sum(1 for r in rows if r.get("status") == "ok")
    zero_cnt = sum(1 for r in rows if r.get("status") == "zero_data")
    err_cnt = sum(1 for r in rows if r.get("status") == "error")
    skip_cnt = sum(1 for r in rows if r.get("status") == "skipped")
    fallback_cnt = sum(1 for r in rows if r.get("used_realtime_fallback"))
    split_ok_cnt = sum(1 for r in rows if r.get("split_report_ok"))
    device_ok_cnt = sum(1 for r in rows if r.get("device_status") == "ok")

    log("=" * 72)
    log(
        f"📊 수집 실행 요약 | 대상일={target_date} | 모드={label_collect_mode(collect_mode)} | 범위={label_sa_scope(sa_scope)} | "
        f"정상={ok_cnt} | 0건={zero_cnt} | 오류={err_cnt} | 건너뜀={skip_cnt} | "
        f"실시간대체={fallback_cnt} | split성공={split_ok_cnt} | PC/M성공={device_ok_cnt}"
    )

    interesting = []
    for r in rows:
        notes = []
        if r.get("status") == "error":
            notes.append(f"error={r.get('error')}")
        else:
            if r.get("used_realtime_fallback"):
                notes.append(f"fallback={r.get('realtime_reason') or 'unknown'}")
            ad_status = r.get("ad_report_status")
            if ad_status not in {"ok", "realtime_only", "not_requested"}:
                notes.append(f"AD={ad_status}")
            device_status = r.get("device_status")
            if r.get("collect_device") and device_status not in {"ok", "disabled", "not_requested", "not_applicable", "realtime_skipped"}:
                notes.append(f"PC/M={device_status}")
            media_source = r.get("media_source")
            if media_source in {"empty", "fallback_total", "fallback_device"}:
                notes.append(f"media={media_source}")
            if r.get("split_attempted") and not r.get("split_report_ok"):
                notes.append("split=미확정")
            if r.get("zero_data"):
                notes.append("0건")
        if notes or r.get("status") in {"error", "zero_data", "skipped"}:
            interesting.append((r, notes))

    if interesting:
        log("🧾 점검 필요 계정")
        for r, notes in interesting[:30]:
            log(
                f"   - {_summary_icon(r.get('status'))} [ {r.get('account_name')} ] "
                f"C={r.get('campaign_rows_saved', 0)} K={r.get('keyword_rows_saved', 0)} A={r.get('ad_rows_saved', 0)} "
                f"PC/M={r.get('device_campaign_rows_saved', 0)}/{r.get('device_ad_rows_saved', 0)} "
                f"media={r.get('media_rows_saved', 0)} | {'; '.join(notes) if notes else '확인 필요 없음'}"
            )
        if len(interesting) > 30:
            log(f"   … 외 {len(interesting) - 30}개 계정은 GitHub Step Summary 표에서 확인하세요.")
    else:
        log("🧾 점검 필요 계정 없음")
    log("=" * 72)

    summary_path = (os.getenv("GITHUB_STEP_SUMMARY") or "").strip()
    if not summary_path:
        return

    lines = [
        f"## 수집 실행 요약 ({target_date})",
        "",
        f"- 수집 모드: **{_markdown_escape(label_collect_mode(collect_mode))}**",
        f"- 쇼핑검색 전용: **{'예' if shopping_only else '아니오'}**",
        f"- 검색광고 수집 범위: **{_markdown_escape(label_sa_scope(sa_scope))}**",
        f"- 대상 계정: **{total}개**",
        f"- 정상 {ok_cnt} / 0건 {zero_cnt} / 오류 {err_cnt} / 건너뜀 {skip_cnt}",
        f"- 실시간 대체 {fallback_cnt} / split 성공 {split_ok_cnt} / PC/M 성공 {device_ok_cnt}",
        "",
        "|업체|상태|캠페인|키워드|소재|PC/M 캠페인|PC/M 소재|매체행|AD|Split|실시간대체|비고|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|",
    ]
    for r in rows:
        note_parts = []
        if r.get("status") == "error" and r.get("error"):
            note_parts.append(str(r.get("error")))
        if r.get("split_attempted") and not r.get("split_report_ok"):
            note_parts.append("split 미확정")
        if r.get("device_missing_campaign_rows"):
            note_parts.append(f"PC/M 매핑누락 {r.get('device_missing_campaign_rows')}")
        note_text = "; ".join(note_parts)
        lines.append(
            "|{account}|{status}|{c}|{k}|{a}|{dc}|{da}|{m}|{ad}|{split}|{fb}|{note}|".format(
                account=_markdown_escape(r.get("account_name")),
                status=_markdown_escape(f"{_summary_icon(r.get('status'))} {r.get('status') or ''}"),
                c=int(r.get("campaign_rows_saved") or 0),
                k=int(r.get("keyword_rows_saved") or 0),
                a=int(r.get("ad_rows_saved") or 0),
                dc=int(r.get("device_campaign_rows_saved") or 0),
                da=int(r.get("device_ad_rows_saved") or 0),
                m=int(r.get("media_rows_saved") or 0),
                ad=_markdown_escape(r.get("ad_report_status")),
                split=_markdown_escape("ok" if r.get("split_report_ok") else ("skip" if not r.get("split_attempted") else "fail")),
                fb=_markdown_escape(r.get("realtime_reason") if r.get("used_realtime_fallback") else "-"),
                note=_markdown_escape(note_text),
            )
        )
    try:
        with open(summary_path, "a", encoding="utf-8") as fp:
            fp.write("\n".join(lines) + "\n")
    except Exception as e:
        log(f"⚠️ GITHUB_STEP_SUMMARY 기록 실패: {e}")


def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)


if not API_KEY or not API_SECRET:
    die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")

thread_local = threading.local()


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def now_millis() -> str:
    return str(int(time.time() * 1000))


def sign_path_only(method: str, path: str, timestamp: str, secret: str) -> str:
    msg = f"{timestamp}.{method}.{path}".encode("utf-8")
    dig = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(dig).decode("utf-8")


def make_headers(method: str, path: str, customer_id: str) -> Dict[str, str]:
    ts = now_millis()
    sig = sign_path_only(method.upper(), path, ts, API_SECRET)
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": sig,
    }


def request_json(method: str, path: str, customer_id: str, params: dict | None = None, json_data: dict | None = None, raise_error=True) -> Tuple[int, Any]:
    url = BASE_URL + path
    max_retries = 8
    session = get_session()

    for attempt in range(max_retries):
        headers = make_headers(method, path, customer_id)
        try:
            r = session.request(method, url, headers=headers, params=params, json=json_data, timeout=TIMEOUT)
            if r.status_code == 403:
                if raise_error:
                    raise requests.HTTPError(f"403 Forbidden: 권한이 없습니다 ({customer_id})", response=r)
                return 403, None
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 + attempt + random.uniform(0.1, 1.5))
                continue
            data = None
            try:
                data = r.json()
            except ValueError:
                data = r.text
            if raise_error and r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code} Error: {data}", response=r)
            return r.status_code, data
        except requests.exceptions.RequestException as e:
            if "403" in str(e):
                raise e
            time.sleep(2 + attempt)
    if raise_error:
        raise Exception(f"최대 재시도 초과: {url}")
    return 0, None


def safe_call(method: str, path: str, customer_id: str, params: dict | None = None) -> Tuple[bool, Any]:
    try:
        _, data = request_json(method, path, customer_id, params=params, raise_error=True)
        return True, data
    except Exception as e:
        _log_best_effort_failure("safe_call", e, ctx=f"method={method} path={path} customer_id={customer_id}")
        return False, None


def lock_key_for_job(customer_id: str, target_date: date, scope: str = "collector_daily") -> int:
    raw = f"{scope}:{str(customer_id).strip()}:{target_date.isoformat()}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], "big", signed=False) & 0x7FFFFFFFFFFFFFFF


def acquire_job_lock(engine: Engine, customer_id: str, target_date: date):
    if not DB_URL or not str(DB_URL).lower().startswith(("postgresql", "postgres://")):
        return None
    raw_conn = None
    cur = None
    try:
        raw_conn = engine.raw_connection()
        cur = raw_conn.cursor()
        lk = lock_key_for_job(customer_id, target_date)
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lk,))
        row = cur.fetchone()
        locked = bool(row[0]) if row else False
        if not locked:
            _safe_close(cur, label="cursor", ctx=f"lock customer_id={customer_id}")
            _safe_close(raw_conn, label="connection", ctx=f"lock customer_id={customer_id}")
            return False
        return raw_conn
    except Exception as e:
        log(f"⚠️ 락 획득 실패 - 무락 모드로 진행합니다: {e}")
        try:
            engine.dispose()
        except Exception:
            pass
        _safe_close(cur, label="cursor", ctx=f"lock customer_id={customer_id}")
        _safe_close(raw_conn, label="connection", ctx=f"lock customer_id={customer_id}")
        return None


def release_job_lock(raw_conn, customer_id: str, target_date: date):
    if raw_conn is None:
        return
    cur = None
    try:
        if getattr(raw_conn, "closed", False):
            return
        cur = raw_conn.cursor()
        lk = lock_key_for_job(customer_id, target_date)
        cur.execute("SELECT pg_advisory_unlock(%s)", (lk,))
    except Exception as e:
        _log_best_effort_failure("advisory unlock", e, ctx=f"customer_id={customer_id} target_date={target_date}")
    finally:
        _safe_close(cur, label="cursor", ctx=f"unlock customer_id={customer_id}")
        _safe_close(raw_conn, label="connection", ctx=f"unlock customer_id={customer_id}")


def get_engine() -> Engine:
    return collector_db_mod.get_engine(DB_URL)


def ensure_column(engine: Engine, table: str, column: str, datatype: str):
    return collector_db_mod.ensure_column(engine, table, column, datatype)


def ensure_tables(engine: Engine):
    return collector_db_mod.ensure_tables(engine)


def upsert_many(engine: Engine, table: str, rows: List[Dict[str, Any]], pk_cols: List[str]):
    return collector_db_mod.upsert_many(engine, table, rows, pk_cols)


def clear_fact_range(engine: Engine, table: str, customer_id: str, d1: date):
    return collector_db_mod.clear_fact_range(engine, table, customer_id, d1)


def clear_fact_scope(engine: Engine, table: str, customer_id: str, d1: date, pk: str, ids: List[str]):
    return collector_db_mod.clear_fact_scope(engine, table, customer_id, d1, pk, ids)


def replace_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1: date):
    return collector_db_mod.replace_fact_range(engine, table, rows, customer_id, d1)


def replace_query_fact_range(engine: Engine, rows: List[Dict[str, Any]], customer_id: str, d1: date):
    return collector_db_mod.replace_query_fact_range(engine, rows, customer_id, d1)


def replace_fact_scope(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1: date, pk: str, ids: List[str]):
    return collector_db_mod.replace_fact_scope(engine, table, rows, customer_id, d1, pk, ids)


def replace_media_fact_range(engine: Engine, rows: List[Dict[str, Any]], customer_id: str, d1: date, scoped_campaign_types: List[str] | None = None):
    return collector_db_mod.replace_media_fact_range(engine, rows, customer_id, d1, scoped_campaign_types=scoped_campaign_types)


def normalize_header(v: str) -> str:
    return collector_parsers_mod.normalize_header(v)


def normalize_keyword_text(v: str) -> str:
    return collector_parsers_mod.normalize_keyword_text(v)


def extract_prefixed_token(vals, prefix: str) -> str:
    return collector_parsers_mod.extract_prefixed_token(vals, prefix)


def keyword_text_candidates(kw_norm: str, rows: list[tuple[str, str]]) -> list[str]:
    return collector_parsers_mod.keyword_text_candidates(kw_norm, rows)


def get_col_idx(headers: List[str], candidates: List[str]) -> int:
    return collector_parsers_mod.get_col_idx(headers, candidates)


def safe_float(v) -> float:
    return collector_parsers_mod.safe_float(v)


def split_enabled_for_date(target_date: date) -> bool:
    return collector_parsers_mod.split_enabled_for_date(target_date, CART_ENABLE_DATE)


def is_shopping_campaign_obj(camp: dict) -> bool:
    return collector_parsers_mod.is_shopping_campaign_obj(camp, SHOPPING_HINT_KEYS)


def merge_split_maps(*maps: dict) -> dict:
    return collector_parsers_mod.merge_split_maps(*maps)


def filter_split_map_excluding_ids(split_map: dict, excluded_ids: set[str] | None = None) -> dict:
    return collector_parsers_mod.filter_split_map_excluding_ids(split_map, excluded_ids)


def summarize_split_map(split_map: dict) -> dict:
    return collector_parsers_mod.summarize_split_map(split_map)


def validate_shopping_split_summary(summary: dict, ad_map: dict) -> tuple[bool, str]:
    return collector_parsers_mod.validate_shopping_split_summary(summary, ad_map)


def empty_split_summary() -> dict:
    return collector_parsers_mod.empty_split_summary()


def add_split_summary(summary: dict, is_purchase: bool, is_cart: bool, is_wishlist: bool, c_val: float, s_val: int):
    return collector_parsers_mod.add_split_summary(summary, is_purchase, is_cart, is_wishlist, c_val, s_val)


def merge_split_summaries(*summaries: dict) -> dict:
    return collector_parsers_mod.merge_split_summaries(*summaries)


def split_summary_has_values(summary: dict) -> bool:
    return collector_parsers_mod.split_summary_has_values(summary)


def format_split_summary(summary: dict) -> str:
    return collector_parsers_mod.format_split_summary(summary)


def process_conversion_report(df: pd.DataFrame, allowed_campaign_ids: set[str] | None = None, report_hint: str = "", keyword_lookup: dict | None = None, keyword_unique_lookup: dict | None = None, live_keyword_resolver=None, debug_account_name: str = "", debug_target_date: str = "") -> Tuple[dict, dict, dict, dict]:
    return collector_parsers_mod.process_conversion_report(
        df,
        allowed_campaign_ids=allowed_campaign_ids,
        report_hint=report_hint,
        keyword_lookup=keyword_lookup,
        keyword_unique_lookup=keyword_unique_lookup,
        live_keyword_resolver=live_keyword_resolver,
        debug_account_name=debug_account_name,
        debug_target_date=debug_target_date,
        fast_mode=FAST_MODE,
    )


def parse_shopping_query_report(df: pd.DataFrame, target_date: date, customer_id: str) -> List[Dict[str, Any]]:
    return collector_parsers_mod.parse_shopping_query_report(df, target_date, customer_id)


def build_keyword_lookup_from_keyword_report(df: pd.DataFrame) -> tuple[dict, dict]:
    return collector_parsers_mod.build_keyword_lookup_from_keyword_report(df)


def parse_base_report(df: pd.DataFrame, report_tp: str, conv_map: dict | None = None, has_conv_report: bool = False) -> dict:
    return collector_parsers_mod.parse_base_report(df, report_tp, conv_map=conv_map, has_conv_report=has_conv_report)


def build_campaign_type_map(engine: Engine, customer_id: str) -> Dict[str, str]:
    return collector_media_mod.build_campaign_type_map(engine, customer_id)


def collect_media_fact(engine: Engine, customer_id: str, target_date: date, ad_report_df: pd.DataFrame | None, ad_to_campaign_map: Dict[str, str], campaign_type_map: Dict[str, str], camp_device_stat: Dict[Tuple[str, str], Dict[str, Any]] | None = None, allowed_campaign_ids: set[str] | None = None, scoped_campaign_types: List[str] | None = None) -> Tuple[int, Dict[str, Any]]:
    return collector_media_mod.collect_media_fact(
        engine,
        customer_id,
        target_date,
        ad_report_df,
        ad_to_campaign_map,
        campaign_type_map,
        camp_device_stat,
        allowed_campaign_ids=allowed_campaign_ids,
        scoped_campaign_types=scoped_campaign_types,
    )


def filter_stat_result(stat_res: dict, allowed_ids: set[str] | None) -> dict:
    if not stat_res or not allowed_ids:
        return stat_res or {}
    allowed = {str(x).strip() for x in allowed_ids if str(x).strip()}
    return {str(k): v for k, v in (stat_res or {}).items() if str(k).strip() in allowed}


def list_campaigns(customer_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/campaigns", customer_id)
    return data if ok and isinstance(data, list) else []


def list_adgroups(customer_id: str, campaign_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/adgroups", customer_id, {"nccCampaignId": campaign_id})
    return data if ok and isinstance(data, list) else []


def list_keywords(customer_id: str, adgroup_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/keywords", customer_id, {"nccAdgroupId": adgroup_id})
    return data if ok and isinstance(data, list) else []


def extract_keyword_text_from_obj(k: dict) -> str:
    return str(
        k.get("keyword")
        or k.get("relKeyword")
        or k.get("keywordPlus")
        or k.get("relKeywordPlus")
        or k.get("keywordName")
        or k.get("name")
        or k.get("userKeyword")
        or k.get("searchKeyword")
        or ""
    ).strip()


def make_live_keyword_resolver(customer_id: str):
    cache: dict[str, dict] = {}

    def _build(adgroup_id: str):
        gid = str(adgroup_id or "").strip()
        if not gid:
            return {"exact": {}, "rows": []}
        if gid in cache:
            return cache[gid]
        exact = {}
        rows = []
        try:
            kws = list_keywords(customer_id, gid)
            for k in kws or []:
                kid = str(k.get("nccKeywordId") or k.get("keywordId") or "").strip()
                kw = extract_keyword_text_from_obj(k)
                if not kid or not kw:
                    continue
                kw_l = kw.lower()
                kw_n = normalize_keyword_text(kw)
                exact[(gid, kw)] = kid
                exact[(gid, kw_l)] = kid
                exact[(gid, kw_n)] = kid
                rows.append((kw_n, kid))
        except Exception:
            pass
        cache[gid] = {"exact": exact, "rows": rows}
        return cache[gid]

    def resolve(adgroup_id: str, keyword_text: str) -> str:
        gid = str(adgroup_id or "").strip()
        kw = str(keyword_text or "").strip()
        if not gid or not kw or kw == '-':
            return ""
        built = _build(gid)
        kw_l = kw.lower()
        kw_n = normalize_keyword_text(kw)
        kid = built["exact"].get((gid, kw)) or built["exact"].get((gid, kw_l)) or built["exact"].get((gid, kw_n))
        if kid:
            return kid
        cands = keyword_text_candidates(kw_n, built.get("rows", []))
        return cands[0] if len(cands) == 1 else ""

    return resolve


def list_ads(customer_id: str, adgroup_id: str) -> List[dict]:
    return collector_api_mod.list_ads(customer_id, adgroup_id, safe_call)


def extract_ad_creative_fields(ad_obj: dict) -> Dict[str, str]:
    return collector_api_mod.extract_ad_creative_fields(ad_obj, json)


def get_stats_range(customer_id: str, ids: List[str], d1: date) -> List[dict]:
    return collector_api_mod.get_stats_range(customer_id, ids, d1, request_json)


def fetch_stats_fallback(engine: Engine, customer_id: str, target_date: date, ids: List[str], id_key: str, table_name: str, split_map: dict | None = None, scoped_replace: bool = False) -> int:
    return collector_api_mod.fetch_stats_fallback(
        engine,
        customer_id,
        target_date,
        ids,
        id_key,
        table_name,
        split_map=split_map,
        scoped_replace=scoped_replace,
        get_stats_range_fn=get_stats_range,
        clear_fact_range_fn=clear_fact_range,
        replace_fact_scope_fn=replace_fact_scope,
        replace_fact_range_fn=replace_fact_range,
    )


def cleanup_ghost_reports(customer_id: str):
    return collector_api_mod.cleanup_ghost_reports(customer_id, request_json, safe_call)


def resolve_download_url(dl_url: str) -> str:
    return collector_api_mod.resolve_download_url(dl_url, BASE_URL)


def parse_report_text_to_df(txt: str) -> pd.DataFrame:
    return collector_api_mod.parse_report_text_to_df(txt)


def download_report_dataframe(customer_id: str, tp: str, job_id: str, initial_url: str) -> pd.DataFrame | None:
    return collector_api_mod.download_report_dataframe(
        customer_id,
        tp,
        job_id,
        initial_url,
        get_session=get_session,
        base_url=BASE_URL,
        make_headers=make_headers,
        request_json=request_json,
        save_debug_report=save_debug_report,
        parse_report_text_to_df_fn=parse_report_text_to_df,
        log_fn=log,
    )


def fetch_multiple_stat_reports(customer_id: str, report_types: List[str], target_date: date) -> Dict[str, pd.DataFrame | None]:
    return collector_api_mod.fetch_multiple_stat_reports(
        customer_id,
        report_types,
        target_date,
        cleanup_ghost_reports_fn=cleanup_ghost_reports,
        request_json=request_json,
        download_report_dataframe_fn=download_report_dataframe,
        safe_call=safe_call,
        fast_mode=FAST_MODE,
        log_fn=log,
    )


def _resolve_split_payload(
    dfs: Dict[str, pd.DataFrame | None],
    *,
    collect_sa: bool,
    target_date: date,
    shopping_only: bool,
    shopping_campaign_ids: set[str],
    shopping_keyword_ids: set[str],
    keyword_lookup: Dict[Tuple[str, str], str],
    keyword_unique_lookup: Dict[str, List[Tuple[str, str]]],
    live_keyword_resolver,
    account_name: str,
    customer_id: str,
    result: Dict[str, Any],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], List[Dict[str, Any]], bool]:
    return collector_runner_mod._resolve_split_payload(
        dfs,
        collect_sa=collect_sa,
        target_date=target_date,
        shopping_only=shopping_only,
        shopping_campaign_ids=shopping_campaign_ids,
        shopping_keyword_ids=shopping_keyword_ids,
        keyword_lookup=keyword_lookup,
        keyword_unique_lookup=keyword_unique_lookup,
        live_keyword_resolver=live_keyword_resolver,
        account_name=account_name,
        customer_id=customer_id,
        result=result,
        split_enabled_for_date_fn=split_enabled_for_date,
        process_conversion_report_fn=process_conversion_report,
        empty_split_summary_fn=empty_split_summary,
        parse_shopping_query_report_fn=parse_shopping_query_report,
        merge_split_maps_fn=merge_split_maps,
        filter_split_map_excluding_ids_fn=filter_split_map_excluding_ids,
        split_summary_has_values_fn=split_summary_has_values,
        validate_shopping_split_summary_fn=validate_shopping_split_summary,
        format_split_summary_fn=format_split_summary,
        log_fn=log,
    )


def _is_ad_only_scope(sa_scope: str) -> bool:
    return collector_runner_mod._is_ad_only_scope(sa_scope, normalize_sa_scope)


def _scope_enabled_collectors(sa_scope: str, collect_sa: bool) -> tuple[bool, bool, bool]:
    return collector_runner_mod._scope_enabled_collectors(sa_scope, collect_sa, normalize_sa_scope)


def _save_report_stats_and_breakdowns(
    engine: Engine,
    *,
    customer_id: str,
    account_name: str,
    target_date: date,
    collect_sa: bool,
    collect_device: bool,
    sa_scope: str,
    shopping_only: bool,
    target_camp_ids: List[str],
    target_kw_ids: List[str],
    target_ad_ids: List[str],
    ad_report_df: pd.DataFrame | None,
    ad_to_campaign_map: Dict[str, str],
    campaign_type_map: Dict[str, str],
    camp_map: Dict[str, Dict[str, Any]],
    kw_map: Dict[str, Dict[str, Any]],
    ad_map: Dict[str, Dict[str, Any]],
    result: Dict[str, Any],
) -> Tuple[int, int, int, int, int, int, Dict[str, Any]]:
    return collector_runner_mod._save_report_stats_and_breakdowns(
        engine,
        customer_id=customer_id,
        account_name=account_name,
        target_date=target_date,
        collect_sa=collect_sa,
        collect_device=collect_device,
        sa_scope=sa_scope,
        shopping_only=shopping_only,
        target_camp_ids=target_camp_ids,
        target_kw_ids=target_kw_ids,
        target_ad_ids=target_ad_ids,
        ad_report_df=ad_report_df,
        ad_to_campaign_map=ad_to_campaign_map,
        campaign_type_map=campaign_type_map,
        camp_map=camp_map,
        kw_map=kw_map,
        ad_map=ad_map,
        result=result,
        normalize_sa_scope_fn=normalize_sa_scope,
        fetch_stats_fallback_fn=fetch_stats_fallback,
        clear_fact_scope_fn=clear_fact_scope,
        parse_ad_device_report_fn=parse_ad_device_report,
        filter_stat_result_fn=filter_stat_result,
        save_device_stats_fn=save_device_stats,
        summarize_stat_res_fn=summarize_stat_res,
        collect_media_fact_fn=collect_media_fact,
        skip_keyword_stats=SKIP_KEYWORD_STATS,
        skip_ad_stats=SKIP_AD_STATS,
        device_parser_version=DEVICE_PARSER_VERSION,
        log_fn=log,
    )


def _sync_structure_and_collect_targets(
    engine: Engine,
    customer_id: str,
    account_name: str,
    collect_sa: bool,
    collect_device: bool,
    shopping_only: bool,
    result: Dict[str, Any],
):
    return collector_runner_mod._sync_structure_and_collect_targets(
        engine,
        customer_id=customer_id,
        account_name=account_name,
        collect_sa=collect_sa,
        collect_device=collect_device,
        shopping_only=shopping_only,
        result=result,
        list_campaigns_fn=list_campaigns,
        list_adgroups_fn=list_adgroups,
        list_keywords_fn=list_keywords,
        list_ads_fn=list_ads,
        is_shopping_campaign_obj_fn=is_shopping_campaign_obj,
        extract_keyword_text_from_obj_fn=extract_keyword_text_from_obj,
        extract_ad_creative_fields_fn=extract_ad_creative_fields,
        upsert_many_fn=upsert_many,
        skip_keyword_dim=SKIP_KEYWORD_DIM,
        skip_ad_dim=SKIP_AD_DIM,
        log_fn=log,
    )


def _load_targets_from_dims(
    engine: Engine,
    customer_id: str,
    collect_sa: bool,
    shopping_only: bool,
    shopping_campaign_ids: set[str],
    shopping_adgroup_ids: set[str],
    shopping_keyword_ids: set[str],
):
    return collector_runner_mod._load_targets_from_dims(
        engine,
        customer_id,
        collect_sa,
        shopping_only,
        shopping_campaign_ids,
        shopping_adgroup_ids,
        shopping_keyword_ids,
    )


def _build_keyword_lookup_bundle(
    engine: Engine,
    customer_id: str,
    shopping_only: bool,
    shopping_adgroup_ids: set[str],
):
    return collector_runner_mod._build_keyword_lookup_bundle(
        engine,
        customer_id,
        shopping_only,
        shopping_adgroup_ids,
        normalize_keyword_text,
    )


def _prepare_account_report_fetch_plan(
    customer_id: str,
    account_name: str,
    target_date: date,
    collect_sa: bool,
    shopping_campaign_ids: set[str],
    result: Dict[str, Any],
):
    return collector_runner_mod._prepare_account_report_fetch_plan(
        customer_id,
        account_name,
        target_date,
        collect_sa,
        shopping_campaign_ids,
        result,
        split_enabled_for_date_fn=split_enabled_for_date,
        fetch_multiple_stat_reports_fn=fetch_multiple_stat_reports,
        df_state_fn=_df_state,
        log_fn=log,
    )


def _finalize_account_result(
    result: Dict[str, Any],
    account_name: str,
    collect_mode: str,
    collect_device: bool,
    split_report_ok: bool,
    c_cnt: int,
    k_cnt: int,
    a_cnt: int,
    device_campaign_cnt: int,
    device_ad_cnt: int,
):
    return collector_runner_mod._finalize_account_result(
        result,
        account_name,
        collect_mode,
        collect_device,
        split_report_ok,
        c_cnt,
        k_cnt,
        a_cnt,
        device_campaign_cnt,
        device_ad_cnt,
        log_fn=log,
    )


def process_account(engine: Engine, customer_id: str, account_name: str, target_date: date, skip_dim: bool = False, fast_mode: bool = False, collect_mode: str = "sa_with_device", sa_scope: str = "full", shopping_only: bool = False):
    return collector_runner_mod.process_account(
        engine,
        customer_id,
        account_name,
        target_date,
        skip_dim,
        fast_mode,
        collect_mode,
        sa_scope,
        shopping_only,
        new_account_collect_result_fn=_new_account_collect_result,
        acquire_job_lock_fn=acquire_job_lock,
        release_job_lock_fn=release_job_lock,
        normalize_sa_scope_fn=normalize_sa_scope,
        label_collect_mode_fn=label_collect_mode,
        label_sa_scope_fn=label_sa_scope,
        sync_structure_and_collect_targets_fn=_sync_structure_and_collect_targets,
        load_targets_from_dims_fn=_load_targets_from_dims,
        build_keyword_lookup_bundle_fn=_build_keyword_lookup_bundle,
        log_best_effort_failure_fn=_log_best_effort_failure,
        make_live_keyword_resolver_fn=make_live_keyword_resolver,
        build_ad_to_campaign_map_fn=build_ad_to_campaign_map,
        build_campaign_type_map_fn=build_campaign_type_map,
        prepare_account_report_fetch_plan_fn=_prepare_account_report_fetch_plan,
        scope_enabled_collectors_fn=_scope_enabled_collectors,
        fetch_stats_fallback_fn=fetch_stats_fallback,
        clear_fact_scope_fn=clear_fact_scope,
        collect_media_fact_fn=collect_media_fact,
        resolve_split_payload_fn=_resolve_split_payload,
        save_report_stats_and_breakdowns_fn=_save_report_stats_and_breakdowns,
        is_ad_only_scope_fn=_is_ad_only_scope,
        replace_query_fact_range_fn=replace_query_fact_range,
        finalize_account_result_fn=_finalize_account_result,
        exc_label_fn=_exc_label,
        traceback_tail_fn=_traceback_tail,
        refresh_overview_report_source_cache_fn=collector_db_mod.refresh_overview_report_source_cache,
        list_campaigns_fn=list_campaigns,
        list_adgroups_fn=list_adgroups,
        list_keywords_fn=list_keywords,
        list_ads_fn=list_ads,
        is_shopping_campaign_obj_fn=is_shopping_campaign_obj,
        skip_keyword_stats=SKIP_KEYWORD_STATS,
        skip_ad_stats=SKIP_AD_STATS,
        log_fn=log,
    )


def build_main_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--customer_id", type=str, default="")
    parser.add_argument("--account_name", type=str, default="", help="단일 업체명 또는 일부 문자열")
    parser.add_argument("--account_names", type=str, default="", help="쉼표(,)로 구분한 여러 업체명")
    parser.add_argument("--skip_dim", action="store_true")
    parser.add_argument("--fast", action="store_true", help="빠른 수집 모드: skip_dim 강제, debug 저장 및 live keyword API fallback 비활성화")
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--collect_mode", type=str, default="sa_with_device", help="sa_only/device_only/sa_with_device 또는 검색광고 전체만/기기만/검색광고 전체+기기")
    parser.add_argument("--sa_scope", type=str, default="full", help="full/ad_only 또는 전체/소재만")
    parser.add_argument("--shopping_only", action="store_true", help="쇼핑검색 캠페인만 수집/재적재")
    parser.add_argument("--include_gfa_accounts", action="store_true", help="이름 끝이 GFA 인 네이버 GFA 계정도 함께 대상으로 포함")
    return parser


def resolve_target_date(raw_date: str) -> date:
    if raw_date:
        return datetime.strptime(raw_date, "%Y-%m-%d").date()
    return (datetime.utcnow() + timedelta(hours=9)).date() - timedelta(days=1)


def emit_main_run_banner(target_date: date, args: argparse.Namespace):
    print("\n" + "=" * 50, flush=True)
    print(f"🚀🚀🚀 [ 현재 수집 진행 날짜: {target_date} ] 🚀🚀🚀", flush=True)
    print("=" * 50, flush=True)
    if FAST_MODE:
        print("⚡ 빠른 수집 모드: 구조 수집 스킵 / debug 저장 중지 / live keyword API fallback 비활성화", flush=True)
    print(f"🧭 수집 모드: {label_collect_mode(args.collect_mode)} ({args.collect_mode})", flush=True)
    print(f"🎯 검색광고 수집 범위: {label_sa_scope(args.sa_scope)} ({args.sa_scope})", flush=True)
    if args.shopping_only:
        print("🛍️ 쇼핑검색 전용 수집", flush=True)
    print("=" * 50 + "\n", flush=True)


def load_accounts_from_legacy_sheet(include_gfa_accounts: bool) -> List[Dict[str, str]]:
    if not os.path.exists("accounts.xlsx"):
        return []

    try:
        df_acc = pd.read_excel("accounts.xlsx")
    except Exception as first_exc:
        log(f"⚠️ accounts.xlsx 엑셀 로드 실패, CSV 재시도: {_exc_label(first_exc)}")
        try:
            df_acc = pd.read_csv("accounts.xlsx")
        except Exception as second_exc:
            _log_best_effort_failure("accounts.xlsx 로드", second_exc, ctx="loader=csv_fallback")
            return []

    id_col, name_col = None, None
    for c in df_acc.columns:
        c_clean = str(c).replace(" ", "").lower()
        if c_clean in ["커스텀id", "customerid", "customer_id", "id"]:
            id_col = c
        if c_clean in ["업체명", "accountname", "account_name", "name"]:
            name_col = c

    if not id_col or not name_col:
        return []

    accounts_info: List[Dict[str, str]] = []
    seen_ids = set()
    for _, row in df_acc.iterrows():
        cid = str(row[id_col]).strip()
        nm = str(row[name_col]).strip()
        if not include_gfa_accounts and nm.lower().endswith(" gfa"):
            continue
        if cid and cid.lower() != "nan" and cid not in seen_ids:
            accounts_info.append({"id": cid, "name": nm})
            seen_ids.add(cid)
    return accounts_info


def load_accounts_from_db(engine: Engine) -> List[Dict[str, str]]:
    try:
        with engine.connect() as conn:
            return [
                {"id": str(row[0]).strip(), "name": str(row[1])}
                for row in conn.execute(text("SELECT customer_id, MAX(account_name) FROM accounts WHERE customer_id IS NOT NULL GROUP BY customer_id"))
            ]
    except Exception as e:
        _log_best_effort_failure("accounts DB 로드", e)
        return []


def resolve_accounts_info(engine: Engine, args: argparse.Namespace) -> List[Dict[str, str]]:
    if args.customer_id:
        return [{"id": args.customer_id, "name": "Target Account"}]

    accounts_info: List[Dict[str, str]] = []
    if load_naver_accounts is not None:
        try:
            accounts_info = load_naver_accounts(include_gfa=args.include_gfa_accounts)
            if accounts_info:
                excluded_msg = "포함" if args.include_gfa_accounts else "제외"
                log(f"🗂️ account_master 기준 계정 로드: {len(accounts_info)}개 (GFA {excluded_msg})")
        except Exception as e:
            log(f"⚠️ account_master 로드 실패, 레거시 accounts.xlsx 로 폴백합니다: {e}")

    if not accounts_info:
        accounts_info = load_accounts_from_legacy_sheet(args.include_gfa_accounts)

    if not accounts_info:
        accounts_info = load_accounts_from_db(engine)

    return accounts_info


def apply_account_name_filters(accounts_info: List[Dict[str, str]], args: argparse.Namespace) -> List[Dict[str, str]]:
    target_name_tokens: List[str] = []
    if args.account_name and str(args.account_name).strip():
        target_name_tokens.append(str(args.account_name).strip())
    if args.account_names and str(args.account_names).strip():
        target_name_tokens.extend([x.strip() for x in str(args.account_names).split(",") if x.strip()])

    if not target_name_tokens:
        return accounts_info

    exact_set = {x for x in target_name_tokens}
    filtered_exact = [acc for acc in accounts_info if str(acc.get("name", "")).strip() in exact_set]
    if filtered_exact:
        filtered = filtered_exact
    else:
        lowered = [x.lower() for x in target_name_tokens]
        filtered = [
            acc for acc in accounts_info
            if any(tok in str(acc.get("name", "")).lower() for tok in lowered)
        ]
    log(f"🎯 업체명 필터 적용: {', '.join(target_name_tokens)} -> {len(filtered)}개")
    return filtered


def dedupe_accounts_info(accounts_info: List[Dict[str, str]]) -> List[Dict[str, str]]:
    unique_accounts: Dict[str, Dict[str, str]] = {}
    for acc in accounts_info:
        acc_id = str(acc.get("id") or "").strip()
        if not acc_id:
            continue
        unique_accounts[acc_id] = acc
    return list(unique_accounts.values())


def build_future_error_result(error: Exception, target_date: date, args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "customer_id": "",
        "account_name": "(future-error)",
        "target_date": str(target_date),
        "status": "error",
        "error": str(error),
        "collect_mode": args.collect_mode,
        "collect_mode_label": label_collect_mode(args.collect_mode),
        "sa_scope": args.sa_scope,
        "sa_scope_label": label_sa_scope(args.sa_scope),
        "collect_sa": args.collect_mode in {"sa_only", "sa_with_device"},
        "collect_device": args.collect_mode in {"device_only", "sa_with_device"},
        "shopping_only": bool(args.shopping_only),
        "campaign_rows_saved": 0,
        "keyword_rows_saved": 0,
        "ad_rows_saved": 0,
        "device_campaign_rows_saved": 0,
        "device_ad_rows_saved": 0,
        "media_rows_saved": 0,
        "ad_report_status": "unknown",
        "split_attempted": False,
        "split_report_ok": False,
        "used_realtime_fallback": False,
        "realtime_reason": "",
        "device_status": "unknown",
        "media_source": "unknown",
        "zero_data": False,
    }


def run_account_collection_tasks(engine: Engine, accounts_info: List[Dict[str, str]], target_date: date, args: argparse.Namespace) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                process_account,
                engine,
                acc["id"],
                acc["name"],
                target_date,
                args.skip_dim,
                args.fast,
                args.collect_mode,
                args.sa_scope,
                args.shopping_only,
            )
            for acc in accounts_info
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                results.append(build_future_error_result(e, target_date, args))
    return results


def main():
    parser = build_main_arg_parser()
    args = parser.parse_args()

    try:
        args.collect_mode = normalize_collect_mode(args.collect_mode)
        args.sa_scope = normalize_sa_scope(args.sa_scope)
    except ValueError as e:
        parser.error(str(e))

    global FAST_MODE
    FAST_MODE = bool(args.fast)
    if FAST_MODE:
        os.environ["COLLECTOR_FAST_MODE"] = "1"
    else:
        os.environ.pop("COLLECTOR_FAST_MODE", None)
    if FAST_MODE:
        args.skip_dim = True

    target_date = resolve_target_date(args.date)
    emit_main_run_banner(target_date, args)

    try:
        engine = get_engine()
        ensure_tables(engine)
    except Exception as e:
        die(f"DB 초기화 실패: {_exc_label(e)}")

    accounts_info = resolve_accounts_info(engine, args)
    accounts_info = apply_account_name_filters(accounts_info, args)
    accounts_info = dedupe_accounts_info(accounts_info)

    if not accounts_info:
        log("⚠️ 수집할 계정이 없습니다.")
        return

    log(f"📋 최종 수집 대상 계정: {len(accounts_info)}개 / 동시 작업: {args.workers}개")
    results = run_account_collection_tasks(engine, accounts_info, target_date, args)
    emit_collection_run_summary(results, target_date, args.collect_mode, args.shopping_only, args.sa_scope)


if __name__ == "__main__":
    main()
