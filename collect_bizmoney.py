# -*- coding: utf-8 -*-
"""collect_bizmoney.py - 네이버 비즈머니(잔액) 전용 수집기

원칙
- 기본은 계정별 개별 수집 (bizmoney_mode=separate)
- 실제 공유 잔액인 경우에만 shared 그룹 단위로 1회 조회 후 멤버 계정에 반영
"""
from __future__ import annotations

import os
import re
import sys
import time
import hmac
import base64
import hashlib
import concurrent.futures
from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Callable

import requests
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.pool import QueuePool

from account_master import load_bizmoney_targets, load_naver_accounts

GFA_IMPORT_ERROR: Optional[Exception] = None
try:
    from collector_gfa import (  # type: ignore
        GFA_API_VERSION,
        GFA_HEADLESS,
        GFA_ID,
        GFA_PW,
        GfaApiClient,
        GfaCrawler,
        get_access_token_if_available,
    )
except Exception as exc:  # pragma: no cover - optional fallback path
    GFA_IMPORT_ERROR = exc
    GFA_API_VERSION = "1.0"
    GFA_HEADLESS = True
    GFA_ID = ""
    GFA_PW = ""
    GfaApiClient = None  # type: ignore
    GfaCrawler = None  # type: ignore
    get_access_token_if_available = None  # type: ignore

load_dotenv(override=True)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
ACCOUNT_MASTER_FILE = (os.getenv("ACCOUNT_MASTER_FILE") or "account_master.xlsx").strip()
BASE_URL = "https://api.searchad.naver.com"
DB_MAX_RETRIES = max(1, int(os.getenv("BIZMONEY_DB_MAX_RETRIES", "3") or 3))
DB_RETRY_BASE_SEC = max(1.0, float(os.getenv("BIZMONEY_DB_RETRY_BASE_SEC", "2") or 2))


def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def die(msg: str):
    log(f"❌ FATAL: {msg}")
    sys.exit(1)


if not DB_URL:
    die("DATABASE_URL이 설정되지 않았습니다.")



def _exc_label(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"



def _dispose_engine_quietly(engine: Engine):
    try:
        engine.dispose()
    except Exception:
        pass



def _safe_rollback(raw_conn):
    if not raw_conn:
        return
    try:
        raw_conn.rollback()
    except Exception:
        pass



def _safe_close(resource):
    if not resource:
        return
    try:
        resource.close()
    except Exception:
        pass



def _sleep_backoff(attempt: int):
    time.sleep(min(DB_RETRY_BASE_SEC * attempt, 8.0))



def _is_retryable_db_error(exc: Exception) -> bool:
    if isinstance(exc, OperationalError):
        return True
    if isinstance(exc, DBAPIError) and getattr(exc, "connection_invalidated", False):
        return True
    msg = str(exc).lower()
    retry_tokens = [
        "ssl connection has been closed unexpectedly",
        "ssl syscall error",
        "connection timed out",
        "server closed the connection unexpectedly",
        "could not receive data from server",
        "connection already closed",
        "connection not open",
        "terminating connection",
        "connection reset by peer",
        "broken pipe",
    ]
    return any(token in msg for token in retry_tokens)



def _run_db_op(engine: Engine, label: str, fn: Callable[[], Any]):
    last_exc: Exception | None = None
    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            log(f"⚠️ {label} 실패 {attempt}/{DB_MAX_RETRIES} | {_exc_label(exc)}")
            if attempt >= DB_MAX_RETRIES or not _is_retryable_db_error(exc):
                raise
            _dispose_engine_quietly(engine)
            _sleep_backoff(attempt)
    if last_exc is not None:
        raise last_exc



def get_header(method: str, uri: str, customer_id: str) -> Dict[str, str]:
    timestamp = str(int(time.time() * 1000))
    sig = hmac.new(
        API_SECRET.encode("utf-8"),
        f"{timestamp}.{method}.{uri}".encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": timestamp,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": base64.b64encode(sig).decode("utf-8"),
    }



def get_bizmoney(customer_id: str) -> Tuple[Optional[int], Optional[dict]]:
    uri = "/billing/bizmoney"
    for attempt in range(3):
        try:
            r = requests.get(BASE_URL + uri, headers=get_header("GET", uri, customer_id), timeout=20)
            if r.status_code == 403:
                return None, None
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 + attempt)
                continue
            if r.status_code != 200:
                return None, None
            data = r.json()
            total_balance = 0
            if isinstance(data, dict):
                for key in ["bizmoney", "freeBizmoney", "couponBizmoney", "prepaidBizmoney", "bizCoupon"]:
                    total_balance += int(data.get(key, 0) or 0)
            return total_balance, data
        except Exception:
            time.sleep(2 + attempt)
    return None, None



def _clean_account_id(value: Any) -> str:
    s = str(value or "").strip()
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return s



def _target_media_type(target: Dict[str, Any]) -> str:
    rep = target.get("representative") or {}
    return str(rep.get("media_type") or "sa").strip().lower()



_BALANCE_KEY_EXCLUDES = {
    "budget",
    "bid",
    "cost",
    "spend",
    "spent",
    "sales",
    "revenue",
    "roas",
    "click",
    "impression",
    "conversion",
    "tax",
    "vat",
    "price",
}
_BALANCE_TOTAL_TOKENS = {
    "bizmoney",
    "businessmoney",
    "balance",
    "cashbalance",
    "remaincash",
    "remainingcash",
    "availablecash",
    "accountbalance",
    "adaccountbalance",
    "totalcash",
    "totalmoney",
}
_BALANCE_COMPONENT_TOKENS = {
    "freebizmoney",
    "couponbizmoney",
    "prepaidbizmoney",
    "paidbizmoney",
    "freecash",
    "couponcash",
    "prepaidcash",
    "paidcash",
    "bizcoupon",
}



def _norm_balance_key(key: Any) -> str:
    return re.sub(r"[^0-9a-z가-힣]+", "", str(key or "").strip().lower())



def _parse_balance_amount(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    match = re.search(r"-?\d[\d,]*(?:\.\d+)?", s.replace(" ", ""))
    if not match:
        return None
    try:
        return int(float(match.group(0).replace(",", "")))
    except Exception:
        return None



def _is_balance_key(key: Any, path: List[str]) -> bool:
    norm = _norm_balance_key(key)
    path_norm = [_norm_balance_key(p) for p in path]
    if not norm:
        return False
    if any(token in norm for token in _BALANCE_KEY_EXCLUDES):
        return False
    korean_hit = any(token in norm for token in ["비즈머니", "잔액", "캐시", "충전금", "보유금액"])
    token_hit = (
        norm in _BALANCE_TOTAL_TOKENS
        or norm in _BALANCE_COMPONENT_TOKENS
        or any(token in norm for token in ["bizmoney", "balance", "remain", "remaining", "available", "cash"])
    )
    context_hit = norm in {"amount", "value", "total", "totalamount"} and any(
        p in _BALANCE_TOTAL_TOKENS or any(token in p for token in ["bizmoney", "balance", "cash", "잔액", "비즈머니"])
        for p in path_norm
    )
    return bool(korean_hit or token_hit or context_hit)



def _is_balance_component_key(key: Any) -> bool:
    norm = _norm_balance_key(key)
    return norm in _BALANCE_COMPONENT_TOKENS or any(token in norm for token in ["coupon", "prepaid", "freecash", "paidcash"])



def _balance_priority(key: str) -> int:
    norm = _norm_balance_key(key)
    if any(token in norm for token in ["balance", "remain", "remaining", "available", "잔액"]):
        return 0
    if any(token in norm for token in ["bizmoney", "비즈머니"]):
        return 1
    if any(token in norm for token in ["cash", "money", "캐시"]):
        return 2
    return 3



def _extract_balance_from_obj(obj: Any) -> Optional[int]:
    totals: List[Tuple[int, str, int]] = []
    components: List[int] = []

    def walk(cur: Any, path: List[str]) -> None:
        if isinstance(cur, dict):
            for key, value in cur.items():
                amount = _parse_balance_amount(value)
                if amount is not None and _is_balance_key(key, path):
                    if _is_balance_component_key(key):
                        components.append(amount)
                    else:
                        totals.append((_balance_priority(str(key)), str(key), amount))
                if isinstance(value, (dict, list)):
                    walk(value, [*path, str(key)])
            return
        if isinstance(cur, list):
            for item in cur:
                if isinstance(item, (dict, list)):
                    walk(item, path)

    walk(obj, [])
    if totals:
        totals.sort(key=lambda x: (x[0], len(x[1])))
        return totals[0][2]
    if components:
        return sum(components)
    return None



def _extract_gfa_balance_from_text(text_value: str) -> Optional[int]:
    text_value = str(text_value or "")
    if not text_value.strip():
        return None
    patterns = [
        r"(?:비즈\s*머니|비즈머니|잔액|보유\s*금액|충전금|캐시)[\s\S]{0,80}?(-?\d[\d,]*(?:\.\d+)?)\s*원",
        r"(-?\d[\d,]*(?:\.\d+)?)\s*원[\s\S]{0,80}?(?:비즈\s*머니|비즈머니|잔액|보유\s*금액|충전금|캐시)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text_value, flags=re.I)
        if match:
            return _parse_balance_amount(match.group(1))
    return None



def _make_gfa_api_client() -> Optional[Any]:
    if GFA_IMPORT_ERROR is not None or GfaApiClient is None or get_access_token_if_available is None:
        if GFA_IMPORT_ERROR is not None:
            log(f"⚠️ GFA 모듈 로드 실패로 API 조회를 건너뜀: {_exc_label(GFA_IMPORT_ERROR)}")
        return None
    try:
        access_token = get_access_token_if_available()
    except Exception as exc:
        log(f"⚠️ GFA access token 준비 실패: {_exc_label(exc)}")
        return None
    if not access_token:
        return None
    return GfaApiClient(access_token)



def _match_gfa_account_item(client: Any, customer_id: str) -> Optional[Dict[str, Any]]:
    try:
        for item in client.list_ad_accounts():
            if not isinstance(item, dict):
                continue
            values = [
                item.get("adAccountNo"),
                item.get("accountNo"),
                item.get("no"),
                item.get("id"),
            ]
            if any(_clean_account_id(v) == _clean_account_id(customer_id) for v in values):
                return item
    except Exception as exc:
        log(f"⚠️ GFA 광고계정 목록 조회 실패: {_exc_label(exc)}")
    return None



def get_gfa_bizmoney_api(customer_id: str, client: Optional[Any]) -> Tuple[Optional[int], Optional[Any]]:
    if client is None:
        return None, None

    item = _match_gfa_account_item(client, customer_id)
    balance = _extract_balance_from_obj(item) if item else None
    if balance is not None:
        return balance, item

    manager_no: Optional[str] = None
    try:
        manager_no = client.resolve_manager_account_no(customer_id)
    except Exception as exc:
        log(f"⚠️ GFA 관리계정 판별 실패 | {customer_id}: {_exc_label(exc)}")

    detail: Any = None
    if manager_no:
        try:
            detail = client.request_json(
                "GET",
                f"/v1/ad-api/{GFA_API_VERSION}/adAccounts/{customer_id}",
                access_manager_account_no=manager_no,
                raise_for_status=False,
            )
            balance = _extract_balance_from_obj(detail)
            if balance is not None:
                return balance, detail
        except Exception as exc:
            log(f"⚠️ GFA 광고계정 상세 조회 실패 | {customer_id}: {_exc_label(exc)}")
    return None, detail or item



def _extract_gfa_balance_from_page(crawler: Any) -> Optional[int]:
    body = ""
    try:
        body = crawler.page.locator("body").inner_text(timeout=5000)
    except Exception:
        try:
            body = crawler.page.content()
        except Exception:
            body = ""
    return _extract_gfa_balance_from_text(body)



def get_gfa_bizmoney_via_crawl(targets: List[Dict[str, Any]]) -> Dict[str, int]:
    balances: Dict[str, int] = {}
    if not targets:
        return balances
    if GfaCrawler is None or GFA_IMPORT_ERROR is not None:
        log("⚪ GFA 크롤러를 사용할 수 없어 GFA 비즈머니 화면 조회를 건너뜀")
        return balances
    if not GFA_ID or not GFA_PW:
        log("⚪ GFA_ID/GFA_PW가 없어 GFA 비즈머니 화면 조회를 건너뜀")
        return balances

    try:
        with GfaCrawler(headless=GFA_HEADLESS) as crawler:
            crawler.login(GFA_ID, GFA_PW)
            crawler.go_gfa_platform()
            for target in targets:
                rep = target["representative"]
                customer_id = _clean_account_id(rep["id"])
                account_name = str(rep.get("name") or customer_id)
                try:
                    crawler.go_gfa_platform()
                    crawler.select_account(customer_id, account_name)
                    crawler.wait_network_idle(3)
                    balance = _extract_gfa_balance_from_page(crawler)
                    if balance is None:
                        log(f"⚪ [GFA:{account_name}] 화면에서 비즈머니 잔액 텍스트를 찾지 못함")
                        continue
                    balances[customer_id] = int(balance)
                except Exception as exc:
                    log(f"⚪ [GFA:{account_name}] 화면 비즈머니 조회 실패: {_exc_label(exc)}")
    except Exception as exc:
        log(f"⚠️ GFA 화면 비즈머니 조회 초기화 실패: {_exc_label(exc)}")
    return balances



def get_engine() -> Engine:
    db_url = DB_URL
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    pool_size = max(1, int(os.getenv("BIZMONEY_DB_POOL_SIZE", "3") or 3))
    max_overflow = max(0, int(os.getenv("BIZMONEY_DB_MAX_OVERFLOW", "6") or 6))
    pool_timeout = max(5, int(os.getenv("BIZMONEY_DB_POOL_TIMEOUT", "30") or 30))
    pool_recycle = max(60, int(os.getenv("BIZMONEY_DB_POOL_RECYCLE", "1800") or 1800))
    return create_engine(
        db_url,
        poolclass=QueuePool,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
        pool_reset_on_return="rollback",
        use_native_hstore=False,
        connect_args={
            "connect_timeout": 15,
            "options": "-c lock_timeout=10000 -c statement_timeout=300000",
        },
        future=True,
    )



def _verify_connection(engine: Engine):
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))



def upsert_dim_account_meta_bulk(engine: Engine, accounts: List[Dict[str, str]]):
    if not accounts:
        return

    def ensure_meta_schema():
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_account_meta (
                    customer_id TEXT PRIMARY KEY,
                    account_name TEXT,
                    manager TEXT,
                    monthly_budget BIGINT DEFAULT 0,
                    platform TEXT,
                    naver_media_type TEXT,
                    bizmoney_group_key TEXT,
                    bizmoney_mode TEXT,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """))
            for ddl in [
                "ALTER TABLE dim_account_meta ADD COLUMN IF NOT EXISTS platform TEXT",
                "ALTER TABLE dim_account_meta ADD COLUMN IF NOT EXISTS naver_media_type TEXT",
                "ALTER TABLE dim_account_meta ADD COLUMN IF NOT EXISTS bizmoney_group_key TEXT",
                "ALTER TABLE dim_account_meta ADD COLUMN IF NOT EXISTS bizmoney_mode TEXT",
                "ALTER TABLE dim_account_meta ADD COLUMN IF NOT EXISTS monthly_budget BIGINT DEFAULT 0",
            ]:
                try:
                    conn.execute(text(ddl))
                except Exception:
                    pass

    _run_db_op(engine, "dim_account_meta 스키마 보장", ensure_meta_schema)

    sql = """
        INSERT INTO dim_account_meta (
            customer_id, account_name, manager, platform, naver_media_type, bizmoney_group_key, bizmoney_mode
        )
        VALUES %s
        ON CONFLICT (customer_id) DO UPDATE SET
            account_name = EXCLUDED.account_name,
            manager = EXCLUDED.manager,
            platform = EXCLUDED.platform,
            naver_media_type = EXCLUDED.naver_media_type,
            bizmoney_group_key = EXCLUDED.bizmoney_group_key,
            bizmoney_mode = EXCLUDED.bizmoney_mode,
            updated_at = NOW()
    """
    tuples = [
        (
            a["id"], a["name"], a.get("manager", ""), "naver",
            a.get("media_type", "sa"), a.get("bizmoney_group_key", ""), a.get("bizmoney_mode", "separate")
        )
        for a in accounts
    ]

    def do_upsert():
        raw_conn = None
        cur = None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=1000)
            raw_conn.commit()
        except Exception:
            _safe_rollback(raw_conn)
            raise
        finally:
            _safe_close(cur)
            _safe_close(raw_conn)

    _run_db_op(engine, "dim_account_meta 업서트", do_upsert)



def ensure_fact_tables(engine: Engine):
    def do_ensure():
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_bizmoney_daily (
                    dt DATE,
                    customer_id TEXT,
                    bizmoney_balance BIGINT,
                    bizmoney_group_key TEXT,
                    bizmoney_mode TEXT,
                    source_customer_id TEXT,
                    is_group_representative BOOLEAN DEFAULT FALSE,
                    PRIMARY KEY(dt, customer_id)
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_bizmoney_group_daily (
                    dt DATE,
                    bizmoney_group_key TEXT,
                    representative_customer_id TEXT,
                    bizmoney_balance BIGINT,
                    bizmoney_mode TEXT,
                    PRIMARY KEY(dt, bizmoney_group_key)
                )
            """))
            for ddl in [
                "ALTER TABLE fact_bizmoney_daily ADD COLUMN IF NOT EXISTS bizmoney_group_key TEXT",
                "ALTER TABLE fact_bizmoney_daily ADD COLUMN IF NOT EXISTS bizmoney_mode TEXT",
                "ALTER TABLE fact_bizmoney_daily ADD COLUMN IF NOT EXISTS source_customer_id TEXT",
                "ALTER TABLE fact_bizmoney_daily ADD COLUMN IF NOT EXISTS is_group_representative BOOLEAN DEFAULT FALSE",
                "ALTER TABLE fact_bizmoney_group_daily ADD COLUMN IF NOT EXISTS bizmoney_mode TEXT",
            ]:
                try:
                    conn.execute(text(ddl))
                except Exception:
                    pass

    _run_db_op(engine, "fact_bizmoney 스키마 보장", do_ensure)



def upsert_bizmoney_bulk(engine: Engine, account_rows: List[Dict[str, Any]], group_rows: List[Dict[str, Any]]):
    ensure_fact_tables(engine)
    if account_rows:
        df = pd.DataFrame(account_rows).drop_duplicates(subset=["dt", "customer_id"], keep="last")
        sql = """
            INSERT INTO fact_bizmoney_daily (
                dt, customer_id, bizmoney_balance, bizmoney_group_key, bizmoney_mode, source_customer_id, is_group_representative
            )
            VALUES %s
            ON CONFLICT (dt, customer_id) DO UPDATE SET
                bizmoney_balance = EXCLUDED.bizmoney_balance,
                bizmoney_group_key = EXCLUDED.bizmoney_group_key,
                bizmoney_mode = EXCLUDED.bizmoney_mode,
                source_customer_id = EXCLUDED.source_customer_id,
                is_group_representative = EXCLUDED.is_group_representative
        """
        tuples = list(df.itertuples(index=False, name=None))

        def upsert_account_rows():
            raw_conn = None
            cur = None
            try:
                raw_conn = engine.raw_connection()
                cur = raw_conn.cursor()
                psycopg2.extras.execute_values(cur, sql, tuples, page_size=1000)
                raw_conn.commit()
            except Exception:
                _safe_rollback(raw_conn)
                raise
            finally:
                _safe_close(cur)
                _safe_close(raw_conn)

        _run_db_op(engine, "fact_bizmoney_daily 업서트", upsert_account_rows)

    if group_rows:
        df = pd.DataFrame(group_rows).drop_duplicates(subset=["dt", "bizmoney_group_key"], keep="last")
        sql = """
            INSERT INTO fact_bizmoney_group_daily (
                dt, bizmoney_group_key, representative_customer_id, bizmoney_balance, bizmoney_mode
            )
            VALUES %s
            ON CONFLICT (dt, bizmoney_group_key) DO UPDATE SET
                representative_customer_id = EXCLUDED.representative_customer_id,
                bizmoney_balance = EXCLUDED.bizmoney_balance,
                bizmoney_mode = EXCLUDED.bizmoney_mode
        """
        tuples = list(df.itertuples(index=False, name=None))

        def upsert_group_rows():
            raw_conn = None
            cur = None
            try:
                raw_conn = engine.raw_connection()
                cur = raw_conn.cursor()
                psycopg2.extras.execute_values(cur, sql, tuples, page_size=1000)
                raw_conn.commit()
            except Exception:
                _safe_rollback(raw_conn)
                raise
            finally:
                _safe_close(cur)
                _safe_close(raw_conn)

        _run_db_op(engine, "fact_bizmoney_group_daily 업서트", upsert_group_rows)



def main():
    engine = get_engine()
    _run_db_op(engine, "DB 연결 확인", lambda: _verify_connection(engine))

    all_naver_accounts = load_naver_accounts(file_path=ACCOUNT_MASTER_FILE, include_gfa=True, media_types=["sa", "gfa"])
    targets = load_bizmoney_targets(file_path=ACCOUNT_MASTER_FILE)

    if not all_naver_accounts:
        log(f"⚠️ 수집할 네이버 계정이 없습니다. ACCOUNT_MASTER_FILE={ACCOUNT_MASTER_FILE}")
        return

    upsert_dim_account_meta_bulk(engine, all_naver_accounts)

    separate_count = sum(1 for t in targets if t.get("bizmoney_mode") == "separate")
    shared_count = sum(1 for t in targets if t.get("bizmoney_mode") == "shared")
    searchad_targets = [t for t in targets if _target_media_type(t) != "gfa"]
    gfa_targets = [t for t in targets if _target_media_type(t) == "gfa"]

    if searchad_targets and (not API_KEY or not API_SECRET):
        die("SearchAd 비즈머니 대상이 있지만 API_KEY 또는 API_SECRET이 설정되지 않았습니다.")

    log(
        f"📋 비즈머니 수집 시작: 계정 {len(all_naver_accounts)}개 / 대상 {len(targets)}개 "
        f"(separate={separate_count}, shared={shared_count}, searchad={len(searchad_targets)}, gfa={len(gfa_targets)})"
    )

    today = date.today()
    account_rows: List[Dict[str, Any]] = []
    group_rows: List[Dict[str, Any]] = []

    def append_target_rows(target: Dict[str, Any], balance: int, source_label: str):
        rep = target["representative"]
        mode = target.get("bizmoney_mode", "separate")
        key = target.get("bizmoney_group_key") or rep.get("bizmoney_group_key") or rep["name"]
        members = target.get("members", [rep])
        source_suffix = f" / {source_label}" if source_label else ""
        if mode == "shared":
            group_rows.append({
                "dt": today,
                "bizmoney_group_key": key,
                "representative_customer_id": rep["id"],
                "bizmoney_balance": int(balance),
                "bizmoney_mode": "shared",
            })
            for m in members:
                account_rows.append({
                    "dt": today,
                    "customer_id": m["id"],
                    "bizmoney_balance": int(balance),
                    "bizmoney_group_key": key,
                    "bizmoney_mode": "shared",
                    "source_customer_id": rep["id"],
                    "is_group_representative": m["id"] == rep["id"],
                })
            log(f"💰 [공유:{key}] {len(members)}개 계정 반영 / 대표={rep['name']} / 잔액={balance:,}원{source_suffix}")
            return

        account_rows.append({
            "dt": today,
            "customer_id": rep["id"],
            "bizmoney_balance": int(balance),
            "bizmoney_group_key": key,
            "bizmoney_mode": "separate",
            "source_customer_id": rep["id"],
            "is_group_representative": True,
        })
        log(f"💰 [개별:{rep['name']}] 잔액={balance:,}원{source_suffix}")

    def task(target: Dict[str, Any]):
        rep = target["representative"]
        balance, raw = get_bizmoney(rep["id"])
        return target, balance, raw

    if searchad_targets:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(task, target): target for target in searchad_targets}
            for future in concurrent.futures.as_completed(futures):
                target, balance, _raw = future.result()
                rep = target["representative"]
                if balance is None:
                    log(f"⚪ [{rep['name']}] 비즈머니 조회 불가/권한없음")
                    continue
                append_target_rows(target, int(balance), "SearchAd")

    if gfa_targets:
        gfa_api_client = _make_gfa_api_client()
        gfa_crawl_targets: List[Dict[str, Any]] = []
        if gfa_api_client is not None:
            log("🔐 GFA 비즈머니 API 조회를 시도합니다.")
        elif GFA_ID and GFA_PW:
            log("🔐 GFA API 토큰이 없어 화면 조회로 비즈머니 잔액을 확인합니다.")
        else:
            log("⚠️ GFA 인증 정보가 없어 GFA 비즈머니 잔액 조회를 건너뜁니다.")

        for target in gfa_targets:
            rep = target["representative"]
            balance, _raw = get_gfa_bizmoney_api(rep["id"], gfa_api_client)
            if balance is None:
                gfa_crawl_targets.append(target)
                continue
            append_target_rows(target, int(balance), "GFA API")

        if gfa_crawl_targets:
            crawl_balances = get_gfa_bizmoney_via_crawl(gfa_crawl_targets)
            for target in gfa_crawl_targets:
                rep = target["representative"]
                balance = crawl_balances.get(_clean_account_id(rep["id"]))
                if balance is None:
                    log(f"⚪ [GFA:{rep['name']}] 비즈머니 조회 불가/권한없음")
                    continue
                append_target_rows(target, int(balance), "GFA 화면")

    upsert_bizmoney_bulk(engine, account_rows, group_rows)
    log(f"✅ 완료: 계정 적재 {len(account_rows)}건 / 그룹 적재 {len(group_rows)}건")


if __name__ == "__main__":
    main()
