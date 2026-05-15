# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from account_master import load_naver_accounts

load_dotenv(override=False)

DB_URL = (os.getenv("DATABASE_URL") or "").strip()
ACCOUNT_MASTER_FILE = (os.getenv("ACCOUNT_MASTER_FILE") or "account_master.xlsx").strip()

GFA_ACCESS_TOKEN = (os.getenv("GFA_ACCESS_TOKEN") or "").strip()
GFA_REFRESH_TOKEN = (os.getenv("GFA_REFRESH_TOKEN") or "").strip()
GFA_CLIENT_ID = (os.getenv("GFA_CLIENT_ID") or "").strip()
GFA_CLIENT_SECRET = (os.getenv("GFA_CLIENT_SECRET") or "").strip()
GFA_MANAGER_ACCOUNT_NO = (os.getenv("GFA_MANAGER_ACCOUNT_NO") or "").strip()
GFA_API_VERSION = (os.getenv("GFA_API_VERSION") or "1.0").strip()

# 크롤링용 네이버 로그인 계정
GFA_ID = (
    os.getenv("GFA_ID")
    or os.getenv("GFA_LOGIN_ID")
    or os.getenv("NAVER_ID")
    or os.getenv("NAVER_LOGIN_ID")
    or ""
).strip()
GFA_PW = (
    os.getenv("GFA_PW")
    or os.getenv("GFA_LOGIN_PW")
    or os.getenv("NAVER_PW")
    or os.getenv("NAVER_PASSWORD")
    or ""
).strip()
GFA_LOGIN_URL = (os.getenv("GFA_LOGIN_URL") or "https://nid.naver.com/nidlogin.login?mode=form&url=https://ads.naver.com/").strip()
ADS_HOME_URL = (os.getenv("ADS_HOME_URL") or "https://ads.naver.com/").strip()
GFA_PLATFORM_URL = (os.getenv("GFA_PLATFORM_URL") or "https://gfa.naver.com/").strip()
GFA_PLATFORM_FALLBACK_URLS = [u.strip() for u in [os.getenv("GFA_PLATFORM_URL_2") or "https://ads.naver.com/gfa", "https://gfa.naver.com/"] if (u or "").strip()]
GFA_HEADLESS = (os.getenv("GFA_HEADLESS") or "true").strip().lower() not in {"0", "false", "no", "n"}
GFA_DEBUG_DIR = Path(os.getenv("GFA_DEBUG_DIR") or "gfa_debug")
GFA_DOWNLOAD_DIR = Path(os.getenv("GFA_DOWNLOAD_DIR") or "gfa_downloads")
GFA_TIMEOUT = int((os.getenv("GFA_TIMEOUT") or "60").strip())
GFA_PLAYWRIGHT_TIMEOUT_MS = int((os.getenv("GFA_PLAYWRIGHT_TIMEOUT_MS") or "40000").strip())

TOKEN_URL = "https://nid.naver.com/oauth2.0/token"
OPENAPI_BASE = "https://openapi.naver.com"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def mask_present(v: str) -> str:
    return "SET" if bool((v or "").strip()) else "EMPTY"


def die(msg: str, code: int = 1) -> None:
    log(f"❌ FATAL: {msg}")
    raise SystemExit(code)


def ensure_dirs() -> None:
    GFA_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    GFA_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


def slugify(value: str) -> str:
    s = re.sub(r"[^0-9A-Za-z가-힣._-]+", "_", str(value or "").strip())
    return s.strip("_") or "item"


def write_debug_text(name: str, content: str) -> None:
    ensure_dirs()
    path = GFA_DEBUG_DIR / name
    path.write_text(content, encoding="utf-8")


def get_engine() -> Engine:
    if not DB_URL:
        die("DATABASE_URL 이 설정되지 않았습니다.")
    db_url = DB_URL
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return create_engine(
        db_url,
        poolclass=NullPool,
        connect_args={"options": "-c lock_timeout=10000 -c statement_timeout=300000"},
        future=True,
    )


def ensure_column(engine: Engine, table: str, column: str, datatype: str) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE {table} ADD COLUMN "{column}" {datatype}'))
    except Exception:
        pass


def ensure_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_campaign (customer_id TEXT, campaign_id TEXT, campaign_name TEXT, campaign_tp TEXT, status TEXT, PRIMARY KEY(customer_id, campaign_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_adgroup (customer_id TEXT, adgroup_id TEXT, adgroup_name TEXT, campaign_id TEXT, status TEXT, PRIMARY KEY(customer_id, adgroup_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_ad (customer_id TEXT, ad_id TEXT, adgroup_id TEXT, ad_name TEXT, status TEXT, ad_title TEXT, ad_desc TEXT, pc_landing_url TEXT, mobile_landing_url TEXT, creative_text TEXT, image_url TEXT, PRIMARY KEY(customer_id, ad_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS fact_campaign_daily (dt DATE, customer_id TEXT, campaign_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, campaign_id))"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS fact_ad_daily (dt DATE, customer_id TEXT, ad_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, ad_id))"))
    for table in ["fact_campaign_daily", "fact_ad_daily"]:
        ensure_column(engine, table, "purchase_conv", "DOUBLE PRECISION")
        ensure_column(engine, table, "purchase_sales", "BIGINT")
        ensure_column(engine, table, "purchase_roas", "DOUBLE PRECISION")
        ensure_column(engine, table, "cart_conv", "DOUBLE PRECISION")
        ensure_column(engine, table, "cart_sales", "BIGINT")
        ensure_column(engine, table, "cart_roas", "DOUBLE PRECISION")
        ensure_column(engine, table, "wishlist_conv", "DOUBLE PRECISION")
        ensure_column(engine, table, "wishlist_sales", "BIGINT")
        ensure_column(engine, table, "wishlist_roas", "DOUBLE PRECISION")
        ensure_column(engine, table, "primary_conv", "DOUBLE PRECISION")
        ensure_column(engine, table, "primary_sales", "BIGINT")
        ensure_column(engine, table, "primary_roas", "DOUBLE PRECISION")
        ensure_column(engine, table, "split_available", "BOOLEAN")
        ensure_column(engine, table, "data_source", "TEXT")


def upsert_many(engine: Engine, table: str, rows: List[Dict[str, Any]], pk_cols: List[str]) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep="last").astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    if update_cols:
        conflict_clause = f'ON CONFLICT ({pk_str}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
    else:
        conflict_clause = f'ON CONFLICT ({pk_str}) DO NOTHING'
    sql = f"INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}"
    tuples = list(df.itertuples(index=False, name=None))
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        psycopg2.extras.execute_values(cur, sql, tuples, page_size=2000)
        raw_conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        raw_conn.close()


def clear_fact_range(engine: Engine, table: str, customer_id: str, target_dt: date) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table} WHERE customer_id = :cid AND dt = :dt"), {"cid": str(customer_id), "dt": target_dt})

def clear_fact_scope(engine: Engine, table: str, customer_id: str, target_dt: date, pk: str, ids: List[str]) -> None:
    ids = [str(x).strip() for x in (ids or []) if str(x).strip()]
    if not ids:
        return
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {table} WHERE customer_id = :cid AND dt = :dt AND {pk} = ANY(:ids)"),
            {"cid": str(customer_id), "dt": target_dt, "ids": ids},
        )


def _fetch_existing_scope_ids(engine: Engine, table: str, customer_id: str, target_dt: date, pk: str, ids: List[str]) -> List[str]:
    ids = [str(x).strip() for x in (ids or []) if str(x).strip()]
    if not ids:
        return []
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT {pk}::text AS scope_id FROM {table} WHERE customer_id = :cid AND dt = :dt AND {pk} = ANY(:ids)"),
            {"cid": str(customer_id), "dt": target_dt, "ids": ids},
        ).fetchall()
    seen = set()
    out: List[str] = []
    for row in rows or []:
        value = str((row[0] if row else '') or '').strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def replace_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, target_dt: date) -> None:
    pk = "campaign_id" if table == "fact_campaign_daily" else "ad_id"
    scope_ids = []
    seen = set()
    for row in rows or []:
        value = str((row or {}).get(pk) or '').strip()
        if value and value not in seen:
            seen.add(value)
            scope_ids.append(value)

    if not scope_ids:
        if not rows:
            clear_fact_range(engine, table, customer_id, target_dt)
            return
        upsert_many(engine, table, rows, ["dt", "customer_id", pk])
        return

    if not rows:
        clear_fact_scope(engine, table, customer_id, target_dt, pk, scope_ids)
        return

    existing_ids = _fetch_existing_scope_ids(engine, table, customer_id, target_dt, pk, scope_ids)
    keep = {str(x).strip() for x in scope_ids if str(x).strip()}
    stale_ids = [str(x).strip() for x in existing_ids if str(x).strip() and str(x).strip() not in keep]
    if stale_ids:
        clear_fact_scope(engine, table, customer_id, target_dt, pk, stale_ids)

    upsert_many(engine, table, rows, ["dt", "customer_id", pk])

def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _to_float(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).replace(",", ""))
        except Exception:
            return default


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s


def _pick_first(d: Dict[str, Any], keys: Iterable[str]) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _pick_id(d: Dict[str, Any], keys: Iterable[str]) -> Optional[str]:
    for k in keys:
        if k in d:
            iv = _to_int(d.get(k))
            if iv is not None:
                return str(iv)
            sv = _to_str(d.get(k))
            if sv:
                return sv
    return None


def _contains_value(obj: Any, target: str) -> bool:
    t = str(target)
    if isinstance(obj, dict):
        return any(_contains_value(v, t) for v in obj.values())
    if isinstance(obj, list):
        return any(_contains_value(v, t) for v in obj)
    return str(obj) == t


def _walk_dicts(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_dicts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_dicts(v)


def _extract_items(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if not isinstance(obj, dict):
        return []
    for key in ["rows", "items", "contents", "content", "data", "list", "result", "results", "campaigns", "adSets", "creatives"]:
        v = obj.get(key)
        if isinstance(v, list) and any(isinstance(x, dict) for x in v):
            return [x for x in v if isinstance(x, dict)]
    for d in _walk_dicts(obj):
        for v in d.values():
            if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
                return v
    return []


# ------------------------------
# API mode (if secrets exist)
# ------------------------------

def get_access_token_if_available() -> Optional[str]:
    if GFA_ACCESS_TOKEN:
        return GFA_ACCESS_TOKEN
    if GFA_REFRESH_TOKEN and GFA_CLIENT_ID and GFA_CLIENT_SECRET:
        params = {
            "grant_type": "refresh_token",
            "client_id": GFA_CLIENT_ID,
            "client_secret": GFA_CLIENT_SECRET,
            "refresh_token": GFA_REFRESH_TOKEN,
        }
        r = requests.get(TOKEN_URL, params=params, timeout=GFA_TIMEOUT)
        data: Dict[str, Any] = {}
        try:
            data = r.json()
        except Exception:
            pass
        if r.status_code >= 400 or not data.get("access_token"):
            raise RuntimeError(f"GFA access token 갱신 실패: {r.status_code} {data or r.text}")
        return str(data["access_token"])
    return None


class GfaApiClient:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.session = requests.Session()

    def request_json(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None, access_manager_account_no: Optional[str] = None, raise_for_status: bool = True) -> Any:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        if access_manager_account_no:
            headers["AccessManagerAccountNo"] = str(access_manager_account_no)
        url = OPENAPI_BASE + path
        last_err: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = self.session.request(method.upper(), url, headers=headers, params=params, timeout=GFA_TIMEOUT)
                try:
                    data: Any = r.json()
                except Exception:
                    data = r.text
                if r.status_code >= 400:
                    if raise_for_status:
                        raise RuntimeError(f"{r.status_code} {data}")
                    return {"_status": r.status_code, "_data": data}
                return data
            except Exception as e:
                last_err = e
                if attempt < 3:
                    time.sleep(1.5 + attempt)
                    continue
                if raise_for_status:
                    raise
                return {"_status": 0, "_data": str(last_err)}
        if last_err:
            raise last_err
        raise RuntimeError("unexpected request failure")

    def list_ad_accounts(self) -> List[Dict[str, Any]]:
        page = 0
        out: List[Dict[str, Any]] = []
        while True:
            data = self.request_json("GET", f"/v1/ad-api/{GFA_API_VERSION}/adAccounts", params={"page": page, "size": 100}, raise_for_status=False)
            items = _extract_items(data)
            if not items:
                break
            out.extend(items)
            if len(items) < 100:
                break
            page += 1
        return out

    def list_manager_accounts(self) -> List[Dict[str, Any]]:
        page = 0
        out: List[Dict[str, Any]] = []
        while True:
            data = self.request_json("GET", f"/v1/ad-api/{GFA_API_VERSION}/managerAccounts", params={"page": page, "size": 100}, raise_for_status=False)
            items = _extract_items(data)
            if not items:
                break
            out.extend(items)
            if len(items) < 100:
                break
            page += 1
        return out

    def resolve_manager_account_no(self, ad_account_no: str) -> str:
        if GFA_MANAGER_ACCOUNT_NO:
            return GFA_MANAGER_ACCOUNT_NO
        ad_accounts = self.list_ad_accounts()
        for item in ad_accounts:
            acc_no = _pick_id(item, ["adAccountNo", "accountNo", "no", "id"])
            if acc_no == str(ad_account_no):
                mgr = _pick_id(item, ["accessManagerAccountNo", "managerAccountNo", "managerNo", "parentManagerAccountNo"])
                if mgr:
                    return mgr
        managers = self.list_manager_accounts()
        manager_nos: List[str] = []
        for item in managers:
            mgr = _pick_id(item, ["managerAccountNo", "accessManagerAccountNo", "no", "id"])
            if mgr:
                manager_nos.append(mgr)
        manager_nos = list(dict.fromkeys(manager_nos))
        if len(manager_nos) == 1:
            return manager_nos[0]
        for mgr in manager_nos:
            detail = self.request_json("GET", f"/v1/ad-api/{GFA_API_VERSION}/managerAccounts/{mgr}", access_manager_account_no=mgr, raise_for_status=False)
            if _contains_value(detail, str(ad_account_no)):
                return mgr
        raise RuntimeError(
            f"광고계정 {ad_account_no} 의 AccessManagerAccountNo 를 자동 판별하지 못했습니다. GFA_MANAGER_ACCOUNT_NO 시크릿을 추가해 주세요."
        )

    def paginate(self, path: str, *, params: Optional[Dict[str, Any]] = None, access_manager_account_no: Optional[str] = None) -> List[Dict[str, Any]]:
        params = dict(params or {})
        out: List[Dict[str, Any]] = []
        next_token: Optional[str] = None
        page = 0
        while True:
            if "limit" in params or "next" in params:
                if next_token:
                    params["next"] = next_token
                else:
                    params.setdefault("limit", 1000)
            else:
                params["page"] = page
                params.setdefault("size", 100)
            data = self.request_json("GET", path, params=params, access_manager_account_no=access_manager_account_no)
            items = _extract_items(data)
            out.extend(items)
            if "next" in params or "limit" in params:
                next_token = _to_str(data.get("next")) if isinstance(data, dict) else ""
                if not next_token:
                    break
            else:
                if len(items) < int(params.get("size", 100)):
                    break
                page += 1
        return out


def normalize_campaign_rows(customer_id: str, items: List[Dict[str, Any]], data_source: str = "gfa_api") -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        campaign_id = _pick_id(it, ["campaignNo", "campaign_id", "campaignId", "id", "no"])
        if not campaign_id:
            continue
        name = _to_str(_pick_first(it, ["name", "campaignName", "campaign_name", "title"])) or f"campaign_{campaign_id}"
        status = _to_str(_pick_first(it, ["status", "deliveryStatus", "campaign_status"]))
        if not status and "activated" in it:
            status = "ON" if bool(it.get("activated")) else "OFF"
        objective = _to_str(_pick_first(it, ["objective", "campaignObjective", "campaign_tp"])) or "GFA"
        rows.append({
            "customer_id": str(customer_id),
            "campaign_id": campaign_id,
            "campaign_name": name,
            "campaign_tp": f"gfa_{objective.lower()}" if not str(objective).lower().startswith("gfa") else str(objective).lower(),
            "status": status,
        })
    return rows


def normalize_adset_rows(customer_id: str, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        adset_id = _pick_id(it, ["adSetNo", "adset_id", "adgroup_id", "adSetId", "id", "no"])
        if not adset_id:
            continue
        campaign_id = _pick_id(it, ["campaignNo", "campaign_id", "campaignId", "parentCampaignNo"])
        name = _to_str(_pick_first(it, ["name", "adSetName", "adgroup_name", "title"])) or f"adset_{adset_id}"
        status = _to_str(_pick_first(it, ["status", "deliveryStatus", "adset_status"]))
        if not status and "activated" in it:
            status = "ON" if bool(it.get("activated")) else "OFF"
        rows.append({
            "customer_id": str(customer_id),
            "adgroup_id": adset_id,
            "adgroup_name": name,
            "campaign_id": campaign_id or "",
            "status": status,
        })
    return rows


def normalize_creative_rows(customer_id: str, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        creative_id = _pick_id(it, ["creativeNo", "creative_id", "ad_id", "creativeId", "id", "no"])
        if not creative_id:
            continue
        adset_id = _pick_id(it, ["adSetNo", "adset_id", "adgroup_id", "adSetId"])
        title = _to_str(_pick_first(it, ["name", "creativeName", "creative_name", "title", "headline"]))
        desc = _to_str(_pick_first(it, ["description", "desc", "subTitle", "body"]))
        status = _to_str(_pick_first(it, ["status", "deliveryStatus", "creative_status"]))
        if not status and "activated" in it:
            status = "ON" if bool(it.get("activated")) else "OFF"
        pc_url = _to_str(_pick_first(it, ["pcLandingUrl", "landingUrl", "url"]))
        mo_url = _to_str(_pick_first(it, ["mobileLandingUrl", "mobileUrl", "landingUrl"]))
        image_url = _to_str(_pick_first(it, ["imageUrl", "thumbnailUrl"]))
        rows.append({
            "customer_id": str(customer_id),
            "ad_id": creative_id,
            "adgroup_id": adset_id or "",
            "ad_name": title or f"creative_{creative_id}",
            "status": status,
            "ad_title": title,
            "ad_desc": desc,
            "pc_landing_url": pc_url,
            "mobile_landing_url": mo_url,
            "creative_text": " | ".join([x for x in [title, desc] if x]),
            "image_url": image_url,
        })
    return rows


def fetch_dimension_lists(client: GfaApiClient, customer_id: str, manager_no: str) -> Dict[str, List[Dict[str, Any]]]:
    dims: Dict[str, List[Dict[str, Any]]] = {"campaigns": [], "adsets": [], "creatives": []}
    campaign_paths = [
        f"/v1/ad-api/{GFA_API_VERSION}/adAccounts/{customer_id}/campaigns",
        f"/v1/ad-api/api/open/{GFA_API_VERSION}/adAccounts/{customer_id}/campaigns",
    ]
    for p in campaign_paths:
        try:
            items = client.paginate(p, access_manager_account_no=manager_no)
            if items:
                dims["campaigns"] = items
                break
        except Exception:
            continue
    try:
        dims["adsets"] = client.paginate(f"/v1/ad-api/{GFA_API_VERSION}/adAccounts/{customer_id}/adSets", access_manager_account_no=manager_no)
    except Exception:
        dims["adsets"] = []
    try:
        dims["creatives"] = client.paginate(f"/v1/ad-api/{GFA_API_VERSION}/adAccounts/{customer_id}/creatives", access_manager_account_no=manager_no)
    except Exception:
        dims["creatives"] = []
    return dims


def fetch_past_performance(client: GfaApiClient, customer_id: str, manager_no: str, target_dt: date, aggregation_type: str) -> List[Dict[str, Any]]:
    params = {
        "startDate": target_dt.isoformat(),
        "endDate": target_dt.isoformat(),
        "timeUnit": "daily",
        "limit": 1000,
    }
    path = f"/v1/ad-api/{GFA_API_VERSION}/adAccounts/{customer_id}/performance/past/{aggregation_type}"
    return client.paginate(path, params=params, access_manager_account_no=manager_no)


def build_campaign_fact_rows(customer_id: str, target_dt: date, perf_rows: List[Dict[str, Any]], data_source: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in perf_rows:
        campaign_id = _pick_id(r, ["campaignNo", "campaign_id", "campaignId", "id"])
        if not campaign_id:
            continue
        cost = int(round(_to_float(_pick_first(r, ["sales", "cost", "spend", "총비용vat포함원", "총비용원"]), 0.0)))
        conv_sales = int(round(_to_float(_pick_first(r, ["convSales", "salesConv", "sales", "전환매출액", "총전환매출액"]), 0.0)))
        conv = _to_float(_pick_first(r, ["convCount", "conversions", "conv", "전환수", "총전환수"]), 0.0)
        roas = (conv_sales / cost * 100.0) if cost > 0 else 0.0
        out.append({
            "dt": target_dt,
            "customer_id": str(customer_id),
            "campaign_id": campaign_id,
            "imp": int(_to_float(_pick_first(r, ["impCount", "impressions", "imp", "노출수"]), 0.0)),
            "clk": int(_to_float(_pick_first(r, ["clickCount", "clicks", "clk", "클릭수"]), 0.0)),
            "cost": cost,
            "conv": conv,
            "sales": conv_sales,
            "roas": roas,
            "avg_rnk": 0.0,
            "purchase_conv": conv,
            "purchase_sales": conv_sales,
            "purchase_roas": roas,
            "cart_conv": 0.0,
            "cart_sales": 0,
            "cart_roas": 0.0,
            "wishlist_conv": 0.0,
            "wishlist_sales": 0,
            "wishlist_roas": 0.0,
            "primary_conv": conv,
            "primary_sales": conv_sales,
            "primary_roas": roas,
            "split_available": False,
            "data_source": data_source,
        })
    return out


def build_ad_fact_rows(customer_id: str, target_dt: date, perf_rows: List[Dict[str, Any]], data_source: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in perf_rows:
        ad_id = _pick_id(r, ["creativeNo", "creative_id", "ad_id", "creativeId", "id"])
        if not ad_id:
            continue
        cost = int(round(_to_float(_pick_first(r, ["sales", "cost", "spend", "총비용vat포함원", "총비용원"]), 0.0)))
        conv_sales = int(round(_to_float(_pick_first(r, ["convSales", "salesConv", "sales", "전환매출액", "총전환매출액"]), 0.0)))
        conv = _to_float(_pick_first(r, ["convCount", "conversions", "conv", "전환수", "총전환수"]), 0.0)
        roas = (conv_sales / cost * 100.0) if cost > 0 else 0.0
        out.append({
            "dt": target_dt,
            "customer_id": str(customer_id),
            "ad_id": ad_id,
            "imp": int(_to_float(_pick_first(r, ["impCount", "impressions", "imp", "노출수"]), 0.0)),
            "clk": int(_to_float(_pick_first(r, ["clickCount", "clicks", "clk", "클릭수"]), 0.0)),
            "cost": cost,
            "conv": conv,
            "sales": conv_sales,
            "roas": roas,
            "avg_rnk": 0.0,
            "purchase_conv": conv,
            "purchase_sales": conv_sales,
            "purchase_roas": roas,
            "cart_conv": 0.0,
            "cart_sales": 0,
            "cart_roas": 0.0,
            "wishlist_conv": 0.0,
            "wishlist_sales": 0,
            "wishlist_roas": 0.0,
            "primary_conv": conv,
            "primary_sales": conv_sales,
            "primary_roas": roas,
            "split_available": False,
            "data_source": data_source,
        })
    return out


def upsert_placeholder_dims_from_perf(engine: Engine, customer_id: str, campaign_rows: List[Dict[str, Any]], creative_rows: List[Dict[str, Any]]) -> None:
    camp_dim: List[Dict[str, Any]] = []
    ad_dim: List[Dict[str, Any]] = []
    adgroup_dim_map: Dict[str, Dict[str, Any]] = {}
    for r in campaign_rows:
        cid = _pick_id(r, ["campaignNo", "campaign_id", "campaignId"])
        if cid:
            camp_dim.append({
                "customer_id": str(customer_id),
                "campaign_id": cid,
                "campaign_name": _to_str(_pick_first(r, ["campaign_name", "campaignName", "name"])) or f"campaign_{cid}",
                "campaign_tp": "gfa",
                "status": _to_str(_pick_first(r, ["status"])),
            })
    for r in creative_rows:
        ad_id = _pick_id(r, ["creativeNo", "creative_id", "ad_id", "creativeId"])
        adset_id = _pick_id(r, ["adSetNo", "adset_id", "adgroup_id", "adSetId"])
        campaign_id = _pick_id(r, ["campaignNo", "campaign_id", "campaignId"])
        if adset_id and adset_id not in adgroup_dim_map:
            adgroup_dim_map[adset_id] = {
                "customer_id": str(customer_id),
                "adgroup_id": adset_id,
                "adgroup_name": _to_str(_pick_first(r, ["adgroup_name", "adSetName", "name"])) or f"adset_{adset_id}",
                "campaign_id": campaign_id or "",
                "status": _to_str(_pick_first(r, ["status"])),
            }
        if ad_id:
            ad_dim.append({
                "customer_id": str(customer_id),
                "ad_id": ad_id,
                "adgroup_id": adset_id or "",
                "ad_name": _to_str(_pick_first(r, ["creative_name", "creativeName", "name"])) or f"creative_{ad_id}",
                "status": _to_str(_pick_first(r, ["status"])),
                "ad_title": _to_str(_pick_first(r, ["ad_title", "title", "headline"])),
                "ad_desc": _to_str(_pick_first(r, ["ad_desc", "description", "desc"])),
                "pc_landing_url": "",
                "mobile_landing_url": "",
                "creative_text": "",
                "image_url": "",
            })
    if camp_dim:
        upsert_many(engine, "dim_campaign", camp_dim, ["customer_id", "campaign_id"])
    if adgroup_dim_map:
        upsert_many(engine, "dim_adgroup", list(adgroup_dim_map.values()), ["customer_id", "adgroup_id"])
    if ad_dim:
        upsert_many(engine, "dim_ad", ad_dim, ["customer_id", "ad_id"])


# ------------------------------
# Crawl mode (ID/PW)
# ------------------------------

def normalize_header(s: str) -> str:
    return re.sub(r"[^0-9a-zA-Z가-힣]+", "", str(s or "")).lower()


def choose_col(headers: List[str], aliases: Iterable[str]) -> Optional[str]:
    norm_map = {normalize_header(h): h for h in headers}
    for alias in aliases:
        key = normalize_header(alias)
        if key in norm_map:
            return norm_map[key]
    for h in headers:
        nh = normalize_header(h)
        for alias in aliases:
            key = normalize_header(alias)
            if key and key in nh:
                return h
    return None


def detect_header_row(df: pd.DataFrame) -> int:
    target_tokens = [
        "캠페인", "광고그룹", "소재", "노출수", "클릭수", "총비용", "전환수", "전환매출액",
        "campaign", "creative", "adset", "impressions", "clicks", "cost",
    ]
    max_rows = min(len(df), 20)
    best_idx = 0
    best_score = -1
    for i in range(max_rows):
        vals = [str(x) for x in df.iloc[i].fillna("").tolist()]
        joined = " ".join(vals)
        score = sum(1 for tok in target_tokens if tok.lower() in joined.lower())
        if score > best_score:
            best_score = score
            best_idx = i
    return best_idx


def read_report_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        raw = pd.read_excel(path, header=None)
    else:
        encodings = ["utf-8-sig", "cp949", "euc-kr", "utf-8"]
        seps = ["\t", ",", None]
        last_err = None
        raw = None
        for enc in encodings:
            for sep in seps:
                try:
                    raw = pd.read_csv(path, header=None, sep=sep, engine="python", encoding=enc)
                    if raw is not None and not raw.empty:
                        break
                except Exception as e:
                    last_err = e
                    continue
            if raw is not None and not raw.empty:
                break
        if raw is None:
            raise RuntimeError(f"리포트 파일 읽기 실패: {path.name} | {last_err}")
    hdr = detect_header_row(raw)
    header = [str(x).strip() for x in raw.iloc[hdr].fillna("").tolist()]
    data = raw.iloc[hdr + 1 :].copy()
    data.columns = header
    data = data.dropna(how="all")
    data = data.loc[:, [str(c).strip() for c in data.columns].count if False else data.columns]
    data.columns = [str(c).strip() or f"col_{i}" for i, c in enumerate(data.columns)]
    data = data.reset_index(drop=True)
    return data


def parse_gfa_report(df: pd.DataFrame, customer_id: str, account_name: str, target_dt: date) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    if df is None or df.empty:
        raise RuntimeError("다운로드된 GFA 리포트가 비어 있습니다.")

    headers = list(df.columns)
    write_debug_text(f"parsed_columns_{slugify(account_name)}_{target_dt.isoformat()}.txt", "\n".join(headers))

    campaign_id_col = choose_col(headers, ["캠페인ID", "캠페인번호", "campaign id", "campaign no", "campaignno"])
    campaign_name_col = choose_col(headers, ["캠페인명", "캠페인", "campaign name", "campaign"])
    adgroup_id_col = choose_col(headers, ["광고그룹ID", "광고그룹번호", "adset id", "adset no", "adgroup id", "ad group id"])
    adgroup_name_col = choose_col(headers, ["광고그룹명", "광고그룹", "adset name", "ad group"])
    ad_id_col = choose_col(headers, ["소재ID", "소재번호", "creative id", "creative no", "ad id"])
    ad_name_col = choose_col(headers, ["소재명", "소재", "creative name", "creative"])
    imp_col = choose_col(headers, ["노출수", "impressions", "imp"])
    clk_col = choose_col(headers, ["클릭수", "clicks", "clk"])
    cost_col = choose_col(headers, ["총비용(VAT포함, 원)", "총비용원", "총비용", "cost", "spend"])
    conv_col = choose_col(headers, ["총 전환수", "총전환수", "전환수", "conversions", "conv"])
    sales_col = choose_col(headers, ["총 전환매출액", "총전환매출액", "전환매출액", "sales", "convsales"])
    campaign_tp_col = choose_col(headers, ["캠페인목적", "목적", "objective"])
    status_col = choose_col(headers, ["상태", "status"])

    if not campaign_id_col and not ad_id_col:
        raise RuntimeError(
            "GFA 리포트에서 캠페인/소재 식별 컬럼을 찾지 못했습니다. 광고 단위를 '캠페인 또는 소재'로 맞춘 리포트인지 확인해 주세요."
        )
    if not imp_col and not clk_col and not cost_col:
        raise RuntimeError("GFA 리포트에서 성과 컬럼(노출/클릭/비용)을 찾지 못했습니다.")

    dim_campaign_map: Dict[str, Dict[str, Any]] = {}
    dim_adgroup_map: Dict[str, Dict[str, Any]] = {}
    dim_ad_map: Dict[str, Dict[str, Any]] = {}
    fact_campaign_map: Dict[Tuple[date, str, str], Dict[str, Any]] = {}
    fact_ad_map: Dict[Tuple[date, str, str], Dict[str, Any]] = {}

    for _, row in df.iterrows():
        campaign_id = _to_str(row.get(campaign_id_col)) if campaign_id_col else ""
        campaign_name = _to_str(row.get(campaign_name_col)) if campaign_name_col else ""
        adgroup_id = _to_str(row.get(adgroup_id_col)) if adgroup_id_col else ""
        adgroup_name = _to_str(row.get(adgroup_name_col)) if adgroup_name_col else ""
        ad_id = _to_str(row.get(ad_id_col)) if ad_id_col else ""
        ad_name = _to_str(row.get(ad_name_col)) if ad_name_col else ""
        status = _to_str(row.get(status_col)) if status_col else ""
        campaign_tp = _to_str(row.get(campaign_tp_col)) if campaign_tp_col else "gfa"

        if not campaign_id and ad_id:
            campaign_id = f"campaign_of_{ad_id}"
        if not campaign_name and campaign_id:
            campaign_name = campaign_id
        if not adgroup_name and adgroup_id:
            adgroup_name = adgroup_id
        if not ad_name and ad_id:
            ad_name = ad_id

        imp = int(round(_to_float(row.get(imp_col), 0.0))) if imp_col else 0
        clk = int(round(_to_float(row.get(clk_col), 0.0))) if clk_col else 0
        cost = int(round(_to_float(row.get(cost_col), 0.0))) if cost_col else 0
        conv = _to_float(row.get(conv_col), 0.0) if conv_col else 0.0
        sales = int(round(_to_float(row.get(sales_col), 0.0))) if sales_col else 0
        roas = (sales / cost * 100.0) if cost > 0 else 0.0

        if campaign_id:
            dim_campaign_map[campaign_id] = {
                "customer_id": str(customer_id),
                "campaign_id": campaign_id,
                "campaign_name": campaign_name or campaign_id,
                "campaign_tp": f"gfa_{campaign_tp.lower()}" if campaign_tp and not campaign_tp.lower().startswith("gfa") else (campaign_tp.lower() or "gfa"),
                "status": status,
            }
            key_c = (target_dt, str(customer_id), campaign_id)
            bucket_c = fact_campaign_map.setdefault(key_c, {
                "dt": target_dt,
                "customer_id": str(customer_id),
                "campaign_id": campaign_id,
                "imp": 0,
                "clk": 0,
                "cost": 0,
                "conv": 0.0,
                "sales": 0,
                "roas": 0.0,
                "avg_rnk": 0.0,
                "purchase_conv": 0.0,
                "purchase_sales": 0,
                "purchase_roas": 0.0,
                "cart_conv": 0.0,
                "cart_sales": 0,
                "cart_roas": 0.0,
                "wishlist_conv": 0.0,
                "wishlist_sales": 0,
                "wishlist_roas": 0.0,
                "primary_conv": 0.0,
                "primary_sales": 0,
                "primary_roas": 0.0,
                "split_available": False,
                "data_source": "gfa_ui_report",
            })
            bucket_c["imp"] += imp
            bucket_c["clk"] += clk
            bucket_c["cost"] += cost
            bucket_c["conv"] += conv
            bucket_c["sales"] += sales
            bucket_c["purchase_conv"] += conv
            bucket_c["purchase_sales"] += sales
            bucket_c["primary_conv"] += conv
            bucket_c["primary_sales"] += sales

        if adgroup_id:
            dim_adgroup_map[adgroup_id] = {
                "customer_id": str(customer_id),
                "adgroup_id": adgroup_id,
                "adgroup_name": adgroup_name or adgroup_id,
                "campaign_id": campaign_id,
                "status": status,
            }

        if ad_id:
            dim_ad_map[ad_id] = {
                "customer_id": str(customer_id),
                "ad_id": ad_id,
                "adgroup_id": adgroup_id,
                "ad_name": ad_name or ad_id,
                "status": status,
                "ad_title": ad_name or "",
                "ad_desc": "",
                "pc_landing_url": "",
                "mobile_landing_url": "",
                "creative_text": ad_name or "",
                "image_url": "",
            }
            key_a = (target_dt, str(customer_id), ad_id)
            bucket_a = fact_ad_map.setdefault(key_a, {
                "dt": target_dt,
                "customer_id": str(customer_id),
                "ad_id": ad_id,
                "imp": 0,
                "clk": 0,
                "cost": 0,
                "conv": 0.0,
                "sales": 0,
                "roas": 0.0,
                "avg_rnk": 0.0,
                "purchase_conv": 0.0,
                "purchase_sales": 0,
                "purchase_roas": 0.0,
                "cart_conv": 0.0,
                "cart_sales": 0,
                "cart_roas": 0.0,
                "wishlist_conv": 0.0,
                "wishlist_sales": 0,
                "wishlist_roas": 0.0,
                "primary_conv": 0.0,
                "primary_sales": 0,
                "primary_roas": 0.0,
                "split_available": False,
                "data_source": "gfa_ui_report",
            })
            bucket_a["imp"] += imp
            bucket_a["clk"] += clk
            bucket_a["cost"] += cost
            bucket_a["conv"] += conv
            bucket_a["sales"] += sales
            bucket_a["purchase_conv"] += conv
            bucket_a["purchase_sales"] += sales
            bucket_a["primary_conv"] += conv
            bucket_a["primary_sales"] += sales

    campaign_facts = list(fact_campaign_map.values())
    ad_facts = list(fact_ad_map.values())
    for row in campaign_facts + ad_facts:
        row["roas"] = (row["sales"] / row["cost"] * 100.0) if row["cost"] > 0 else 0.0
        row["purchase_roas"] = (row["purchase_sales"] / row["cost"] * 100.0) if row["cost"] > 0 else 0.0
        row["primary_roas"] = (row["primary_sales"] / row["cost"] * 100.0) if row["cost"] > 0 else 0.0

    if not campaign_facts and ad_facts:
        # 소재 레벨만 있고 캠페인 식별이 불완전한 경우 최소 placeholder 생성
        placeholder_id = f"gfa_{customer_id}"
        dim_campaign_map.setdefault(placeholder_id, {
            "customer_id": str(customer_id),
            "campaign_id": placeholder_id,
            "campaign_name": account_name,
            "campaign_tp": "gfa",
            "status": "",
        })
        campaign_facts.append({
            "dt": target_dt,
            "customer_id": str(customer_id),
            "campaign_id": placeholder_id,
            "imp": sum(int(x["imp"]) for x in ad_facts),
            "clk": sum(int(x["clk"]) for x in ad_facts),
            "cost": sum(int(x["cost"]) for x in ad_facts),
            "conv": sum(float(x["conv"]) for x in ad_facts),
            "sales": sum(int(x["sales"]) for x in ad_facts),
            "roas": 0.0,
            "avg_rnk": 0.0,
            "purchase_conv": sum(float(x["purchase_conv"]) for x in ad_facts),
            "purchase_sales": sum(int(x["purchase_sales"]) for x in ad_facts),
            "purchase_roas": 0.0,
            "cart_conv": 0.0,
            "cart_sales": 0,
            "cart_roas": 0.0,
            "wishlist_conv": 0.0,
            "wishlist_sales": 0,
            "wishlist_roas": 0.0,
            "primary_conv": sum(float(x["primary_conv"]) for x in ad_facts),
            "primary_sales": sum(int(x["primary_sales"]) for x in ad_facts),
            "primary_roas": 0.0,
            "split_available": False,
            "data_source": "gfa_ui_report",
        })
        campaign_facts[-1]["roas"] = (campaign_facts[-1]["sales"] / campaign_facts[-1]["cost"] * 100.0) if campaign_facts[-1]["cost"] > 0 else 0.0
        campaign_facts[-1]["purchase_roas"] = (campaign_facts[-1]["purchase_sales"] / campaign_facts[-1]["cost"] * 100.0) if campaign_facts[-1]["cost"] > 0 else 0.0
        campaign_facts[-1]["primary_roas"] = (campaign_facts[-1]["primary_sales"] / campaign_facts[-1]["cost"] * 100.0) if campaign_facts[-1]["cost"] > 0 else 0.0

    return list(dim_campaign_map.values()), list(dim_adgroup_map.values()), list(dim_ad_map.values()), campaign_facts, ad_facts


class GfaCrawler:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.play = None
        self.browser = None
        self.context = None
        self.page = None

    def __enter__(self):
        try:
            from playwright.sync_api import sync_playwright
        except Exception as e:
            raise RuntimeError(
                "playwright 가 설치되어 있지 않습니다. requirements.txt 와 workflow 에 playwright/chromium 설치가 필요합니다."
            ) from e
        self.play = sync_playwright().start()
        self.browser = self.play.chromium.launch(headless=self.headless, args=["--disable-blink-features=AutomationControlled"])
        self.context = self.browser.new_context(accept_downloads=True, viewport={"width": 1600, "height": 1200})
        self.page = self.context.new_page()
        self.page.set_default_timeout(GFA_PLAYWRIGHT_TIMEOUT_MS)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.context:
                self.context.close()
        except Exception:
            pass
        try:
            if self.browser:
                self.browser.close()
        except Exception:
            pass
        try:
            if self.play:
                self.play.stop()
        except Exception:
            pass

    def screenshot(self, name: str) -> Path:
        ensure_dirs()
        p = GFA_DEBUG_DIR / name
        try:
            self.page.screenshot(path=str(p), full_page=True)
        except Exception:
            pass
        return p

    def page_dump(self, name: str) -> Path:
        ensure_dirs()
        p = GFA_DEBUG_DIR / name
        try:
            p.write_text(self.page.content(), encoding="utf-8")
        except Exception:
            pass
        return p

    def contains_text(self, texts: Iterable[str]) -> bool:
        body = ""
        try:
            body = self.page.locator("body").inner_text(timeout=3000)
        except Exception:
            try:
                body = self.page.content()
            except Exception:
                body = ""
        body = body or ""
        return any(t in body for t in texts)

    def _adopt_latest_page(self) -> None:
        try:
            pages = list(self.context.pages) if self.context else []
        except Exception:
            pages = []
        chosen = None
        for pg in reversed(pages):
            try:
                url = (pg.url or "").strip()
            except Exception:
                url = ""
            if url and url != "about:blank":
                chosen = pg
                break
        if chosen is not None and chosen is not self.page:
            self.page = chosen
            try:
                self.page.set_default_timeout(GFA_PLAYWRIGHT_TIMEOUT_MS)
            except Exception:
                pass

    def goto_soft(self, url: str, *, wait_until: str = "domcontentloaded", timeout_ms: Optional[int] = None) -> None:
        try:
            self.page.goto(url, wait_until=wait_until, timeout=timeout_ms or GFA_PLAYWRIGHT_TIMEOUT_MS)
        except Exception as e:
            msg = str(e)
            if "ERR_ABORTED" in msg:
                time.sleep(2.0)
                self._adopt_latest_page()
                return
            raise
        self._adopt_latest_page()

    def click_first(self, selectors: Iterable[str], *, timeout_ms: int = 3000, allow_popup: bool = False) -> bool:
        for sel in selectors:
            try:
                before_n = len(list(self.context.pages)) if (allow_popup and self.context) else 0
                loc = self.page.locator(sel).first
                if loc.count() > 0:
                    loc.click(timeout=timeout_ms)
                    if allow_popup:
                        time.sleep(1.5)
                        try:
                            after_n = len(list(self.context.pages)) if self.context else 0
                        except Exception:
                            after_n = 0
                        if after_n > before_n:
                            self._adopt_latest_page()
                    return True
            except Exception:
                continue
        return False

    def fill_first(self, selectors: Iterable[str], value: str) -> bool:
        for sel in selectors:
            try:
                loc = self.page.locator(sel).first
                if loc.count() > 0:
                    loc.fill("")
                    loc.fill(value)
                    return True
            except Exception:
                continue
        return False

    def wait_network_idle(self, sec: float = 2.0) -> None:
        try:
            self.page.wait_for_load_state("networkidle", timeout=int(max(sec, 1.0) * 1000))
        except Exception:
            time.sleep(sec)

    def login(self, user_id: str, password: str) -> None:
        log("🌐 네이버 로그인 페이지 진입")
        self.goto_soft(GFA_LOGIN_URL, wait_until="domcontentloaded")
        self.wait_network_idle(2)

        id_ok = self.fill_first([
            'input[name="id"]',
            'input#id',
            'input[placeholder*="아이디"]',
            'input[autocomplete="username"]',
        ], user_id)
        pw_ok = self.fill_first([
            'input[name="pw"]',
            'input#pw',
            'input[type="password"]',
            'input[autocomplete="current-password"]',
        ], password)
        if not id_ok or not pw_ok:
            self.screenshot("01_login_form_not_found.png")
            self.page_dump("01_login_form_not_found.html")
            raise RuntimeError("네이버 로그인 입력창을 찾지 못했습니다. 로그인 UI가 변경되었을 수 있습니다.")

        clicked = self.click_first([
            'button[type="submit"]',
            'button:has-text("로그인")',
            'input[type="submit"]',
            '.btn_login',
        ], timeout_ms=5000)
        if not clicked:
            self.page.keyboard.press("Enter")
        self.wait_network_idle(4)

        if self.contains_text(["2단계 인증", "보안 인증", "캡차", "자동입력 방지", "OTP", "인증번호"]):
            self.screenshot("02_login_security_required.png")
            self.page_dump("02_login_security_required.html")
            raise RuntimeError("네이버 2단계 인증/보안인증 화면이 감지되었습니다. 자동 우회는 지원하지 않습니다. 해당 인증을 수동으로 해제하거나 API 토큰 방식이 필요합니다.")
        if self.contains_text(["아이디를 확인", "비밀번호를 확인", "로그인에 실패"]):
            self.screenshot("02_login_failed.png")
            self.page_dump("02_login_failed.html")
            raise RuntimeError("네이버 로그인에 실패했습니다. GFA_ID / GFA_PW 값을 확인해 주세요.")

        self.screenshot("03_after_login.png")

    def go_gfa_platform(self) -> None:
        log("🧭 GFA 플랫폼 진입 시도")

        # 1) 광고주센터 홈으로 이동 후 헤더 메뉴에서 진입 시도
        if "ads.naver.com" not in (self.page.url or ""):
            self.goto_soft(ADS_HOME_URL, wait_until="domcontentloaded")
            self.wait_network_idle(4)

        success_markers = [
            "광고 계정", "성과 리포트", "대시보드", "광고관리", "캠페인 만들기",
            "성과형 디스플레이 광고", "디스플레이 광고",
        ]

        # 통합 광고주센터에서 플랫폼 선택이 필요한 경우
        if self.contains_text(["광고 플랫폼", "검색광고", "성과형 디스플레이 광고", "디스플레이 광고"]):
            self.click_first([
                'button:has-text("광고 플랫폼")',
                'a:has-text("광고 플랫폼")',
                'text="광고 플랫폼"',
                '[aria-label*="광고 플랫폼"]',
            ], timeout_ms=4000, allow_popup=True)
            time.sleep(1.0)
            self.click_first([
                'text="성과형 디스플레이 광고"',
                'button:has-text("성과형 디스플레이 광고")',
                'a:has-text("성과형 디스플레이 광고")',
                'text="디스플레이 광고"',
                'button:has-text("디스플레이 광고")',
                'a:has-text("디스플레이 광고")',
            ], timeout_ms=7000, allow_popup=True)
            self.wait_network_idle(5)
            self._adopt_latest_page()

        if self.contains_text(success_markers):
            self.screenshot("04_gfa_platform_entry.png")
            return

        # 2) 직접 URL 진입은 fallback 으로만 시도 (ERR_ABORTED 는 무시 후 현재 페이지 검사)
        for url in [u for u in [GFA_PLATFORM_URL, *GFA_PLATFORM_FALLBACK_URLS] if u]:
            try:
                self.goto_soft(url, wait_until="domcontentloaded")
                self.wait_network_idle(4)
                self._adopt_latest_page()
                if self.contains_text(success_markers) or any(token in (self.page.url or "") for token in ["gfa", "display"]):
                    self.screenshot("04_gfa_platform_entry.png")
                    return
            except Exception:
                continue

        self.screenshot("04_gfa_platform_entry_failed.png")
        self.page_dump("04_gfa_platform_entry_failed.html")
        raise RuntimeError("GFA 플랫폼 진입에 실패했습니다. 통합 광고주센터에서 '광고 플랫폼 > 성과형 디스플레이 광고(또는 디스플레이 광고)' 메뉴 구조가 변경되었을 수 있습니다.")

    def select_account(self, customer_id: str, account_name: str) -> None:
        log(f"🎯 계정 선택 시도 | {account_name} ({customer_id})")
        # 먼저 직접 텍스트 노출 여부 확인
        if self.contains_text([customer_id, account_name]):
            return

        # 계정 레이어 열기
        self.click_first([
            'button:has-text("광고 계정")',
            'text="광고 계정"',
            'button:has-text("계정")',
            'text="계정"',
            '[aria-label*="계정"]',
        ], timeout_ms=3000)
        time.sleep(1.0)

        # 검색 입력
        self.fill_first([
            'input[placeholder*="계정"]',
            'input[placeholder*="검색"]',
            'input[type="search"]',
        ], customer_id or account_name)
        time.sleep(1.0)

        if not self.click_first([
            f'text="{customer_id}"',
            f'text="{account_name}"',
            f'button:has-text("{customer_id}")',
            f'button:has-text("{account_name}")',
            f'a:has-text("{customer_id}")',
            f'a:has-text("{account_name}")',
        ], timeout_ms=4000):
            # 계정 전환을 못 해도 현재 계정이 맞을 수 있으니 body 점검
            if not self.contains_text([customer_id, account_name]):
                self.screenshot(f"05_account_select_failed_{slugify(account_name)}.png")
                self.page_dump(f"05_account_select_failed_{slugify(account_name)}.html")
                raise RuntimeError(f"GFA 계정 선택에 실패했습니다: {account_name} ({customer_id})")
        self.wait_network_idle(4)
        self.screenshot(f"06_account_selected_{slugify(account_name)}.png")

    def go_report_area(self) -> None:
        log("📊 성과 리포트/광고관리 영역 진입 시도")
        candidates = [
            'text="성과 리포트"',
            'text="성과리포트"',
            'text="리포트"',
            'text="보고서"',
            'text="광고관리"',
            'text="대시보드"',
            'a:has-text("성과 리포트")',
            'button:has-text("성과 리포트")',
        ]
        self.click_first(candidates, timeout_ms=5000)
        self.wait_network_idle(4)
        self.screenshot("07_report_area.png")

    def set_date_filter(self, target_dt: date) -> None:
        log(f"📅 날짜 설정 시도 | {target_dt.isoformat()}")
        # 어제는 quick preset 우선
        if target_dt == (date.today() - timedelta(days=1)):
            if self.click_first([
                'text="어제"',
                'button:has-text("어제")',
                'label:has-text("어제")',
            ], timeout_ms=2000):
                self.wait_network_idle(2)
                return

        formatted = target_dt.isoformat()
        start_ok = self.fill_first([
            'input[placeholder*="시작"]',
            'input[placeholder*="from"]',
            'input[type="date"]',
        ], formatted)
        # 같은 selector 첫 번째만 잡힐 수 있으므로 nth 활용
        end_ok = False
        for sel in ['input[type="date"]', 'input[placeholder*="종료"]', 'input[placeholder*="to"]']:
            try:
                loc = self.page.locator(sel)
                if loc.count() >= 2:
                    loc.nth(1).fill(formatted)
                    end_ok = True
                    break
            except Exception:
                continue
        if start_ok or end_ok:
            self.click_first(['text="조회"', 'button:has-text("조회")', 'button:has-text("적용")'], timeout_ms=2000)
            self.wait_network_idle(4)

    def switch_to_creative_level(self) -> None:
        log("🧩 소재 단위 전환 시도")
        self.click_first([
            'text="소재"',
            'text="광고 소재"',
            'text="크리에이티브"',
            'button:has-text("소재")',
            'button:has-text("광고 소재")',
        ], timeout_ms=3000)
        self.wait_network_idle(2)

    def download_report(self, account_name: str, target_dt: date) -> Path:
        log("⬇️ 리포트 다운로드 시도")
        ensure_dirs()
        selectors = [
            'text="다운로드"',
            'text="내려받기"',
            'button:has-text("다운로드")',
            'button:has-text("내려받기")',
            'button:has-text("Excel")',
            'button:has-text("엑셀")',
            'a:has-text("다운로드")',
        ]
        with self.page.expect_download(timeout=20000) as dl_info:
            if not self.click_first(selectors, timeout_ms=5000):
                self.screenshot(f"08_download_button_not_found_{slugify(account_name)}.png")
                self.page_dump(f"08_download_button_not_found_{slugify(account_name)}.html")
                raise RuntimeError("다운로드 버튼을 찾지 못했습니다. GFA 화면 구조가 변경되었을 수 있습니다.")
        download = dl_info.value
        suggested = download.suggested_filename or f"gfa_{slugify(account_name)}_{target_dt.isoformat()}.xlsx"
        ext = Path(suggested).suffix or ".xlsx"
        save_path = GFA_DOWNLOAD_DIR / f"gfa_{slugify(account_name)}_{target_dt.isoformat()}{ext}"
        download.save_as(str(save_path))
        self.screenshot(f"09_after_download_{slugify(account_name)}.png")
        return save_path


def collect_via_crawl(engine: Engine, accounts: List[Dict[str, str]], target_dt: date) -> None:
    if not GFA_ID or not GFA_PW:
        die("GFA_ID / GFA_PW 가 설정되지 않았습니다. ID/PW 크롤링 모드에서는 두 시크릿이 모두 필요합니다.")

    ensure_dirs()
    errors: List[str] = []

    with GfaCrawler(headless=GFA_HEADLESS) as crawler:
        crawler.login(GFA_ID, GFA_PW)
        crawler.go_gfa_platform()

        for acc in accounts:
            customer_id = _to_str(acc.get("id"))
            account_name = _to_str(acc.get("name")) or customer_id
            try:
                log(f"🚀 [CRAWL] [{account_name}] ({customer_id}) 수집 시작 | {target_dt.isoformat()}")
                upsert_many(engine, "dim_account", [{"customer_id": customer_id, "account_name": account_name}], ["customer_id"])
                crawler.go_gfa_platform()
                crawler.select_account(customer_id, account_name)
                crawler.go_report_area()
                crawler.switch_to_creative_level()
                crawler.set_date_filter(target_dt)
                report_path = crawler.download_report(account_name, target_dt)
                log(f"   ↳ 다운로드 완료: {report_path}")
                parsed = read_report_file(report_path)
                dim_campaign_rows, dim_adgroup_rows, dim_ad_rows, fact_campaign_rows, fact_ad_rows = parse_gfa_report(parsed, customer_id, account_name, target_dt)

                if dim_campaign_rows:
                    upsert_many(engine, "dim_campaign", dim_campaign_rows, ["customer_id", "campaign_id"])
                if dim_adgroup_rows:
                    upsert_many(engine, "dim_adgroup", dim_adgroup_rows, ["customer_id", "adgroup_id"])
                if dim_ad_rows:
                    upsert_many(engine, "dim_ad", dim_ad_rows, ["customer_id", "ad_id"])
                replace_fact_range(engine, "fact_campaign_daily", fact_campaign_rows, customer_id, target_dt)
                replace_fact_range(engine, "fact_ad_daily", fact_ad_rows, customer_id, target_dt)
                log(f"✅ [CRAWL] [{account_name}] 완료 | campaigns={len(fact_campaign_rows)} rows, creatives={len(fact_ad_rows)} rows")
            except Exception as e:
                msg = f"[{account_name}] {customer_id} 실패: {e}"
                errors.append(msg)
                log(f"❌ {msg}")
                try:
                    crawler.screenshot(f"err_{slugify(account_name)}.png")
                    crawler.page_dump(f"err_{slugify(account_name)}.html")
                except Exception:
                    pass

    if errors:
        die("; ".join(errors))


def filter_accounts(accounts: List[Dict[str, str]], account_name: str, account_names: str) -> List[Dict[str, str]]:
    out = accounts
    single = _to_str(account_name)
    multi_raw = _to_str(account_names)
    if single:
        out = [a for a in out if _to_str(a.get("name")) == single]
    if multi_raw:
        wanted = {x.strip() for x in multi_raw.split(",") if x.strip()}
        out = [a for a in out if _to_str(a.get("name")) in wanted]
    return out


def collect_via_api(engine: Engine, accounts: List[Dict[str, str]], target_dt: date, access_token: str) -> None:
    client = GfaApiClient(access_token)
    errors: List[str] = []
    for acc in accounts:
        customer_id = _to_str(acc.get("id"))
        account_name = _to_str(acc.get("name")) or customer_id
        if not customer_id:
            continue
        log(f"🚀 [API] [{account_name}] ({customer_id}) 수집 시작 | {target_dt.isoformat()}")
        try:
            manager_no = client.resolve_manager_account_no(customer_id)
            log(f"   ↳ AccessManagerAccountNo = {manager_no}")
            upsert_many(engine, "dim_account", [{"customer_id": customer_id, "account_name": account_name}], ["customer_id"])
            dims = fetch_dimension_lists(client, customer_id, manager_no)
            dim_campaign_rows = normalize_campaign_rows(customer_id, dims.get("campaigns", []))
            dim_adset_rows = normalize_adset_rows(customer_id, dims.get("adsets", []))
            dim_creative_rows = normalize_creative_rows(customer_id, dims.get("creatives", []))
            if dim_campaign_rows:
                upsert_many(engine, "dim_campaign", dim_campaign_rows, ["customer_id", "campaign_id"])
            if dim_adset_rows:
                upsert_many(engine, "dim_adgroup", dim_adset_rows, ["customer_id", "adgroup_id"])
            if dim_creative_rows:
                upsert_many(engine, "dim_ad", dim_creative_rows, ["customer_id", "ad_id"])
            perf_campaigns = fetch_past_performance(client, customer_id, manager_no, target_dt, "campaigns")
            perf_creatives = fetch_past_performance(client, customer_id, manager_no, target_dt, "creatives")
            if not dim_campaign_rows or not dim_adset_rows or not dim_creative_rows:
                upsert_placeholder_dims_from_perf(engine, customer_id, perf_campaigns, perf_creatives)
            fact_campaign_rows = build_campaign_fact_rows(customer_id, target_dt, perf_campaigns, data_source="gfa_api")
            fact_ad_rows = build_ad_fact_rows(customer_id, target_dt, perf_creatives, data_source="gfa_api")
            replace_fact_range(engine, "fact_campaign_daily", fact_campaign_rows, customer_id, target_dt)
            replace_fact_range(engine, "fact_ad_daily", fact_ad_rows, customer_id, target_dt)
            log(f"✅ [API] [{account_name}] 완료 | campaigns={len(fact_campaign_rows)} rows, creatives={len(fact_ad_rows)} rows")
        except Exception as e:
            msg = f"[{account_name}] {customer_id} 실패: {e}"
            errors.append(msg)
            log(f"❌ {msg}")
    if errors:
        die("; ".join(errors))


def main() -> None:
    parser = argparse.ArgumentParser(description="NAVER GFA collector (API or ID/PW crawl)")
    parser.add_argument("--date", dest="target_date", default="", help="수집일 (YYYY-MM-DD). 미입력 시 어제")
    parser.add_argument("--account_name", default="", help="단일 업체명")
    parser.add_argument("--account_names", default="", help="여러 업체명 콤마구분")
    args = parser.parse_args()

    if args.target_date:
        target_dt = datetime.strptime(args.target_date, "%Y-%m-%d").date()
    else:
        target_dt = date.today() - timedelta(days=1)

    engine = get_engine()
    ensure_tables(engine)
    ensure_dirs()

    accounts = load_naver_accounts(file_path=ACCOUNT_MASTER_FILE, include_gfa=True, media_types=["gfa"])
    accounts = filter_accounts(accounts, args.account_name, args.account_names)
    if not accounts:
        die("account_master 기준 GFA 계정이 없습니다. account_master.xlsx 에 naver_media_type=gfa 계정이 있어야 합니다.")
    log(f"🗂️ GFA 계정 로드: {len(accounts)}개")
    log(
        "🔎 GFA auth env 상태 | "
        f"ACCESS_TOKEN={mask_present(GFA_ACCESS_TOKEN)} "
        f"REFRESH_TOKEN={mask_present(GFA_REFRESH_TOKEN)} "
        f"CLIENT_ID={mask_present(GFA_CLIENT_ID)} "
        f"CLIENT_SECRET={mask_present(GFA_CLIENT_SECRET)} "
        f"GFA_ID={mask_present(GFA_ID)} "
        f"GFA_PW={mask_present(GFA_PW)}"
    )

    access_token = get_access_token_if_available()
    if access_token:
        log("🔐 GFA API 토큰이 감지되어 API 모드로 실행합니다.")
        collect_via_api(engine, accounts, target_dt, access_token)
    else:
        if GFA_ID and GFA_PW:
            log("🔐 GFA_ACCESS_TOKEN 이 없어 ID/PW 크롤링 모드로 실행합니다.")
            collect_via_crawl(engine, accounts, target_dt)
        else:
            die(
                "GFA 인증 정보가 부족합니다. API 모드는 GFA_ACCESS_TOKEN 또는 GFA_REFRESH_TOKEN + GFA_CLIENT_ID/GFA_CLIENT_SECRET 이 필요하고, "
                "크롤링 모드는 GFA_ID / GFA_PW 가 필요합니다."
            )

    log("🎉 GFA 수집 완료")


if __name__ == "__main__":
    main()
