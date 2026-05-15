# -*- coding: utf-8 -*-
"""
collector_shop_ext.py - 네이버 검색광고 확장소재 수집기

수집 전략
- 성과는 /stats 직접 조회보다 stat-report 기반(ADEXTENSION / ADEXTENSION_CONVERSION)을 우선 사용
- 쇼핑검색/파워링크외 버킷은 구조 매핑 후 ad_id 기준으로 필터링
- 같은 날짜 재실행 시 dt+customer_id+ad_id 기준 업서트
"""

import os
import time
import io
import csv
import json
import hmac
import base64
import hashlib
import argparse
import random
import re
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from urllib.parse import urlparse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import psycopg2.extras
from sqlalchemy.pool import NullPool

try:
    from account_master import load_naver_accounts
except Exception:
    load_naver_accounts = None

load_dotenv(override=True)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()

SHOPPING_ADEXTENSION_FIXED_DEFAULTS = {
    "ad_idx": 5,
    "id_idx": 6,
    "device_idx": 9,
    "imp_idx": 10,
    "clk_idx": 11,
    "cost_idx": -1,
    "conv_idx": -1,
    "sales_idx": -1,
}

def _load_shopping_fixed_override() -> dict:
    """Optional env override for shopping ADEXTENSION fixed-column parsing.
    Format: ad=5,ext=6,imp=10,clk=11,cost=-1,conv=-1,sales=-1
    """
    cols = dict(SHOPPING_ADEXTENSION_FIXED_DEFAULTS)
    raw = (os.getenv("SHOPPING_ADEXTENSION_FIXED_OVERRIDE") or "").strip()
    if not raw:
        return cols
    key_map = {
        "ad": "ad_idx", "ad_idx": "ad_idx",
        "ext": "id_idx", "id": "id_idx", "ext_idx": "id_idx", "id_idx": "id_idx",
        "device": "device_idx", "device_idx": "device_idx",
        "imp": "imp_idx", "imp_idx": "imp_idx",
        "clk": "clk_idx", "click": "clk_idx", "clk_idx": "clk_idx",
        "cost": "cost_idx", "cost_idx": "cost_idx",
        "conv": "conv_idx", "conv_idx": "conv_idx",
        "sales": "sales_idx", "sales_idx": "sales_idx",
    }
    try:
        for part in raw.split(','):
            if '=' not in part:
                continue
            k, v = [x.strip().lower() for x in part.split('=', 1)]
            if k not in key_map:
                continue
            cols[key_map[k]] = int(v)
    except Exception as e:
        log(f"⚠️ SHOPPING_ADEXTENSION_FIXED_OVERRIDE 파싱 실패: {e}")
    return cols

BASE_URL = "https://api.searchad.naver.com"
TIMEOUT = 60
DEBUG_REPORT_DIR = os.getenv("DEBUG_REPORT_DIR", "debug_reports")


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _new_run_result(customer_id: str, target_date: date, ext_bucket: str) -> dict:
    return {
        "customer_id": str(customer_id),
        "target_date": str(target_date),
        "bucket": str(ext_bucket),
        "status": "started",
        "reason": "",
        "campaign_count": 0,
        "shopping_campaign_count": 0,
        "non_shopping_campaign_count": 0,
        "campaign_rows": 0,
        "adgroup_rows": 0,
        "extension_rows": 0,
        "target_ad_ids": 0,
        "report_base_rows": 0,
        "report_conv_rows": 0,
        "stats_rows": 0,
        "metric_rows": 0,
        "fact_rows": 0,
        "missing_target_ads": 0,
        "shopping_zero_clk": 0,
        "nonzero_clk_rows": 0,
        "nonzero_cost_rows": 0,
        "nonzero_conv_rows": 0,
        "report_status": "pending",
        "stats_status": "skipped",
        "delete_status": "pending",
        "upsert_status": "pending",
        "stage": "init",
    }


def _set_result_stage(result: dict, stage: str) -> str:
    result["stage"] = str(stage or "")
    return result["stage"]


def _finalize_run_result(result: dict, status: str, reason: str = "") -> dict:
    result["status"] = status
    result["reason"] = str(reason or "")[:500]
    return result


def _write_step_summary(rows: list[dict], target_date: date, ext_bucket: str):
    path = (os.getenv("GITHUB_STEP_SUMMARY") or "").strip()
    if not path:
        return
    try:
        total = len(rows)
        ok_cnt = sum(1 for r in rows if r.get("status") == "ok")
        zero_cnt = sum(1 for r in rows if r.get("status") == "zero_data")
        skipped_cnt = sum(1 for r in rows if r.get("status") == "skipped")
        err_cnt = sum(1 for r in rows if r.get("status") == "error")
        bucket_name = bucket_label(ext_bucket)
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"\n## 확장소재 수집 요약 ({bucket_name})\n\n")
            f.write(f"- 대상일: `{target_date}`\n")
            f.write(f"- 계정수: `{total}` | ok `{ok_cnt}` | zero_data `{zero_cnt}` | skipped `{skipped_cnt}` | error `{err_cnt}`\n\n")
            f.write("| customer_id | status | campaigns | ext_map | target_ids | base_rows | conv_rows | stats_rows | fact_rows | reason |\n")
            f.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|\n")
            for r in rows:
                reason = str(r.get("reason") or "").replace("\n", " ").replace("|", "/")[:120]
                f.write(
                    f"| {r.get('customer_id','')} | {r.get('status','')} | {int(r.get('campaign_count',0) or 0)} | {int(r.get('extension_rows',0) or 0)} | {int(r.get('target_ad_ids',0) or 0)} | {int(r.get('report_base_rows',0) or 0)} | {int(r.get('report_conv_rows',0) or 0)} | {int(r.get('stats_rows',0) or 0)} | {int(r.get('fact_rows',0) or 0)} | {reason} |\n"
                )
    except Exception as e:
        log(f"⚠️ GITHUB_STEP_SUMMARY 기록 실패: {e}")


def now_millis() -> str:
    return str(int(time.time() * 1000))


def sign_path_only(method: str, path: str, timestamp: str, secret: str) -> str:
    msg = f"{timestamp}.{method}.{path}".encode("utf-8")
    dig = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(dig).decode("utf-8")


def make_headers(method: str, path: str, customer_id: str) -> dict:
    ts = now_millis()
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": API_KEY,
        "X-Customer": str(customer_id),
        "X-Signature": sign_path_only(method.upper(), path, ts, API_SECRET),
    }


def request_json(method: str, path: str, customer_id: str, params: dict | None = None, json_data: dict | None = None, raise_error=False):
    url = BASE_URL + path
    session = requests.Session()
    max_retries = 6
    for attempt in range(max_retries):
        try:
            r = session.request(method, url, headers=make_headers(method, path, customer_id), params=params, json=json_data, timeout=TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 + attempt + random.uniform(0.1, 0.8))
                continue
            data = None
            try:
                data = r.json()
            except Exception:
                data = r.text
            if raise_error and r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code} Error: {data}", response=r)
            return r.status_code, data
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                if raise_error:
                    raise
                return 0, str(e)
            time.sleep(2 + attempt)
    return 0, None


def get_engine():
    db_url = DB_URL + ("&sslmode=require" if "?" in DB_URL else "?sslmode=require")
    return create_engine(db_url, poolclass=NullPool, future=True)


def save_debug_report(tp: str, customer_id: str, job_id: str, content: str):
    try:
        if not content:
            return
        os.makedirs(DEBUG_REPORT_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(DEBUG_REPORT_DIR, f"{ts}_{customer_id}_{tp}_{job_id}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception:
        pass


def _report_parse_delimiters(lines: list[str]) -> list[str]:
    sample = [ln for ln in (lines or [])[:20] if str(ln).strip()]
    tab_hits = sum(ln.count("	") for ln in sample)
    comma_hits = sum(ln.count(",") for ln in sample)
    primary = "	" if tab_hits >= comma_hits else ","
    return ["	", ","] if primary == "	" else [",", "	"]


def _manual_report_parse(txt: str, delim: str) -> pd.DataFrame:
    rows = []
    reader = csv.reader(io.StringIO(txt), delimiter=delim)
    max_cols = 0
    for row in reader:
        row = [str(c).strip() for c in row]
        rows.append(row)
        max_cols = max(max_cols, len(row))
    if not rows:
        return pd.DataFrame()
    norm_rows = [r + [""] * (max_cols - len(r)) for r in rows]
    return pd.DataFrame(norm_rows)


def _pandas_report_parse(txt: str, delim: str) -> pd.DataFrame:
    return pd.read_csv(io.StringIO(txt), sep=delim, header=None, dtype=str, on_bad_lines="skip").fillna("")


def _log_report_parse_diag(mode: str, delim: str, df: pd.DataFrame):
    rows, cols = (0, 0) if df is None or df.empty else (int(df.shape[0]), int(df.shape[1]))
    delim_name = "TAB" if delim == "	" else "COMMA"
    log(f"   ↪ 리포트 원문 파싱: mode={mode} | delim={delim_name} | rows={rows} | cols={cols}")


def parse_report_text_to_df(txt: str) -> pd.DataFrame:
    txt = (txt or "").replace("﻿", "").strip()
    if not txt:
        return pd.DataFrame()

    lines = [ln for ln in txt.splitlines() if ln is not None]
    if not lines:
        return pd.DataFrame()

    tries = _report_parse_delimiters(lines)

    for delim in tries:
        try:
            df = _manual_report_parse(txt, delim)
            if not df.empty and max(df.shape) > 1:
                _log_report_parse_diag("manual", delim, df)
                return df
        except Exception:
            pass

    for delim in tries:
        try:
            df = _pandas_report_parse(txt, delim)
            if not df.empty:
                _log_report_parse_diag("pandas", delim, df)
                return df
        except Exception:
            pass

    log("⚠️ 리포트 원문 파싱 실패: 지원 가능한 구분자/TXT 구조를 찾지 못했습니다.")
    return pd.DataFrame()

def resolve_download_url(dl_url: str) -> str:
    if not dl_url:
        return ""
    dl_url = str(dl_url).strip()
    if dl_url.startswith("http://") or dl_url.startswith("https://"):
        return dl_url
    if dl_url.startswith("/"):
        return BASE_URL + dl_url
    return f"{BASE_URL}/{dl_url.lstrip('/')}"


def download_report_dataframe(customer_id: str, tp: str, job_id: str, initial_url: str) -> pd.DataFrame:
    session = requests.Session()
    current_url = initial_url
    for retry in range(3):
        url = resolve_download_url(current_url)
        try:
            r = session.get(url, timeout=60, allow_redirects=True)
            if r.status_code == 200:
                r.encoding = "utf-8"
                save_debug_report(tp, customer_id, job_id, r.text)
                return parse_report_text_to_df(r.text)

            parsed = urlparse(url)
            if url.startswith(BASE_URL):
                r2 = session.get(url, headers=make_headers("GET", parsed.path or "/", customer_id), timeout=60, allow_redirects=True)
                if r2.status_code == 200:
                    r2.encoding = "utf-8"
                    save_debug_report(tp, customer_id, job_id, r2.text)
                    return parse_report_text_to_df(r2.text)

            s_status, s_data = request_json("GET", f"/stat-reports/{job_id}", customer_id)
            if s_status == 200 and isinstance(s_data, dict) and s_data.get("downloadUrl"):
                current_url = s_data.get("downloadUrl")
            log(f"⚠️ [{tp}] 다운로드 실패 재시도 {retry+1}/3")
            time.sleep(2)
        except Exception as e:
            log(f"⚠️ [{tp}] 다운로드 예외: {e} (재시도 {retry+1}/3)")
            time.sleep(2)
    return pd.DataFrame()


def fetch_stat_report(customer_id: str, report_tp: str, target_date: date) -> pd.DataFrame:
    payload = {"reportTp": report_tp, "statDt": target_date.strftime("%Y%m%d")}
    status, data = request_json("POST", "/stat-reports", customer_id, json_data=payload)
    if status != 200 or not isinstance(data, dict) or not data.get("reportJobId"):
        log(f"⚠️ [{report_tp}] 리포트 요청 실패: HTTP {status} | {data}")
        return pd.DataFrame()

    job_id = data["reportJobId"]
    for _ in range(120):
        s_status, s_data = request_json("GET", f"/stat-reports/{job_id}", customer_id)
        if s_status == 200 and isinstance(s_data, dict):
            st = s_data.get("status")
            if st == "BUILT":
                try:
                    return download_report_dataframe(customer_id, report_tp, job_id, s_data.get("downloadUrl", ""))
                finally:
                    request_json("DELETE", f"/stat-reports/{job_id}", customer_id)
            if st == "NONE":
                request_json("DELETE", f"/stat-reports/{job_id}", customer_id)
                log(f"⚠️ [{report_tp}] NONE 응답")
                return pd.DataFrame()
            if st == "ERROR":
                request_json("DELETE", f"/stat-reports/{job_id}", customer_id)
                log(f"⚠️ [{report_tp}] ERROR 응답")
                return pd.DataFrame()
        time.sleep(1)
    request_json("DELETE", f"/stat-reports/{job_id}", customer_id)
    log(f"⚠️ [{report_tp}] 타임아웃")
    return pd.DataFrame()


def upsert_many(engine, table: str, rows: list, pk_cols: list):
    if not rows:
        return
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep="last")
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    conflict = (
        f'ON CONFLICT ({pk_str}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
        if update_cols else f'ON CONFLICT ({pk_str}) DO NOTHING'
    )
    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict}'
    tuples = list(df.itertuples(index=False, name=None))
    raw_conn, cur = None, None
    try:
        raw_conn = engine.raw_connection()
        cur = raw_conn.cursor()
        psycopg2.extras.execute_values(cur, sql, tuples, page_size=2000)
        raw_conn.commit()
    except Exception as e:
        if raw_conn:
            raw_conn.rollback()
        log(f"⚠️ {table} 저장 중 오류: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if raw_conn:
            raw_conn.close()


def clear_fact_scope(engine, customer_id: str, target_date: date, ad_ids: list[str]):
    ad_ids = sorted({str(x).strip() for x in (ad_ids or []) if str(x).strip()})
    if not ad_ids:
        return True
    try:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM fact_ad_daily WHERE customer_id=:cid AND dt=:dt AND ad_id = ANY(:ids)"),
                {"cid": str(customer_id), "dt": target_date, "ids": ad_ids},
            )
        return True
    except Exception as e:
        log(f"⚠️ fact_ad_daily 범위 삭제 실패: {e}")
        return False


def normalize_header(v: str) -> str:
    return str(v).lower().replace(" ", "").replace("_", "").replace("-", "").replace('"', '').replace("'", "")


def get_col_idx(headers, candidates):
    norm_headers = [normalize_header(h) for h in headers]
    norm_candidates = [normalize_header(c) for c in candidates]
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c == h:
                return i
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c in h and "그룹" not in h:
                return i
    return -1


def safe_float(v) -> float:
    if pd.isna(v):
        return 0.0
    s = str(v).replace(",", "").strip()
    if not s or s == "-":
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _normalize_ext_info(ext: dict):
    ext_info = ext.get("adExtension")
    if isinstance(ext_info, (dict, list)):
        return ext_info
    return ext or {}


def _iter_dicts(value):
    if isinstance(value, dict):
        yield value
        for v in value.values():
            yield from _iter_dicts(v)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_dicts(item)


def _iter_text_values(value):
    if isinstance(value, str):
        v = value.strip()
        if v and not v.startswith("http"):
            yield v
        return
    if isinstance(value, dict):
        skip_keys = {"extensionType", "status", "nccAdExtensionId", "ownerId", "customer_id", "type"}
        for k, v in value.items():
            if k in skip_keys:
                continue
            yield from _iter_text_values(v)
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_text_values(item)


def _first_non_empty(value, keys):
    for d in _iter_dicts(value):
        for k in keys:
            v = d.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""


def parse_ext_name(ext: dict) -> str:
    ext_info = _normalize_ext_info(ext)
    ext_type = ext.get("extensionType") or ext.get("type") or "확장소재"
    cands = ["promoText", "addPromoText", "subLinkName", "pcText", "mobileText", "description", "title", "text", "name"]
    text_val = _first_non_empty(ext_info, cands)
    if not text_val:
        vals = []
        seen = set()
        for v in _iter_text_values(ext_info):
            if v not in seen:
                seen.add(v)
                vals.append(v)
            if len(vals) >= 5:
                break
        text_val = " / ".join(vals) if vals else str(ext_info)[:150]
    return f"[확장소재] {ext_type} | {text_val}"


def campaign_bucket(campaign_tp: str | None) -> str:
    return "shopping" if str(campaign_tp or "").upper() == "SHOPPING" else "non_shopping"


def bucket_label(ext_bucket: str) -> str:
    return {"shopping": "쇼핑검색(SSA)", "non_shopping": "파워링크 외 검색광고", "all": "전체"}.get(ext_bucket, ext_bucket)


def match_bucket(campaign_tp: str | None, ext_bucket: str) -> bool:
    bucket = campaign_bucket(campaign_tp)
    return ext_bucket == "all" or bucket == ext_bucket


REPORT_ID_ALIASES = [
    "확장소재id", "광고확장소재id", "nccadextensionid", "adextensionid", "adextension",
    "소재id", "광고id", "adid", "id", "nccadid"
]
REPORT_IMP_ALIASES = [
    "노출수", "노출", "impressions", "impression", "imp", "impcnt", "exposurecount"
]
REPORT_CLK_ALIASES = [
    "클릭수", "클릭", "clicks", "click", "clk", "clkcnt", "clickcount"
]
REPORT_COST_ALIASES = [
    "총비용", "비용", "광고비", "소진비용", "cost", "spend", "salesamt", "amount"
]
REPORT_CONV_ALIASES = [
    "전환수", "전환", "conversion", "conversions", "ccnt", "conv", "conversioncount"
]
REPORT_SALES_ALIASES = [
    "전환매출액", "전환매출", "매출", "매출액", "conversionvalue", "sales", "convamt", "salesamount"
]


def _row_non_empty_count(row_vals: list[str]) -> int:
    return sum(1 for x in row_vals if str(x).strip())


def _row_contains_any(row_vals: list[str], candidates: list[str]) -> bool:
    cand_norm = [normalize_header(c) for c in candidates]
    for rv in row_vals:
        for c in cand_norm:
            if c and c in rv:
                return True
    return False


def _find_header_row(df: pd.DataFrame, id_candidates: list[str]) -> int:
    max_rows = min(100, len(df))
    metric_aliases = REPORT_IMP_ALIASES + REPORT_CLK_ALIASES + REPORT_COST_ALIASES + REPORT_CONV_ALIASES + REPORT_SALES_ALIASES
    best_idx = -1
    best_score = -1
    for i in range(max_rows):
        row_vals = [normalize_header(str(x)) for x in df.iloc[i].fillna("")]
        if _row_non_empty_count(row_vals) < 3:
            continue
        score = 0
        if _row_contains_any(row_vals, id_candidates):
            score += 4
        if _row_contains_any(row_vals, REPORT_IMP_ALIASES):
            score += 2
        if _row_contains_any(row_vals, REPORT_CLK_ALIASES):
            score += 2
        if _row_contains_any(row_vals, REPORT_COST_ALIASES):
            score += 2
        if _row_contains_any(row_vals, REPORT_CONV_ALIASES):
            score += 1
        if _row_contains_any(row_vals, REPORT_SALES_ALIASES):
            score += 1
        if any(rv in {"date", "기준일", "일자", "statdt"} for rv in row_vals):
            score += 1
        if score > best_score:
            best_score = score
            best_idx = i
        if score >= 6:
            return i
    return best_idx if best_score >= 3 else -1


def _debug_preview_rows(df: pd.DataFrame, max_rows: int = 8):
    for i in range(min(max_rows, len(df))):
        row = [str(x).strip() for x in df.iloc[i].fillna("").tolist()]
        row = [x for x in row if x]
        if row:
            log(f"   ↪ 헤더탐지 preview[{i}] {row[:12]}")


def _guess_id_col_from_rows(data_df: pd.DataFrame) -> int:
    max_scan = min(50, len(data_df))
    best_idx = -1
    best_hits = 0
    for col_idx in range(data_df.shape[1]):
        hits = 0
        for i in range(max_scan):
            try:
                val = str(data_df.iloc[i, col_idx]).strip().lower()
            except Exception:
                continue
            if val.startswith("ext-") or val.startswith("nad-"):
                hits += 1
        if hits > best_hits:
            best_hits = hits
            best_idx = col_idx
    return best_idx if best_hits >= 2 else -1




def _debug_fixed_row_dump(df: pd.DataFrame, max_rows: int = 5, max_cols: int = 40):
    if df is None or df.empty:
        return
    n_rows = min(max_rows, len(df))
    n_cols = min(max_cols, df.shape[1])
    log(f"   ↪ 쇼핑검색 ADEXTENSION raw dump 준비판 | rows={len(df)} cols={df.shape[1]} preview_rows={n_rows}")
    for i in range(n_rows):
        vals = [str(x).strip() for x in df.iloc[i].fillna("").tolist()]
        indexed = ' | '.join(f"{idx}:{vals[idx]}" for idx in range(min(n_cols, len(vals))))
        log(f"   ↪ raw[{i}] {indexed}")
        numeric = []
        for idx, v in enumerate(vals):
            vv = str(v).replace(',', '')
            if re.fullmatch(r"-?\d+(?:\.\d+)?", vv):
                numeric.append(f"{idx}:{v}")
        if numeric:
            log(f"   ↪ raw[{i}] numeric_cols {' | '.join(numeric[:20])}")


def _looks_like_fixed_adext_report(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    scan_rows = min(20, len(df))
    ext_hits = 0
    num_tail_hits = 0
    for i in range(scan_rows):
        vals = [str(x).strip() for x in df.iloc[i].fillna("").tolist()]
        if len(vals) < 12:
            continue
        if len(vals) > 6 and str(vals[6]).strip().lower().startswith("ext-"):
            ext_hits += 1
        tail = vals[-2:]
        if len(tail) == 2 and all(re.fullmatch(r"-?\d+(?:\.\d+)?", str(x).replace(",", "")) for x in tail):
            num_tail_hits += 1
    return ext_hits >= 2 and num_tail_hits >= 2


def _scan_prefixed_id_hits(df: pd.DataFrame, prefixes: tuple[str, ...], scan_rows: int) -> tuple[int, int]:
    best = (-1, -1)
    if df is None or df.empty:
        return best
    for col_idx in range(df.shape[1]):
        hits = 0
        for i in range(scan_rows):
            try:
                v = str(df.iloc[i, col_idx]).strip().lower()
            except Exception:
                continue
            if any(v.startswith(p) for p in prefixes):
                hits += 1
        if hits > best[1]:
            best = (col_idx, hits)
    return best


def _guess_tail_numeric_pair(df: pd.DataFrame, scan_rows: int) -> tuple[int | None, int | None]:
    imp_guess = None
    clk_guess = None
    if df is None or df.empty:
        return imp_guess, clk_guess
    for i in range(scan_rows):
        vals = [str(x).strip() for x in df.iloc[i].fillna("").tolist()]
        num_cols = []
        for idx, v in enumerate(vals):
            vv = str(v).replace(',', '')
            if re.fullmatch(r"-?\d+(?:\.\d+)?", vv):
                num_cols.append(idx)
        if len(num_cols) >= 2:
            imp_guess = num_cols[-2]
            clk_guess = num_cols[-1]
            break
    return imp_guess, clk_guess


def _log_fixed_adext_diag(cols: dict, ext_best: tuple[int, int], nad_best: tuple[int, int], imp_guess, clk_guess):
    log(
        "   ↪ 고정포맷 컬럼 추정: "
        f"ext_col={ext_best[0]}({ext_best[1]}hits) | "
        f"ad_col={nad_best[0]}({nad_best[1]}hits) | "
        f"imp_guess={imp_guess} | clk_guess={clk_guess} | "
        f"final={{ad:{cols.get('ad_idx')}, ext:{cols.get('id_idx')}, imp:{cols.get('imp_idx')}, clk:{cols.get('clk_idx')}}}"
    )


def _guess_fixed_adext_columns(df: pd.DataFrame, base_cols: dict | None = None) -> dict:
    # 기본 포맷(로그 preview 기준)
    # [0]dt [1]customer_id [2]campaign_id [3]adgroup_id [4]keyword_id/- [5]ad_id(nad-*) [6]ext_id(ext-*) ... [9]device [10]imp [11]clk
    cols = dict(base_cols or SHOPPING_ADEXTENSION_FIXED_DEFAULTS)
    if df is None or df.empty:
        return cols
    scan_rows = min(50, len(df))
    ext_best = _scan_prefixed_id_hits(df, ("ext-",), scan_rows)
    nad_best = _scan_prefixed_id_hits(df, ("nad-",), scan_rows)
    if ext_best[1] >= 2:
        cols['id_idx'] = ext_best[0]
    if nad_best[1] >= 2:
        cols['ad_idx'] = nad_best[0]

    imp_guess, clk_guess = _guess_tail_numeric_pair(df, scan_rows)
    if imp_guess is not None and clk_guess is not None:
        cols['imp_idx'] = imp_guess
        cols['clk_idx'] = clk_guess
    _log_fixed_adext_diag(cols, ext_best, nad_best, imp_guess, clk_guess)
    return cols

def _parse_fixed_adext_report(df: pd.DataFrame, include_conv_cols: bool, fixed_cols: dict | None = None, dump_label: str = "") -> dict:
    cols = _guess_fixed_adext_columns(df, base_cols=fixed_cols)
    dump_suffix = f" [{dump_label}]" if dump_label else ""
    log(
        f"   ↪ 고정포맷 fallback 적용{dump_suffix}: ad={cols['ad_idx']} ext={cols['id_idx']} imp={cols['imp_idx']} clk={cols['clk_idx']} cost={cols.get('cost_idx', -1)} conv={cols.get('conv_idx', -1)} sales={cols.get('sales_idx', -1)}"
    )
    out = {}
    for _, r in df.iterrows():
        vals = [str(x).strip() for x in r.fillna("").tolist()]
        if len(vals) <= max(cols['id_idx'], cols['imp_idx'], cols['clk_idx']):
            continue
        obj_id = vals[cols['id_idx']].strip()
        obj_id_l = obj_id.lower()
        if not obj_id or obj_id == '-' or not obj_id_l.startswith('ext-'):
            continue
        bucket = out.setdefault(obj_id, {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0})
        bucket['imp'] += int(safe_float(vals[cols['imp_idx']])) if cols['imp_idx'] != -1 else 0
        bucket['clk'] += int(safe_float(vals[cols['clk_idx']])) if cols['clk_idx'] != -1 else 0
        if cols['cost_idx'] != -1 and len(vals) > cols['cost_idx']:
            bucket['cost'] += int(safe_float(vals[cols['cost_idx']]))
        if include_conv_cols and cols['conv_idx'] != -1 and len(vals) > cols['conv_idx']:
            bucket['conv'] += safe_float(vals[cols['conv_idx']])
        if include_conv_cols and cols['sales_idx'] != -1 and len(vals) > cols['sales_idx']:
            bucket['sales'] += int(safe_float(vals[cols['sales_idx']]))
    return out

def _detect_metric_report_layout(df: pd.DataFrame, id_candidates: list[str]) -> dict:
    header_idx = _find_header_row(df, id_candidates)
    if header_idx == -1:
        return {"header_idx": -1, "raw_headers": [], "headers": [], "data_df": pd.DataFrame(), "idxs": {}}

    raw_headers = [str(x).strip() for x in df.iloc[header_idx].fillna("")]
    headers = [normalize_header(str(x)) for x in raw_headers]
    data_df = df.iloc[header_idx + 1:].reset_index(drop=True)
    idxs = {
        "id_idx": get_col_idx(headers, REPORT_ID_ALIASES + id_candidates),
        "imp_idx": get_col_idx(headers, REPORT_IMP_ALIASES),
        "clk_idx": get_col_idx(headers, REPORT_CLK_ALIASES),
        "cost_idx": get_col_idx(headers, REPORT_COST_ALIASES),
        "conv_idx": get_col_idx(headers, REPORT_CONV_ALIASES),
        "sales_idx": get_col_idx(headers, REPORT_SALES_ALIASES),
    }
    return {"header_idx": header_idx, "raw_headers": raw_headers, "headers": headers, "data_df": data_df, "idxs": idxs}


def _metric_should_skip_id(obj_id: str) -> bool:
    obj_id_l = str(obj_id or "").strip().lower()
    if not obj_id_l or obj_id_l == "-":
        return True
    if obj_id_l in {"id", "adid", "adextensionid", "nccadextensionid", "확장소재id", "광고id", "소재id"}:
        return True
    return not (obj_id_l.startswith("ext-") or obj_id_l.startswith("nad-"))


def _accumulate_metric_bucket(bucket: dict, row, idxs: dict, include_conv_cols: bool):
    imp_idx = idxs.get("imp_idx", -1)
    clk_idx = idxs.get("clk_idx", -1)
    cost_idx = idxs.get("cost_idx", -1)
    conv_idx = idxs.get("conv_idx", -1)
    sales_idx = idxs.get("sales_idx", -1)
    if imp_idx != -1 and len(row) > imp_idx:
        bucket["imp"] += int(safe_float(row.iloc[imp_idx]))
    if clk_idx != -1 and len(row) > clk_idx:
        bucket["clk"] += int(safe_float(row.iloc[clk_idx]))
    if cost_idx != -1 and len(row) > cost_idx:
        bucket["cost"] += int(safe_float(row.iloc[cost_idx]))
    if include_conv_cols and conv_idx != -1 and len(row) > conv_idx:
        bucket["conv"] += safe_float(row.iloc[conv_idx])
    if include_conv_cols and sales_idx != -1 and len(row) > sales_idx:
        bucket["sales"] += int(safe_float(row.iloc[sales_idx]))


def _log_metric_report_diag(report_name: str, bucket: str, mode: str, row_count: int, kept: int, idxs: dict):
    log(
        "   ↪ 확장소재 리포트 파싱: "
        f"report={report_name or '-'} | bucket={bucket or '-'} | mode={mode} | rows={row_count} | kept={kept} | "
        f"id={idxs.get('id_idx', -1)} imp={idxs.get('imp_idx', -1)} clk={idxs.get('clk_idx', -1)} cost={idxs.get('cost_idx', -1)} conv={idxs.get('conv_idx', -1)} sales={idxs.get('sales_idx', -1)}"
    )


def _parse_metric_report(df: pd.DataFrame, id_candidates: list[str], include_conv_cols: bool, report_name: str = "", bucket: str = "") -> dict:
    if df is None or df.empty:
        return {}

    layout = _detect_metric_report_layout(df, id_candidates)
    if layout["header_idx"] == -1:
        if _looks_like_fixed_adext_report(df):
            log("⚠️ 리포트 헤더를 찾지 못했습니다. 쇼핑검색 ADEXTENSION 고정포맷 fallback으로 계속 진행합니다.")
            _debug_preview_rows(df)
            if (bucket or "").lower() == "shopping" and (report_name or "").upper() == "ADEXTENSION":
                _debug_fixed_row_dump(df)
                fixed_cols = _load_shopping_fixed_override()
                log(f"   ↪ 쇼핑검색 ADEXTENSION 전용 인덱스 준비판: {fixed_cols}")
                out = _parse_fixed_adext_report(df, include_conv_cols, fixed_cols=fixed_cols, dump_label="shopping_ADEXTENSION")
            else:
                out = _parse_fixed_adext_report(df, include_conv_cols, dump_label=report_name or bucket)
            _log_metric_report_diag(report_name, bucket, "fixed_fallback", len(df), len(out), {"id_idx": -1, "imp_idx": -1, "clk_idx": -1, "cost_idx": -1, "conv_idx": -1, "sales_idx": -1})
            return out
        log("⚠️ 리포트 헤더를 찾지 못했습니다. preview를 남기고 이 리포트는 건너뜁니다.")
        _debug_preview_rows(df)
        return {}

    raw_headers = layout["raw_headers"]
    data_df = layout["data_df"]
    idxs = layout["idxs"]
    if idxs["id_idx"] == -1:
        idxs["id_idx"] = _guess_id_col_from_rows(data_df)
        if idxs["id_idx"] != -1:
            header_name = raw_headers[idxs['id_idx']] if idxs['id_idx'] < len(raw_headers) else ''
            log(f"   ↪ 확장소재 ID 컬럼 fallback 적용: col={idxs['id_idx']} header='{header_name}'")
    if idxs["id_idx"] == -1:
        log("⚠️ 리포트에 확장소재 ID 컬럼을 찾지 못했습니다. preview를 남깁니다.")
        _debug_preview_rows(df)
        return {}

    out = {}
    kept = 0
    for _, r in data_df.iterrows():
        if len(r) <= idxs["id_idx"]:
            continue
        obj_id = str(r.iloc[idxs["id_idx"]]).strip()
        if _metric_should_skip_id(obj_id):
            continue
        kept += 1
        bucket_row = out.setdefault(obj_id, {"imp": 0, "clk": 0, "cost": 0, "conv": 0.0, "sales": 0})
        _accumulate_metric_bucket(bucket_row, r, idxs, include_conv_cols)
    _log_metric_report_diag(report_name, bucket, "header", len(data_df), kept, idxs)
    return out

def _merge_metric_maps(base_map: dict, conv_map: dict) -> dict:
    out = {}
    keys = set(base_map.keys()) | set(conv_map.keys())
    for k in keys:
        b = base_map.get(k, {})
        c = conv_map.get(k, {})
        out[k] = {
            "imp": int(b.get("imp", 0) or 0),
            "clk": int(b.get("clk", 0) or 0),
            "cost": int(b.get("cost", 0) or 0),
            "conv": float(c.get("conv", b.get("conv", 0.0)) or 0.0),
            "sales": int(c.get("sales", b.get("sales", 0)) or 0),
        }
    return out


def _fetch_stats_chunk(customer_id: str, ids: list[str], target_date: date) -> list[dict]:
    if not ids:
        return []
    d_str = target_date.strftime("%Y-%m-%d")
    fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt"], separators=(",", ":"))
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(",", ":"))
    params = {"ids": ",".join(ids), "fields": fields, "timeRange": time_range}
    status, data = request_json("GET", "/stats", customer_id, params=params, raise_error=False)
    if status == 200 and isinstance(data, dict) and isinstance(data.get("data"), list):
        return data.get("data") or []
    return []


def fetch_extension_stats_map(customer_id: str, ids: list[str], target_date: date) -> dict:
    ids = [str(x).strip() for x in ids if str(x).strip().startswith("ext-")]
    if not ids:
        return {}
    chunks = [ids[i:i+50] for i in range(0, len(ids), 50)]
    out: dict[str, dict] = {}
    max_workers = min(6, max(1, len(chunks)))
    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_fetch_stats_chunk, customer_id, chunk, target_date) for chunk in chunks]
            for fut in futures:
                for r in fut.result() or []:
                    obj_id = str(r.get("id") or "").strip()
                    if not obj_id or not obj_id.startswith("ext-"):
                        continue
                    out[obj_id] = {
                        "imp": int(r.get("impCnt", 0) or 0),
                        "clk": int(r.get("clkCnt", 0) or 0),
                        "cost": int(float(r.get("salesAmt", 0) or 0)),
                        "conv": float(r.get("ccnt", 0) or 0.0),
                        "sales": int(float(r.get("convAmt", 0) or 0)),
                    }
    except Exception as e:
        log(f"⚠️ /stats 확장소재 보강 조회 실패: {e}")
        return {}
    return out


def _combine_report_and_stats_metrics(report_map: dict, stats_map: dict) -> tuple[dict, dict]:
    combined = {}
    source_counts = {"report_only": 0, "stats_only": 0, "stats_enriched": 0, "report_preferred": 0}
    keys = set(report_map.keys()) | set(stats_map.keys())
    for k in keys:
        r = report_map.get(k, {})
        s = stats_map.get(k, {})
        if r and not s:
            combined[k] = {
                "imp": int(r.get("imp", 0) or 0),
                "clk": int(r.get("clk", 0) or 0),
                "cost": int(r.get("cost", 0) or 0),
                "conv": float(r.get("conv", 0.0) or 0.0),
                "sales": int(r.get("sales", 0) or 0),
            }
            source_counts["report_only"] += 1
            continue
        if s and not r:
            combined[k] = {
                "imp": int(s.get("imp", 0) or 0),
                "clk": int(s.get("clk", 0) or 0),
                "cost": int(s.get("cost", 0) or 0),
                "conv": float(s.get("conv", 0.0) or 0.0),
                "sales": int(s.get("sales", 0) or 0),
            }
            source_counts["stats_only"] += 1
            continue
        row = {
            "imp": max(int(r.get("imp", 0) or 0), int(s.get("imp", 0) or 0)),
            "clk": max(int(r.get("clk", 0) or 0), int(s.get("clk", 0) or 0)),
            "cost": max(int(r.get("cost", 0) or 0), int(s.get("cost", 0) or 0)),
            "conv": max(float(r.get("conv", 0.0) or 0.0), float(s.get("conv", 0.0) or 0.0)),
            "sales": max(int(r.get("sales", 0) or 0), int(s.get("sales", 0) or 0)),
        }
        combined[k] = row
        if any((int(s.get("clk", 0) or 0) > int(r.get("clk", 0) or 0),
                int(s.get("cost", 0) or 0) > int(r.get("cost", 0) or 0),
                float(s.get("conv", 0.0) or 0.0) > float(r.get("conv", 0.0) or 0.0),
                int(s.get("sales", 0) or 0) > int(r.get("sales", 0) or 0))):
            source_counts["stats_enriched"] += 1
        else:
            source_counts["report_preferred"] += 1
    return combined, source_counts


def _report_sample(metric_map: dict, label: str):
    items = list(metric_map.items())[:5]
    for ad_id, m in items:
        log(f"   ↪ 샘플[{label}] ad_id={ad_id} imp={m.get('imp',0)} clk={m.get('clk',0)} cost={m.get('cost',0)} conv={m.get('conv',0)} sales={m.get('sales',0)}")


def _fetch_selected_campaigns(customer_id: str, ext_bucket: str, result: dict):
    _set_result_stage(result, "fetch_campaigns")
    status, camps = request_json("GET", "/ncc/campaigns", customer_id)
    if status != 200 or not isinstance(camps, list):
        log("⚠️ 캠페인 조회 실패")
        return None, f"campaign_fetch_failed:{status}"

    selected_camps = [c for c in camps if match_bucket(c.get("campaignTp"), ext_bucket)]
    shopping_cnt = sum(1 for c in selected_camps if campaign_bucket(c.get("campaignTp")) == "shopping")
    non_shopping_cnt = len(selected_camps) - shopping_cnt
    result["campaign_count"] = len(selected_camps)
    result["shopping_campaign_count"] = shopping_cnt
    result["non_shopping_campaign_count"] = non_shopping_cnt
    log(f"   ▶ 대상 캠페인 {len(selected_camps)}개 | 쇼핑검색 {shopping_cnt}개 | 파워링크외 {non_shopping_cnt}개")
    return selected_camps, ""


def _collect_extension_targets(customer_id: str, selected_camps: list, result: dict):
    _set_result_stage(result, "collect_extension_targets")
    camp_rows, ag_rows, ad_rows = [], [], []
    ad_bucket_map = {}
    target_ad_ids = []

    for c in selected_camps:
        cid = str(c.get("nccCampaignId"))
        bucket = campaign_bucket(c.get("campaignTp"))
        camp_rows.append({
            "customer_id": str(customer_id),
            "campaign_id": cid,
            "campaign_name": c.get("name"),
            "campaign_tp": c.get("campaignTp"),
            "status": c.get("status"),
        })

        for owner_id, agid, agname in [(cid, f"CAMP_{cid}", "[캠페인 공통 소재]")]:
            s, camp_exts = request_json("GET", "/ncc/ad-extensions", customer_id, params={"ownerId": owner_id})
            camp_exts = camp_exts if s == 200 and isinstance(camp_exts, list) else []
            if camp_exts:
                ag_rows.append({
                    "customer_id": str(customer_id),
                    "adgroup_id": agid,
                    "campaign_id": cid,
                    "adgroup_name": agname,
                    "status": "ELIGIBLE",
                })
                for ext in camp_exts:
                    ext_id = str(ext.get("nccAdExtensionId") or "").strip()
                    if not ext_id:
                        continue
                    target_ad_ids.append(ext_id)
                    ad_bucket_map[ext_id] = bucket
                    ext_info = _normalize_ext_info(ext)
                    display_name = parse_ext_name(ext)
                    ad_rows.append({
                        "customer_id": str(customer_id),
                        "ad_id": ext_id,
                        "adgroup_id": agid,
                        "ad_name": display_name,
                        "status": ext.get("status"),
                        "ad_title": display_name,
                        "ad_desc": display_name,
                        "pc_landing_url": _first_non_empty(ext_info, ["pcLandingUrl", "landingUrl", "pcUrl", "url"]),
                        "mobile_landing_url": _first_non_empty(ext_info, ["mobileLandingUrl", "landingUrl", "mobileUrl", "url"]),
                        "creative_text": display_name[:500],
                    })

        s_groups, groups = request_json("GET", "/ncc/adgroups", customer_id, params={"nccCampaignId": cid})
        groups = groups if s_groups == 200 and isinstance(groups, list) else []
        for g in groups:
            gid = str(g.get("nccAdgroupId"))
            ag_rows.append({
                "customer_id": str(customer_id),
                "adgroup_id": gid,
                "campaign_id": cid,
                "adgroup_name": g.get("name"),
                "status": g.get("status"),
            })
            s_exts, exts = request_json("GET", "/ncc/ad-extensions", customer_id, params={"ownerId": gid})
            exts = exts if s_exts == 200 and isinstance(exts, list) else []
            for ext in exts:
                ext_id = str(ext.get("nccAdExtensionId") or "").strip()
                if not ext_id:
                    continue
                target_ad_ids.append(ext_id)
                ad_bucket_map[ext_id] = bucket
                ext_info = _normalize_ext_info(ext)
                display_name = parse_ext_name(ext)
                ad_rows.append({
                    "customer_id": str(customer_id),
                    "ad_id": ext_id,
                    "adgroup_id": gid,
                    "ad_name": display_name,
                    "status": ext.get("status"),
                    "ad_title": display_name,
                    "ad_desc": display_name,
                    "pc_landing_url": _first_non_empty(ext_info, ["pcLandingUrl", "landingUrl", "pcUrl", "url"]),
                    "mobile_landing_url": _first_non_empty(ext_info, ["mobileLandingUrl", "landingUrl", "mobileUrl", "url"]),
                    "creative_text": display_name[:500],
                })

    result["campaign_rows"] = len(camp_rows)
    result["adgroup_rows"] = len(ag_rows)
    result["extension_rows"] = len(ad_rows)
    result["target_ad_ids"] = len({x for x in target_ad_ids if x})
    return camp_rows, ag_rows, ad_rows, sorted({x for x in target_ad_ids if x}), ad_bucket_map


def _persist_extension_dimensions(engine, camp_rows: list, ag_rows: list, ad_rows: list):
    upsert_many(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
    upsert_many(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
    upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])


def _load_extension_report_metrics(customer_id: str, target_date: date, target_ad_ids: list[str], ext_bucket: str, result: dict):
    _set_result_stage(result, "fetch_reports")
    log(f"   ▶ ADEXTENSION / ADEXTENSION_CONVERSION 리포트 수집 중...")
    base_df = fetch_stat_report(customer_id, "ADEXTENSION", target_date)
    conv_df = fetch_stat_report(customer_id, "ADEXTENSION_CONVERSION", target_date)
    result["report_base_rows"] = int(len(base_df) if base_df is not None else 0)
    result["report_conv_rows"] = int(len(conv_df) if conv_df is not None else 0)
    result["report_status"] = "ok" if (result["report_base_rows"] > 0 or result["report_conv_rows"] > 0) else "zero_data"

    id_candidates = REPORT_ID_ALIASES
    base_map = _parse_metric_report(base_df, id_candidates, include_conv_cols=False, report_name="ADEXTENSION", bucket=ext_bucket)
    conv_map = _parse_metric_report(conv_df, id_candidates, include_conv_cols=True, report_name="ADEXTENSION_CONVERSION", bucket=ext_bucket)
    metric_map = _merge_metric_maps(base_map, conv_map)
    metric_map = {ad_id: m for ad_id, m in metric_map.items() if ad_id in target_ad_ids}

    if base_map:
        _report_sample({k: v for k, v in base_map.items() if k in target_ad_ids}, "ADEXTENSION")
    if conv_map:
        _report_sample({k: v for k, v in conv_map.items() if k in target_ad_ids}, "ADEXTENSION_CONVERSION")
    return metric_map, base_map, conv_map, base_df, conv_df


def _should_fetch_extension_stats(metric_map: dict, conv_map: dict) -> bool:
    return (
        not metric_map
        or not conv_map
        or not any(int(m.get("cost", 0) or 0) > 0 or float(m.get("conv", 0.0) or 0.0) > 0 or int(m.get("sales", 0) or 0) > 0 for m in metric_map.values())
        or sum(1 for m in metric_map.values() if int(m.get("clk", 0) or 0) > 0) < max(1, len(metric_map) // 20)
    )


def _enrich_extension_metrics_with_stats(customer_id: str, target_ad_ids: list[str], target_date: date, metric_map: dict, conv_map: dict, result: dict):
    _set_result_stage(result, "maybe_fetch_stats")
    stats_needed = _should_fetch_extension_stats(metric_map, conv_map)
    stats_map = {}
    if stats_needed:
        result["stats_status"] = "started"
        log(f"   ↪ /stats 확장소재 보강 조회 시작: target_ids={len(target_ad_ids)}")
        stats_map = fetch_extension_stats_map(customer_id, target_ad_ids, target_date)
        result["stats_rows"] = len(stats_map)
        result["stats_status"] = "ok" if stats_map else "zero_data"
        if stats_map:
            _report_sample({k: v for k, v in stats_map.items() if k in target_ad_ids}, "ADEXTENSION_STATS")
        log(f"   ↪ /stats 확장소재 보강 조회 완료: rows={len(stats_map)}")
        metric_map, source_counts = _combine_report_and_stats_metrics(metric_map, stats_map)
        log(
            "   ↪ 확장소재 metric 결합 결과: "
            f"report_only={source_counts['report_only']} | stats_only={source_counts['stats_only']} | "
            f"stats_enriched={source_counts['stats_enriched']} | report_preferred={source_counts['report_preferred']}"
        )
    else:
        result["stats_status"] = "skipped"
    return metric_map, stats_map


def _build_extension_fact_rows(metric_map: dict, ad_bucket_map: dict, result: dict):
    _set_result_stage(result, "build_fact_rows")
    fact_rows = []
    shopping_zero = 0
    target_date = datetime.strptime(str(result.get("target_date")), "%Y-%m-%d").date()
    for ad_id, m in metric_map.items():
        imp = int(m.get("imp", 0) or 0)
        clk = int(m.get("clk", 0) or 0)
        cost = int(m.get("cost", 0) or 0)
        conv = float(m.get("conv", 0.0) or 0.0)
        sales = int(m.get("sales", 0) or 0)
        if imp == 0 and clk == 0 and cost == 0 and conv == 0 and sales == 0:
            continue
        if ad_bucket_map.get(ad_id) == "shopping" and imp > 0 and clk == 0:
            shopping_zero += 1
        fact_rows.append({
            "dt": target_date,
            "customer_id": str(result.get("customer_id")),
            "ad_id": str(ad_id),
            "imp": imp,
            "clk": clk,
            "cost": cost,
            "conv": conv,
            "sales": sales,
            "roas": (sales / cost * 100.0) if cost > 0 else 0.0,
        })
    result["fact_rows"] = len(fact_rows)
    result["shopping_zero_clk"] = shopping_zero
    return fact_rows


def _log_extension_fact_quality(fact_rows: list[dict], result: dict):
    nonzero_clk = sum(1 for r in fact_rows if int(r.get("clk", 0) or 0) > 0)
    nonzero_cost = sum(1 for r in fact_rows if int(r.get("cost", 0) or 0) > 0)
    nonzero_conv = sum(1 for r in fact_rows if float(r.get("conv", 0.0) or 0.0) > 0 or int(r.get("sales", 0) or 0) > 0)
    result["nonzero_clk_rows"] = nonzero_clk
    result["nonzero_cost_rows"] = nonzero_cost
    result["nonzero_conv_rows"] = nonzero_conv
    log(f"   ↪ 확장소재 최종 품질: rows={len(fact_rows)} | clk>0 {nonzero_clk} | cost>0 {nonzero_cost} | conv/sales>0 {nonzero_conv}")


def _persist_extension_fact(engine, customer_id: str, target_date: date, target_ad_ids: list[str], fact_rows: list[dict], result: dict):
    _set_result_stage(result, "persist_fact")
    if not clear_fact_scope(engine, customer_id, target_date, target_ad_ids):
        result["delete_status"] = "error"
        log("   ❌ fact_ad_daily 범위 삭제 실패로 적재를 중단합니다.")
        return False
    result["delete_status"] = "ok"
    upsert_many(engine, "fact_ad_daily", fact_rows, ["dt", "customer_id", "ad_id"])
    result["upsert_status"] = "ok"
    log(f"   ✅ 통계가 있는 확장소재 {len(fact_rows)}건 DB 적재 성공!")
    if result.get("shopping_zero_clk"):
        log(f"   ↪ 쇼핑검색(SSA)에서 imp>0 이지만 clk=0 인 확장소재 {int(result.get('shopping_zero_clk', 0) or 0)}건")
    return True


def process_account(engine, customer_id: str, target_date: date, ext_bucket: str = "shopping"):
    result = _new_run_result(customer_id, target_date, ext_bucket)
    log(f"--- [ {customer_id} ] {bucket_label(ext_bucket)} 확장소재 수집 시작 ({target_date}) ---")
    try:
        selected_camps, reason = _fetch_selected_campaigns(customer_id, ext_bucket, result)
        if selected_camps is None:
            return _finalize_run_result(result, "error", reason)

        camp_rows, ag_rows, ad_rows, target_ad_ids, ad_bucket_map = _collect_extension_targets(customer_id, selected_camps, result)
        _set_result_stage(result, "persist_dimensions")
        _persist_extension_dimensions(engine, camp_rows, ag_rows, ad_rows)
        log(f"   ▶ 캠페인({len(camp_rows)}), 광고그룹({len(ag_rows)}), 확장소재({len(ad_rows)}) 매핑 완료!")

        if not target_ad_ids:
            log("   ⚠️ 수집 대상 확장소재가 없습니다.")
            return _finalize_run_result(result, "zero_data", "no_target_extensions")

        metric_map, base_map, conv_map, base_df, conv_df = _load_extension_report_metrics(customer_id, target_date, target_ad_ids, ext_bucket, result)
        metric_map, stats_map = _enrich_extension_metrics_with_stats(customer_id, target_ad_ids, target_date, metric_map, conv_map, result)

        result["metric_rows"] = len(metric_map)
        if not metric_map:
            log("   ⚠️ 확장소재 성과 리포트에서 대상 ad_id 데이터를 찾지 못했습니다.")
            log(f"   ↪ ADEXTENSION rows={len(base_df) if base_df is not None else 0} | ADEXTENSION_CONVERSION rows={len(conv_df) if conv_df is not None else 0}")
            log("   ↪ debug_reports 폴더의 ADEXTENSION / ADEXTENSION_CONVERSION 원본을 확인하세요.")
            return _finalize_run_result(result, "zero_data", "metric_map_empty")

        missing = [ad_id for ad_id in target_ad_ids if ad_id not in metric_map]
        result["missing_target_ads"] = len(missing)
        if missing:
            log(f"   ↪ 리포트 미포착 확장소재 {len(missing)}건")
            for ad_id in missing[:10]:
                log(f"      missing ad_id={ad_id} bucket={ad_bucket_map.get(ad_id)}")

        fact_rows = _build_extension_fact_rows(metric_map, ad_bucket_map, result)
        if not fact_rows:
            log("   ⚠️ 리포트상 성과가 있는 확장소재가 없습니다.")
            return _finalize_run_result(result, "zero_data", "fact_rows_empty")

        _log_extension_fact_quality(fact_rows, result)
        if not _persist_extension_fact(engine, customer_id, target_date, target_ad_ids, fact_rows, result):
            return _finalize_run_result(result, "error", "clear_fact_scope_failed")

        _set_result_stage(result, "finalize")
        return _finalize_run_result(result, "ok")
    except Exception as e:
        stage = result.get("stage") or "unknown"
        log(f"❌ 확장소재 수집 중 예외 발생 | stage={stage}: {type(e).__name__}: {e}")
        return _finalize_run_result(result, "error", f"stage={stage} | {type(e).__name__}: {e}")


def normalize_ext_bucket(v: str) -> str:
    mapping = {
        "shopping": "shopping",
        "쇼핑검색": "shopping",
        "쇼핑검색(ssa)": "shopping",
        "non_shopping": "non_shopping",
        "파워링크외": "non_shopping",
        "파워링크 외": "non_shopping",
        "파워링크 외 검색광고": "non_shopping",
        "all": "all",
        "전체": "all",
    }
    key = str(v or "shopping").strip().lower()
    return mapping.get(key, str(v or "shopping").strip())


def main():
    engine = get_engine()
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--account_name", type=str, default="")
    parser.add_argument("--account_names", type=str, default="")
    parser.add_argument("--ext_bucket", type=str, default="shopping")
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today() - timedelta(days=1)
    ext_bucket = normalize_ext_bucket(args.ext_bucket)

    print("\n" + "=" * 50, flush=True)
    print(f"🧩 확장소재 수집기 [날짜: {target_date}]", flush=True)
    print("=" * 50 + "\n", flush=True)

    accounts = []
    if load_naver_accounts is not None:
        try:
            rows = load_naver_accounts(include_gfa=False, media_types=["sa"])
            accounts = [str(r["id"]).strip() for r in rows if str(r.get("id", "")).strip()]
        except Exception as e:
            log(f"⚠️ account_master 로드 실패, dim_account_meta 로 폴백합니다: {e}")

    if not accounts:
        try:
            with engine.connect() as conn:
                accounts = [str(r[0]) for r in conn.execute(text("SELECT DISTINCT customer_id FROM dim_account_meta WHERE COALESCE(naver_media_type, 'sa') <> 'gfa'"))]
        except Exception:
            pass

    target_name_tokens = []
    if getattr(args, "account_name", ""):
        target_name_tokens.append(str(args.account_name).strip())
    if getattr(args, "account_names", ""):
        target_name_tokens.extend([x.strip() for x in str(args.account_names).split(",") if x.strip()])

    if target_name_tokens and load_naver_accounts is not None:
        try:
            rows = load_naver_accounts(include_gfa=False, media_types=["sa"])
            exact_set = {x for x in target_name_tokens}
            filtered = [r for r in rows if r["name"] in exact_set]
            if not filtered:
                lowered = [x.lower() for x in target_name_tokens]
                filtered = [r for r in rows if any(tok in r["name"].lower() for tok in lowered)]
            if filtered:
                accounts = [str(r["id"]).strip() for r in filtered]
                log(f"🎯 업체명 필터 적용: {', '.join(target_name_tokens)} -> {len(accounts)}개")
        except Exception:
            pass

    log(f"🧩 확장소재 수집 구분: {bucket_label(ext_bucket)} ({ext_bucket})")
    results = []
    for acc in accounts:
        results.append(process_account(engine, acc, target_date, ext_bucket))

    total = len(results)
    ok_cnt = sum(1 for r in results if r.get("status") == "ok")
    zero_cnt = sum(1 for r in results if r.get("status") == "zero_data")
    skipped_cnt = sum(1 for r in results if r.get("status") == "skipped")
    err_cnt = sum(1 for r in results if r.get("status") == "error")
    bucket_name = bucket_label(ext_bucket)

    log("=" * 72)
    log(f"📋 확장소재 수집 실행 요약 | bucket={bucket_name}({ext_bucket}) | date={target_date}")
    log(f"   계정수={total} | ok={ok_cnt} | zero_data={zero_cnt} | skipped={skipped_cnt} | error={err_cnt}")
    for r in results:
        log(
            "   - "
            f"{r.get('customer_id')} | status={r.get('status')} | stage={r.get('stage')} | "
            f"campaigns={r.get('campaign_count', 0)} | ext={r.get('extension_rows', 0)} | "
            f"target_ids={r.get('target_ad_ids', 0)} | base={r.get('report_base_rows', 0)} | "
            f"conv={r.get('report_conv_rows', 0)} | stats={r.get('stats_rows', 0)} | fact={r.get('fact_rows', 0)}"
            + (f" | reason={r.get('reason')}" if r.get('reason') else "")
        )
    log("=" * 72)
    _write_step_summary(results, target_date, ext_bucket)



if __name__ == "__main__":
    main()
