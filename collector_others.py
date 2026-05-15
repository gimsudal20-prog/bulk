# -*- coding: utf-8 -*-
"""collector_others.py - Meta(Facebook/Instagram) 등 기타 매체 수집기.

현재 구현 범위
- account_master.xlsx 에서 platform=meta 인 계정만 읽음
- Meta 광고계정 일별 캠페인 성과 수집
- fact_campaign_daily 로 업서트
"""
from __future__ import annotations

import os
import time
from datetime import date, timedelta
from typing import List

import pandas as pd
import requests
import psycopg2.extras
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from account_master import load_meta_accounts

load_dotenv()
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
DB_URL = os.getenv("DATABASE_URL", "").strip()
META_API_VERSION = os.getenv("META_API_VERSION", "v19.0")


def log(msg: str):
    print(msg, flush=True)


def get_engine() -> Engine:
    db_url = DB_URL
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return create_engine(db_url, poolclass=NullPool, future=True)


def _extract_action_value(actions_list: list, action_type: str) -> float:
    if not isinstance(actions_list, list):
        return 0.0
    for action in actions_list:
        if action.get("action_type") == action_type:
            try:
                return float(action.get("value", 0.0) or 0.0)
            except Exception:
                return 0.0
    return 0.0


def fetch_meta_campaign_daily(act_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    if not META_ACCESS_TOKEN:
        log("❌ META_ACCESS_TOKEN이 설정되지 않았습니다.")
        return pd.DataFrame()

    account_id = act_id if str(act_id).startswith("act_") else f"act_{act_id}"
    url = f"https://graph.facebook.com/{META_API_VERSION}/{account_id}/insights"
    fields = ["campaign_id", "campaign_name", "impressions", "clicks", "spend", "actions", "action_values"]
    params = {
        "access_token": META_ACCESS_TOKEN,
        "level": "campaign",
        "fields": ",".join(fields),
        "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
        "time_increment": 1,
        "limit": 1000,
    }
    all_data = []
    try:
        while True:
            response = requests.get(url, params=params, timeout=60)
            res_json = response.json()
            if "error" in res_json:
                log(f"⚠️ Meta API 에러 ({account_id}): {res_json['error'].get('message')}")
                break
            all_data.extend(res_json.get("data", []))
            paging = res_json.get("paging", {})
            if "next" not in paging:
                break
            url = paging["next"]
            params = {}
            time.sleep(0.5)
    except Exception as e:
        log(f"⚠️ Meta 요청 예외 ({account_id}): {e}")

    if not all_data:
        return pd.DataFrame()

    df_raw = pd.DataFrame(all_data)
    df = pd.DataFrame()
    df["dt"] = pd.to_datetime(df_raw["date_start"]).dt.date
    df["customer_id"] = str(account_id).replace("act_", "")
    df["campaign_id"] = df_raw["campaign_id"]
    df["campaign_name"] = df_raw["campaign_name"]
    df["campaign_type"] = "META"
    df["imp"] = pd.to_numeric(df_raw["impressions"], errors="coerce").fillna(0)
    df["clk"] = pd.to_numeric(df_raw["clicks"], errors="coerce").fillna(0)
    df["cost"] = pd.to_numeric(df_raw["spend"], errors="coerce").fillna(0)
    df["conv"] = df_raw.get("actions", []).apply(lambda x: _extract_action_value(x, "purchase"))
    df["sales"] = df_raw.get("action_values", []).apply(lambda x: _extract_action_value(x, "purchase"))
    df["cart_conv"] = df_raw.get("actions", []).apply(lambda x: _extract_action_value(x, "add_to_cart"))
    df["cart_sales"] = df_raw.get("action_values", []).apply(lambda x: _extract_action_value(x, "add_to_cart"))
    return df


def upsert_df(engine: Engine, table_name: str, df: pd.DataFrame, pk_cols: List[str]):
    if df is None or df.empty:
        return 0
    df = df.drop_duplicates(subset=pk_cols, keep="last").copy()
    with engine.begin() as conn:
        cols = ",".join(f'"{c}"' for c in df.columns)
        pk = ",".join(f'"{c}"' for c in pk_cols)
        updates = ",".join(f'"{c}"=EXCLUDED."{c}"' for c in df.columns if c not in pk_cols)
        sql = f'INSERT INTO {table_name} ({cols}) VALUES %s ON CONFLICT ({pk}) DO UPDATE SET {updates}'
        raw_conn = conn.connection.connection
        cur = raw_conn.cursor()
        try:
            psycopg2.extras.execute_values(cur, sql, list(df.itertuples(index=False, name=None)), page_size=1000)
        finally:
            cur.close()
    return len(df)


def run_meta_collector(start_date: str, end_date: str):
    log(f"🚀 Meta Ads 수집 시작: {start_date} ~ {end_date}")
    accounts = load_meta_accounts()
    if not accounts:
        log("⚠️ account_master.xlsx 에 platform=meta 계정이 없습니다.")
        return
    engine = get_engine()
    total = 0
    for acc in accounts:
        log(f"👉 수집 중: [{acc['name']}] / account={acc['id']}")
        df = fetch_meta_campaign_daily(acc['id'], start_date, end_date)
        if df.empty:
            log("   - 데이터 없음")
            continue
        try:
            cnt = upsert_df(engine, "fact_campaign_daily", df, ["dt", "customer_id", "campaign_id"])
            total += cnt
            log(f"   ✅ {cnt}행 적재")
        except Exception as e:
            log(f"   ❌ DB 적재 실패: {e}")
    log(f"🎉 Meta Ads 수집 완료! 총 {total}행 적재됨.")


if __name__ == "__main__":
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    run_meta_collector(yesterday, yesterday)
