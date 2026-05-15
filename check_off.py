# -*- coding: utf-8 -*-
"""check_off.py - 매시간 캠페인 및 광고그룹 꺼짐(EXHAUSTED) 여부를 가볍게 스캔하는 스크립트"""

import os
import time
import hmac
import base64
import hashlib
import requests
import concurrent.futures
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine
import psycopg2.extras
from sqlalchemy.pool import NullPool

try:
    from account_master import load_naver_accounts
except Exception:
    load_naver_accounts = None
from dotenv import load_dotenv

load_dotenv(override=True)

API_KEY = (os.getenv("NAVER_API_KEY") or os.getenv("NAVER_ADS_API_KEY") or "").strip()
API_SECRET = (os.getenv("NAVER_API_SECRET") or os.getenv("NAVER_ADS_SECRET") or "").strip()
DB_URL = os.getenv("DATABASE_URL", "").strip()
BASE_URL = "https://api.searchad.naver.com"

def sign(method, path, ts):
    msg = f"{ts}.{method}.{path}".encode("utf-8")
    dig = hmac.new(API_SECRET.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(dig).decode("utf-8")

def get_data(cid, path):
    """네이버 API에서 캠페인/그룹 정보를 가져오는 공통 함수"""
    ts = str(int(time.time() * 1000))
    headers = {
        "X-Timestamp": ts, 
        "X-API-KEY": API_KEY, 
        "X-Customer": str(cid), 
        "X-Signature": sign("GET", path, ts)
    }
    try:
        r = requests.get(BASE_URL + path, headers=headers, timeout=10)
        if r.status_code == 200: return r.json()
    except Exception: 
        pass
    return []

def init_db(engine):
    """테이블이 없으면 자동으로 생성해주는 안전장치"""
    sql_camp = """
    CREATE TABLE IF NOT EXISTS fact_campaign_off_log (
        dt DATE, customer_id VARCHAR(50), campaign_id VARCHAR(50), off_time VARCHAR(20),
        PRIMARY KEY (dt, customer_id, campaign_id)
    );
    """
    sql_grp = """
    CREATE TABLE IF NOT EXISTS fact_adgroup_off_log (
        dt DATE, customer_id VARCHAR(50), adgroup_id VARCHAR(50), off_time VARCHAR(20),
        PRIMARY KEY (dt, customer_id, adgroup_id)
    );
    """
    try:
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.execute(sql_camp)
        cur.execute(sql_grp)
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def check_account(engine, cid):
    # 1. 네이버에서 캠페인과 그룹 전체 리스트를 한 번에 다운로드
    camps = get_data(cid, "/ncc/campaigns")
    grps = get_data(cid, "/ncc/adgroups")
    
    target_date = date.today()
    camp_off = []
    grp_off = []
    
    scan_kst = datetime.utcnow() + timedelta(hours=9)

    def _is_budget_stop(status: str, reason: str) -> bool:
        s = (status or "").upper()
        r = (reason or "").upper()
        return ("EXHAUSTED" in s) or any(k in r for k in ["LIMIT", "BUDGET", "EXHAUSTED"])

    # 2. 캠페인 꺼짐 검사
    if camps:
        for c in camps:
            if _is_budget_stop(c.get("status", ""), c.get("statusReason", "")):
                edit_tm = c.get("editTm", "")
                off_time = scan_kst.strftime("%H:%M")
                if edit_tm:
                    try:
                        utc_dt = datetime.strptime(edit_tm[:19], "%Y-%m-%dT%H:%M:%S")
                        kst_dt = utc_dt + timedelta(hours=9)
                        off_time = kst_dt.strftime("%H:%M") if kst_dt.date() == target_date else off_time
                    except Exception:
                        pass
                camp_off.append((target_date, str(cid), str(c["nccCampaignId"]), off_time))

    # 3. 🚨 추가된 기능: 광고그룹 꺼짐 검사
    if grps:
        for g in grps:
            if _is_budget_stop(g.get("status", ""), g.get("statusReason", "")):
                edit_tm = g.get("editTm", "")
                off_time = scan_kst.strftime("%H:%M")
                if edit_tm:
                    try:
                        utc_dt = datetime.strptime(edit_tm[:19], "%Y-%m-%dT%H:%M:%S")
                        kst_dt = utc_dt + timedelta(hours=9)
                        off_time = kst_dt.strftime("%H:%M") if kst_dt.date() == target_date else off_time
                    except Exception:
                        pass
                grp_off.append((target_date, str(cid), str(g["nccAdgroupId"]), off_time))

    # 4. 발견된 내역 DB에 꽂아넣기
    raw_conn = None
    cur = None
    try:
        raw_conn = engine.raw_connection()
        cur = raw_conn.cursor()
        
        # 캠페인 기록
        if camp_off:
            sql_c = 'INSERT INTO fact_campaign_off_log ("dt", "customer_id", "campaign_id", "off_time") VALUES %s ON CONFLICT ("dt", "customer_id", "campaign_id") DO UPDATE SET "off_time"=EXCLUDED."off_time"'
            psycopg2.extras.execute_values(cur, sql_c, camp_off)
            
        # 광고그룹 기록
        if grp_off:
            sql_g = 'INSERT INTO fact_adgroup_off_log ("dt", "customer_id", "adgroup_id", "off_time") VALUES %s ON CONFLICT ("dt", "customer_id", "adgroup_id") DO UPDATE SET "off_time"=EXCLUDED."off_time"'
            psycopg2.extras.execute_values(cur, sql_g, grp_off)
            
        if camp_off or grp_off:
            raw_conn.commit()
            print(f"✅ [{cid}] 꺼짐 감지! (캠페인 {len(camp_off)}개, 그룹 {len(grp_off)}개 기록 완료)")
            
    except Exception as e:
        if raw_conn: raw_conn.rollback()
    finally:
        if cur: cur.close()
        if raw_conn: raw_conn.close()

def main():
    print("="*60)
    print(f"🚀 캠페인/그룹 꺼짐 1초 컷 스캔 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print("="*60)
    
    if not DB_URL:
        print("❌ DATABASE_URL 설정 오류")
        return
        
    db_url = DB_URL + ("&sslmode=require" if "?" in DB_URL else "?sslmode=require")
    engine = create_engine(db_url, poolclass=NullPool)
    
    # 테이블 자동 생성
    init_db(engine)
    
    accounts = []
    if load_naver_accounts is not None:
        try:
            rows = load_naver_accounts(include_gfa=False)
            accounts = [str(r["id"]).strip() for r in rows if str(r.get("id", "")).strip()]
        except Exception:
            accounts = []

    if not accounts and os.path.exists("accounts.xlsx"):
        try:
            df = pd.read_excel("accounts.xlsx")
            id_col = next((c for c in df.columns if str(c).replace(" ", "").lower() in ["커스텀id", "customerid", "customer_id", "id"]), None)
            name_col = next((c for c in df.columns if str(c).replace(" ", "").lower() in ["업체명", "accountname", "account_name", "name"]), None)
            if id_col:
                if name_col:
                    rows = []
                    for _, row in df.iterrows():
                        nm = str(row.get(name_col, "")).strip()
                        if nm.lower().endswith(" gfa"):
                            continue
                        cid = str(row[id_col]).strip()
                        if cid and cid.lower() != "nan":
                            rows.append(cid)
                    accounts = list(dict.fromkeys(rows))
                else:
                    accounts = df[id_col].dropna().astype(str).unique().tolist()
        except Exception:
            pass

    if not accounts and os.getenv("CUSTOMER_ID"):
        accounts = [os.getenv("CUSTOMER_ID")]

    # DB 부하를 막기 위해 워커 3개로 천천히 스캔
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as exe:
        for a in accounts:
            exe.submit(check_account, engine, a)
        
    print("🎉 순찰 100% 완료! (DB 안전 해제)")

if __name__ == "__main__": 
    main()

