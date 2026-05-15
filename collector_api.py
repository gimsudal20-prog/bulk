# -*- coding: utf-8 -*-
from __future__ import annotations

import io
import json
import random
import time
from datetime import date
from typing import Any, Callable, Dict, List, Tuple
from urllib.parse import urlparse

import pandas as pd
from sqlalchemy.engine import Engine


def list_ads(customer_id: str, adgroup_id: str, safe_call: Callable[..., Tuple[bool, Any]]) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/ads", customer_id, {"nccAdgroupId": adgroup_id})
    if ok and isinstance(data, list) and data:
        return data
    ok_owner, data_owner = safe_call("GET", "/ncc/ads", customer_id, {"ownerId": adgroup_id})
    if ok_owner and isinstance(data_owner, list):
        return data_owner
    return data if ok and isinstance(data, list) else []



def extract_ad_creative_fields(ad_obj: dict, json_module=json) -> Dict[str, str]:
    ad_inner = ad_obj.get("ad", {})
    image_url, title, desc = "", "", ""
    vd = ad_inner.get("valData")
    val_data = {}
    if isinstance(vd, str):
        try:
            val_data = json_module.loads(vd)
        except Exception:
            pass
    elif isinstance(vd, dict):
        val_data = vd

    if val_data:
        title = title or val_data.get("customProductName") or val_data.get("productName") or val_data.get("title") or ""
        image_url = image_url or val_data.get("imageUrl") or val_data.get("image") or ""

    sp = ad_inner.get("shoppingProduct")
    sp_data = {}
    if isinstance(sp, str):
        try:
            sp_data = json_module.loads(sp)
        except Exception:
            pass
    elif isinstance(sp, dict):
        sp_data = sp

    if sp_data:
        title = title or sp_data.get("name") or sp_data.get("productName") or ""
        image_url = image_url or sp_data.get("imageUrl") or ""

    if not image_url:
        image_url = ad_inner.get("image", {}).get("imageUrl", "") if isinstance(ad_inner.get("image"), dict) else ""
    if not image_url:
        image_url = ad_inner.get("imageUrl") or ad_inner.get("mobileImageUrl") or ad_inner.get("pcImageUrl") or ""

    title = title or ad_inner.get("headline") or ad_inner.get("title") or ""
    desc = ad_inner.get("description") or ad_inner.get("desc") or ad_inner.get("addPromoText") or ""

    pc_url = ad_inner.get("pcLandingUrl") or ad_obj.get("pcLandingUrl") or ""
    m_url = ad_inner.get("mobileLandingUrl") or ad_obj.get("mobileLandingUrl") or ""

    creative_text = f"{title} | {desc}".strip(" |")
    if pc_url:
        creative_text += f" | {pc_url}"

    return {
        "ad_title": str(title)[:200],
        "ad_desc": str(desc)[:200],
        "pc_landing_url": str(pc_url)[:500],
        "mobile_landing_url": str(m_url)[:500],
        "creative_text": str(creative_text)[:500],
        "image_url": str(image_url)[:1000],
    }



def get_stats_range(customer_id: str, ids: List[str], d1: date, request_json: Callable[..., Tuple[int, Any]]) -> List[dict]:
    if not ids:
        return []
    out: List[dict] = []
    d_str = d1.strftime("%Y-%m-%d")
    fields = json.dumps(["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt", "avgRnk"], separators=(",", ":"))
    time_range = json.dumps({"since": d_str, "until": d_str}, separators=(",", ":"))
    chunks = [ids[i : i + 50] for i in range(0, len(ids), 50)]

    def fetch_chunk(chunk: List[str]) -> List[dict]:
        params = {"ids": ",".join(chunk), "fields": fields, "timeRange": time_range}
        status, data = request_json("GET", "/stats", customer_id, params=params, raise_error=False)
        if status == 200 and isinstance(data, dict) and "data" in data:
            return data["data"]
        return []

    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(20, max(1, len(chunks)))) as executor:
        results = executor.map(fetch_chunk, chunks)
        for res in results:
            out.extend(res)
    return out



def fetch_stats_fallback(
    engine: Engine,
    customer_id: str,
    target_date: date,
    ids: List[str],
    id_key: str,
    table_name: str,
    *,
    split_map: dict | None = None,
    scoped_replace: bool = False,
    get_stats_range_fn: Callable[[str, List[str], date], List[dict]],
    clear_fact_range_fn: Callable[[Engine, str, str, date], None],
    replace_fact_scope_fn: Callable[[Engine, str, List[Dict[str, Any]], str, date, str, List[str]], None],
    replace_fact_range_fn: Callable[[Engine, str, List[Dict[str, Any]], str, date], None],
) -> int:
    if not ids:
        if not scoped_replace:
            clear_fact_range_fn(engine, table_name, customer_id, target_date)
        return 0

    raw_stats = get_stats_range_fn(customer_id, ids, target_date)
    rows: List[Dict[str, Any]] = []
    for r in raw_stats or []:
        obj_id = str(r.get("id") or "").strip()
        if not obj_id:
            continue

        imp = int(r.get("impCnt", 0) or 0)
        clk = int(r.get("clkCnt", 0) or 0)
        cost = int(float(r.get("salesAmt", 0) or 0))
        total_conv = float(r.get("ccnt", 0) or 0)
        total_sales = int(float(r.get("convAmt", 0) or 0))

        if imp == 0 and clk == 0 and cost == 0 and total_conv == 0 and total_sales == 0:
            continue

        split = split_map.get(obj_id) if split_map else None
        purchase_conv = split.get("purchase_conv", 0.0) if split else None
        purchase_sales = split.get("purchase_sales", 0) if split else None
        cart_conv = split.get("cart_conv", 0.0) if split else None
        cart_sales = split.get("cart_sales", 0) if split else None
        wishlist_conv = split.get("wishlist_conv", 0.0) if split else None
        wishlist_sales = split.get("wishlist_sales", 0) if split else None

        total_roas = (total_sales / cost * 100.0) if cost > 0 else 0.0
        purchase_roas = None if purchase_sales is None or cost <= 0 else (purchase_sales / cost * 100.0)
        cart_roas = None if cart_sales is None or cost <= 0 else (cart_sales / cost * 100.0)
        wishlist_roas = None if wishlist_sales is None or cost <= 0 else (wishlist_sales / cost * 100.0)

        row: Dict[str, Any] = {
            "dt": target_date,
            "customer_id": str(customer_id),
            id_key: obj_id,
            "imp": imp,
            "clk": clk,
            "cost": cost,
            "conv": total_conv,
            "sales": total_sales,
            "roas": total_roas,
            "purchase_conv": purchase_conv,
            "purchase_sales": purchase_sales,
            "purchase_roas": purchase_roas,
            "cart_conv": cart_conv,
            "cart_sales": cart_sales,
            "cart_roas": cart_roas,
            "wishlist_conv": wishlist_conv,
            "wishlist_sales": wishlist_sales,
            "wishlist_roas": wishlist_roas,
            "split_available": bool(split),
            "data_source": "stats_total_plus_split" if split else "stats_total_only",
        }
        if id_key in ["campaign_id", "keyword_id", "ad_id"]:
            row["avg_rnk"] = float(r.get("avgRnk", 0) or 0)
        rows.append(row)

    pk_name = id_key
    if scoped_replace:
        replace_fact_scope_fn(engine, table_name, rows, customer_id, target_date, pk_name, ids)
    else:
        replace_fact_range_fn(engine, table_name, rows, customer_id, target_date)
    return len(rows)



def cleanup_ghost_reports(
    customer_id: str,
    request_json: Callable[..., Tuple[int, Any]],
    safe_call: Callable[..., Tuple[bool, Any]],
) -> None:
    status, data = request_json("GET", "/stat-reports", customer_id, raise_error=False)
    if status == 200 and isinstance(data, list):
        for job in data:
            job_id = job.get("reportJobId")
            if job_id:
                safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)



def resolve_download_url(dl_url: str, base_url: str) -> str:
    if not dl_url:
        return ""
    dl_url = str(dl_url).strip()
    if dl_url.startswith("http://") or dl_url.startswith("https://"):
        return dl_url
    if dl_url.startswith("/"):
        return base_url + dl_url
    return f"{base_url}/{dl_url.lstrip('/')}"



def parse_report_text_to_df(txt: str) -> pd.DataFrame:
    txt = txt.strip()
    if not txt:
        return pd.DataFrame()
    sep = "\t" if "\t" in txt else ","
    return pd.read_csv(io.StringIO(txt), sep=sep, header=None, dtype=str, on_bad_lines="skip")



def download_report_dataframe(
    customer_id: str,
    tp: str,
    job_id: str,
    initial_url: str,
    *,
    get_session: Callable[[], Any],
    base_url: str,
    make_headers: Callable[[str, str, str], Dict[str, str]],
    request_json: Callable[..., Tuple[int, Any]],
    save_debug_report: Callable[[str, str, str, str], None],
    parse_report_text_to_df_fn: Callable[[str], pd.DataFrame],
    log_fn: Callable[[str], None],
) -> pd.DataFrame | None:
    session = get_session()
    current_url = initial_url
    last_error = ""

    for retry in range(3):
        url = resolve_download_url(current_url, base_url)
        try:
            r = session.get(url, timeout=60, allow_redirects=True)
            if r.status_code == 200:
                r.encoding = "utf-8"
                save_debug_report(tp, customer_id, job_id, r.text)
                return parse_report_text_to_df_fn(r.text)

            last_error = f"plain HTTP {r.status_code}"

            parsed = urlparse(url)
            if url.startswith(base_url):
                auth_headers = make_headers("GET", parsed.path or "/", customer_id)
                r2 = session.get(url, headers=auth_headers, timeout=60, allow_redirects=True)
                if r2.status_code == 200:
                    r2.encoding = "utf-8"
                    save_debug_report(tp, customer_id, job_id, r2.text)
                    return parse_report_text_to_df_fn(r2.text)
                last_error = f"plain HTTP {r.status_code} / auth HTTP {r2.status_code}"

            s_status, s_data = request_json("GET", f"/stat-reports/{job_id}", customer_id, raise_error=False)
            if s_status == 200 and isinstance(s_data, dict) and s_data.get("downloadUrl"):
                current_url = s_data.get("downloadUrl")

            log_fn(f"⚠️ [{tp}] 대용량 리포트 다운로드 실패 {last_error} (재시도 {retry+1}/3)")
            time.sleep(2)
        except Exception as e:
            last_error = str(e)
            log_fn(f"⚠️ [{tp}] 대용량 리포트 처리 중 에러: {e} (재시도 {retry+1}/3)")
            time.sleep(2)

    log_fn(f"⚠️ [{tp}] 다운로드 최종 실패: {last_error}")
    return None



def fetch_multiple_stat_reports(
    customer_id: str,
    report_types: List[str],
    target_date: date,
    *,
    cleanup_ghost_reports_fn: Callable[[str], None],
    request_json: Callable[..., Tuple[int, Any]],
    download_report_dataframe_fn: Callable[[str, str, str, str], pd.DataFrame | None],
    safe_call: Callable[..., Tuple[bool, Any]],
    fast_mode: bool,
    log_fn: Callable[[str], None],
) -> Dict[str, pd.DataFrame | None]:
    cleanup_ghost_reports_fn(customer_id)
    results: Dict[str, pd.DataFrame | None] = {tp: None for tp in report_types}

    for i in range(0, len(report_types), 3):
        batch = report_types[i : i + 3]
        jobs: Dict[str, str] = {}

        for tp in batch:
            time.sleep(random.uniform(0.1, 0.3) if fast_mode else random.uniform(0.5, 1.5))
            payload = {"reportTp": tp, "statDt": target_date.strftime("%Y%m%d")}
            status, data = request_json("POST", "/stat-reports", customer_id, json_data=payload, raise_error=False)
            if status == 200 and data and "reportJobId" in data:
                jobs[tp] = data["reportJobId"]
            else:
                log_fn(f"⚠️ [{tp}] 대용량 리포트 요청 실패: HTTP {status} - {data}")

        max_wait = 120
        while jobs and max_wait > 0:
            for tp, job_id in list(jobs.items()):
                s_status, s_data = request_json("GET", f"/stat-reports/{job_id}", customer_id, raise_error=False)
                if s_status == 200 and s_data:
                    stt = s_data.get("status")
                    if stt == "BUILT":
                        dl_url = s_data.get("downloadUrl")
                        if dl_url:
                            results[tp] = download_report_dataframe_fn(customer_id, tp, job_id, dl_url)
                        else:
                            log_fn(f"⚠️ [{tp}] BUILT 상태지만 downloadUrl 이 없습니다.")
                            results[tp] = None
                        safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)
                        del jobs[tp]
                    elif stt in ["NONE", "ERROR"]:
                        if stt == "ERROR":
                            log_fn(f"⚠️ [{tp}] 네이버 API 내부 리포트 생성 ERROR 발생")
                        results[tp] = pd.DataFrame() if stt == "NONE" else None
                        safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)
                        del jobs[tp]
            if jobs:
                time.sleep(0.5 if fast_mode else 1.0)
            max_wait -= 1

        for job_id in jobs.values():
            safe_call("DELETE", f"/stat-reports/{job_id}", customer_id)

    return results
