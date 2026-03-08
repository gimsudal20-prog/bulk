# -*- coding: utf-8 -*-
"""
네이버 검색광고 대량 등록 + 일괄 관리 도구
- 기존 복사/일괄 수정 로직을 유지하면서 대량 등록 기능 추가
- CSV/TSV 업로드 또는 붙여넣기로 캠페인/광고그룹/키워드/소재/확장소재/제외키워드 등록
"""

from __future__ import annotations

import base64
import copy
import csv
import hashlib
import hmac
import io
import json
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
import requests
from flask import Flask, jsonify, render_template, request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
SAMPLES_DIR = os.path.join(BASE_DIR, "samples")
OPENAPI_BASE_URL = "https://api.searchad.naver.com"

app = Flask(__name__, template_folder=TEMPLATES_DIR)

DAY_NUM_TO_CODE = {1: "MON", 2: "TUE", 3: "WED", 4: "THU", 5: "FRI", 6: "SAT", 7: "SUN"}
SHOPPING_AD_TYPES = {"SHOPPING_PRODUCT_AD", "CATALOG_PRODUCT_AD"}
SYSTEM_FIELDS = {
    "nccAdId", "adExtensionId", "nccKeywordId",
    "regTm", "editTm", "status", "statusReason", "inspectStatus", "delFlag",
    "managedKeyword", "referenceData", "referenceKeyData", "nccQi",
}
BOOL_FIELDS = {
    "useDailyBudget", "useGroupBidAmt", "userLock", "enable", "keywordPlusFlag",
    "useStoreUrl", "paused", "mobilePreferred", "descriptionPin", "headlinePin",
}
INT_FIELDS = {
    "customerId", "dailyBudget", "bidAmt", "contentsNetworkBidAmt", "priority",
    "displayOrder", "price", "discountPrice", "extraCost", "bidWeight",
}
FLOAT_FIELDS = {"pcCtr", "mobileCtr"}
ENTITY_SAMPLE_HEADERS: Dict[str, List[str]] = {
    "campaign": ["name", "campaignTp", "useDailyBudget", "dailyBudget"],
    "adgroup": [
        "nccCampaignId", "name", "adgroupType", "useDailyBudget", "dailyBudget",
        "bidAmt", "pcChannelId", "mobileChannelId"
    ],
    "keyword": ["nccAdgroupId", "keyword", "useGroupBidAmt", "bidAmt", "userLock"],
    "ad": ["nccAdgroupId", "type", "ad", "referenceKey", "userLock"],
    "ad_extension": ["ownerId", "type", "title", "description", "pcChannelId", "mobileChannelId"],
    "restricted_keyword": ["nccAdgroupId", "keyword"],
}

DELETE_SAMPLE_HEADERS: Dict[str, List[str]] = {
    "campaign": ["nccCampaignId"],
    "adgroup": ["nccAdgroupId"],
    "keyword": ["nccKeywordId", "nccAdgroupId", "keyword"],
    "ad": ["nccAdId"],
    "ad_extension": ["adExtensionId", "ownerId"],
    "restricted_keyword": ["restrictedKeywordId", "nccAdgroupId", "keyword"],
}


@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({"error": f"서버 내부 오류: {str(e)}"}), 500


def _sig(ts: str, method: str, uri: str, secret_key: str) -> str:
    msg = f"{ts}.{method.upper()}.{uri}"
    dig = hmac.new(str(secret_key).strip().encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(dig).decode()


def _open_headers(api_key: str, secret_key: str, customer_id: str, method: str, uri: str) -> dict:
    ts = str(int(time.time() * 1000))
    return {
        "X-Timestamp": ts,
        "X-API-KEY": str(api_key).strip(),
        "X-Customer": str(customer_id).strip(),
        "X-Signature": _sig(ts, method, uri, secret_key),
        "Content-Type": "application/json; charset=UTF-8",
    }


def _do_req(method, api_key, secret_key, cid, uri, params=None, json_body=None, max_retries=3):
    url = OPENAPI_BASE_URL + uri
    for i in range(max_retries):
        headers = _open_headers(api_key, secret_key, cid, method, uri)
        try:
            r = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=20)
            if r.status_code in [200, 201, 204]:
                return r
            if r.status_code == 429:
                time.sleep(1.5)
                continue
            if r.status_code == 404 and "1018" in r.text:
                time.sleep(1.0)
                continue
            return r
        except requests.exceptions.RequestException as e:
            time.sleep(1.5)
            if i == max_retries - 1:
                class FakeResponse:
                    status_code = 500
                    text = f"네트워크 통신 실패: {str(e)}"

                    @staticmethod
                    def json():
                        return {"error": f"네트워크 통신 실패: {str(e)}"}

                return FakeResponse()


def _snake_to_camel(key: str) -> str:
    parts = str(key).strip().split("_")
    if len(parts) == 1:
        return parts[0]
    return parts[0] + "".join(p[:1].upper() + p[1:] for p in parts[1:])


def _boolify(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {"1", "true", "y", "yes", "t", "on"}


def _normalize_value(key: str, value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
        if value.startswith("{") or value.startswith("["):
            try:
                return json.loads(value)
            except Exception:
                pass
    if key in BOOL_FIELDS:
        return _boolify(value)
    if key in INT_FIELDS:
        try:
            return int(float(str(value).strip()))
        except Exception:
            return value
    if key in FLOAT_FIELDS:
        try:
            return float(str(value).strip())
        except Exception:
            return value
    return value


def _strip_empty(data: Any) -> Any:
    if isinstance(data, dict):
        cleaned = {}
        for k, v in data.items():
            vv = _strip_empty(v)
            if vv is None:
                continue
            if vv == "":
                continue
            cleaned[k] = vv
        return cleaned
    if isinstance(data, list):
        out = []
        for item in data:
            vv = _strip_empty(item)
            if vv is None:
                continue
            out.append(vv)
        return out
    return data


def _parse_table_text(text: str) -> List[Dict[str, Any]]:
    if not text or not text.strip():
        return []
    sample = text[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
        sep = dialect.delimiter
    except Exception:
        sep = "\t" if "\t" in sample else ","
    df = pd.read_csv(io.StringIO(text), sep=sep, dtype=str, keep_default_na=False)
    df.columns = [str(c).strip() for c in df.columns]
    rows = df.to_dict(orient="records")
    return rows


def _special_alias_map(entity_type: str) -> Dict[str, str]:
    base = {
        "customer_id": "customerId",
        "customerid": "customerId",
        "campaign_tp": "campaignTp",
        "daily_budget": "dailyBudget",
        "use_daily_budget": "useDailyBudget",
        "bid_amt": "bidAmt",
        "use_group_bid_amt": "useGroupBidAmt",
        "user_lock": "userLock",
        "pc_channel_id": "pcChannelId",
        "mobile_channel_id": "mobileChannelId",
        "owner_id": "ownerId",
        "reference_key": "referenceKey",
        "contents_network_bid_amt": "contentsNetworkBidAmt",
        "keyword_plus_flag": "keywordPlusFlag",
        "use_store_url": "useStoreUrl",
        "display_order": "displayOrder",
    }
    if entity_type == "adgroup":
        base.update({"campaign_id": "nccCampaignId"})
    elif entity_type in {"keyword", "ad", "restricted_keyword"}:
        base.update({"adgroup_id": "nccAdgroupId"})
    elif entity_type == "ad_extension":
        base.update({"adgroup_id": "ownerId"})
    return base


def _prepare_payload_row(row: Dict[str, Any], entity_type: str, cid: str) -> Dict[str, Any]:
    alias_map = _special_alias_map(entity_type)
    payload: Dict[str, Any] = {}
    for raw_key, raw_value in row.items():
        if raw_key is None:
            continue
        key0 = str(raw_key).strip()
        if not key0:
            continue
        key = alias_map.get(key0, key0)
        if "_" in key and key not in {"nccCampaignId", "nccAdgroupId", "ownerId", "referenceKey"}:
            key = _snake_to_camel(key)
        value = _normalize_value(key, raw_value)
        if value is None:
            continue
        payload[key] = value

    payload["customerId"] = int(cid)

    if entity_type == "ad":
        # ad 컬럼이 문자열이면 JSON 파싱 시도
        if isinstance(payload.get("ad"), str):
            try:
                payload["ad"] = json.loads(payload["ad"])
            except Exception:
                payload["ad"] = {"description": payload["ad"]}

    if entity_type == "restricted_keyword":
        payload = {
            "nccAdgroupId": str(payload.get("nccAdgroupId") or "").strip(),
            "customerId": int(cid),
            "keyword": str(payload.get("keyword") or payload.get("restrictedKeyword") or "").strip(),
        }

    if entity_type == "campaign":
        payload.pop("nccCampaignId", None)
    if entity_type == "ad_extension":
        payload.pop("nccAdgroupId", None)
    if entity_type != "ad":
        payload.pop("referenceData", None)

    for field in list(payload.keys()):
        if field in SYSTEM_FIELDS:
            payload.pop(field, None)

    return _strip_empty(payload)


def _result_item(row_no: int, ok: bool, name: str, detail: str = "") -> Dict[str, Any]:
    return {"row_no": row_no, "ok": ok, "name": name, "detail": detail}


def _bulk_create_campaigns(api_key: str, secret_key: str, cid: str, rows: List[Dict[str, Any]]):
    results = []
    success = fail = 0
    for idx, row in enumerate(rows, start=1):
        payload = _prepare_payload_row(row, "campaign", cid)
        name = str(payload.get("name") or f"{idx}행")
        if not payload.get("name") or not payload.get("campaignTp"):
            fail += 1
            results.append(_result_item(idx, False, name, "name / campaignTp는 필수입니다."))
            continue
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/campaigns", json_body=payload)
        if res.status_code in [200, 201]:
            success += 1
            results.append(_result_item(idx, True, name, "생성 완료"))
        else:
            fail += 1
            results.append(_result_item(idx, False, name, res.text))
    return success, fail, results


def _bulk_create_adgroups(api_key: str, secret_key: str, cid: str, rows: List[Dict[str, Any]]):
    results = []
    success = fail = 0
    for idx, row in enumerate(rows, start=1):
        payload = _prepare_payload_row(row, "adgroup", cid)
        name = str(payload.get("name") or f"{idx}행")
        if not payload.get("nccCampaignId") or not payload.get("name") or not payload.get("adgroupType"):
            fail += 1
            results.append(_result_item(idx, False, name, "nccCampaignId / name / adgroupType는 필수입니다."))
            continue
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/adgroups", json_body=payload)
        if res.status_code in [200, 201]:
            success += 1
            results.append(_result_item(idx, True, name, "생성 완료"))
        else:
            fail += 1
            results.append(_result_item(idx, False, name, res.text))
    return success, fail, results


def _bulk_create_keywords(api_key: str, secret_key: str, cid: str, rows: List[Dict[str, Any]]):
    grouped: Dict[str, List[Tuple[int, Dict[str, Any]]]] = defaultdict(list)
    results = []
    success = fail = 0
    for idx, row in enumerate(rows, start=1):
        payload = _prepare_payload_row(row, "keyword", cid)
        adg_id = str(payload.get("nccAdgroupId") or "").strip()
        name = str(payload.get("keyword") or f"{idx}행")
        if not adg_id or not payload.get("keyword"):
            fail += 1
            results.append(_result_item(idx, False, name, "nccAdgroupId / keyword는 필수입니다."))
            continue
        grouped[adg_id].append((idx, payload))

    for adg_id, items in grouped.items():
        payloads = [x[1] for x in items]
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": adg_id}, json_body=payloads)
        if res.status_code in [200, 201]:
            for idx, payload in items:
                success += 1
                results.append(_result_item(idx, True, str(payload.get("keyword")), "생성 완료"))
        else:
            for idx, payload in items:
                single = _do_req("POST", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": adg_id}, json_body=payload)
                if single.status_code in [200, 201]:
                    success += 1
                    results.append(_result_item(idx, True, str(payload.get("keyword")), "생성 완료"))
                else:
                    fail += 1
                    results.append(_result_item(idx, False, str(payload.get("keyword")), single.text))
    return success, fail, sorted(results, key=lambda x: x["row_no"])


def _post_one_ad(api_key: str, secret_key: str, cid: str, row_no: int, payload: Dict[str, Any]):
    adg_id = str(payload.get("nccAdgroupId") or "").strip()
    ad_type = str(payload.get("type") or "")
    name = str(payload.get("type") or f"{row_no}행")
    if not adg_id or not ad_type:
        return _result_item(row_no, False, name, "nccAdgroupId / type는 필수입니다.")

    body = copy.deepcopy(payload)
    if ad_type in SHOPPING_AD_TYPES:
        if "ad" not in body:
            body["ad"] = {}
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": adg_id, "isList": "true"}, json_body=[body])
    else:
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": adg_id}, json_body=body)

    if res.status_code in [200, 201]:
        return _result_item(row_no, True, name, "생성 완료")
    return _result_item(row_no, False, name, res.text)


def _bulk_create_ads(api_key: str, secret_key: str, cid: str, rows: List[Dict[str, Any]]):
    prepared = []
    for idx, row in enumerate(rows, start=1):
        prepared.append((idx, _prepare_payload_row(row, "ad", cid)))

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(_post_one_ad, api_key, secret_key, cid, idx, payload) for idx, payload in prepared]
        for future in as_completed(futures):
            results.append(future.result())

    success = sum(1 for x in results if x["ok"])
    fail = sum(1 for x in results if not x["ok"])
    return success, fail, sorted(results, key=lambda x: x["row_no"])


def _bulk_create_extensions(api_key: str, secret_key: str, cid: str, rows: List[Dict[str, Any]]):
    results = []
    success = fail = 0
    for idx, row in enumerate(rows, start=1):
        payload = _prepare_payload_row(row, "ad_extension", cid)
        owner_id = str(payload.get("ownerId") or "").strip()
        ext_type = str(payload.get("type") or "")
        name = str(payload.get("title") or payload.get("type") or f"{idx}행")
        if not owner_id or not ext_type:
            fail += 1
            results.append(_result_item(idx, False, name, "ownerId / type는 필수입니다."))
            continue
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": owner_id}, json_body=payload)
        if res.status_code in [200, 201]:
            success += 1
            results.append(_result_item(idx, True, name, "생성 완료"))
        else:
            fail += 1
            results.append(_result_item(idx, False, name, res.text))
    return success, fail, results


def _bulk_create_restricted_keywords(api_key: str, secret_key: str, cid: str, rows: List[Dict[str, Any]]):
    grouped: Dict[str, List[Tuple[int, Dict[str, Any]]]] = defaultdict(list)
    results = []
    success = fail = 0
    for idx, row in enumerate(rows, start=1):
        payload = _prepare_payload_row(row, "restricted_keyword", cid)
        adg_id = str(payload.get("nccAdgroupId") or "").strip()
        keyword = str(payload.get("keyword") or "").strip()
        if not adg_id or not keyword:
            fail += 1
            results.append(_result_item(idx, False, keyword or f"{idx}행", "nccAdgroupId / keyword는 필수입니다."))
            continue
        grouped[adg_id].append((idx, payload))

    for adg_id, items in grouped.items():
        payloads = [x[1] for x in items]
        res = _do_req("POST", api_key, secret_key, cid, "/ncc/restricted-keywords", json_body=payloads)
        if res.status_code in [200, 201, 204]:
            for idx, payload in items:
                success += 1
                results.append(_result_item(idx, True, str(payload.get("keyword")), "생성 완료"))
        else:
            for idx, payload in items:
                single = _do_req("POST", api_key, secret_key, cid, "/ncc/restricted-keywords", json_body=[payload])
                if single.status_code in [200, 201, 204]:
                    success += 1
                    results.append(_result_item(idx, True, str(payload.get("keyword")), "생성 완료"))
                else:
                    fail += 1
                    results.append(_result_item(idx, False, str(payload.get("keyword")), single.text))
    return success, fail, sorted(results, key=lambda x: x["row_no"])


def _copy_adgroup_children(api_key, secret_key, cid, old_adg_id, new_adg_id, biz_channel_id):
    errors = []

    # 1. 키워드
    r_kw = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": old_adg_id})
    if r_kw.status_code == 200:
        new_kws = []
        for kw in r_kw.json():
            item = copy.deepcopy(kw)
            for k in ['nccKeywordId', 'regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'managedKeyword', 'referenceKey']:
                item.pop(k, None)
            item.update({"nccAdgroupId": str(new_adg_id), "customerId": int(cid)})
            new_kws.append(item)
        if new_kws:
            for i in range(0, len(new_kws), 100):
                batch = new_kws[i:i + 100]
                res = _do_req("POST", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": new_adg_id}, json_body=batch)
                if res.status_code not in [200, 201]:
                    for item in batch:
                        r_single = _do_req("POST", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": new_adg_id}, json_body=item)
                        if r_single.status_code not in [200, 201]:
                            errors.append(f"키워드 에러: {r_single.text}")

    # 2. 소재
    r_ad = _do_req("GET", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": old_adg_id})
    if r_ad.status_code == 200:
        ads = r_ad.json()

        def _post_ad(ad):
            item = copy.deepcopy(ad)
            ad_type = item.get("type", "")
            ref_data = item.get("referenceData", {})

            if ad_type in SHOPPING_AD_TYPES:
                item["ad"] = {}
                ref_key = item.get("referenceKey") or ref_data.get("mallProductId") or ref_data.get("id")
                if ref_key:
                    item["referenceKey"] = str(ref_key)
            else:
                item.pop('referenceKey', None)

            for k in ['nccAdId', 'regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'referenceData', 'nccQi', 'enable']:
                item.pop(k, None)

            item.update({"nccAdgroupId": str(new_adg_id), "customerId": int(cid)})
            if "userLock" not in item:
                item["userLock"] = False

            if ad_type in SHOPPING_AD_TYPES:
                res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": new_adg_id, "isList": "true"}, json_body=[item])
            else:
                res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": new_adg_id}, json_body=item)

            if res.status_code not in [200, 201]:
                return f"소재 에러: {res.text}"
            return None

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_post_ad, ad) for ad in ads]
            for f in as_completed(futures):
                err_msg = f.result()
                if err_msg:
                    errors.append(err_msg)

    # 3. 확장소재
    r_ext = _do_req("GET", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": old_adg_id})
    if r_ext.status_code == 200:
        for ext in r_ext.json():
            item = copy.deepcopy(ext)
            for k in ['adExtensionId', 'regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'referenceKey']:
                item.pop(k, None)
            item.update({"ownerId": str(new_adg_id), "customerId": int(cid)})
            ext_type = item.get("type")
            if biz_channel_id and biz_channel_id not in ["keep", "undefined"] and ext_type in ["SUB_LINK", "THUMBNAIL", "IMAGE", "TEXT"]:
                item["pcChannelId"] = item["mobileChannelId"] = str(biz_channel_id)
            res = _do_req("POST", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": new_adg_id}, json_body=item)
            if res.status_code not in [200, 201] and "4003" not in res.text:
                errors.append(f"확장소재 에러: {res.text}")

    # 4. 제외검색어
    rk_list = []
    r_rk = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{old_adg_id}/restricted-keywords")
    if r_rk.status_code == 200 and r_rk.json():
        rk_list = r_rk.json()
    else:
        r_rk2 = _do_req("GET", api_key, secret_key, cid, "/ncc/restricted-keywords", params={"nccAdgroupId": old_adg_id})
        if r_rk2.status_code == 200 and r_rk2.json():
            rk_list = r_rk2.json()

    if rk_list:
        clean_kws = []
        for rk in rk_list:
            if isinstance(rk, dict):
                kw = rk.get("keyword") or rk.get("restrictedKeyword")
                if kw:
                    clean_kws.append(kw)
            elif isinstance(rk, str):
                clean_kws.append(rk)

        if clean_kws:
            post_payload = [{"nccAdgroupId": str(new_adg_id), "customerId": int(cid), "keyword": k} for k in clean_kws]
            r_rk_post = _do_req("POST", api_key, secret_key, cid, "/ncc/restricted-keywords", json_body=post_payload)
            if r_rk_post.status_code not in [200, 201, 204]:
                errors.append(f"제외검색어 에러: {r_rk_post.text}")

    return list(set(errors))


def _extract_adgroup(src, target_camp_id, cid, biz_channel_id):
    res = {
        "nccCampaignId": str(target_camp_id),
        "customerId": int(cid),
        "name": src.get("name"),
        "adgroupType": src.get("adgroupType"),
        "useDailyBudget": src.get("useDailyBudget", False),
        "dailyBudget": src.get("dailyBudget", 0),
        "bidAmt": src.get("bidAmt", 70),
    }
    adgroup_type = src.get("adgroupType", "")
    if biz_channel_id and biz_channel_id not in ["keep", "undefined"] and adgroup_type == "WEB_SITE":
        res["pcChannelId"] = res["mobileChannelId"] = str(biz_channel_id)
    else:
        if src.get("pcChannelId"):
            res["pcChannelId"] = str(src.get("pcChannelId"))
        if src.get("mobileChannelId"):
            res["mobileChannelId"] = str(src.get("mobileChannelId"))
    for k in ["useStoreUrl", "nccProductGroupId", "contentsNetworkBidAmt", "keywordPlusFlag", "contractId"]:
        if k in src:
            res[k] = src[k]
    return res


@app.route("/")
def index():
    accounts = []
    csv_path = os.path.join(BASE_DIR, "accounts.csv")
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
        except Exception:
            df = pd.read_csv(csv_path, encoding="cp949")
        cols = {c.lower().strip(): c for c in df.columns}
        cid_col = cols.get("customer_id") or cols.get("customerid") or df.columns[0]
        name_col = cols.get("account_name") or cols.get("name") or (df.columns[1] if len(df.columns) > 1 else df.columns[0])
        df2 = df[[cid_col, name_col]].copy()
        df2.columns = ["customer_id", "account_name"]
        df2["customer_id"] = df2["customer_id"].astype(str).str.strip()
        df2["account_name"] = df2["account_name"].astype(str).str.strip()
        accounts = df2.to_dict(orient="records")
    return render_template("index.html", accounts=accounts)


@app.route("/get_campaigns", methods=["POST"])
def get_campaigns():
    d = request.json or {}
    res = _do_req("GET", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/campaigns")
    if res.status_code == 200:
        return jsonify(res.json())
    return jsonify({"error": "캠페인 조회 실패", "details": res.text}), 400


@app.route("/get_adgroups", methods=["POST"])
def get_adgroups():
    d = request.json or {}
    res = _do_req("GET", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/adgroups", params={"nccCampaignId": d.get("campaign_id")})
    if res.status_code == 200:
        return jsonify(res.json())
    return jsonify({"error": "광고그룹 조회 실패", "details": res.text}), 400


@app.route("/get_biz_channels", methods=["POST"])
def get_biz_channels():
    d = request.json or {}
    res = _do_req("GET", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/channels")
    if res.status_code == 200:
        return jsonify(res.json())
    return jsonify({"error": "비즈채널 조회 실패", "details": res.text}), 400


@app.route("/copy_campaigns", methods=["POST"])
def copy_campaigns():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    src_ids, suffix = d.get("source_ids", []), d.get("suffix", "_복사본")

    results, all_errors = {"success": 0, "fail": 0}, []
    for src_id in src_ids:
        r_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/campaigns/{src_id}")
        if r_get.status_code != 200:
            results["fail"] += 1
            continue

        src = r_get.json()
        new_camp = {
            "customerId": int(cid),
            "name": src.get("name", "") + suffix,
            "campaignTp": src.get("campaignTp"),
            "useDailyBudget": src.get("useDailyBudget", False),
            "dailyBudget": src.get("dailyBudget", 0),
        }

        r_post = _do_req("POST", api_key, secret_key, cid, "/ncc/campaigns", json_body=new_camp)
        if r_post.status_code in [200, 201]:
            results["success"] += 1
        else:
            results["fail"] += 1
            all_errors.append(f"[{new_camp['name']}] 생성 실패: {r_post.text}")

    msg = f"캠페인 복사 완료!\n(성공: {results['success']}개, 실패: {results['fail']}개)"
    if all_errors:
        msg += "\n" + "\n".join(all_errors[:5])
    return jsonify({"ok": True, "message": msg})


@app.route("/update_budget", methods=["POST"])
def update_budget():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type, entity_ids, budget = d.get("entity_type"), d.get("entity_ids", []), int(d.get("budget", 0))
    if not entity_ids:
        return jsonify({"error": "선택된 대상이 없습니다."}), 400
    results = {"success": 0, "fail": 0}
    for eid in entity_ids:
        uri = f"/ncc/campaigns/{str(eid).strip()}" if entity_type == "campaign" else f"/ncc/adgroups/{str(eid).strip()}"
        r_get = _do_req("GET", api_key, secret_key, cid, uri)
        if r_get.status_code != 200:
            results["fail"] += 1
            continue
        obj = r_get.json()
        obj["useDailyBudget"], obj["dailyBudget"] = (budget > 0), budget
        if "budget" in obj:
            del obj["budget"]
        r_put = _do_req("PUT", api_key, secret_key, cid, uri, params={"fields": "budget"}, json_body=obj)
        if r_put.status_code == 200:
            results["success"] += 1
        else:
            results["fail"] += 1
    return jsonify({"ok": True, "message": f"총 {len(entity_ids)}개 예산 업데이트 성공: {results['success']}개 / 실패: {results['fail']}개"})


@app.route("/update_schedule", methods=["POST"])
def update_schedule():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    adgroup_ids = d.get("adgroup_ids", [])
    days, hours, bid_weight = d.get("days", []), d.get("hours", []), int(d.get("bidWeight", 100))

    codes = [f"SD{DAY_NUM_TO_CODE[int(d_num)]}{int(h):02d}{(int(h) + 1):02d}" for d_num in days for h in hours]

    results = {"success": 0, "fail": 0}
    for owner_id in adgroup_ids:
        owner_id = str(owner_id).strip()
        uri = f"/ncc/criterion/{owner_id}/SD"
        target_body = [{"customerId": int(cid), "ownerId": owner_id, "dictionaryCode": c, "type": "SD"} for c in codes]
        put_res = _do_req("PUT", api_key, secret_key, cid, uri, json_body=target_body)
        if put_res.status_code != 200:
            results["fail"] += 1
            continue
        bid_fail = False
        if codes:
            for i in range(0, len(codes), 50):
                bw_res = _do_req(
                    "PUT",
                    api_key,
                    secret_key,
                    cid,
                    f"/ncc/criterion/{owner_id}/bidWeight",
                    params={"codes": ",".join(codes[i:i + 50]), "bidWeight": bid_weight},
                )
                if bw_res.status_code != 200:
                    bid_fail = True
                    break
        if bid_fail:
            results["fail"] += 1
        else:
            results["success"] += 1
    return jsonify({"ok": True, "message": f"총 {len(adgroup_ids)}개 스케줄 업데이트 성공: {results['success']}개 / 실패: {results['fail']}개"})


@app.route("/update_schedule_campaign_bulk", methods=["POST"])
def update_schedule_campaign_bulk():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    campaign_ids = d.get("campaign_ids", [])
    days, hours, bid_weight = d.get("days", []), d.get("hours", []), int(d.get("bidWeight", 100))

    adgroup_ids = []
    for camp_id in campaign_ids:
        r_adgs = _do_req("GET", api_key, secret_key, cid, "/ncc/adgroups", params={"nccCampaignId": camp_id})
        if r_adgs.status_code == 200:
            adgroup_ids.extend([adg.get("nccAdgroupId") for adg in r_adgs.json()])

    codes = [f"SD{DAY_NUM_TO_CODE[int(d_num)]}{int(h):02d}{(int(h) + 1):02d}" for d_num in days for h in hours]

    results = {"success": 0, "fail": 0}
    for owner_id in adgroup_ids:
        uri = f"/ncc/criterion/{owner_id}/SD"
        target_body = [{"customerId": int(cid), "ownerId": owner_id, "dictionaryCode": c, "type": "SD"} for c in codes]
        if _do_req("PUT", api_key, secret_key, cid, uri, json_body=target_body).status_code != 200:
            results["fail"] += 1
            continue
        bid_fail = False
        if codes:
            for i in range(0, len(codes), 50):
                bw_res = _do_req(
                    "PUT",
                    api_key,
                    secret_key,
                    cid,
                    f"/ncc/criterion/{owner_id}/bidWeight",
                    params={"codes": ",".join(codes[i:i + 50]), "bidWeight": bid_weight},
                )
                if bw_res.status_code != 200:
                    bid_fail = True
                    break
        if bid_fail:
            results["fail"] += 1
        else:
            results["success"] += 1
    return jsonify({"ok": True, "message": f"하위 광고그룹 총 {len(adgroup_ids)}개 스케줄 일괄 변경 완료!\n(성공: {results['success']} / 실패: {results['fail']})"})


@app.route("/update_keyword_bids", methods=["POST"])
def update_keyword_bids():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type, entity_ids = d.get("entity_type"), d.get("entity_ids", [])
    bid_amt = int(d.get("bid_amt", 70))

    if not entity_ids:
        return jsonify({"error": "대상이 없습니다."}), 400

    adgroup_ids = []
    if entity_type == "campaign":
        for camp_id in entity_ids:
            r_adgs = _do_req("GET", api_key, secret_key, cid, "/ncc/adgroups", params={"nccCampaignId": camp_id})
            if r_adgs.status_code == 200:
                adgroup_ids.extend([adg.get("nccAdgroupId") for adg in r_adgs.json()])
    else:
        adgroup_ids = entity_ids

    use_group_bid = bid_amt == 0
    target_bid = bid_amt if bid_amt >= 70 else 70

    success_cnt, fail_cnt = 0, 0
    err_details = []

    for adg_id in adgroup_ids:
        r_kw = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": adg_id})
        if r_kw.status_code == 200:
            kws = r_kw.json()
            if not kws:
                continue

            update_payload = []
            for kw in kws:
                item = copy.deepcopy(kw)
                item["useGroupBidAmt"] = use_group_bid
                item["bidAmt"] = target_bid
                for k in ['regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'managedKeyword', 'referenceKey']:
                    item.pop(k, None)
                update_payload.append(item)

            for i in range(0, len(update_payload), 100):
                batch = update_payload[i:i + 100]
                r_put = _do_req("PUT", api_key, secret_key, cid, "/ncc/keywords", params={"fields": "bidAmt,useGroupBidAmt"}, json_body=batch)
                if r_put.status_code in [200, 201]:
                    success_cnt += len(batch)
                else:
                    for item in batch:
                        r_single = _do_req("PUT", api_key, secret_key, cid, f"/ncc/keywords/{item['nccKeywordId']}", params={"fields": "bidAmt,useGroupBidAmt"}, json_body=item)
                        if r_single.status_code in [200, 201]:
                            success_cnt += 1
                        else:
                            fail_cnt += 1
                            if len(err_details) < 5:
                                err_details.append(f"[{item.get('keyword', '알수없음')}] 실패: {r_single.text}")

    msg = f"키워드 입찰가 변경 완료!\n(성공: {success_cnt}개, 실패: {fail_cnt}개)"
    if err_details:
        msg += "\n\n[상세 에러 내역]\n" + "\n".join(err_details)
    return jsonify({"ok": True, "message": msg})


@app.route("/copy_adgroups_to_target", methods=["POST"])
def copy_adgroups_to_target():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    src_ids = d.get("source_ids", [])
    target_camp_id = d.get("target_campaign_id")
    suffix = d.get("suffix", "_복사본")
    biz_channel_id = d.get("biz_channel_id")

    results, all_errors = {"success": 0, "fail": 0}, []
    for src_id in src_ids:
        r_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{src_id}")
        if r_get.status_code != 200:
            results["fail"] += 1
            continue

        new_adg = _extract_adgroup(r_get.json(), target_camp_id, cid, biz_channel_id)
        new_adg["name"] = str(new_adg.get("name") or "") + suffix
        r_post = _do_req("POST", api_key, secret_key, cid, "/ncc/adgroups", json_body=new_adg)

        if r_post.status_code in [200, 201]:
            results["success"] += 1
            errs = _copy_adgroup_children(api_key, secret_key, cid, src_id, r_post.json().get("nccAdgroupId"), biz_channel_id)
            all_errors.extend([f"[{new_adg['name']}] {e}" for e in errs])
        else:
            results["fail"] += 1
            all_errors.append(f"[{new_adg['name']}] 생성 실패: {r_post.text}")

    return jsonify({"ok": True, "message": f"복사 완료! (성공: {results['success']}, 실패: {results['fail']})\n" + "\n".join(all_errors[:10])})


@app.route("/sample_headers", methods=["GET"])
def sample_headers():
    entity_type = request.args.get("entity_type", "campaign")
    headers = ENTITY_SAMPLE_HEADERS.get(entity_type, [])
    sample_row = {h: "" for h in headers}
    if entity_type == "campaign":
        sample_row.update({"name": "브랜드_검색", "campaignTp": "WEB_SITE", "useDailyBudget": "true", "dailyBudget": "50000"})
    elif entity_type == "adgroup":
        sample_row.update({"nccCampaignId": "cmp-a001", "name": "브랜드_기본", "adgroupType": "WEB_SITE", "useDailyBudget": "true", "dailyBudget": "10000", "bidAmt": "100"})
    elif entity_type == "keyword":
        sample_row.update({"nccAdgroupId": "grp-a001", "keyword": "브랜드키워드", "useGroupBidAmt": "false", "bidAmt": "120", "userLock": "false"})
    elif entity_type == "ad":
        sample_row.update({"nccAdgroupId": "grp-a001", "type": "TEXT_45", "ad": '{"headline":"제목","description":"설명"}', "userLock": "false"})
    elif entity_type == "ad_extension":
        sample_row.update({"ownerId": "grp-a001", "type": "SUB_LINK", "title": "서브링크제목", "description": "설명"})
    elif entity_type == "restricted_keyword":
        sample_row.update({"nccAdgroupId": "grp-a001", "keyword": "제외키워드"})
    return jsonify({"headers": headers, "sample_row": sample_row})


@app.route("/bulk_register", methods=["POST"])
def bulk_register():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type = d.get("entity_type")
    rows = d.get("rows") or []
    raw_text = d.get("raw_text") or ""

    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400

    if not rows and raw_text:
        rows = _parse_table_text(raw_text)

    if not rows:
        return jsonify({"error": "등록할 데이터가 없습니다."}), 400

    handlers = {
        "campaign": _bulk_create_campaigns,
        "adgroup": _bulk_create_adgroups,
        "keyword": _bulk_create_keywords,
        "ad": _bulk_create_ads,
        "ad_extension": _bulk_create_extensions,
        "restricted_keyword": _bulk_create_restricted_keywords,
    }
    handler = handlers.get(entity_type)
    if not handler:
        return jsonify({"error": f"지원하지 않는 entity_type: {entity_type}"}), 400

    success, fail, results = handler(api_key, secret_key, cid, rows)
    return jsonify({
        "ok": True,
        "entity_type": entity_type,
        "total": len(rows),
        "success": success,
        "fail": fail,
        "results": results,
    })


def _get_first(row: Dict[str, Any], *keys: str) -> Any:
    lower_map = {str(k).strip().lower(): v for k, v in row.items()}
    for key in keys:
        if key in row and row.get(key) not in [None, ""]:
            return row.get(key)
        lk = str(key).strip().lower()
        if lk in lower_map and lower_map[lk] not in [None, ""]:
            return lower_map[lk]
    return None


def _resolve_delete_target(api_key: str, secret_key: str, cid: str, entity_type: str, row: Dict[str, Any]) -> Tuple[str | None, Dict[str, Any], str]:
    params: Dict[str, Any] = {}
    rid = None

    if entity_type == "campaign":
        rid = _get_first(row, "nccCampaignId", "campaign_id", "id")
    elif entity_type == "adgroup":
        rid = _get_first(row, "nccAdgroupId", "adgroup_id", "id")
    elif entity_type == "keyword":
        rid = _get_first(row, "nccKeywordId", "keyword_id", "id")
        if not rid:
            adg_id = _get_first(row, "nccAdgroupId", "adgroup_id")
            kw = _get_first(row, "keyword")
            if adg_id and kw:
                r = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": str(adg_id).strip()})
                if r.status_code == 200:
                    for item in r.json() or []:
                        if str(item.get("keyword") or "").strip() == str(kw).strip():
                            rid = item.get("nccKeywordId")
                            break
        return (str(rid).strip() if rid else None, params, str(_get_first(row, "keyword") or rid or ""))
    elif entity_type == "ad":
        rid = _get_first(row, "nccAdId", "ad_id", "id")
    elif entity_type == "ad_extension":
        rid = _get_first(row, "adExtensionId", "ad_extension_id", "id")
        owner_id = _get_first(row, "ownerId", "owner_id")
        if owner_id:
            params["ownerId"] = str(owner_id).strip()
    elif entity_type == "restricted_keyword":
        rid = _get_first(row, "restrictedKeywordId", "nccRestrictedKeywordId", "criterionId", "id")
        kw = _get_first(row, "keyword", "restrictedKeyword")
        adg_id = _get_first(row, "nccAdgroupId", "adgroup_id")
        if not rid and adg_id and kw:
            lookup_attempts = [
                ("/ncc/restricted-keywords", {"nccAdgroupId": str(adg_id).strip()}),
                (f"/ncc/restricted-keywords/{str(adg_id).strip()}", None),
            ]
            for uri, q in lookup_attempts:
                r = _do_req("GET", api_key, secret_key, cid, uri, params=q)
                if r.status_code != 200:
                    continue
                data = r.json() or []
                if isinstance(data, dict):
                    data = data.get("items") or data.get("list") or []
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    item_kw = str(item.get("keyword") or item.get("restrictedKeyword") or "").strip()
                    if item_kw != str(kw).strip():
                        continue
                    rid = item.get("restrictedKeywordId") or item.get("nccRestrictedKeywordId") or item.get("criterionId") or item.get("id")
                    break
                if rid:
                    break
        return (str(rid).strip() if rid else None, params, str(kw or rid or ""))

    return (str(rid).strip() if rid else None, params, str(_get_first(row, "name", "keyword", "title") or rid or ""))


def _delete_uri_for(entity_type: str, rid: str) -> str:
    mapping = {
        "campaign": f"/ncc/campaigns/{rid}",
        "adgroup": f"/ncc/adgroups/{rid}",
        "keyword": f"/ncc/keywords/{rid}",
        "ad": f"/ncc/ads/{rid}",
        "ad_extension": f"/ncc/ad-extensions/{rid}",
        "restricted_keyword": f"/ncc/restricted-keywords/{rid}",
    }
    return mapping[entity_type]


def _bulk_delete_entities(api_key: str, secret_key: str, cid: str, entity_type: str, rows: List[Dict[str, Any]]):
    results = []
    success = fail = 0

    for idx, row in enumerate(rows, start=1):
        rid, params, label = _resolve_delete_target(api_key, secret_key, cid, entity_type, row)
        name = str(label or f"{idx}행")
        if not rid:
            fail += 1
            results.append(_result_item(idx, False, name, "삭제 대상 ID를 찾지 못했습니다."))
            continue

        uri = _delete_uri_for(entity_type, rid)
        res = _do_req("DELETE", api_key, secret_key, cid, uri, params=params or None)
        if res.status_code in [200, 201, 202, 204]:
            success += 1
            results.append(_result_item(idx, True, name, f"삭제 완료 ({rid})"))
        else:
            fail += 1
            results.append(_result_item(idx, False, name, f"{rid} | {res.text}"))

    return success, fail, results


@app.route("/delete_sample_headers", methods=["GET"])
def delete_sample_headers():
    entity_type = request.args.get("entity_type", "campaign")
    headers = DELETE_SAMPLE_HEADERS.get(entity_type, [])
    sample_row = {h: "" for h in headers}
    if entity_type == "campaign":
        sample_row.update({"nccCampaignId": "cmp-a001"})
    elif entity_type == "adgroup":
        sample_row.update({"nccAdgroupId": "grp-a001"})
    elif entity_type == "keyword":
        sample_row.update({"nccKeywordId": "nkw-a001", "nccAdgroupId": "grp-a001", "keyword": "브랜드키워드"})
    elif entity_type == "ad":
        sample_row.update({"nccAdId": "nad-a001"})
    elif entity_type == "ad_extension":
        sample_row.update({"adExtensionId": "ext-a001", "ownerId": "grp-a001"})
    elif entity_type == "restricted_keyword":
        sample_row.update({"restrictedKeywordId": "rkw-a001", "nccAdgroupId": "grp-a001", "keyword": "제외키워드"})
    return jsonify({"headers": headers, "sample_row": sample_row})


@app.route("/bulk_delete", methods=["POST"])
def bulk_delete():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type = d.get("entity_type")
    rows = d.get("rows") or []
    raw_text = d.get("raw_text") or ""

    if not api_key or not secret_key or not cid:
        return jsonify({"error": "API Key / Secret Key / Customer ID가 필요합니다."}), 400

    if not rows and raw_text:
        rows = _parse_table_text(raw_text)

    if not rows:
        return jsonify({"error": "삭제할 데이터가 없습니다."}), 400

    if entity_type not in DELETE_SAMPLE_HEADERS:
        return jsonify({"error": f"지원하지 않는 entity_type: {entity_type}"}), 400

    success, fail, results = _bulk_delete_entities(api_key, secret_key, cid, entity_type, rows)
    return jsonify({
        "ok": True,
        "entity_type": entity_type,
        "total": len(rows),
        "success": success,
        "fail": fail,
        "results": results,
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "naver-bulk-manager"})


if __name__ == "__main__":
    os.makedirs(SAMPLES_DIR, exist_ok=True)
    app.run(debug=True, port=5000)
