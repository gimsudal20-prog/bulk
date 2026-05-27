# -*- coding: utf-8 -*-
"""Collect Meta Ads insights into the shared dashboard schema.

Required env:
- DATABASE_URL
- META_ACCESS_TOKEN

Account sources:
- account_master.xlsx rows where platform=meta and meta_ad_account_id is filled
- or --ad_account_id / META_AD_ACCOUNT_ID for a single account
"""
from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List

from sqlalchemy import text
from sqlalchemy.engine import Engine

from account_master import load_meta_accounts
from collector_db import ensure_column, ensure_tables, get_engine, replace_fact_range, upsert_many


DEFAULT_API_VERSION = "v25.0"
PURCHASE_ACTION_PRIORITY = [
    "omni_purchase",
    "purchase",
    "offsite_conversion.fb_pixel_purchase",
    "onsite_conversion.purchase",
    "app_custom_event.fb_mobile_purchase",
]


def log(message: str) -> None:
    print(message, flush=True)


def _clean_text(value: object) -> str:
    text_value = str(value or "").strip()
    return "" if text_value.lower() in {"nan", "none", "nat"} else text_value


def _normalize_ad_account_id(value: object) -> str:
    account_id = _clean_text(value)
    if account_id.startswith("act_"):
        account_id = account_id[4:]
    if account_id.endswith(".0") and account_id[:-2].isdigit():
        account_id = account_id[:-2]
    return account_id


def _api_account_id(value: object) -> str:
    account_id = _normalize_ad_account_id(value)
    return f"act_{account_id}" if account_id else ""


def _date_range(start: date, end: date) -> list[date]:
    days = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _as_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _as_int(value: object, default: int = 0) -> int:
    return int(round(_as_float(value, float(default))))


def _extract_action_value(items: object) -> float:
    if not isinstance(items, list):
        return 0.0
    by_type: dict[str, float] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        action_type = _clean_text(item.get("action_type"))
        if not action_type:
            continue
        by_type[action_type] = by_type.get(action_type, 0.0) + _as_float(item.get("value"))
    for action_type in PURCHASE_ACTION_PRIORITY:
        if action_type in by_type:
            return by_type[action_type]
    return sum(value for action_type, value in by_type.items() if "purchase" in action_type)


class MetaApiClient:
    def __init__(self, access_token: str, api_version: str = DEFAULT_API_VERSION, timeout: int = 60):
        self.access_token = access_token
        self.api_version = api_version.strip() or DEFAULT_API_VERSION
        self.timeout = timeout
        self.base_url = f"https://graph.facebook.com/{self.api_version}"

    def get(self, path_or_url: str, params: dict[str, object] | None = None) -> dict[str, Any]:
        if path_or_url.startswith("http"):
            url = path_or_url
        else:
            path = path_or_url if path_or_url.startswith("/") else f"/{path_or_url}"
            query = dict(params or {})
            query["access_token"] = self.access_token
            url = f"{self.base_url}{path}?{urllib.parse.urlencode(query)}"

        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                try:
                    payload = json.loads(raw)
                    message = payload.get("error", {}).get("message", raw)
                except Exception:
                    message = raw
                raise RuntimeError(f"Meta API error {exc.code}: {message}") from exc
            except Exception as exc:
                last_error = exc
                log(f"WARNING Meta API retry {attempt}/3: {type(exc).__name__}: {exc}")
                time.sleep(min(8, 1 + attempt * 2))
        raise RuntimeError(f"Meta API request failed: {last_error}") from last_error

    def list_all(self, path: str, params: dict[str, object] | None = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        payload = self.get(path, params)
        while True:
            data = payload.get("data", [])
            if isinstance(data, list):
                rows.extend([item for item in data if isinstance(item, dict)])
            next_url = payload.get("paging", {}).get("next")
            if not next_url:
                break
            payload = self.get(next_url)
        return rows


def ensure_meta_schema(engine: Engine) -> None:
    ensure_tables(engine)
    for table in ["fact_campaign_daily", "fact_ad_daily"]:
        ensure_column(engine, table, "reach", "BIGINT")
        ensure_column(engine, table, "frequency", "DOUBLE PRECISION")
        ensure_column(engine, table, "cpm", "DOUBLE PRECISION")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_customer (customer_id TEXT PRIMARY KEY, account_name TEXT, manager TEXT, monthly_budget BIGINT DEFAULT 0, operating_weekdays TEXT DEFAULT '0,1,2,3,4,5,6')"))
        conn.execute(text("ALTER TABLE dim_customer ADD COLUMN IF NOT EXISTS manager TEXT"))
        conn.execute(text("ALTER TABLE dim_customer ADD COLUMN IF NOT EXISTS monthly_budget BIGINT DEFAULT 0"))
        conn.execute(text("ALTER TABLE dim_customer ADD COLUMN IF NOT EXISTS operating_weekdays TEXT DEFAULT '0,1,2,3,4,5,6'"))


def _ensure_dashboard_account(engine: Engine, account: dict[str, str]) -> None:
    customer_id = _normalize_ad_account_id(account.get("id"))
    if not customer_id:
        return
    account_name = _clean_text(account.get("name")) or customer_id
    manager = _clean_text(account.get("manager")) or "미배정"
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO dim_customer (customer_id, account_name, manager, monthly_budget, operating_weekdays)
                VALUES (:customer_id, :account_name, :manager, 0, '0,1,2,3,4,5,6')
                ON CONFLICT (customer_id) DO UPDATE SET
                    account_name = EXCLUDED.account_name,
                    manager = EXCLUDED.manager
                """
            ),
            {"customer_id": customer_id, "account_name": account_name, "manager": manager},
        )


def _build_campaign_dim_rows(customer_id: str, campaigns: Iterable[dict[str, Any]], insights: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for item in campaigns:
        campaign_id = _clean_text(item.get("id"))
        if not campaign_id:
            continue
        rows[campaign_id] = {
            "customer_id": customer_id,
            "campaign_id": campaign_id,
            "campaign_name": _clean_text(item.get("name")) or campaign_id,
            "campaign_tp": _clean_text(item.get("objective")) or "META",
            "status": _clean_text(item.get("effective_status")) or _clean_text(item.get("status")),
        }
    for item in insights:
        campaign_id = _clean_text(item.get("campaign_id"))
        if campaign_id and campaign_id not in rows:
            rows[campaign_id] = {
                "customer_id": customer_id,
                "campaign_id": campaign_id,
                "campaign_name": _clean_text(item.get("campaign_name")) or campaign_id,
                "campaign_tp": "META",
                "status": "",
            }
    return list(rows.values())


def _build_adset_dim_rows(customer_id: str, adsets: Iterable[dict[str, Any]], ad_insights: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for item in adsets:
        adset_id = _clean_text(item.get("id"))
        if not adset_id:
            continue
        rows[adset_id] = {
            "customer_id": customer_id,
            "adgroup_id": adset_id,
            "adgroup_name": _clean_text(item.get("name")) or adset_id,
            "campaign_id": _clean_text(item.get("campaign_id")),
            "status": _clean_text(item.get("effective_status")) or _clean_text(item.get("status")),
        }
    for item in ad_insights:
        adset_id = _clean_text(item.get("adset_id"))
        if adset_id and adset_id not in rows:
            rows[adset_id] = {
                "customer_id": customer_id,
                "adgroup_id": adset_id,
                "adgroup_name": _clean_text(item.get("adset_name")) or adset_id,
                "campaign_id": _clean_text(item.get("campaign_id")),
                "status": "",
            }
    return list(rows.values())


def _build_ad_dim_rows(customer_id: str, ads: Iterable[dict[str, Any]], ad_insights: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for item in ads:
        ad_id = _clean_text(item.get("id"))
        if not ad_id:
            continue
        creative = item.get("creative") if isinstance(item.get("creative"), dict) else {}
        creative_text = _clean_text(creative.get("body")) or _clean_text(creative.get("title"))
        image_url = _clean_text(creative.get("image_url")) or _clean_text(creative.get("thumbnail_url"))
        rows[ad_id] = {
            "customer_id": customer_id,
            "ad_id": ad_id,
            "adgroup_id": _clean_text(item.get("adset_id")),
            "ad_name": _clean_text(item.get("name")) or ad_id,
            "status": _clean_text(item.get("effective_status")) or _clean_text(item.get("status")),
            "ad_title": _clean_text(creative.get("title")),
            "ad_desc": _clean_text(creative.get("body")),
            "pc_landing_url": _clean_text(creative.get("object_url")),
            "mobile_landing_url": _clean_text(creative.get("object_url")),
            "creative_text": creative_text,
            "image_url": image_url,
        }
    for item in ad_insights:
        ad_id = _clean_text(item.get("ad_id"))
        if ad_id and ad_id not in rows:
            rows[ad_id] = {
                "customer_id": customer_id,
                "ad_id": ad_id,
                "adgroup_id": _clean_text(item.get("adset_id")),
                "ad_name": _clean_text(item.get("ad_name")) or ad_id,
                "status": "",
                "ad_title": "",
                "ad_desc": "",
                "pc_landing_url": "",
                "mobile_landing_url": "",
                "creative_text": "",
                "image_url": "",
            }
    return list(rows.values())


def _metric_row_base(item: dict[str, Any]) -> dict[str, Any]:
    spend = _as_float(item.get("spend"))
    purchase_conv = _extract_action_value(item.get("actions"))
    purchase_sales = _as_int(_extract_action_value(item.get("action_values")))
    purchase_roas = (purchase_sales / spend * 100.0) if spend > 0 else 0.0
    return {
        "dt": _parse_date(_clean_text(item.get("date_start"))),
        "imp": _as_int(item.get("impressions")),
        "clk": _as_int(item.get("clicks")),
        "cost": _as_int(spend),
        "conv": purchase_conv,
        "sales": purchase_sales,
        "roas": purchase_roas,
        "avg_rnk": 0.0,
        "purchase_conv": purchase_conv,
        "purchase_sales": purchase_sales,
        "purchase_roas": purchase_roas,
        "cart_conv": 0.0,
        "cart_sales": 0,
        "cart_roas": 0.0,
        "wishlist_conv": 0.0,
        "wishlist_sales": 0,
        "wishlist_roas": 0.0,
        "primary_conv": purchase_conv,
        "primary_sales": purchase_sales,
        "primary_roas": purchase_roas,
        "split_available": True,
        "data_source": "meta_api",
        "reach": _as_int(item.get("reach")),
        "frequency": _as_float(item.get("frequency")),
        "cpm": _as_float(item.get("cpm")),
    }


def _build_campaign_fact_rows(customer_id: str, insights: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for item in insights:
        campaign_id = _clean_text(item.get("campaign_id"))
        if not campaign_id or not _clean_text(item.get("date_start")):
            continue
        row = _metric_row_base(item)
        row.update({"customer_id": customer_id, "campaign_id": campaign_id})
        rows.append(row)
    return rows


def _build_ad_fact_rows(customer_id: str, insights: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for item in insights:
        ad_id = _clean_text(item.get("ad_id"))
        if not ad_id or not _clean_text(item.get("date_start")):
            continue
        row = _metric_row_base(item)
        row.update({"customer_id": customer_id, "ad_id": ad_id})
        rows.append(row)
    return rows


def _insights_params(level: str, start: date, end: date) -> dict[str, object]:
    fields = [
        "campaign_id",
        "campaign_name",
        "impressions",
        "clicks",
        "spend",
        "reach",
        "frequency",
        "cpm",
        "actions",
        "action_values",
        "purchase_roas",
    ]
    if level == "ad":
        fields.extend(["adset_id", "adset_name", "ad_id", "ad_name"])
    return {
        "level": level,
        "time_increment": 1,
        "time_range": json.dumps({"since": start.isoformat(), "until": end.isoformat()}),
        "use_account_attribution_setting": "true",
        "fields": ",".join(fields),
        "limit": 500,
    }


def _persist_fact_rows(engine: Engine, table: str, rows: list[dict[str, Any]], customer_id: str, days: list[date]) -> None:
    by_date: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        row_dt = row.get("dt")
        if isinstance(row_dt, date):
            by_date[row_dt].append(row)
    for day in days:
        replace_fact_range(engine, table, by_date.get(day, []), customer_id, day)


def collect_account(engine: Engine, client: MetaApiClient, account: dict[str, str], start: date, end: date) -> dict[str, Any]:
    raw_account_id = account.get("id")
    customer_id = _normalize_ad_account_id(raw_account_id)
    api_account_id = _api_account_id(raw_account_id)
    if not customer_id or not api_account_id:
        return {"account": account.get("name") or raw_account_id, "status": "skipped", "reason": "missing_ad_account_id"}

    account_name = _clean_text(account.get("name")) or customer_id
    log(f"[Meta] {account_name} ({api_account_id}) {start}~{end} start")

    _ensure_dashboard_account(engine, {**account, "id": customer_id})

    campaigns = client.list_all(
        f"/{api_account_id}/campaigns",
        {"fields": "id,name,status,effective_status,objective", "limit": 500},
    )
    adsets = client.list_all(
        f"/{api_account_id}/adsets",
        {"fields": "id,name,campaign_id,status,effective_status", "limit": 500},
    )
    try:
        ads = client.list_all(
            f"/{api_account_id}/ads",
            {"fields": "id,name,adset_id,campaign_id,status,effective_status,creative{id,name,title,body,image_url,thumbnail_url,object_url}", "limit": 500},
        )
    except RuntimeError as exc:
        log(f"[Meta] creative fields unavailable, retry ads without creative payload | {exc}")
        ads = client.list_all(
            f"/{api_account_id}/ads",
            {"fields": "id,name,adset_id,campaign_id,status,effective_status", "limit": 500},
        )

    campaign_insights = client.list_all(f"/{api_account_id}/insights", _insights_params("campaign", start, end))
    ad_insights = client.list_all(f"/{api_account_id}/insights", _insights_params("ad", start, end))

    upsert_many(engine, "dim_account", [{"customer_id": customer_id, "account_name": account_name}], ["customer_id"])
    upsert_many(engine, "dim_campaign", _build_campaign_dim_rows(customer_id, campaigns, campaign_insights), ["customer_id", "campaign_id"])
    upsert_many(engine, "dim_adgroup", _build_adset_dim_rows(customer_id, adsets, ad_insights), ["customer_id", "adgroup_id"])
    upsert_many(engine, "dim_ad", _build_ad_dim_rows(customer_id, ads, ad_insights), ["customer_id", "ad_id"])

    days = _date_range(start, end)
    campaign_rows = _build_campaign_fact_rows(customer_id, campaign_insights)
    ad_rows = _build_ad_fact_rows(customer_id, ad_insights)
    _persist_fact_rows(engine, "fact_campaign_daily", campaign_rows, customer_id, days)
    _persist_fact_rows(engine, "fact_ad_daily", ad_rows, customer_id, days)

    log(f"[Meta] {account_name} done | campaigns={len(campaign_rows)} ads={len(ad_rows)}")
    return {
        "account": account_name,
        "status": "ok",
        "campaign_dim": len(campaigns),
        "adset_dim": len(adsets),
        "ad_dim": len(ads),
        "campaign_rows": len(campaign_rows),
        "ad_rows": len(ad_rows),
    }


def _load_accounts(args: argparse.Namespace) -> list[dict[str, str]]:
    accounts = load_meta_accounts(file_path=args.account_master)
    explicit_id = _clean_text(args.ad_account_id or os.getenv("META_AD_ACCOUNT_ID"))
    if explicit_id:
        accounts = [{
            "id": explicit_id,
            "name": _clean_text(args.account_name) or _normalize_ad_account_id(explicit_id),
            "manager": "",
            "group_name": "",
            "pixel_id": "",
        }]
    if args.account_name and not explicit_id:
        token = args.account_name.strip().lower()
        accounts = [a for a in accounts if token in str(a.get("name", "")).lower()]
    if args.account_names and not explicit_id:
        wanted = {name.strip() for name in args.account_names.split(",") if name.strip()}
        accounts = [a for a in accounts if str(a.get("name", "")).strip() in wanted]
    return accounts


def main() -> int:
    yesterday = date.today() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Collect Meta Ads campaign/ad insights into dashboard tables.")
    parser.add_argument("--date", default="", help="single date YYYY-MM-DD; defaults to yesterday")
    parser.add_argument("--start", default="", help="start date YYYY-MM-DD")
    parser.add_argument("--end", default="", help="end date YYYY-MM-DD")
    parser.add_argument("--account_name", default="", help="filter by account name substring")
    parser.add_argument("--account_names", default="", help="comma-separated exact account names")
    parser.add_argument("--ad_account_id", default="", help="single Meta ad account id, with or without act_")
    parser.add_argument("--account-master", default=os.getenv("ACCOUNT_MASTER_FILE", "account_master.xlsx"), help="account master workbook path")
    parser.add_argument("--api-version", default=os.getenv("META_GRAPH_API_VERSION", DEFAULT_API_VERSION), help="Meta Graph API version")
    args = parser.parse_args()

    if args.date:
        start = end = _parse_date(args.date)
    else:
        start = _parse_date(args.start) if args.start else yesterday
        end = _parse_date(args.end) if args.end else start
    if end < start:
        raise SystemExit("--end must be on or after --start")

    access_token = _clean_text(os.getenv("META_ACCESS_TOKEN"))
    if not access_token:
        raise SystemExit("META_ACCESS_TOKEN 환경변수가 필요합니다.")

    db_url = _clean_text(os.getenv("DATABASE_URL"))
    if not db_url:
        raise SystemExit("DATABASE_URL 환경변수가 필요합니다.")

    accounts = _load_accounts(args)
    if not accounts:
        raise SystemExit("수집할 Meta 계정이 없습니다. account_master.xlsx의 platform=meta/meta_ad_account_id 또는 --ad_account_id를 확인하세요.")

    engine = get_engine(db_url)
    ensure_meta_schema(engine)
    client = MetaApiClient(access_token=access_token, api_version=args.api_version)

    results = []
    failures = 0
    for account in accounts:
        try:
            results.append(collect_account(engine, client, account, start, end))
        except Exception as exc:
            failures += 1
            account_label = account.get("name") or account.get("id")
            log(f"[Meta] ERROR {account_label}: {type(exc).__name__}: {exc}")
            results.append({"account": account_label, "status": "error", "reason": str(exc)})

    ok_count = sum(1 for row in results if row.get("status") == "ok")
    log(f"[Meta] summary ok={ok_count} failed={failures} total={len(results)}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
