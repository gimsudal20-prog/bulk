"""Collect Google Ads performance into the shared dashboard schema.

Required env:
- DATABASE_URL
- GOOGLE_ADS_DEVELOPER_TOKEN
- GOOGLE_ADS_CLIENT_ID
- GOOGLE_ADS_CLIENT_SECRET
- GOOGLE_ADS_REFRESH_TOKEN, unless a per-account refresh token is stored

Account sources:
- platform_credentials rows where platform is google/google_ads
- or --customer_id / GOOGLE_ADS_CUSTOMER_ID for a single account
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
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

from collector_db import ensure_tables, get_engine, replace_fact_range, upsert_many


DEFAULT_API_VERSION = "v22"
GOOGLE_PLATFORMS = ("google", "google_ads", "googleads")


def log(message: str) -> None:
    print(message, flush=True)


def _clean_text(value: object) -> str:
    text_value = str(value or "").strip()
    return "" if text_value.lower() in {"nan", "none", "nat"} else text_value


def _normalize_customer_id(value: object) -> str:
    customer_id = _clean_text(value)
    if customer_id.endswith(".0") and customer_id[:-2].isdigit():
        customer_id = customer_id[:-2]
    return "".join(ch for ch in customer_id if ch.isdigit())


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> list[date]:
    days: list[date] = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _as_int(value: object) -> int:
    try:
        return int(round(float(value or 0)))
    except Exception:
        return 0


def _as_float(value: object) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _cost_from_micros(value: object) -> int:
    return _as_int(_as_float(value) / 1_000_000)


def _enum_name(value: object) -> str:
    raw = _clean_text(value)
    return raw.split(".")[-1] if raw else ""


class GoogleAdsClient:
    def __init__(
        self,
        developer_token: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        login_customer_id: str = "",
        api_version: str = DEFAULT_API_VERSION,
        timeout: int = 60,
    ):
        self.developer_token = developer_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.login_customer_id = _normalize_customer_id(login_customer_id)
        self.api_version = api_version.strip() or DEFAULT_API_VERSION
        self.timeout = timeout
        self.base_url = f"https://googleads.googleapis.com/{self.api_version}"
        self._access_token = ""
        self._library_client: Any | None = None
        self._library_unavailable = False
        self.transport = _clean_text(os.getenv("GOOGLE_ADS_TRANSPORT")).lower() or "library"

    def _format_library_error(self, exc: Exception) -> str:
        failure = getattr(exc, "failure", None)
        errors = getattr(failure, "errors", None)
        if errors:
            parts = []
            for error in errors:
                code = getattr(error, "error_code", None)
                message = getattr(error, "message", "")
                parts.append(f"{code}: {message}".strip())
            return "; ".join(parts)
        return str(exc)

    def _get_library_client(self) -> Any | None:
        if self.transport == "rest" or self._library_unavailable:
            return None
        if self._library_client is not None:
            return self._library_client
        try:
            from google.ads.googleads.client import GoogleAdsClient as LibraryGoogleAdsClient
        except Exception:
            self._library_unavailable = True
            return None

        config: dict[str, Any] = {
            "developer_token": self.developer_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "use_proto_plus": True,
        }
        if self.login_customer_id:
            config["login_customer_id"] = self.login_customer_id
        self._library_client = LibraryGoogleAdsClient.load_from_dict(config, version=self.api_version)
        return self._library_client

    def _message_to_dict(self, value: Any) -> dict[str, Any]:
        from google.protobuf.json_format import MessageToDict

        proto = getattr(value, "_pb", value)
        return MessageToDict(proto, preserving_proto_field_name=False)

    def access_token(self) -> str:
        if self._access_token:
            return self._access_token
        body = urllib.parse.urlencode(
            {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            "https://oauth2.googleapis.com/token",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Google OAuth error {exc.code}: {raw}") from exc
        self._access_token = _clean_text(payload.get("access_token"))
        if not self._access_token:
            raise RuntimeError("Google OAuth access_token 응답이 비어 있습니다.")
        return self._access_token

    def list_accessible_customers(self) -> list[str]:
        library_client = self._get_library_client()
        if library_client is not None:
            try:
                service = library_client.get_service("CustomerService")
                response = service.list_accessible_customers()
                return [_normalize_customer_id(name) for name in response.resource_names if _normalize_customer_id(name)]
            except Exception as exc:
                raise RuntimeError(f"Google Ads accessible customers error: {self._format_library_error(exc)}") from exc

        url = f"{self.base_url}/customers:listAccessibleCustomers"
        req = urllib.request.Request(
            url,
            headers=self._headers(),
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(raw)
                message = parsed.get("error", {}).get("message", raw)
            except Exception:
                message = raw
            raise RuntimeError(f"Google Ads accessible customers error {exc.code}: {message}") from exc
        names = payload.get("resourceNames") or []
        return [_normalize_customer_id(name) for name in names if _normalize_customer_id(name)]

    def search_stream(self, customer_id: str, query: str) -> list[dict[str, Any]]:
        library_client = self._get_library_client()
        if library_client is not None:
            try:
                service = library_client.get_service("GoogleAdsService")
                rows: list[dict[str, Any]] = []
                stream = service.search_stream(customer_id=_normalize_customer_id(customer_id), query=query)
                for batch in stream:
                    rows.extend(self._message_to_dict(row) for row in batch.results)
                return rows
            except Exception as exc:
                raise RuntimeError(f"Google Ads API error: {self._format_library_error(exc)}") from exc

        cid = _normalize_customer_id(customer_id)
        url = f"{self.base_url}/customers/{cid}/googleAds:searchStream"
        req = urllib.request.Request(
            url,
            data=json.dumps({"query": query}).encode("utf-8"),
            headers=self._headers(),
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(raw)
                message = parsed.get("error", {}).get("message", raw)
            except Exception:
                message = raw
            if exc.code >= 500:
                log(f"[Google Ads] searchStream {exc.code}; retrying with paged search")
                return self.search(customer_id, query)
            raise RuntimeError(f"Google Ads API error {exc.code}: {message}") from exc

        rows: list[dict[str, Any]] = []
        if isinstance(payload, list):
            for batch in payload:
                rows.extend(batch.get("results") or [])
        elif isinstance(payload, dict):
            rows.extend(payload.get("results") or [])
        return rows

    def _headers(self) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.access_token()}",
            "developer-token": self.developer_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.login_customer_id:
            headers["login-customer-id"] = self.login_customer_id
        return headers

    def search(self, customer_id: str, query: str) -> list[dict[str, Any]]:
        cid = _normalize_customer_id(customer_id)
        url = f"{self.base_url}/customers/{cid}/googleAds:search"
        rows: list[dict[str, Any]] = []
        page_token = ""
        while True:
            body: dict[str, Any] = {"query": query}
            if page_token:
                body["pageToken"] = page_token
            req = urllib.request.Request(
                url,
                data=json.dumps(body).encode("utf-8"),
                headers=self._headers(),
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                try:
                    parsed = json.loads(raw)
                    message = parsed.get("error", {}).get("message", raw)
                except Exception:
                    message = raw
                raise RuntimeError(f"Google Ads API error {exc.code}: {message}") from exc
            rows.extend(payload.get("results") or [])
            page_token = _clean_text(payload.get("nextPageToken"))
            if not page_token:
                return rows


def _ensure_platform_connections_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS platform_credentials (
                    id BIGSERIAL PRIMARY KEY,
                    platform VARCHAR(30) NOT NULL,
                    account_label VARCHAR(120) NOT NULL,
                    manager VARCHAR(120) NULL,
                    customer_id BIGINT NULL,
                    account_id VARCHAR(120) NULL,
                    access_token TEXT NULL,
                    refresh_token TEXT NULL,
                    app_id VARCHAR(200) NULL,
                    app_secret TEXT NULL,
                    extra_json JSONB DEFAULT '{}'::jsonb,
                    is_active BOOLEAN DEFAULT TRUE,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(text("ALTER TABLE platform_credentials ADD COLUMN IF NOT EXISTS manager VARCHAR(120)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_platform_credentials_platform ON platform_credentials(platform)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_platform_credentials_customer_id ON platform_credentials(customer_id)"))


def _ensure_dashboard_account(engine: Engine, account: dict[str, str]) -> None:
    customer_id = _normalize_customer_id(account.get("id"))
    if not customer_id:
        return
    account_name = _clean_text(account.get("name")) or customer_id
    manager = _clean_text(account.get("manager"))
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS dim_customer (
                    customer_id TEXT PRIMARY KEY,
                    account_name TEXT,
                    manager TEXT,
                    monthly_budget BIGINT DEFAULT 0,
                    operating_weekdays TEXT DEFAULT '0,1,2,3,4,5,6'
                )
                """
            )
        )
        conn.execute(text("ALTER TABLE dim_customer ADD COLUMN IF NOT EXISTS manager TEXT"))
        conn.execute(
            text(
                """
                INSERT INTO dim_customer (customer_id, account_name, manager, monthly_budget, operating_weekdays)
                VALUES (:customer_id, :account_name, :manager, 0, '0,1,2,3,4,5,6')
                ON CONFLICT (customer_id) DO UPDATE SET
                    account_name = EXCLUDED.account_name,
                    manager = CASE
                        WHEN EXCLUDED.manager <> '' AND EXCLUDED.manager <> '미배정'
                        THEN EXCLUDED.manager
                        ELSE dim_customer.manager
                    END
                """
            ),
            {"customer_id": customer_id, "account_name": account_name, "manager": manager or "미배정"},
        )


def _load_db_google_accounts(engine: Engine) -> list[dict[str, str]]:
    try:
        _ensure_platform_connections_schema(engine)
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT account_label, account_id, customer_id, manager, refresh_token
                    FROM platform_credentials
                    WHERE LOWER(platform) IN ('google', 'google_ads', 'googleads')
                      AND COALESCE(is_active, TRUE) = TRUE
                    ORDER BY updated_at DESC, id DESC
                    """
                )
            ).mappings().all()
    except Exception as exc:
        log(f"[Google Ads] platform connection load skipped | {type(exc).__name__}: {exc}")
        return []

    accounts: list[dict[str, str]] = []
    for row in rows:
        account_id = _normalize_customer_id(row.get("account_id") or row.get("customer_id"))
        if not account_id:
            continue
        accounts.append(
            {
                "id": account_id,
                "name": _clean_text(row.get("account_label")) or account_id,
                "manager": _clean_text(row.get("manager")),
                "refresh_token": _clean_text(row.get("refresh_token")),
            }
        )
    return accounts


def _remember_db_google_account(engine: Engine, account: dict[str, str], refresh_token: str = "") -> None:
    account_id = _normalize_customer_id(account.get("id"))
    account_name = _clean_text(account.get("name"))
    if not account_id or not account_name:
        return
    manager = _clean_text(account.get("manager")) or "미배정"
    token = _clean_text(refresh_token)
    _ensure_platform_connections_schema(engine)
    with engine.begin() as conn:
        existing_id = conn.execute(
            text(
                """
                SELECT id
                FROM platform_credentials
                WHERE LOWER(platform) = 'google'
                  AND account_id = :account_id
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"account_id": account_id},
        ).scalar()
        if existing_id:
            conn.execute(
                text(
                    """
                    UPDATE platform_credentials
                       SET account_label = :account_label,
                           manager = :manager,
                           refresh_token = CASE
                               WHEN :refresh_token <> '' THEN :refresh_token
                               ELSE refresh_token
                           END,
                           is_active = TRUE,
                           updated_at = NOW()
                     WHERE id = :id
                    """
                ),
                {"id": existing_id, "account_label": account_name, "manager": manager, "refresh_token": token},
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO platform_credentials (platform, account_label, manager, account_id, refresh_token, is_active, updated_at)
                    VALUES ('google', :account_label, :manager, :account_id, :refresh_token, TRUE, NOW())
                    """
                ),
                {"account_label": account_name, "manager": manager, "account_id": account_id, "refresh_token": token},
            )


def _metric_row_base(item: dict[str, Any]) -> dict[str, Any]:
    metrics = item.get("metrics") or {}
    segments = item.get("segments") or {}
    cost = _cost_from_micros(metrics.get("costMicros"))
    conv = _as_float(metrics.get("conversions"))
    sales = _as_int(metrics.get("conversionsValue"))
    roas = (sales / cost * 100.0) if cost > 0 else 0.0
    return {
        "dt": _parse_date(_clean_text(segments.get("date"))),
        "imp": _as_int(metrics.get("impressions")),
        "clk": _as_int(metrics.get("clicks")),
        "cost": cost,
        "conv": conv,
        "sales": sales,
        "roas": roas,
        "avg_rnk": 0.0,
        "purchase_conv": conv,
        "purchase_sales": sales,
        "purchase_roas": roas,
        "cart_conv": 0.0,
        "cart_sales": 0,
        "cart_roas": 0.0,
        "wishlist_conv": 0.0,
        "wishlist_sales": 0,
        "wishlist_roas": 0.0,
        "primary_conv": conv,
        "primary_sales": sales,
        "primary_roas": roas,
        "split_available": False,
        "data_source": "google_ads_api",
    }


def _build_campaign_dim_rows(customer_id: str, rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in rows:
        campaign = item.get("campaign") or {}
        campaign_id = _clean_text(campaign.get("id"))
        if not campaign_id:
            continue
        out[campaign_id] = {
            "customer_id": customer_id,
            "campaign_id": campaign_id,
            "campaign_name": _clean_text(campaign.get("name")) or campaign_id,
            "campaign_tp": "구글",
            "status": _enum_name(campaign.get("status")),
        }
    return list(out.values())


def _build_campaign_fact_rows(customer_id: str, rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in rows:
        campaign = item.get("campaign") or {}
        campaign_id = _clean_text(campaign.get("id"))
        if not campaign_id or not _clean_text((item.get("segments") or {}).get("date")):
            continue
        row = _metric_row_base(item)
        row.update({"customer_id": customer_id, "campaign_id": campaign_id})
        out.append(row)
    return out


def _build_asset_group_rows(customer_id: str, rows: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    adgroups: dict[str, dict[str, Any]] = {}
    ads: dict[str, dict[str, Any]] = {}
    facts: list[dict[str, Any]] = []
    for item in rows:
        asset_group = item.get("assetGroup") or {}
        campaign = item.get("campaign") or {}
        group_id_raw = _clean_text(asset_group.get("id"))
        campaign_id = _clean_text(campaign.get("id"))
        if not group_id_raw:
            continue
        group_id = f"asset_group_{group_id_raw}"
        group_name = _clean_text(asset_group.get("name")) or group_id
        adgroups[group_id] = {
            "customer_id": customer_id,
            "adgroup_id": group_id,
            "adgroup_name": group_name,
            "campaign_id": campaign_id,
            "status": _enum_name(asset_group.get("status")),
        }
        ads[group_id] = {
            "customer_id": customer_id,
            "ad_id": group_id,
            "adgroup_id": group_id,
            "ad_name": group_name,
            "status": _enum_name(asset_group.get("status")),
            "ad_title": "",
            "ad_desc": "",
            "pc_landing_url": "",
            "mobile_landing_url": "",
            "creative_text": "",
            "image_url": "",
        }
        if _clean_text((item.get("segments") or {}).get("date")):
            row = _metric_row_base(item)
            row.update({"customer_id": customer_id, "ad_id": group_id})
            facts.append(row)
    return list(adgroups.values()), list(ads.values()), facts


def _build_ad_group_ad_rows(customer_id: str, rows: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    adgroups: dict[str, dict[str, Any]] = {}
    ads: dict[str, dict[str, Any]] = {}
    facts: list[dict[str, Any]] = []
    for item in rows:
        ad_group = item.get("adGroup") or {}
        ad_group_ad = item.get("adGroupAd") or {}
        ad = ad_group_ad.get("ad") or {}
        campaign = item.get("campaign") or {}
        group_id = _clean_text(ad_group.get("id"))
        ad_id = _clean_text(ad.get("id"))
        if not group_id or not ad_id:
            continue
        adgroups[group_id] = {
            "customer_id": customer_id,
            "adgroup_id": group_id,
            "adgroup_name": _clean_text(ad_group.get("name")) or group_id,
            "campaign_id": _clean_text(campaign.get("id")),
            "status": _enum_name(ad_group.get("status")),
        }
        ads[ad_id] = {
            "customer_id": customer_id,
            "ad_id": ad_id,
            "adgroup_id": group_id,
            "ad_name": _clean_text(ad.get("name")) or ad_id,
            "status": _enum_name(ad_group_ad.get("status")),
            "ad_title": "",
            "ad_desc": "",
            "pc_landing_url": "",
            "mobile_landing_url": "",
            "creative_text": "",
            "image_url": "",
        }
        if _clean_text((item.get("segments") or {}).get("date")):
            row = _metric_row_base(item)
            row.update({"customer_id": customer_id, "ad_id": ad_id})
            facts.append(row)
    return list(adgroups.values()), list(ads.values()), facts


def _persist_fact_rows(engine: Engine, table: str, rows: list[dict[str, Any]], customer_id: str, days: list[date]) -> None:
    by_date: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        row_dt = row.get("dt")
        if isinstance(row_dt, date):
            by_date[row_dt].append(row)
    for day in days:
        replace_fact_range(engine, table, by_date.get(day, []), customer_id, day)


def _compact_query(query: str) -> str:
    return " ".join(line.strip() for line in query.strip().splitlines() if line.strip())


def diagnose_google_ads(client: GoogleAdsClient, customer_id: str, start: date, end: date) -> int:
    customer_id = _normalize_customer_id(customer_id)
    if not customer_id:
        raise SystemExit("--diagnose-only requires --customer_id")

    accessible = client.list_accessible_customers()
    log(f"[Google Ads diagnose] accessible_customers={','.join(accessible) or '(none)'}")
    if customer_id not in accessible:
        log(f"[Google Ads diagnose] target_not_in_accessible_customers target={customer_id}")

    queries = [
        (
            "customer",
            """
            SELECT
              customer.id,
              customer.descriptive_name
            FROM customer
            LIMIT 1
            """,
        ),
        (
            "campaign_dim",
            """
            SELECT
              campaign.id,
              campaign.name,
              campaign.status
            FROM campaign
            LIMIT 10
            """,
        ),
        (
            "customer_metrics",
            f"""
            SELECT
              segments.date,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros
            FROM customer
            WHERE segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
            """,
        ),
        (
            "campaign_metrics_base",
            f"""
            SELECT
              segments.date,
              campaign.id,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros
            FROM campaign
            WHERE segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
            """,
        ),
        (
            "campaign_metrics_conversions",
            f"""
            SELECT
              segments.date,
              campaign.id,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions,
              metrics.conversions_value
            FROM campaign
            WHERE segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
            """,
        ),
    ]

    failed = 0
    for name, query in queries:
        log(f"[Google Ads diagnose] query_start name={name} gaql={_compact_query(query)}")
        try:
            rows = client.search_stream(customer_id, query)
        except Exception as exc:
            failed += 1
            log(f"[Google Ads diagnose] query_fail name={name} error={type(exc).__name__}: {exc}")
            continue
        log(f"[Google Ads diagnose] query_ok name={name} rows={len(rows)}")
    return 1 if failed else 0


def collect_account(engine: Engine, client: GoogleAdsClient, account: dict[str, str], start: date, end: date) -> dict[str, Any]:
    customer_id = _normalize_customer_id(account.get("id"))
    if not customer_id:
        return {"account": account.get("name") or account.get("id"), "status": "skipped", "reason": "missing_customer_id"}

    account_name = _clean_text(account.get("name")) or customer_id
    log(f"[Google Ads] {account_name} ({customer_id}) {start}~{end} start")
    _ensure_dashboard_account(engine, {**account, "id": customer_id})

    campaign_query = f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.advertising_channel_type,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          metrics.conversions,
          metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
        ORDER BY segments.date, campaign.id
    """
    campaign_rows_raw = client.search_stream(customer_id, campaign_query)

    asset_group_rows_raw: list[dict[str, Any]] = []
    asset_group_query = f"""
        SELECT
          segments.date,
          campaign.id,
          asset_group.id,
          asset_group.name,
          asset_group.status,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          metrics.conversions,
          metrics.conversions_value
        FROM asset_group
        WHERE segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
        ORDER BY segments.date, asset_group.id
    """
    try:
        asset_group_rows_raw = client.search_stream(customer_id, asset_group_query)
    except RuntimeError as exc:
        log(f"[Google Ads] asset_group query skipped | {exc}")

    ad_rows_raw: list[dict[str, Any]] = []
    ad_query = f"""
        SELECT
          segments.date,
          campaign.id,
          ad_group.id,
          ad_group.name,
          ad_group.status,
          ad_group_ad.ad.id,
          ad_group_ad.ad.name,
          ad_group_ad.status,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          metrics.conversions,
          metrics.conversions_value
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
        ORDER BY segments.date, ad_group_ad.ad.id
    """
    try:
        ad_rows_raw = client.search_stream(customer_id, ad_query)
    except RuntimeError as exc:
        log(f"[Google Ads] ad_group_ad query skipped | {exc}")

    campaign_dim_rows = _build_campaign_dim_rows(customer_id, campaign_rows_raw)
    campaign_fact_rows = _build_campaign_fact_rows(customer_id, campaign_rows_raw)
    asset_group_dims, asset_group_ads, asset_group_facts = _build_asset_group_rows(customer_id, asset_group_rows_raw)
    adgroup_dims, ad_dims, ad_facts = _build_ad_group_ad_rows(customer_id, ad_rows_raw)

    upsert_many(engine, "dim_account", [{"customer_id": customer_id, "account_name": account_name}], ["customer_id"])
    upsert_many(engine, "dim_campaign", campaign_dim_rows, ["customer_id", "campaign_id"])
    upsert_many(engine, "dim_adgroup", asset_group_dims + adgroup_dims, ["customer_id", "adgroup_id"])
    upsert_many(engine, "dim_ad", asset_group_ads + ad_dims, ["customer_id", "ad_id"])

    days = _date_range(start, end)
    _persist_fact_rows(engine, "fact_campaign_daily", campaign_fact_rows, customer_id, days)
    _persist_fact_rows(engine, "fact_ad_daily", asset_group_facts + ad_facts, customer_id, days)

    log(f"[Google Ads] {account_name} done | campaigns={len(campaign_fact_rows)} ads={len(asset_group_facts) + len(ad_facts)}")
    return {
        "account": account_name,
        "status": "ok",
        "campaign_rows": len(campaign_fact_rows),
        "ad_rows": len(asset_group_facts) + len(ad_facts),
    }


def _account_matches_id(account: dict[str, str], explicit_id: str) -> bool:
    return _normalize_customer_id(account.get("id")) == _normalize_customer_id(explicit_id)


def _load_accounts(args: argparse.Namespace, engine: Engine) -> list[dict[str, str]]:
    accounts = _load_db_google_accounts(engine)
    if accounts:
        log(f"[Google Ads] DB platform connections loaded count={len(accounts)}")
    explicit_id = _clean_text(args.customer_id or os.getenv("GOOGLE_ADS_CUSTOMER_ID"))
    explicit_manager = _clean_text(args.manager or os.getenv("GOOGLE_ADS_MANAGER"))
    if explicit_id:
        matched = next((account for account in accounts if _account_matches_id(account, explicit_id)), None)
        if matched:
            account = dict(matched)
            if args.account_name:
                account["name"] = _clean_text(args.account_name)
            if explicit_manager:
                account["manager"] = explicit_manager
            account["id"] = explicit_id
            log(
                "[Google Ads] account connection match "
                f"id={_normalize_customer_id(explicit_id)} "
                f"name={_clean_text(account.get('name')) or '(blank)'} "
                f"manager={_clean_text(account.get('manager')) or '(blank)'}"
            )
            return [account]
        log(f"[Google Ads] account connection match not found id={_normalize_customer_id(explicit_id)}")
        return [
            {
                "id": explicit_id,
                "name": _clean_text(args.account_name) or _normalize_customer_id(explicit_id),
                "manager": explicit_manager,
                "refresh_token": "",
            }
        ]
    if args.account_name:
        token = args.account_name.strip().lower()
        accounts = [a for a in accounts if token in str(a.get("name", "")).lower()]
    return accounts


def main() -> int:
    yesterday = date.today() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Collect Google Ads campaign/ad insights into dashboard tables.")
    parser.add_argument("--date", default="", help="single date YYYY-MM-DD; defaults to yesterday")
    parser.add_argument("--start", default="", help="start date YYYY-MM-DD")
    parser.add_argument("--end", default="", help="end date YYYY-MM-DD")
    parser.add_argument("--account_name", default="", help="dashboard account name")
    parser.add_argument("--customer_id", default="", help="Google Ads customer ID, with or without dashes")
    parser.add_argument("--manager", default="", help="dashboard manager name for explicit Google Ads account runs")
    parser.add_argument("--login_customer_id", default=os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", ""), help="optional manager account ID for login-customer-id header")
    parser.add_argument("--api-version", default=os.getenv("GOOGLE_ADS_API_VERSION", DEFAULT_API_VERSION), help="Google Ads API version")
    parser.add_argument("--diagnose-only", action="store_true", help="run lightweight Google Ads API diagnostics and exit")
    args = parser.parse_args()

    if args.date:
        start = end = _parse_date(args.date)
    else:
        start = _parse_date(args.start) if args.start else yesterday
        end = _parse_date(args.end) if args.end else start
    if end < start:
        raise SystemExit("--end must be on or after --start")

    developer_token = _clean_text(os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"))
    client_id = _clean_text(os.getenv("GOOGLE_ADS_CLIENT_ID"))
    client_secret = _clean_text(os.getenv("GOOGLE_ADS_CLIENT_SECRET"))
    global_refresh_token = _clean_text(os.getenv("GOOGLE_ADS_REFRESH_TOKEN"))
    missing = [
        name
        for name, value in [
            ("GOOGLE_ADS_DEVELOPER_TOKEN", developer_token),
            ("GOOGLE_ADS_CLIENT_ID", client_id),
            ("GOOGLE_ADS_CLIENT_SECRET", client_secret),
        ]
        if not value
    ]
    if missing:
        raise SystemExit("누락된 Google Ads 인증 환경변수: " + ", ".join(missing))

    db_url = _clean_text(os.getenv("DATABASE_URL"))
    if not db_url:
        raise SystemExit("DATABASE_URL 환경변수가 필요합니다.")

    engine = get_engine(db_url)
    ensure_tables(engine)
    _ensure_platform_connections_schema(engine)

    if args.diagnose_only:
        refresh_token = global_refresh_token
        if not refresh_token:
            raise SystemExit("--diagnose-only requires GOOGLE_ADS_REFRESH_TOKEN")
        client = GoogleAdsClient(
            developer_token=developer_token,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            login_customer_id=args.login_customer_id,
            api_version=args.api_version,
        )
        return diagnose_google_ads(client, _clean_text(args.customer_id or os.getenv("GOOGLE_ADS_CUSTOMER_ID")), start, end)

    accounts = _load_accounts(args, engine)
    if not accounts:
        raise SystemExit("수집할 Google Ads 계정이 없습니다. 설정 > 플랫폼 계정 연결 또는 --customer_id를 확인하세요.")

    explicit_id = _clean_text(args.customer_id or os.getenv("GOOGLE_ADS_CUSTOMER_ID"))
    if explicit_id and global_refresh_token:
        for account in accounts:
            if _account_matches_id(account, explicit_id):
                _remember_db_google_account(engine, account, refresh_token=global_refresh_token)
                break

    has_account_tokens = any(_clean_text(account.get("refresh_token")) for account in accounts)
    if not global_refresh_token and not has_account_tokens:
        raise SystemExit("GOOGLE_ADS_REFRESH_TOKEN 환경변수 또는 플랫폼 계정별 refresh_token이 필요합니다.")

    results = []
    failures = 0
    skipped = 0
    for account in accounts:
        refresh_token = _clean_text(account.get("refresh_token")) or global_refresh_token
        if not refresh_token:
            skipped += 1
            account_label = account.get("name") or account.get("id")
            log(f"[Google Ads] SKIP {account_label}: refresh_token이 없어 건너뜁니다.")
            results.append({"account": account_label, "status": "skipped", "reason": "missing_refresh_token"})
            continue
        client = GoogleAdsClient(
            developer_token=developer_token,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            login_customer_id=args.login_customer_id,
            api_version=args.api_version,
        )
        try:
            results.append(collect_account(engine, client, account, start, end))
        except Exception as exc:
            failures += 1
            account_label = account.get("name") or account.get("id")
            log(f"[Google Ads] ERROR {account_label}: {type(exc).__name__}: {exc}")
            results.append({"account": account_label, "status": "error", "reason": str(exc)})

    ok_count = sum(1 for row in results if row.get("status") == "ok")
    log(f"[Google Ads] summary ok={ok_count} failed={failures} skipped={skipped} total={len(results)}")
    if failures:
        return 1
    if ok_count == 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
