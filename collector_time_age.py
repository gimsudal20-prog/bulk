# -*- coding: utf-8 -*-
"""Collect only campaign hourly and age-range breakdown stats."""
from __future__ import annotations

import argparse
import concurrent.futures
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.engine import Engine

import collector
from targeting_collector_helpers import AGE_BUCKETS


SHOPPING_TYPE_HINTS = ("shopping", "쇼핑", "shop", "product", "catalog")


def is_shopping_campaign_type(value: str) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    lower = raw.lower()
    return any(hint in lower for hint in SHOPPING_TYPE_HINTS)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="시간대/연령대 breakdown 전용 수집기")
    p.add_argument("--date", default="", help="수집일 YYYY-MM-DD. 비우면 KST 어제")
    p.add_argument("--account_name", default="", help="단일 계정명 또는 일부 일치")
    p.add_argument("--account_names", default="", help="쉼표로 구분한 계정명")
    p.add_argument("--customer_id", default="", help="customer_id 직접 지정")
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--shopping_only", action="store_true", help="쇼핑 캠페인만 수집")
    p.add_argument("--include_gfa_accounts", action="store_true")
    p.add_argument("--sync_campaigns", action="store_true", help="캠페인 목록만 API로 갱신 후 수집")
    p.add_argument("--verify", action="store_true", help="수집 후 24시간/연령대 표준 구간 누락 검사")
    p.add_argument("--require_age", action="store_true", help="쇼핑 캠페인이 없거나 연령대 행이 없으면 실패 처리")
    p.add_argument("--report_path", default="", help="검사 JSON 저장 경로")
    return p.parse_args()


def resolve_target_date(raw: str):
    if raw:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    return (datetime.utcnow() + timedelta(hours=9)).date() - timedelta(days=1)


def resolve_accounts(engine: Engine, args: argparse.Namespace) -> List[Dict[str, str]]:
    base_args = argparse.Namespace(
        customer_id=args.customer_id,
        include_gfa_accounts=args.include_gfa_accounts,
        account_name=args.account_name,
        account_names=args.account_names,
    )
    accounts = collector.resolve_accounts_info(engine, base_args)
    accounts = collector.apply_account_name_filters(accounts, base_args)
    return collector.dedupe_accounts_info(accounts)


def sync_campaigns(engine: Engine, customer_id: str) -> None:
    rows: List[Dict[str, str]] = []
    adgroup_rows: List[Dict[str, str]] = []
    ad_rows: List[Dict[str, str]] = []
    for camp in collector.list_campaigns(customer_id) or []:
        cid = str(camp.get("nccCampaignId") or "").strip()
        if not cid:
            continue
        rows.append({
            "customer_id": str(customer_id),
            "campaign_id": cid,
            "campaign_name": str(camp.get("name", "")),
            "campaign_tp": str(camp.get("campaignTp", "")),
            "status": str(camp.get("status", "")),
        })
        for adgroup in collector.list_adgroups(customer_id, cid) or []:
            gid = str(adgroup.get("nccAdgroupId") or "").strip()
            if not gid:
                continue
            adgroup_rows.append({
                "customer_id": str(customer_id),
                "adgroup_id": gid,
                "campaign_id": cid,
                "adgroup_name": str(adgroup.get("name", "")),
                "status": str(adgroup.get("status", "")),
            })
            for ad in collector.list_ads(customer_id, gid) or []:
                ad_id = str(ad.get("nccAdId") or "").strip()
                if not ad_id:
                    continue
                ad_rows.append({
                    "customer_id": str(customer_id),
                    "ad_id": ad_id,
                    "adgroup_id": gid,
                    "ad_name": str(ad.get("name") or ad.get("adName") or ""),
                    "status": str(ad.get("status", "")),
                })
    if rows:
        collector.upsert_many(engine, "dim_campaign", rows, ["customer_id", "campaign_id"])
    if adgroup_rows:
        collector.upsert_many(engine, "dim_adgroup", adgroup_rows, ["customer_id", "adgroup_id"])
    if ad_rows:
        collector.upsert_many(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])


def load_campaign_targets(engine: Engine, customer_id: str, shopping_only: bool) -> tuple[List[str], set[str], Dict[str, str]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT campaign_id, COALESCE(campaign_tp, '') AS campaign_tp
                FROM dim_campaign
                WHERE customer_id = :cid
                  AND campaign_id IS NOT NULL
                ORDER BY campaign_id
                """
            ),
            {"cid": str(customer_id)},
        ).mappings().all()

    campaign_ids: List[str] = []
    shopping_ids: set[str] = set()
    type_map: Dict[str, str] = {}
    for row in rows:
        cid = str(row["campaign_id"]).strip()
        if not cid:
            continue
        ctype = str(row["campaign_tp"] or "")
        is_shopping = is_shopping_campaign_type(ctype)
        if shopping_only and not is_shopping:
            continue
        campaign_ids.append(cid)
        type_map[cid] = ctype
        if is_shopping:
            shopping_ids.add(cid)
    return campaign_ids, shopping_ids, type_map


def verify_account(engine: Engine, customer_id: str, target_date, campaign_ids: List[str], shopping_ids: set[str], require_age: bool) -> Dict[str, Any]:
    with engine.connect() as conn:
        hourly_rows = conn.execute(
            text(
                """
                SELECT campaign_id, hour_of_day
                FROM fact_campaign_hourly_daily
                WHERE dt = :dt
                  AND customer_id = :cid
                """
            ),
            {"dt": target_date, "cid": str(customer_id)},
        ).mappings().all()
        age_rows = conn.execute(
            text(
                """
                SELECT campaign_id, age_range
                FROM fact_campaign_age_daily
                WHERE dt = :dt
                  AND customer_id = :cid
                """
            ),
            {"dt": target_date, "cid": str(customer_id)},
        ).mappings().all()

    campaign_set = set(campaign_ids)
    shopping_set = set(shopping_ids)
    existing_hours = {
        (str(r["campaign_id"]), int(r["hour_of_day"]))
        for r in hourly_rows
        if str(r["campaign_id"]) in campaign_set and r["hour_of_day"] is not None
    }
    existing_ages = {
        (str(r["campaign_id"]), str(r["age_range"]))
        for r in age_rows
        if str(r["campaign_id"]) in campaign_set
    }
    missing_hours = [
        {"campaign_id": cid, "hour_of_day": hour}
        for cid in sorted(campaign_set)
        for hour in range(24)
        if (cid, hour) not in existing_hours
    ]
    missing_ages = [
        {"campaign_id": cid, "age_range": age}
        for cid in sorted(campaign_set)
        for age in AGE_BUCKETS
        if (cid, age) not in existing_ages
    ]
    ok = bool(campaign_ids) and not missing_hours and not missing_ages
    if require_age:
        ok = ok and bool(campaign_ids) and bool(existing_ages)
    return {
        "campaign_count": len(campaign_ids),
        "shopping_campaign_count": len(shopping_ids),
        "hourly_rows": len(existing_hours),
        "hourly_expected_rows": len(campaign_ids) * 24,
        "hourly_missing_count": len(missing_hours),
        "hourly_missing_sample": missing_hours[:20],
        "age_rows": len(existing_ages),
        "age_expected_rows": len(campaign_ids) * len(AGE_BUCKETS),
        "age_missing_count": len(missing_ages),
        "age_missing_sample": missing_ages[:20],
        "ok": ok,
    }


def process_account(engine: Engine, account: Dict[str, str], target_date, args: argparse.Namespace) -> Dict[str, Any]:
    customer_id = str(account.get("id") or "").strip()
    account_name = str(account.get("name") or customer_id)
    if args.sync_campaigns:
        sync_campaigns(engine, customer_id)
    campaign_ids, shopping_ids, type_map = load_campaign_targets(engine, customer_id, args.shopping_only)
    collector.log(
        f"▶️ [ {account_name} ] 시간대/연령대 전용 수집 시작 | campaigns={len(campaign_ids)} shopping={len(shopping_ids)}"
    )
    meta = collector.collect_time_age_stats(
        engine,
        customer_id,
        target_date,
        campaign_ids=campaign_ids,
        shopping_campaign_ids=shopping_ids,
        campaign_type_map=type_map,
        shopping_only=args.shopping_only,
    )
    result: Dict[str, Any] = {
        "customer_id": customer_id,
        "account_name": account_name,
        "target_date": str(target_date),
        **meta,
    }
    if args.verify:
        result["verification"] = verify_account(engine, customer_id, target_date, campaign_ids, shopping_ids, args.require_age)
        if args.require_age:
            result["verification"]["hour_raw_rows_ok"] = int(result.get("hour_raw_rows", 0) or 0) > 0
            result["verification"]["age_raw_rows_ok"] = int(result.get("age_raw_rows", 0) or 0) > 0
            result["verification"]["age_parsed_rows_ok"] = int(result.get("age_meta", {}).get("parsed_rows", 0) or 0) > 0
            result["verification"]["ok"] = bool(
                result["verification"]["ok"]
                and result["verification"]["hour_raw_rows_ok"]
                and result["verification"]["age_raw_rows_ok"]
                and result["verification"]["age_parsed_rows_ok"]
            )
    collector.log(
        f"✅ [ {account_name} ] 시간대/연령대 전용 수집 완료 | hour={result.get('hour_rows_saved', 0)} age={result.get('age_rows_saved', 0)}"
    )
    return result


def main() -> int:
    args = parse_args()
    if not collector.API_KEY or not collector.API_SECRET:
        collector.die("API_KEY 또는 API_SECRET이 설정되지 않았습니다.")
    target_date = resolve_target_date(args.date)
    engine = collector.get_engine()
    collector.ensure_tables(engine)
    accounts = resolve_accounts(engine, args)
    if not accounts:
        collector.log("⚠️ 시간대/연령대 수집 대상 계정이 없습니다.")
        return 1

    results: List[Dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = [executor.submit(process_account, engine, acc, target_date, args) for acc in accounts]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    report = {
        "target_date": str(target_date),
        "account_count": len(accounts),
        "results": results,
        "ok": all(r.get("verification", {}).get("ok", True) for r in results),
    }
    if args.report_path:
        path = Path(args.report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
