# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _resolve_split_payload(
    dfs: Dict[str, Any],
    *,
    collect_sa: bool,
    target_date: date,
    shopping_only: bool,
    shopping_campaign_ids: set[str],
    shopping_keyword_ids: set[str],
    keyword_lookup: Dict[Tuple[str, str], str],
    keyword_unique_lookup: Dict[str, List[Tuple[str, str]]],
    live_keyword_resolver,
    account_name: str,
    customer_id: str,
    result: Dict[str, Any],
    split_enabled_for_date_fn: Callable[[date], bool],
    process_conversion_report_fn: Callable[..., Tuple[dict, dict, dict, dict]],
    empty_split_summary_fn: Callable[[], dict],
    parse_shopping_query_report_fn: Callable[..., List[Dict[str, Any]]],
    merge_split_maps_fn: Callable[..., dict],
    filter_split_map_excluding_ids_fn: Callable[..., dict],
    split_summary_has_values_fn: Callable[[dict], bool],
    validate_shopping_split_summary_fn: Callable[[dict, dict], tuple[bool, str]],
    format_split_summary_fn: Callable[[dict], str],
    log_fn: Callable[[str], None] = _log,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], List[Dict[str, Any]], bool]:
    camp_map: Dict[str, Dict[str, Any]] = {}
    kw_map: Dict[str, Dict[str, Any]] = {}
    ad_map: Dict[str, Dict[str, Any]] = {}
    shop_query_rows: List[Dict[str, Any]] = []
    split_report_ok = False

    if not collect_sa:
        return camp_map, kw_map, ad_map, shop_query_rows, split_report_ok

    if not split_enabled_for_date_fn(target_date):
        log_fn(f"   ℹ️ [ {account_name} ] 2026-03-11 이전 날짜는 purchase/cart/wishlist 분리 수집을 시도하지 않습니다.")
        return camp_map, kw_map, ad_map, shop_query_rows, split_report_ok

    if not shopping_campaign_ids:
        log_fn(f"   ℹ️ [ {account_name} ] 쇼핑검색 캠페인이 없어 purchase/cart/wishlist 분리 수집을 건너뜁니다.")
        return camp_map, kw_map, ad_map, shop_query_rows, split_report_ok

    source_maps: Dict[str, Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Any]]] = {}
    report_candidates = ["AD_CONVERSION", "SHOPPINGKEYWORD_CONVERSION_DETAIL"]
    for tp in report_candidates:
        conv_df = dfs.get(tp)
        if conv_df is None:
            log_fn(f"   ⚠️ [ {account_name} ] {tp} 리포트 실패 → 다음 전환 리포트로 진행합니다.")
            continue
        if getattr(conv_df, "empty", False):
            log_fn(f"   ℹ️ [ {account_name} ] {tp} 리포트가 비어 있습니다. purchase/cart/wishlist 는 미확정(NULL)로 유지합니다.")
            continue

        report_allowed_campaign_ids = None
        one_camp_map, one_kw_map, one_ad_map, one_summary = process_conversion_report_fn(
            conv_df,
            allowed_campaign_ids=report_allowed_campaign_ids,
            report_hint=tp,
            keyword_lookup=keyword_lookup,
            keyword_unique_lookup=keyword_unique_lookup,
            live_keyword_resolver=live_keyword_resolver,
            debug_account_name=account_name,
            debug_target_date=str(target_date),
        )

        if len(one_camp_map) == 0 and len(one_kw_map) == 0 and len(one_ad_map) == 0:
            log_fn(f"   ⚠️ [ {account_name} ] {tp} 데이터는 있으나 shopping purchase/cart/wishlist 파싱에 실패했습니다. debug_reports 원본을 확인하세요.")
            continue

        source_maps[tp] = (one_camp_map, one_kw_map, one_ad_map, one_summary)

    ad_conv_maps = source_maps.get("AD_CONVERSION", ({}, {}, {}, empty_split_summary_fn()))
    shop_kw_maps = source_maps.get("SHOPPINGKEYWORD_CONVERSION_DETAIL", ({}, {}, {}, empty_split_summary_fn()))

    ad_camp_map, ad_kw_map, ad_ad_map, ad_summary = ad_conv_maps
    shop_camp_map, shop_kw_map, shop_ad_map, shop_summary = shop_kw_maps

    shop_query_df = dfs.get("SHOPPINGKEYWORD_CONVERSION_DETAIL")
    if shop_query_df is not None and not getattr(shop_query_df, "empty", False):
        try:
            shop_query_rows = parse_shopping_query_report_fn(shop_query_df, target_date, customer_id)
        except Exception as e:
            log_fn(f"   ⚠️ [ {account_name} ] 쇼핑검색어 분리 저장 파싱 실패: {e}")
            shop_query_rows = []

    camp_map = ad_camp_map if ad_camp_map else shop_camp_map
    ad_map = ad_ad_map if ad_ad_map else shop_ad_map
    raw_kw_map = merge_split_maps_fn(ad_kw_map, shop_kw_map)
    if shopping_only:
        kw_map = {}
    else:
        kw_map = filter_split_map_excluding_ids_fn(raw_kw_map, shopping_keyword_ids)
        removed_kw = max(0, len(raw_kw_map) - len(kw_map))
        if removed_kw:
            log_fn(f"   ℹ️ [ {account_name} ] 쇼핑 키워드 split {removed_kw}건은 fact_keyword_daily 적재에서 제외합니다.")

    split_report_ok = bool(camp_map or kw_map or ad_map)

    final_split_summary = ad_summary if split_summary_has_values_fn(ad_summary) else shop_summary
    if shopping_only and split_report_ok and split_summary_has_values_fn(final_split_summary):
        split_ok, split_reason = validate_shopping_split_summary_fn(final_split_summary, ad_map)
        if not split_ok:
            log_fn(f"   ⚠️ [ {account_name} ] shopping split 검증 실패 → 상세 split 저장을 건너뛰고 총합만 적재합니다. ({split_reason})")
            camp_map, kw_map, ad_map = {}, {}, {}
            shop_query_rows = []
            result["stage"] = "resolve_split_payload"
            split_report_ok = False

    if split_report_ok:
        camp_ad_src = 'AD_CONVERSION' if ad_camp_map or ad_ad_map else ('SHOPPINGKEYWORD_CONVERSION_DETAIL' if shop_camp_map or shop_ad_map else 'none')
        kw_src = 'AD_CONVERSION+SHOPPINGKEYWORD_CONVERSION_DETAIL' if (ad_kw_map and shop_kw_map) else ('AD_CONVERSION' if ad_kw_map else ('SHOPPINGKEYWORD_CONVERSION_DETAIL' if shop_kw_map else 'none'))
        summary_src = 'AD_CONVERSION' if split_summary_has_values_fn(ad_summary) else ('SHOPPINGKEYWORD_CONVERSION_DETAIL' if split_summary_has_values_fn(shop_summary) else 'none')
        query_src = 'SHOPPINGKEYWORD_CONVERSION_DETAIL' if shop_query_rows else 'none'
        result["split_source"] = f"summary={summary_src},campaign/ad={camp_ad_src},keyword={kw_src},query={query_src}"
        log_fn(
            f"   ✅ [ {account_name} ] shopping split 원천 사용: "
            f"summary={summary_src}, campaign/ad={camp_ad_src}, keyword={kw_src}, query={query_src}"
        )
        if split_summary_has_values_fn(final_split_summary):
            log_fn(f"   ℹ️ [ {account_name} ] detail split 파싱: {format_split_summary_fn(final_split_summary)}")

    return camp_map, kw_map, ad_map, shop_query_rows, split_report_ok


def _is_ad_only_scope(sa_scope: str, normalize_sa_scope_fn: Callable[[str | None], str]) -> bool:
    return normalize_sa_scope_fn(sa_scope) == "ad_only"



def _scope_enabled_collectors(sa_scope: str, collect_sa: bool, normalize_sa_scope_fn: Callable[[str | None], str]) -> tuple[bool, bool, bool]:
    if not collect_sa:
        return False, False, False
    scope = normalize_sa_scope_fn(sa_scope)
    if scope == "ad_only":
        return False, False, True
    return True, True, True



def _save_report_stats_and_breakdowns(
    engine: Engine,
    *,
    customer_id: str,
    account_name: str,
    target_date: date,
    collect_sa: bool,
    collect_device: bool,
    sa_scope: str,
    shopping_only: bool,
    target_camp_ids: List[str],
    target_kw_ids: List[str],
    target_ad_ids: List[str],
    ad_report_df,
    ad_to_campaign_map: Dict[str, str],
    campaign_type_map: Dict[str, str],
    camp_map: Dict[str, Dict[str, Any]],
    kw_map: Dict[str, Dict[str, Any]],
    ad_map: Dict[str, Dict[str, Any]],
    result: Dict[str, Any],
    normalize_sa_scope_fn: Callable[[str | None], str],
    fetch_stats_fallback_fn: Callable[..., int],
    clear_fact_scope_fn: Callable[..., Any],
    parse_ad_device_report_fn: Callable[..., tuple],
    filter_stat_result_fn: Callable[..., dict],
    save_device_stats_fn: Callable[..., int],
    summarize_stat_res_fn: Callable[[dict], dict],
    collect_media_fact_fn: Callable[..., tuple],
    skip_keyword_stats: bool,
    skip_ad_stats: bool,
    device_parser_version: str,
    log_fn: Callable[[str], None] = _log,
) -> Tuple[int, int, int, int, int, int, Dict[str, Any]]:
    collect_campaign_stats, collect_keyword_stats, collect_ad_stats = _scope_enabled_collectors(sa_scope, collect_sa, normalize_sa_scope_fn)
    c_cnt = fetch_stats_fallback_fn(engine, customer_id, target_date, target_camp_ids, "campaign_id", "fact_campaign_daily", split_map=camp_map, scoped_replace=shopping_only) if collect_campaign_stats else 0
    if collect_keyword_stats:
        if shopping_only and target_kw_ids:
            clear_fact_scope_fn(engine, "fact_keyword_daily", customer_id, target_date, "keyword_id", target_kw_ids)
            k_cnt = 0
        else:
            k_cnt = fetch_stats_fallback_fn(engine, customer_id, target_date, target_kw_ids, "keyword_id", "fact_keyword_daily", split_map=kw_map, scoped_replace=shopping_only) if not skip_keyword_stats else 0
    else:
        k_cnt = 0

    device_ad_cnt = 0
    device_campaign_cnt = 0
    a_cnt = 0
    ad_stat: Dict[str, Dict[str, Any]] = {}
    camp_device_stat: Dict[Tuple[str, str], Dict[str, Any]] = {}

    if not skip_ad_stats:
        if ad_report_df is not None and not getattr(ad_report_df, "empty", False):
            if collect_ad_stats:
                a_cnt = fetch_stats_fallback_fn(
                    engine,
                    customer_id,
                    target_date,
                    target_ad_ids,
                    "ad_id",
                    "fact_ad_daily",
                    split_map=ad_map,
                    scoped_replace=shopping_only,
                )
                ad_stat = {}
            else:
                a_cnt = 0
                ad_stat = {}

            if collect_device:
                ad_device_stat, camp_device_stat, device_meta = parse_ad_device_report_fn(ad_report_df, ad_to_campaign=ad_to_campaign_map)
                if shopping_only:
                    ad_device_stat = filter_stat_result_fn(ad_device_stat, set(target_ad_ids))
                    camp_device_stat = filter_stat_result_fn(camp_device_stat, set(target_camp_ids))
            else:
                ad_device_stat, camp_device_stat, device_meta = {}, {}, {"status": "disabled", "reason": "collect_mode=sa_only"}
                result["device_status"] = "disabled"

            if collect_device and device_meta.get("status") == "ok":
                result["device_status"] = "ok"
                result["device_missing_campaign_rows"] = int(device_meta.get("missing_campaign_rows", 0) or 0)
                device_ad_cnt = save_device_stats_fn(
                    engine, customer_id, target_date, "fact_ad_device_daily", "ad_id", ad_device_stat,
                    data_source="report_device_total_only", source_report="AD"
                )
                device_campaign_cnt = save_device_stats_fn(
                    engine, customer_id, target_date, "fact_campaign_device_daily", "campaign_id", camp_device_stat,
                    data_source="report_device_total_only", source_report="AD"
                )
                if ad_stat:
                    total_from_ad = {
                        "imp": sum(int(v.get("imp", 0) or 0) for v in ad_stat.values()),
                        "clk": sum(int(v.get("clk", 0) or 0) for v in ad_stat.values()),
                        "cost": sum(int(v.get("cost", 0) or 0) for v in ad_stat.values()),
                        "conv": sum(float(v.get("conv", 0.0) or 0.0) for v in ad_stat.values()),
                        "sales": sum(int(v.get("sales", 0) or 0) for v in ad_stat.values()),
                    }
                    total_from_device = summarize_stat_res_fn(ad_device_stat)
                    diff_cost = total_from_ad["cost"] - total_from_device["cost"]
                    diff_sales = total_from_ad["sales"] - total_from_device["sales"]
                    diff_conv = round(total_from_ad["conv"] - total_from_device["conv"], 4)
                    if diff_cost or diff_sales or diff_conv:
                        log_fn(
                            f"   ⚠️ [ {account_name} ] PC/M 검증 차이 감지: cost={diff_cost}, sales={diff_sales}, conv={diff_conv} "
                            f"(source_report=AD, device_rows={device_meta.get('ad_rows', 0)})"
                        )
                miss = int(device_meta.get("missing_campaign_rows", 0) or 0)
                miss_msg = f", 캠페인 매핑누락={miss}건" if miss else ""
                log_fn(
                    f"   ✅ [ {account_name} ] PC/M 분리 저장 완료: 캠페인({device_campaign_cnt}) | 소재({device_ad_cnt})"
                    f"{miss_msg} | parser={device_parser_version}"
                )
            elif collect_device:
                result["device_status"] = str(device_meta.get("status") or "unknown")
                result["device_missing_campaign_rows"] = int(device_meta.get("missing_campaign_rows", 0) or 0)
                debug_keys = [
                    "header_idx", "ad_idx", "camp_idx", "device_idx", "imp_idx", "clk_idx", "cost_idx",
                    "conv_idx", "sales_idx", "rank_idx", "scan_rows", "reject_short", "reject_empty_ad",
                    "reject_empty_device", "reject_zero_metrics", "sample_headers", "preview_rows"
                ]
                extra_parts = []
                for _k in debug_keys:
                    _v = device_meta.get(_k)
                    if _v in (None, "", [], {}):
                        continue
                    extra_parts.append(f"{_k}={_v}")
                extra_msg = f" | {' | '.join(extra_parts)}" if extra_parts else ""
                log_fn(
                    f"   ℹ️ [ {account_name} ] AD 리포트에서 PC/M 컬럼을 확인하지 못해 기기 분리 저장은 건너뜁니다. "
                    f"status={device_meta.get('status')} | parser={device_parser_version}{extra_msg}"
                )
        else:
            if collect_ad_stats:
                log_fn(f"   ⚠️ [ {account_name} ] AD 리포트 없음 → 소재만 실시간 stats 총합으로 대체합니다.")
                a_cnt = fetch_stats_fallback_fn(engine, customer_id, target_date, target_ad_ids, "ad_id", "fact_ad_daily", split_map=ad_map, scoped_replace=shopping_only)
            else:
                log_fn(f"   ℹ️ [ {account_name} ] AD 리포트가 없어 PC/M 전용 적재를 건너뜁니다.")
                a_cnt = 0
            if collect_device:
                result["device_status"] = "ad_report_missing"
    else:
        a_cnt = 0
        result["device_status"] = "not_requested"

    media_cnt, media_meta = collect_media_fact_fn(
        engine, customer_id, target_date, ad_report_df, ad_to_campaign_map, campaign_type_map, camp_device_stat,
        allowed_campaign_ids=set(target_camp_ids) if target_camp_ids else None,
        scoped_campaign_types=['쇼핑검색'] if shopping_only else None,
    )
    detail_rows = int(media_meta.get('detail_rows', 0) or 0)
    summary_rows = int(media_meta.get('summary_rows', 0) or 0)
    result["media_rows_saved"] = int(media_cnt or 0)
    result["media_source"] = str(media_meta.get('status') or 'unknown')
    result["media_detail_rows"] = detail_rows
    result["media_summary_rows"] = summary_rows
    distinct_media_count = int(media_meta.get('distinct_media_count', 0) or 0)
    media_preview = media_meta.get('distinct_media_preview') or []
    preview_msg = f" | media_preview={media_preview}" if media_preview else ""
    if media_cnt:
        log_fn(
            f"   ✅ [ {account_name} ] 매체/지역/기기 저장 완료: total_rows={media_cnt} | detail_rows={detail_rows} | "
            f"summary_rows={summary_rows} | media_codes={distinct_media_count} | source={media_meta.get('status')}{preview_msg}"
        )
    else:
        log_fn(
            f"   ℹ️ [ {account_name} ] 매체/지역 자동 분해 원천이 없어 요약 행만 유지합니다. "
            f"source={media_meta.get('status')} | detail_rows={detail_rows} | summary_rows={summary_rows}"
        )

    return c_cnt, k_cnt, a_cnt, device_ad_cnt, device_campaign_cnt, media_cnt, media_meta



def _sync_structure_and_collect_targets(
    engine: Engine,
    customer_id: str,
    account_name: str,
    collect_sa: bool,
    collect_device: bool,
    shopping_only: bool,
    result: Dict[str, Any],
    list_campaigns_fn: Callable[[str], List[dict]],
    list_adgroups_fn: Callable[[str, str], List[dict]],
    list_keywords_fn: Callable[[str, str], List[dict]],
    list_ads_fn: Callable[[str, str], List[dict]],
    is_shopping_campaign_obj_fn: Callable[[dict], bool],
    extract_keyword_text_from_obj_fn: Callable[[dict], str],
    extract_ad_creative_fields_fn: Callable[[dict], Dict[str, str]],
    upsert_many_fn: Callable[..., Any],
    skip_keyword_dim: bool,
    skip_ad_dim: bool,
    log_fn: Callable[[str], None] = _log,
):
    target_camp_ids: List[str] = []
    target_kw_ids: List[str] = []
    target_ad_ids: List[str] = []
    shopping_campaign_ids: set[str] = set()
    shopping_adgroup_ids: set[str] = set()
    shopping_keyword_ids: set[str] = set()
    camp_rows, ag_rows, kw_rows, ad_rows = [], [], [], []

    log_fn(f"   📥 [ {account_name} ] 구조 데이터 동기화 시작...")
    camps = list_campaigns_fn(customer_id)
    for c in camps:
        cid = str(c.get("nccCampaignId"))
        camp_tp = str(c.get("campaignTp", ""))
        is_shopping = is_shopping_campaign_obj_fn(c)
        if shopping_only and not is_shopping:
            continue

        target_camp_ids.append(cid)
        if is_shopping:
            shopping_campaign_ids.add(cid)
        camp_rows.append({
            "customer_id": str(customer_id),
            "campaign_id": cid,
            "campaign_name": str(c.get("name", "")),
            "campaign_tp": camp_tp,
            "status": str(c.get("status", "")),
        })

        groups = list_adgroups_fn(customer_id, cid)
        for g in groups:
            gid = str(g.get("nccAdgroupId"))
            if is_shopping:
                shopping_adgroup_ids.add(gid)
            ag_rows.append({
                "customer_id": str(customer_id),
                "adgroup_id": gid,
                "campaign_id": cid,
                "adgroup_name": str(g.get("name", "")),
                "status": str(g.get("status", "")),
            })

            if collect_sa and not skip_keyword_dim:
                kws = list_keywords_fn(customer_id, gid)
                for k in kws:
                    kid = str(k.get("nccKeywordId"))
                    target_kw_ids.append(kid)
                    kw_rows.append({
                        "customer_id": str(customer_id),
                        "keyword_id": kid,
                        "adgroup_id": gid,
                        "keyword": extract_keyword_text_from_obj_fn(k),
                        "status": str(k.get("status", "")),
                    })

            if (collect_sa or collect_device) and not skip_ad_dim:
                ads = list_ads_fn(customer_id, gid)
                for ad in ads:
                    adid = str(ad.get("nccAdId"))
                    target_ad_ids.append(adid)
                    ext = extract_ad_creative_fields_fn(ad)
                    ad_rows.append({
                        "customer_id": str(customer_id),
                        "ad_id": adid,
                        "adgroup_id": gid,
                        "ad_name": str(ad.get("name") or ad.get("adName") or ""),
                        "status": str(ad.get("status", "")),
                        "ad_title": ext["ad_title"],
                        "ad_desc": ext["ad_desc"],
                        "pc_landing_url": ext["pc_landing_url"],
                        "mobile_landing_url": ext["mobile_landing_url"],
                        "creative_text": ext["creative_text"],
                        "image_url": ext["image_url"],
                    })

    upsert_many_fn(engine, "dim_campaign", camp_rows, ["customer_id", "campaign_id"])
    upsert_many_fn(engine, "dim_adgroup", ag_rows, ["customer_id", "adgroup_id"])
    if not skip_keyword_dim:
        upsert_many_fn(engine, "dim_keyword", kw_rows, ["customer_id", "keyword_id"])
        kw_text_filled = sum(1 for r in kw_rows if str(r.get("keyword") or "").strip())
        log_fn(f"   🔎 [ {account_name} ] 구조 키워드 텍스트 적재: {kw_text_filled}/{len(kw_rows)}")
    if not skip_ad_dim:
        upsert_many_fn(engine, "dim_ad", ad_rows, ["customer_id", "ad_id"])
    shopping_keyword_ids = set(target_kw_ids) if shopping_adgroup_ids else set()
    result["dim_campaigns"] = len(camp_rows)
    result["dim_adgroups"] = len(ag_rows)
    result["dim_keywords"] = len(kw_rows)
    result["dim_ads"] = len(ad_rows)
    log_fn(f"   ✅ [ {account_name} ] 구조 적재 완료")
    return {
        "target_camp_ids": target_camp_ids,
        "target_kw_ids": target_kw_ids,
        "target_ad_ids": target_ad_ids,
        "shopping_campaign_ids": shopping_campaign_ids,
        "shopping_adgroup_ids": shopping_adgroup_ids,
        "shopping_keyword_ids": shopping_keyword_ids,
    }



def _load_targets_from_dims(
    engine: Engine,
    customer_id: str,
    collect_sa: bool,
    shopping_only: bool,
    shopping_campaign_ids: set[str],
    shopping_adgroup_ids: set[str],
    shopping_keyword_ids: set[str],
):
    with engine.connect() as conn:
        shopping_campaign_ids = {str(r[0]) for r in conn.execute(text("SELECT campaign_id FROM dim_campaign WHERE customer_id = :cid AND lower(coalesce(campaign_tp,'')) LIKE :kw"), {"cid": customer_id, "kw": '%shopping%'})}
        shopping_adgroup_ids = {
            str(r[0]) for r in conn.execute(
                text("SELECT adgroup_id FROM dim_adgroup WHERE customer_id = :cid AND campaign_id = ANY(:cids)"),
                {"cid": customer_id, "cids": list(shopping_campaign_ids)},
            )
        } if shopping_campaign_ids else set()
        shopping_keyword_ids = {
            str(r[0]) for r in conn.execute(
                text("SELECT keyword_id FROM dim_keyword WHERE customer_id = :cid AND adgroup_id = ANY(:gids)"),
                {"cid": customer_id, "gids": list(shopping_adgroup_ids)},
            )
        } if shopping_adgroup_ids else set()

        if shopping_only:
            target_camp_ids = sorted(shopping_campaign_ids)
            target_kw_ids = sorted(shopping_keyword_ids) if collect_sa else []
            target_ad_ids = [
                str(r[0]) for r in conn.execute(
                    text("SELECT ad_id FROM dim_ad WHERE customer_id = :cid AND adgroup_id = ANY(:gids)"),
                    {"cid": customer_id, "gids": list(shopping_adgroup_ids)},
                )
            ] if shopping_adgroup_ids else []
        else:
            target_camp_ids = [str(r[0]) for r in conn.execute(text("SELECT campaign_id FROM dim_campaign WHERE customer_id = :cid"), {"cid": customer_id})]
            target_kw_ids = [str(r[0]) for r in conn.execute(text("SELECT keyword_id FROM dim_keyword WHERE customer_id = :cid"), {"cid": customer_id})] if collect_sa else []
            target_ad_ids = [str(r[0]) for r in conn.execute(text("SELECT ad_id FROM dim_ad WHERE customer_id = :cid"), {"cid": customer_id})]

    return {
        "target_camp_ids": target_camp_ids,
        "target_kw_ids": target_kw_ids,
        "target_ad_ids": target_ad_ids,
        "shopping_campaign_ids": shopping_campaign_ids,
        "shopping_adgroup_ids": shopping_adgroup_ids,
        "shopping_keyword_ids": shopping_keyword_ids,
    }





def _refresh_live_target_ids_minimal(
    customer_id: str,
    collect_sa: bool,
    collect_device: bool,
    shopping_only: bool,
    list_campaigns_fn: Callable[[str], List[dict]],
    list_adgroups_fn: Callable[[str, str], List[dict]],
    list_keywords_fn: Callable[[str, str], List[dict]],
    list_ads_fn: Callable[[str, str], List[dict]],
    is_shopping_campaign_obj_fn: Callable[[dict], bool],
    log_fn: Callable[[str], None] = _log,
):
    target_camp_ids: List[str] = []
    target_kw_ids: List[str] = []
    target_ad_ids: List[str] = []
    shopping_campaign_ids: set[str] = set()
    shopping_adgroup_ids: set[str] = set()
    shopping_keyword_ids: set[str] = set()

    camps = list_campaigns_fn(customer_id) or []
    for c in camps:
        cid = str(c.get("nccCampaignId") or "").strip()
        if not cid:
            continue
        is_shopping = is_shopping_campaign_obj_fn(c)
        if shopping_only and not is_shopping:
            continue
        target_camp_ids.append(cid)
        if is_shopping:
            shopping_campaign_ids.add(cid)

        groups = list_adgroups_fn(customer_id, cid) or []
        for g in groups:
            gid = str(g.get("nccAdgroupId") or "").strip()
            if not gid:
                continue
            if is_shopping:
                shopping_adgroup_ids.add(gid)

            if collect_sa:
                kws = list_keywords_fn(customer_id, gid) or []
                for k in kws:
                    kid = str(k.get("nccKeywordId") or "").strip()
                    if kid:
                        target_kw_ids.append(kid)
                        if is_shopping:
                            shopping_keyword_ids.add(kid)

            if collect_sa or collect_device:
                ads = list_ads_fn(customer_id, gid) or []
                for ad in ads:
                    adid = str(ad.get("nccAdId") or "").strip()
                    if adid:
                        target_ad_ids.append(adid)

    return {
        "target_camp_ids": sorted(set(target_camp_ids)),
        "target_kw_ids": sorted(set(target_kw_ids)),
        "target_ad_ids": sorted(set(target_ad_ids)),
        "shopping_campaign_ids": shopping_campaign_ids,
        "shopping_adgroup_ids": shopping_adgroup_ids,
        "shopping_keyword_ids": shopping_keyword_ids,
    }

def _build_keyword_lookup_bundle(
    engine: Engine,
    customer_id: str,
    shopping_only: bool,
    shopping_adgroup_ids: set[str],
    normalize_keyword_text_fn: Callable[[str], str],
):
    keyword_lookup = {}
    keyword_unique_lookup = {}
    text_freq = {}
    temp_rows = []
    group_rows = {}
    with engine.connect() as conn:
        kw_sql = "SELECT keyword_id, adgroup_id, keyword FROM dim_keyword WHERE customer_id = :cid"
        kw_params = {"cid": customer_id}
        if shopping_only and shopping_adgroup_ids:
            kw_sql += " AND adgroup_id = ANY(:gids)"
            kw_params["gids"] = list(shopping_adgroup_ids)
        for kid, gid, kw in conn.execute(text(kw_sql), kw_params):
            if kid and gid and kw:
                gid_s = str(gid)
                kw_s = str(kw).strip()
                kw_l = kw_s.lower()
                kw_n = normalize_keyword_text_fn(kw_s)
                kid_s = str(kid)
                keyword_lookup[(gid_s, kw_s)] = kid_s
                keyword_lookup[(gid_s, kw_l)] = kid_s
                keyword_lookup[(gid_s, kw_n)] = kid_s
                group_rows.setdefault(gid_s, []).append((kw_n, kid_s))
                text_freq[kw_n] = text_freq.get(kw_n, 0) + 1
                temp_rows.append((kw_n, kid_s))
    for gid_s, rows in group_rows.items():
        keyword_lookup[(gid_s, '__rows__')] = rows
    unique_map = {}
    for kw_n, kid_s in temp_rows:
        if kw_n and text_freq.get(kw_n) == 1:
            unique_map.setdefault(kw_n, []).append(kid_s)
    keyword_unique_lookup = unique_map
    return keyword_lookup, keyword_unique_lookup



def _prepare_account_report_fetch_plan(
    customer_id: str,
    account_name: str,
    target_date: date,
    collect_sa: bool,
    shopping_campaign_ids: set[str],
    result: Dict[str, Any],
    split_enabled_for_date_fn: Callable[[date], bool],
    fetch_multiple_stat_reports_fn: Callable[..., Dict[str, Any]],
    df_state_fn: Callable[[Any], tuple[str, int]],
    log_fn: Callable[[str], None] = _log,
):
    kst_now = datetime.utcnow() + timedelta(hours=9)
    use_realtime_fallback = False
    realtime_reason = ""
    dfs: Dict[str, Any] = {}
    split_candidate_reports: List[str] = []
    split_attempted = False

    if target_date >= kst_now.date():
        use_realtime_fallback = True
        realtime_reason = "today"
        result["ad_report_status"] = "realtime_only"
        result["ad_conversion_status"] = "realtime_only"
        result["shopping_keyword_conversion_status"] = "realtime_only"
        log_fn(f"   ℹ️ [ {account_name} ] 당일 데이터는 실시간 stats 총합만 수집합니다.")
    else:
        log_fn(f"   ⏳ [ {account_name} ] 리포트 생성 대기 중...")
        report_types = ["AD"]
        if split_enabled_for_date_fn(target_date) and shopping_campaign_ids:
            split_candidate_reports = ["AD_CONVERSION", "SHOPPINGKEYWORD_CONVERSION_DETAIL"]
            report_types.extend(split_candidate_reports)
            split_attempted = bool(collect_sa)
        dfs = fetch_multiple_stat_reports_fn(customer_id, report_types, target_date)
        result["ad_report_status"], result["ad_report_rows"] = df_state_fn(dfs.get("AD"))
        ad_conv_df = dfs.get("AD_CONVERSION") if "AD_CONVERSION" in report_types else None
        shop_kw_conv_df = dfs.get("SHOPPINGKEYWORD_CONVERSION_DETAIL") if "SHOPPINGKEYWORD_CONVERSION_DETAIL" in report_types else None
        result["ad_conversion_status"], result["ad_conversion_rows"] = df_state_fn(ad_conv_df) if split_candidate_reports else ("not_requested", 0)
        result["shopping_keyword_conversion_status"], result["shopping_keyword_conversion_rows"] = df_state_fn(shop_kw_conv_df) if split_candidate_reports else ("not_requested", 0)

        if dfs.get("AD") is None and all(dfs.get(tp) is None for tp in split_candidate_reports):
            log_fn(f"   ⚠️ [ {account_name} ] AD / 전환 리포트가 모두 실패 → 실시간 stats 총합으로 대체합니다. (purchase/cart 미분리)")
            use_realtime_fallback = True
            realtime_reason = "report_missing"

    result["used_realtime_fallback"] = bool(use_realtime_fallback)
    result["realtime_reason"] = realtime_reason
    result["split_attempted"] = bool(split_attempted)
    return dfs, split_candidate_reports, split_attempted, use_realtime_fallback, realtime_reason



def _finalize_account_result(
    result: Dict[str, Any],
    account_name: str,
    collect_mode: str,
    collect_device: bool,
    split_report_ok: bool,
    c_cnt: int,
    k_cnt: int,
    a_cnt: int,
    device_campaign_cnt: int,
    device_ad_cnt: int,
    log_fn: Callable[[str], None] = _log,
):
    result["campaign_rows_saved"] = int(c_cnt or 0)
    result["keyword_rows_saved"] = int(k_cnt or 0)
    result["ad_rows_saved"] = int(a_cnt or 0)
    result["device_campaign_rows_saved"] = int(device_campaign_cnt or 0)
    result["device_ad_rows_saved"] = int(device_ad_cnt or 0)
    result["split_report_ok"] = bool(split_report_ok)
    result["zero_data"] = bool(c_cnt == 0 and k_cnt == 0 and a_cnt == 0 and device_ad_cnt == 0 and device_campaign_cnt == 0)

    if result["zero_data"]:
        result["status"] = "zero_data"
        log_fn(f"❌ [ {account_name} ] 수집된 데이터가 0건입니다! (해당 날짜에 발생한 클릭/노출 성과가 없음)")
    else:
        result["status"] = "ok"
        if collect_mode == "device_only":
            log_fn(f"   ✅ [ {account_name} ] PC/M 전용 수집 완료: 캠페인({device_campaign_cnt}) | 소재({device_ad_cnt})")
        else:
            mode_msg = "총합 + purchase/cart/wishlist 분리" if split_report_ok else "총합만 저장 / purchase.cart.wishlist 미분리"
            if collect_device:
                mode_msg += " + PC/M"
            log_fn(f"   ✅ [ {account_name} ] 리포트 수집 완료 ({mode_msg}): 캠페인({c_cnt}) | 키워드({k_cnt}) | 소재({a_cnt})")



def process_account(
    engine: Engine,
    customer_id: str,
    account_name: str,
    target_date: date,
    skip_dim: bool = False,
    fast_mode: bool = False,
    collect_mode: str = "sa_with_device",
    sa_scope: str = "full",
    shopping_only: bool = False,
    *,
    new_account_collect_result_fn: Callable[..., Dict[str, Any]],
    acquire_job_lock_fn: Callable[..., Any],
    release_job_lock_fn: Callable[..., Any],
    normalize_sa_scope_fn: Callable[[str], str],
    label_collect_mode_fn: Callable[[str], str],
    label_sa_scope_fn: Callable[[str], str],
    sync_structure_and_collect_targets_fn: Callable[..., Dict[str, Any]],
    load_targets_from_dims_fn: Callable[..., Dict[str, Any]],
    build_keyword_lookup_bundle_fn: Callable[..., Tuple[dict, dict]],
    log_best_effort_failure_fn: Callable[..., None],
    make_live_keyword_resolver_fn: Callable[[str], Any],
    build_ad_to_campaign_map_fn: Callable[..., Dict[str, str]],
    build_campaign_type_map_fn: Callable[..., Dict[str, str]],
    prepare_account_report_fetch_plan_fn: Callable[..., Tuple[Dict[str, Any], List[str], bool, bool, str]],
    scope_enabled_collectors_fn: Callable[[str, bool], tuple[bool, bool, bool]],
    fetch_stats_fallback_fn: Callable[..., int],
    clear_fact_scope_fn: Callable[..., None],
    collect_media_fact_fn: Callable[..., Tuple[int, Dict[str, Any]]],
    resolve_split_payload_fn: Callable[..., Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], List[Dict[str, Any]], bool]],
    save_report_stats_and_breakdowns_fn: Callable[..., Tuple[int, int, int, int, int, int, Dict[str, Any]]],
    is_ad_only_scope_fn: Callable[[str], bool],
    replace_query_fact_range_fn: Callable[..., None],
    finalize_account_result_fn: Callable[..., None],
    exc_label_fn: Callable[[Exception], str],
    traceback_tail_fn: Callable[[Exception, int], str],
    refresh_overview_report_source_cache_fn: Callable[..., None] | None = None,
    list_campaigns_fn: Callable[[str], List[dict]] | None = None,
    list_adgroups_fn: Callable[[str, str], List[dict]] | None = None,
    list_keywords_fn: Callable[[str, str], List[dict]] | None = None,
    list_ads_fn: Callable[[str, str], List[dict]] | None = None,
    is_shopping_campaign_obj_fn: Callable[[dict], bool] | None = None,
    skip_keyword_stats: bool = False,
    skip_ad_stats: bool = False,
    log_fn: Callable[[str], None] = _log,
) -> Dict[str, Any]:
    log_fn(f"▶️ [ {account_name} ] 업체 데이터 조회 시작...")

    result = new_account_collect_result_fn(customer_id, account_name, target_date, collect_mode, sa_scope, skip_dim, fast_mode, shopping_only)
    stage = "init"
    result["stage"] = stage
    job_lock = acquire_job_lock_fn(engine, customer_id, target_date)
    if job_lock is False:
        result["status"] = "skipped"
        result["notes"].append("job_lock_busy")
        log_fn(f"⏭️ [ {account_name} ] 동일 날짜/계정 수집이 이미 실행 중이라 건너뜁니다. ({target_date})")
        return result

    try:
        stage = "normalize_collect_mode"
        result["stage"] = stage
        collect_mode = (collect_mode or "sa_with_device").strip().lower()
        sa_scope = normalize_sa_scope_fn(sa_scope)
        collect_sa = collect_mode in {"sa_only", "sa_with_device"}
        collect_device = collect_mode in {"device_only", "sa_with_device"}
        result["collect_mode"] = collect_mode
        result["collect_mode_label"] = label_collect_mode_fn(collect_mode)
        result["sa_scope"] = sa_scope
        result["sa_scope_label"] = label_sa_scope_fn(sa_scope)
        result["collect_sa"] = collect_sa
        result["collect_device"] = collect_device
        target_camp_ids, target_kw_ids, target_ad_ids = [], [], []
        shopping_campaign_ids: set[str] = set()
        shopping_adgroup_ids: set[str] = set()
        shopping_keyword_ids: set[str] = set()
        c_cnt = k_cnt = a_cnt = 0
        device_ad_cnt = device_campaign_cnt = 0
        media_cnt = 0
        media_meta: Dict[str, Any] = {}
        shop_query_rows: List[Dict[str, Any]] = []
        split_report_ok = False
        if shopping_only:
            log_fn(f"   🛍️ [ {account_name} ] 쇼핑검색 전용 수집 모드")

        stage = "load_dim_targets"
        result["stage"] = stage
        target_bundle = sync_structure_and_collect_targets_fn(
            engine,
            customer_id=customer_id,
            account_name=account_name,
            collect_sa=collect_sa,
            collect_device=collect_device,
            shopping_only=shopping_only,
            result=result,
        ) if not skip_dim else load_targets_from_dims_fn(
            engine,
            customer_id=customer_id,
            collect_sa=collect_sa,
            shopping_only=shopping_only,
            shopping_campaign_ids=shopping_campaign_ids,
            shopping_adgroup_ids=shopping_adgroup_ids,
            shopping_keyword_ids=shopping_keyword_ids,
        )
        target_camp_ids = target_bundle["target_camp_ids"]
        target_kw_ids = target_bundle["target_kw_ids"]
        target_ad_ids = target_bundle["target_ad_ids"]
        shopping_campaign_ids = target_bundle["shopping_campaign_ids"]
        shopping_adgroup_ids = target_bundle["shopping_adgroup_ids"]
        shopping_keyword_ids = target_bundle["shopping_keyword_ids"]
        result["campaign_targets"] = len(target_camp_ids)
        result["keyword_targets"] = len(target_kw_ids)
        result["ad_targets"] = len(target_ad_ids)
        result["shopping_campaign_targets"] = len(shopping_campaign_ids)

        try:
            kst_today = (datetime.utcnow() + timedelta(hours=9)).date()
            recent_fast_skip_dim = bool(skip_dim and fast_mode and target_date >= (kst_today - timedelta(days=1)))
        except Exception:
            recent_fast_skip_dim = bool(skip_dim and fast_mode)

        if recent_fast_skip_dim and callable(list_campaigns_fn) and callable(list_adgroups_fn) and callable(list_keywords_fn) and callable(list_ads_fn) and callable(is_shopping_campaign_obj_fn):
            try:
                live_bundle = _refresh_live_target_ids_minimal(
                    customer_id=customer_id,
                    collect_sa=collect_sa,
                    collect_device=collect_device,
                    shopping_only=shopping_only,
                    list_campaigns_fn=list_campaigns_fn,
                    list_adgroups_fn=list_adgroups_fn,
                    list_keywords_fn=list_keywords_fn,
                    list_ads_fn=list_ads_fn,
                    is_shopping_campaign_obj_fn=is_shopping_campaign_obj_fn,
                    log_fn=log_fn,
                )
                old_counts = (len(target_camp_ids), len(target_kw_ids), len(target_ad_ids))
                target_camp_ids = live_bundle["target_camp_ids"]
                target_kw_ids = live_bundle["target_kw_ids"]
                target_ad_ids = live_bundle["target_ad_ids"]
                shopping_campaign_ids = live_bundle["shopping_campaign_ids"]
                shopping_adgroup_ids = live_bundle["shopping_adgroup_ids"]
                shopping_keyword_ids = live_bundle["shopping_keyword_ids"]
                new_counts = (len(target_camp_ids), len(target_kw_ids), len(target_ad_ids))
                result["campaign_targets"] = new_counts[0]
                result["keyword_targets"] = new_counts[1]
                result["ad_targets"] = new_counts[2]
                result["shopping_campaign_targets"] = len(shopping_campaign_ids)
                result["live_target_refresh"] = True
                if new_counts != old_counts:
                    log_fn(
                        f"   🔄 [ {account_name} ] 최근일 fast 보정: live target 재확인 적용 "
                        f"(campaign {old_counts[0]}→{new_counts[0]}, keyword {old_counts[1]}→{new_counts[1]}, ad {old_counts[2]}→{new_counts[2]})"
                    )
            except Exception as e:
                log_best_effort_failure_fn("live target refresh", e, ctx=f"customer_id={customer_id} fast_mode={fast_mode} skip_dim={skip_dim}")

        stage = "build_keyword_lookup"
        result["stage"] = stage
        try:
            keyword_lookup, keyword_unique_lookup = build_keyword_lookup_bundle_fn(
                engine,
                customer_id=customer_id,
                shopping_only=shopping_only,
                shopping_adgroup_ids=shopping_adgroup_ids,
            )
        except Exception as e:
            log_best_effort_failure_fn("keyword lookup 빌드", e, ctx=f"customer_id={customer_id}")
            keyword_lookup = {}
            keyword_unique_lookup = {}

        live_keyword_resolver = None if fast_mode else make_live_keyword_resolver_fn(customer_id)

        stage = "load_maps"
        result["stage"] = stage
        ad_to_campaign_map = build_ad_to_campaign_map_fn(engine, customer_id)
        campaign_type_map = build_campaign_type_map_fn(engine, customer_id)

        stage = "fetch_reports"
        result["stage"] = stage
        dfs, split_candidate_reports, split_attempted, use_realtime_fallback, realtime_reason = prepare_account_report_fetch_plan_fn(
            customer_id=customer_id,
            account_name=account_name,
            target_date=target_date,
            collect_sa=collect_sa,
            shopping_campaign_ids=shopping_campaign_ids,
            result=result,
        )

        stage = "save_realtime_fallback" if use_realtime_fallback else "resolve_split_payload"
        result["stage"] = stage
        if use_realtime_fallback:
            collect_campaign_stats, collect_keyword_stats, collect_ad_stats = scope_enabled_collectors_fn(sa_scope, collect_sa)
            if collect_sa:
                c_cnt = fetch_stats_fallback_fn(engine, customer_id, target_date, target_camp_ids, "campaign_id", "fact_campaign_daily", scoped_replace=shopping_only) if collect_campaign_stats else 0
                if collect_keyword_stats:
                    if shopping_only and target_kw_ids:
                        clear_fact_scope_fn(engine, "fact_keyword_daily", customer_id, target_date, "keyword_id", target_kw_ids)
                        k_cnt = 0
                    else:
                        k_cnt = fetch_stats_fallback_fn(engine, customer_id, target_date, target_kw_ids, "keyword_id", "fact_keyword_daily", scoped_replace=shopping_only) if not skip_keyword_stats else 0
                else:
                    k_cnt = 0
                a_cnt = fetch_stats_fallback_fn(engine, customer_id, target_date, target_ad_ids, "ad_id", "fact_ad_daily", scoped_replace=shopping_only) if (collect_ad_stats and not skip_ad_stats) else 0
                log_fn(f"   ✅ [ {account_name} ] 실시간 총합 수집 완료: 캠페인({c_cnt}) | 키워드({k_cnt}) | 소재({a_cnt}) | 범위={label_sa_scope_fn(sa_scope)}")
            else:
                log_fn(f"   ℹ️ [ {account_name} ] 당일/실시간 모드에서는 PC/M 전용 수집을 수행하지 않습니다.")
            device_ad_cnt = 0
            device_campaign_cnt = 0
            result["device_status"] = "realtime_skipped" if collect_device else "not_applicable"
            media_cnt, media_meta = collect_media_fact_fn(
                engine, customer_id, target_date, None, ad_to_campaign_map, campaign_type_map, None,
                allowed_campaign_ids=set(target_camp_ids) if target_camp_ids else None,
                scoped_campaign_types=['쇼핑검색'] if shopping_only else None,
            )
            if media_cnt:
                log_fn(f"   ✅ [ {account_name} ] 매체/지역/기기 요약 저장 완료: {media_cnt}건 | source={media_meta.get('status')}")
        else:
            split_report_ok = False
            ad_report_df = dfs.get("AD")
            camp_map, kw_map, ad_map, shop_query_rows, split_report_ok = resolve_split_payload_fn(
                dfs,
                collect_sa=collect_sa,
                target_date=target_date,
                shopping_only=shopping_only,
                shopping_campaign_ids=shopping_campaign_ids,
                shopping_keyword_ids=shopping_keyword_ids,
                keyword_lookup=keyword_lookup,
                keyword_unique_lookup=keyword_unique_lookup,
                live_keyword_resolver=live_keyword_resolver,
                account_name=account_name,
                customer_id=customer_id,
                result=result,
            )

            stage = "save_stats_and_breakdowns"
            result["stage"] = stage
            c_cnt, k_cnt, a_cnt, device_ad_cnt, device_campaign_cnt, media_cnt, media_meta = save_report_stats_and_breakdowns_fn(
                engine,
                customer_id=customer_id,
                account_name=account_name,
                target_date=target_date,
                collect_sa=collect_sa,
                collect_device=collect_device,
                sa_scope=sa_scope,
                shopping_only=shopping_only,
                target_camp_ids=target_camp_ids,
                target_kw_ids=target_kw_ids,
                target_ad_ids=target_ad_ids,
                ad_report_df=ad_report_df,
                ad_to_campaign_map=ad_to_campaign_map,
                campaign_type_map=campaign_type_map,
                camp_map=camp_map,
                kw_map=kw_map,
                ad_map=ad_map,
                result=result,
            )

            if collect_sa and not is_ad_only_scope_fn(sa_scope):
                stage = "save_shopping_query_split"
                result["stage"] = stage
                replace_query_fact_range_fn(engine, shop_query_rows, customer_id, target_date)
                if shop_query_rows:
                    log_fn(f"   ✅ [ {account_name} ] 쇼핑검색어 분리 저장 완료: {len(shop_query_rows)}건")

            result["shopping_query_rows_saved"] = int(len(shop_query_rows) if shop_query_rows else 0)

            stage = "finalize_result"
            result["stage"] = stage
            finalize_account_result_fn(
                result,
                account_name=account_name,
                collect_mode=collect_mode,
                collect_device=collect_device,
                split_report_ok=split_report_ok,
                c_cnt=c_cnt,
                k_cnt=k_cnt,
                a_cnt=a_cnt,
                device_campaign_cnt=device_campaign_cnt,
                device_ad_cnt=device_ad_cnt,
            )

            if callable(refresh_overview_report_source_cache_fn) and result.get("status") == "ok":
                try:
                    if int(result.get("k_cnt", 0) or 0) > 0 or int(result.get("shopping_query_rows_saved", 0) or 0) > 0:
                        refresh_overview_report_source_cache_fn(engine, customer_id, target_date, target_date)
                        log_fn(f"   ✅ [ {account_name} ] 오버뷰 보고서 소스 캐시 갱신 완료")
                except Exception as e:
                    log_best_effort_failure_fn("overview report cache refresh", e, ctx=f"customer_id={customer_id} dt={target_date}")

    except Exception as e:
        result["status"] = "error"
        result["stage"] = stage
        result["error"] = f"stage={stage} | {exc_label_fn(e)}"
        tb_tail = traceback_tail_fn(e, limit=4)
        if tb_tail:
            result["notes"].append(f"traceback={tb_tail}")
        log_fn(f"❌ [ {account_name} ] 계정 처리 중 오류 발생 | stage={stage} | {exc_label_fn(e)}")
        if tb_tail:
            log_fn(f"   ↳ traceback: {tb_tail}")
    finally:
        if job_lock is not False:
            release_job_lock_fn(job_lock, customer_id, target_date)
    return result
