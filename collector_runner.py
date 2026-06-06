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
    adgroup_to_campaign_map: Dict[str, str],
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

    owner_campaign_lookup = adgroup_to_campaign_map or {}
    source_maps: Dict[str, Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Any]]] = {}
    # AD_CONVERSION mostly covers campaign/ad split.  Powerlink keyword purchase
    # completions can appear in CRITERION_CONVERSION with a criterion value such
    # as ``nkw-...~...``.  Parse both and merge only keyword-level split from
    # CRITERION_CONVERSION into fact_keyword_daily.
    report_candidates = ["AD_CONVERSION", "CRITERION_CONVERSION"]
    if shopping_campaign_ids:
        report_candidates.append("SHOPPINGKEYWORD_CONVERSION_DETAIL")
    else:
        log_fn(f"   ℹ️ [ {account_name} ] 쇼핑검색 캠페인이 없어 쇼핑검색어 전환 리포트만 건너뜁니다. AD/CRITERION 전환 리포트로 키워드 구매완료는 계속 수집합니다.")
    # keep order while removing duplicate report types
    report_candidates = list(dict.fromkeys(report_candidates))
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
            owner_campaign_lookup=owner_campaign_lookup,
            live_keyword_resolver=live_keyword_resolver,
            debug_account_name=account_name,
            debug_target_date=str(target_date),
        )

        if len(one_camp_map) == 0 and len(one_kw_map) == 0 and len(one_ad_map) == 0:
            log_fn(f"   ⚠️ [ {account_name} ] {tp} 데이터는 있으나 purchase/cart/wishlist 파싱에 실패했습니다. debug_reports 원본을 확인하세요.")
            continue

        source_maps[tp] = (one_camp_map, one_kw_map, one_ad_map, one_summary)

    ad_conv_maps = source_maps.get("AD_CONVERSION", ({}, {}, {}, empty_split_summary_fn()))
    criterion_conv_maps = source_maps.get("CRITERION_CONVERSION", ({}, {}, {}, empty_split_summary_fn()))
    shop_kw_maps = source_maps.get("SHOPPINGKEYWORD_CONVERSION_DETAIL", ({}, {}, {}, empty_split_summary_fn()))

    ad_camp_map, ad_kw_map, ad_ad_map, ad_summary = ad_conv_maps
    criterion_camp_map, criterion_kw_map, criterion_ad_map, criterion_summary = criterion_conv_maps
    shop_camp_map, shop_kw_map, shop_ad_map, shop_summary = shop_kw_maps

    shop_query_df = dfs.get("SHOPPINGKEYWORD_CONVERSION_DETAIL")
    if shop_query_df is not None and not getattr(shop_query_df, "empty", False):
        try:
            shop_query_rows = parse_shopping_query_report_fn(shop_query_df, target_date, customer_id)
        except Exception as e:
            log_fn(f"   ⚠️ [ {account_name} ] 쇼핑검색어 분리 저장 파싱 실패: {e}")
            shop_query_rows = []

    camp_map = ad_camp_map if ad_camp_map else (criterion_camp_map if criterion_camp_map else shop_camp_map)
    ad_map = ad_ad_map if ad_ad_map else shop_ad_map
    # SHOPPINGKEYWORD_CONVERSION_DETAIL is a search-term report, not a keyword report.
    # Powerlink keyword purchase completion should come from AD_CONVERSION and
    # CRITERION_CONVERSION only.  The shopping search-term rows are stored in
    # fact_search_term_daily, not fact_keyword_daily.
    raw_kw_map = merge_split_maps_fn(ad_kw_map, criterion_kw_map)
    kw_map = {} if shopping_only else raw_kw_map

    split_report_ok = bool(camp_map or kw_map or ad_map)

    final_split_summary = ad_summary if split_summary_has_values_fn(ad_summary) else (criterion_summary if split_summary_has_values_fn(criterion_summary) else shop_summary)
    if shopping_only and split_report_ok and split_summary_has_values_fn(final_split_summary):
        split_ok, split_reason = validate_shopping_split_summary_fn(final_split_summary, ad_map)
        if not split_ok:
            log_fn(f"   ⚠️ [ {account_name} ] 상세 split 검증 실패 → 상세 split 저장을 건너뛰고 총합만 적재합니다. ({split_reason})")
            camp_map, kw_map, ad_map = {}, {}, {}
            shop_query_rows = []
            result["stage"] = "resolve_split_payload"
            split_report_ok = False

    if split_report_ok:
        camp_ad_src = 'AD_CONVERSION' if ad_camp_map or ad_ad_map else ('CRITERION_CONVERSION' if criterion_camp_map or criterion_ad_map else ('SHOPPINGKEYWORD_CONVERSION_DETAIL' if shop_camp_map or shop_ad_map else 'none'))
        if ad_kw_map and criterion_kw_map:
            kw_src = 'AD_CONVERSION+CRITERION_CONVERSION'
        elif criterion_kw_map:
            kw_src = 'CRITERION_CONVERSION'
        elif ad_kw_map:
            kw_src = 'AD_CONVERSION'
        else:
            kw_src = 'none'
        summary_src = 'AD_CONVERSION' if split_summary_has_values_fn(ad_summary) else ('CRITERION_CONVERSION' if split_summary_has_values_fn(criterion_summary) else ('SHOPPINGKEYWORD_CONVERSION_DETAIL' if split_summary_has_values_fn(shop_summary) else 'none'))
        query_src = 'SHOPPINGKEYWORD_CONVERSION_DETAIL' if shop_query_rows else 'none'
        result["split_source"] = f"summary={summary_src},campaign/ad={camp_ad_src},keyword={kw_src},query={query_src}"
        log_fn(
            f"   ✅ [ {account_name} ] detail split 원천 사용: "
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


def _merge_device_stats(primary: Dict[Tuple[str, str], dict], fallback: Dict[Tuple[str, str], dict]) -> Dict[Tuple[str, str], dict]:
    merged = dict(primary or {})
    for key, value in (fallback or {}).items():
        merged[key] = value
    return merged


def _save_device_totals_with_unsegmented_fallback(
    engine: Engine,
    *,
    customer_id: str,
    account_name: str,
    target_date: date,
    target_camp_ids: List[str],
    target_ad_ids: List[str],
    ad_device_stat: Dict[Tuple[str, str], dict],
    camp_device_stat: Dict[Tuple[str, str], dict],
    save_device_stats_fn: Callable[..., int],
    build_unsegmented_device_stat_from_totals_fn: Callable[..., Dict[Tuple[str, str], dict]],
    source_report: str,
    log_fn: Callable[[str], None] = _log,
) -> tuple[int, int, int]:
    ad_fallback = build_unsegmented_device_stat_from_totals_fn(
        engine,
        customer_id,
        target_date,
        "fact_ad_daily",
        "ad_id",
        target_ad_ids,
        ad_device_stat,
    )
    camp_fallback = build_unsegmented_device_stat_from_totals_fn(
        engine,
        customer_id,
        target_date,
        "fact_campaign_daily",
        "campaign_id",
        target_camp_ids,
        camp_device_stat,
    )

    merged_ad_stat = _merge_device_stats(ad_device_stat, ad_fallback)
    merged_camp_stat = _merge_device_stats(camp_device_stat, camp_fallback)
    data_source = "report_device_with_unsegmented_total" if (ad_fallback or camp_fallback) else (
        "report_device_total_only" if (ad_device_stat or camp_device_stat) else "stats_unsegmented_total"
    )
    device_ad_cnt = save_device_stats_fn(
        engine,
        customer_id,
        target_date,
        "fact_ad_device_daily",
        "ad_id",
        merged_ad_stat,
        data_source=data_source,
        source_report=source_report,
    )
    device_campaign_cnt = save_device_stats_fn(
        engine,
        customer_id,
        target_date,
        "fact_campaign_device_daily",
        "campaign_id",
        merged_camp_stat,
        data_source=data_source,
        source_report=source_report,
    )
    fallback_cnt = len(ad_fallback) + len(camp_fallback)
    if fallback_cnt:
        log_fn(
            f"   ℹ️ [ {account_name} ] PC/M 미분리 총합 보정 저장: "
            f"캠페인 {len(camp_fallback)}건, 소재 {len(ad_fallback)}건"
        )
    return device_ad_cnt, device_campaign_cnt, fallback_cnt



def _collect_pc_mobile_device_stats_from_stats(
    *,
    customer_id: str,
    target_date: date,
    target_camp_ids: List[str],
    target_ad_ids: List[str],
    get_stats_breakdown_range_fn: Callable[..., List[dict]] | None,
    build_pc_mobile_device_stat_from_stats_fn: Callable[..., tuple] | None,
    log_fn: Callable[[str], None] = _log,
) -> Tuple[Dict[Tuple[str, str], dict], Dict[Tuple[str, str], dict], Dict[str, Any]]:
    """Collect actual PC/MOBILE performance from /stats breakdown=pcMblTp.

    This intentionally does not use adgroup device targeting settings. Even when an
    adgroup is set to "모두", the Stat API can return separate PC/MOBILE rows.
    """
    meta: Dict[str, Any] = {
        "status": "not_requested",
        "source_report": "STATS_PCMobile",
        "breakdown": "pcMblTp",
        "campaign_raw_rows": 0,
        "ad_raw_rows": 0,
        "campaign_rows": 0,
        "ad_rows": 0,
    }
    if not callable(get_stats_breakdown_range_fn) or not callable(build_pc_mobile_device_stat_from_stats_fn):
        meta["status"] = "not_available"
        return {}, {}, meta

    camp_raw: List[dict] = []
    ad_raw: List[dict] = []
    errors: List[str] = []
    try:
        camp_raw = get_stats_breakdown_range_fn(customer_id, target_camp_ids, target_date, "pcMblTp", log_fn=log_fn) if target_camp_ids else []
    except Exception as exc:
        errors.append(f"campaign={type(exc).__name__}: {exc}")
    try:
        ad_raw = get_stats_breakdown_range_fn(customer_id, target_ad_ids, target_date, "pcMblTp", log_fn=log_fn) if target_ad_ids else []
    except Exception as exc:
        errors.append(f"ad={type(exc).__name__}: {exc}")

    camp_stat, camp_meta = build_pc_mobile_device_stat_from_stats_fn(camp_raw, valid_ids=set(target_camp_ids or []))
    ad_stat, ad_meta = build_pc_mobile_device_stat_from_stats_fn(ad_raw, valid_ids=set(target_ad_ids or []))
    meta.update({
        "status": "ok" if (camp_stat or ad_stat) else ("error" if errors else "no_rows"),
        "errors": errors,
        "campaign_raw_rows": len(camp_raw or []),
        "ad_raw_rows": len(ad_raw or []),
        "campaign_rows": len(camp_stat),
        "ad_rows": len(ad_stat),
        "campaign_meta": camp_meta,
        "ad_meta": ad_meta,
    })
    return ad_stat, camp_stat, meta


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
    criterion_report_df=None,
    criterion_conversion_report_df=None,
    ad_to_campaign_map: Dict[str, str] | None = None,
    adgroup_to_campaign_map: Dict[str, str] | None = None,
    campaign_type_map: Dict[str, str] | None = None,
    camp_map: Dict[str, Dict[str, Any]],
    kw_map: Dict[str, Dict[str, Any]],
    ad_map: Dict[str, Dict[str, Any]],
    result: Dict[str, Any],
    normalize_sa_scope_fn: Callable[[str | None], str],
    fetch_stats_fallback_fn: Callable[..., int],
    get_stats_breakdown_range_fn: Callable[..., List[dict]] | None,
    clear_fact_scope_fn: Callable[..., Any],
    parse_ad_device_report_fn: Callable[..., tuple],
    parse_criterion_device_reports_fn: Callable[..., tuple] | None,
    build_pc_mobile_device_stat_from_stats_fn: Callable[..., tuple] | None,
    filter_stat_result_fn: Callable[..., dict],
    save_device_stats_fn: Callable[..., int],
    build_unsegmented_device_stat_from_totals_fn: Callable[..., Dict[Tuple[str, str], dict]],
    summarize_stat_res_fn: Callable[[dict], dict],
    collect_media_fact_fn: Callable[..., tuple],
    skip_keyword_stats: bool,
    skip_ad_stats: bool,
    device_parser_version: str,
    log_fn: Callable[[str], None] = _log,
) -> Tuple[int, int, int, int, int, int, Dict[str, Any]]:
    ad_to_campaign_map = ad_to_campaign_map or {}
    collect_campaign_stats, collect_keyword_stats, collect_ad_stats = _scope_enabled_collectors(sa_scope, collect_sa, normalize_sa_scope_fn)

    c_cnt = fetch_stats_fallback_fn(
        engine, customer_id, target_date, target_camp_ids, "campaign_id", "fact_campaign_daily",
        split_map=camp_map, scoped_replace=shopping_only
    ) if collect_campaign_stats else 0

    if collect_keyword_stats:
        if shopping_only and target_kw_ids:
            clear_fact_scope_fn(engine, "fact_keyword_daily", customer_id, target_date, "keyword_id", target_kw_ids)
            k_cnt = 0
        else:
            k_cnt = fetch_stats_fallback_fn(
                engine, customer_id, target_date, target_kw_ids, "keyword_id", "fact_keyword_daily",
                split_map=kw_map, scoped_replace=shopping_only
            ) if not skip_keyword_stats else 0
    else:
        k_cnt = 0

    a_cnt = 0
    device_ad_cnt = 0
    device_campaign_cnt = 0

    if not skip_ad_stats:
        if collect_ad_stats:
            if ad_report_df is None or getattr(ad_report_df, "empty", False):
                log_fn(f"   ⚠️ [ {account_name} ] AD 리포트 없음 → 소재 실시간 stats 총합으로 대체합니다.")
            a_cnt = fetch_stats_fallback_fn(
                engine, customer_id, target_date, target_ad_ids, "ad_id", "fact_ad_daily",
                split_map=ad_map, scoped_replace=shopping_only,
            )
        else:
            a_cnt = 0
    else:
        a_cnt = 0

    if collect_device:
        # 1순위: 실제 성과 기준 /stats breakdown=pcMblTp.
        # 광고그룹의 기기 타게팅 설정값(모두/PC/모바일)이 아니라 PC/MOBILE 실적 row를 직접 받습니다.
        stats_ad_device_stat, stats_camp_device_stat, stats_device_meta = _collect_pc_mobile_device_stats_from_stats(
            customer_id=customer_id,
            target_date=target_date,
            target_camp_ids=target_camp_ids,
            target_ad_ids=target_ad_ids,
            get_stats_breakdown_range_fn=get_stats_breakdown_range_fn,
            build_pc_mobile_device_stat_from_stats_fn=build_pc_mobile_device_stat_from_stats_fn,
            log_fn=log_fn,
        )
        result["device_stats_breakdown_status"] = str(stats_device_meta.get("status") or "unknown")
        result["device_stats_campaign_rows"] = int(stats_device_meta.get("campaign_rows", 0) or 0)
        result["device_stats_ad_rows"] = int(stats_device_meta.get("ad_rows", 0) or 0)

        ad_device_stat = stats_ad_device_stat
        camp_device_stat = stats_camp_device_stat
        source_parts: List[str] = []
        if ad_device_stat or camp_device_stat:
            source_parts.append("STATS_PCMobile")

        # 2순위: 구버전/일부 계정에서 pcMblTp breakdown row가 비어 있을 때만 AD 리포트 컬럼을 보조로 사용합니다.
        report_meta: Dict[str, Any] = {"status": "not_requested"}
        if ad_report_df is not None and not getattr(ad_report_df, "empty", False):
            report_ad_stat, report_camp_stat, report_meta = parse_ad_device_report_fn(ad_report_df, ad_to_campaign=ad_to_campaign_map)
            result["device_report_status"] = str(report_meta.get("status") or "unknown")
            if not ad_device_stat and report_ad_stat:
                ad_device_stat = report_ad_stat
                source_parts.append("AD_REPORT_AD_FALLBACK")
            if not camp_device_stat and report_camp_stat:
                camp_device_stat = report_camp_stat
                source_parts.append("AD_REPORT_CAMPAIGN_FALLBACK")
        else:
            result["device_report_status"] = "ad_report_missing"

        # CRITERION 리포트는 광고그룹 타게팅/criterion 기반이라 기기 설정값과 섞일 수 있어
        # 더 이상 PC/M 실제 성과의 주 수집원으로 저장하지 않습니다.
        result["criterion_device_status"] = "not_used_actual_pcMblTp"
        result["criterion_device_rows"] = 0

        if shopping_only:
            ad_device_stat = filter_stat_result_fn(ad_device_stat, set(target_ad_ids))
            camp_device_stat = filter_stat_result_fn(camp_device_stat, set(target_camp_ids))

        source_report = "+".join(dict.fromkeys(source_parts)) or "STATS_TOTAL"
        device_ad_cnt, device_campaign_cnt, fallback_cnt = _save_device_totals_with_unsegmented_fallback(
            engine,
            customer_id=customer_id,
            account_name=account_name,
            target_date=target_date,
            target_camp_ids=target_camp_ids,
            target_ad_ids=target_ad_ids,
            ad_device_stat=ad_device_stat,
            camp_device_stat=camp_device_stat,
            save_device_stats_fn=save_device_stats_fn,
            build_unsegmented_device_stat_from_totals_fn=build_unsegmented_device_stat_from_totals_fn,
            source_report=source_report,
            log_fn=log_fn,
        )
        result["device_ad_source"] = "STATS_PCMobile" if stats_ad_device_stat else ("AD_REPORT" if ad_device_stat else "STATS_TOTAL")
        result["device_campaign_source"] = "STATS_PCMobile" if stats_camp_device_stat else ("AD_REPORT" if camp_device_stat else "STATS_TOTAL")
        result["device_status"] = "actual_pc_mobile_ok_with_unsegmented_total" if fallback_cnt else (
            "actual_pc_mobile_ok" if (stats_ad_device_stat or stats_camp_device_stat) else (
                "ad_report_fallback_ok" if (ad_device_stat or camp_device_stat) else (
                    "unsegmented_total_fallback" if fallback_cnt else "no_device_rows"
                )
            )
        )
        log_fn(
            f"   ✅ [ {account_name} ] PC/M 실제 성과 저장 완료: 캠페인({device_campaign_cnt}) | 소재({device_ad_cnt}) "
            f"| source={source_report} | stats_rows=캠페인 {result.get('device_stats_campaign_rows', 0)}, 소재 {result.get('device_stats_ad_rows', 0)} "
            f"| parser={device_parser_version}"
        )
    else:
        result["device_status"] = "not_requested"

    media_cnt = 0
    media_meta = {"status": "disabled", "reason": "media_collection_removed"}
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
    ad_to_campaign_map: Dict[str, str] = {}
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
        shopping_campaign_ids = {
            str(r[0]) for r in conn.execute(
                text(
                    """
                    SELECT campaign_id
                    FROM dim_campaign
                    WHERE customer_id = :cid
                      AND (
                        lower(coalesce(campaign_tp,'')) LIKE :shopping_kw
                        OR lower(coalesce(campaign_tp,'')) LIKE :shop_kw
                        OR coalesce(campaign_tp,'') LIKE :shopping_ko
                      )
                    """
                ),
                {
                    "cid": customer_id,
                    "shopping_kw": "%shopping%",
                    "shop_kw": "%shop%",
                    "shopping_ko": "%쇼핑%",
                },
            )
        }
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
    campaign_type_map: Dict[str, str] = {}
    shopping_campaign_ids: set[str] = set()
    shopping_adgroup_ids: set[str] = set()
    shopping_keyword_ids: set[str] = set()

    camps = list_campaigns_fn(customer_id) or []
    for c in camps:
        cid = str(c.get("nccCampaignId") or "").strip()
        if not cid:
            continue
        campaign_type_map[cid] = str(c.get("campaignTp", "") or "")
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
                        ad_to_campaign_map[adid] = cid

    return {
        "target_camp_ids": sorted(set(target_camp_ids)),
        "target_kw_ids": sorted(set(target_kw_ids)),
        "target_ad_ids": sorted(set(target_ad_ids)),
        "ad_to_campaign_map": ad_to_campaign_map,
        "campaign_type_map": campaign_type_map,
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
    collect_device: bool,
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
        if collect_device:
            report_types.extend(["CRITERION", "CRITERION_CONVERSION"])
        if split_enabled_for_date_fn(target_date) and collect_sa:
            split_candidate_reports = ["AD_CONVERSION", "CRITERION_CONVERSION"]
            if shopping_campaign_ids:
                split_candidate_reports.append("SHOPPINGKEYWORD_CONVERSION_DETAIL")
            for _tp in split_candidate_reports:
                if _tp not in report_types:
                    report_types.append(_tp)
            split_attempted = bool(collect_sa)
        dfs = fetch_multiple_stat_reports_fn(customer_id, report_types, target_date)
        result["ad_report_status"], result["ad_report_rows"] = df_state_fn(dfs.get("AD"))
        result["criterion_report_status"], result["criterion_report_rows"] = df_state_fn(dfs.get("CRITERION")) if "CRITERION" in report_types else ("not_requested", 0)
        result["criterion_conversion_status"], result["criterion_conversion_rows"] = df_state_fn(dfs.get("CRITERION_CONVERSION")) if "CRITERION_CONVERSION" in report_types else ("not_requested", 0)
        ad_conv_df = dfs.get("AD_CONVERSION") if "AD_CONVERSION" in report_types else None
        shop_kw_conv_df = dfs.get("SHOPPINGKEYWORD_CONVERSION_DETAIL") if "SHOPPINGKEYWORD_CONVERSION_DETAIL" in report_types else None
        result["ad_conversion_status"], result["ad_conversion_rows"] = df_state_fn(ad_conv_df) if split_candidate_reports else ("not_requested", 0)
        result["shopping_keyword_conversion_status"], result["shopping_keyword_conversion_rows"] = df_state_fn(shop_kw_conv_df) if split_candidate_reports else ("not_requested", 0)

        required_fallback_reports = ["AD"] + split_candidate_reports
        if collect_device:
            required_fallback_reports.extend(["CRITERION", "CRITERION_CONVERSION"])
        if all(dfs.get(tp) is None for tp in required_fallback_reports):
            log_fn(f"   ⚠️ [ {account_name} ] AD / 전환 / CRITERION 리포트가 모두 실패 → 실시간 stats 총합으로 대체합니다. (purchase/cart 및 PC/M 미분리)")
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
    build_adgroup_to_campaign_map_fn: Callable[..., Dict[str, str]] | None,
    build_campaign_type_map_fn: Callable[..., Dict[str, str]],
    prepare_account_report_fetch_plan_fn: Callable[..., Tuple[Dict[str, Any], List[str], bool, bool, str]],
    scope_enabled_collectors_fn: Callable[[str, bool], tuple[bool, bool, bool]],
    fetch_stats_fallback_fn: Callable[..., int],
    # collector.py has passed this dependency since the PC/M actual-performance patch.
    # The actual call path uses the wrapped save_report_stats_and_breakdowns_fn,
    # but accepting the keyword here keeps the public runner contract stable.
    get_stats_breakdown_range_fn: Callable[..., List[dict]] | None = None,
    clear_fact_scope_fn: Callable[..., None],
    save_device_stats_fn: Callable[..., int],
    build_unsegmented_device_stat_from_totals_fn: Callable[..., Dict[Tuple[str, str], dict]],
    collect_media_fact_fn: Callable[..., Tuple[int, Dict[str, Any]]],
    collect_time_age_stats_fn: Callable[..., Dict[str, Any]] | None = None,
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
        time_age_meta: Dict[str, Any] = {}
        media_cnt = 0
        media_meta: Dict[str, Any] = {}
        shop_query_rows: List[Dict[str, Any]] = []
        split_report_ok = False
        live_campaign_type_map: Dict[str, str] = {}
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

        live_ad_to_campaign_map: Dict[str, str] = {}
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
                live_ad_to_campaign_map = live_bundle.get("ad_to_campaign_map", {})
                live_campaign_type_map = live_bundle.get("campaign_type_map", {})
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
        if live_ad_to_campaign_map:
            ad_to_campaign_map.update(live_ad_to_campaign_map)
        adgroup_to_campaign_map = build_adgroup_to_campaign_map_fn(engine, customer_id) if callable(build_adgroup_to_campaign_map_fn) else {}
        campaign_type_map = build_campaign_type_map_fn(engine, customer_id)
        if live_campaign_type_map:
            campaign_type_map.update(live_campaign_type_map)

        stage = "fetch_reports"
        result["stage"] = stage
        dfs, split_candidate_reports, split_attempted, use_realtime_fallback, realtime_reason = prepare_account_report_fetch_plan_fn(
            customer_id=customer_id,
            account_name=account_name,
            target_date=target_date,
            collect_sa=collect_sa,
            collect_device=collect_device,
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
            if collect_device:
                device_ad_cnt, device_campaign_cnt, fallback_cnt = _save_device_totals_with_unsegmented_fallback(
                    engine,
                    customer_id=customer_id,
                    account_name=account_name,
                    target_date=target_date,
                    target_camp_ids=target_camp_ids,
                    target_ad_ids=target_ad_ids,
                    ad_device_stat={},
                    camp_device_stat={},
                    save_device_stats_fn=save_device_stats_fn,
                    build_unsegmented_device_stat_from_totals_fn=build_unsegmented_device_stat_from_totals_fn,
                    source_report="STATS_TOTAL_REALTIME",
                    log_fn=log_fn,
                )
                result["device_status"] = "realtime_unsegmented_total" if fallback_cnt else "realtime_skipped"
            else:
                device_ad_cnt = 0
                device_campaign_cnt = 0
                result["device_status"] = "not_applicable"

            # 시간대/연령대는 STATREPORT가 아니라 /stats breakdown 전용 데이터입니다.
            # collect_sa가 켜진 경우에만 캠페인 단위로 저장합니다.
            if collect_sa and callable(collect_time_age_stats_fn) and not is_ad_only_scope_fn(sa_scope):
                try:
                    time_age_meta = collect_time_age_stats_fn(
                        engine,
                        customer_id,
                        target_date,
                        campaign_ids=target_camp_ids,
                        shopping_campaign_ids=shopping_campaign_ids,
                        campaign_type_map=campaign_type_map,
                        shopping_only=shopping_only,
                    )
                    result["hour_rows_saved"] = int(time_age_meta.get("hour_rows_saved", 0) or 0)
                    result["age_rows_saved"] = int(time_age_meta.get("age_rows_saved", 0) or 0)
                    result["time_age_status"] = "ok"
                    log_fn(
                        f"   ✅ [ {account_name} ] 시간대/연령대 분리 저장 완료: "
                        f"시간대({result['hour_rows_saved']}) | 연령대({result['age_rows_saved']})"
                    )
                except Exception as e:
                    result["time_age_status"] = f"error:{type(e).__name__}"
                    log_best_effort_failure_fn("시간대/연령대 breakdown 수집", e, ctx=f"customer_id={customer_id}")
            else:
                result["time_age_status"] = "not_requested"
            media_cnt = 0
            media_meta = {"status": "disabled", "reason": "media_collection_removed"}
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
                adgroup_to_campaign_map=adgroup_to_campaign_map,
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
                criterion_report_df=dfs.get("CRITERION"),
                criterion_conversion_report_df=dfs.get("CRITERION_CONVERSION"),
                ad_to_campaign_map=ad_to_campaign_map,
                adgroup_to_campaign_map=adgroup_to_campaign_map,
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

            # 시간대/연령대는 STATREPORT가 아니라 /stats breakdown 전용 데이터입니다.
            # collect_sa가 켜진 경우에만 캠페인 단위로 저장합니다.
            if collect_sa and callable(collect_time_age_stats_fn) and not is_ad_only_scope_fn(sa_scope):
                try:
                    time_age_meta = collect_time_age_stats_fn(
                        engine,
                        customer_id,
                        target_date,
                        campaign_ids=target_camp_ids,
                        shopping_campaign_ids=shopping_campaign_ids,
                        campaign_type_map=campaign_type_map,
                        shopping_only=shopping_only,
                    )
                    result["hour_rows_saved"] = int(time_age_meta.get("hour_rows_saved", 0) or 0)
                    result["age_rows_saved"] = int(time_age_meta.get("age_rows_saved", 0) or 0)
                    result["time_age_status"] = "ok"
                    log_fn(
                        f"   ✅ [ {account_name} ] 시간대/연령대 분리 저장 완료: "
                        f"시간대({result['hour_rows_saved']}) | 연령대({result['age_rows_saved']})"
                    )
                except Exception as e:
                    result["time_age_status"] = f"error:{type(e).__name__}"
                    log_best_effort_failure_fn("시간대/연령대 breakdown 수집", e, ctx=f"customer_id={customer_id}")
            else:
                result["time_age_status"] = "not_requested"

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
                    keyword_rows_saved = int(result.get("keyword_rows_saved", k_cnt) or 0)
                    shopping_query_rows_saved = int(result.get("shopping_query_rows_saved", 0) or 0)
                    if keyword_rows_saved > 0 or shopping_query_rows_saved > 0:
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
