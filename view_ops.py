# -*- coding: utf-8 -*-
"""view_ops.py - Daily operations center for action items, sync status, and audit trail."""

from __future__ import annotations

import io
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import streamlit as st

from data import (
    format_currency,
    query_action_items,
    query_budget_bundle,
    query_campaign_bundle,
    query_campaign_off_log,
    query_collection_status,
    query_dashboard_audit_log,
    query_keyword_bundle,
    query_shopping_warning_source,
    update_action_item,
    upsert_action_items,
)
from ui import render_empty_state, render_kpi_strip, render_toolbar, safe_numeric_col


OPS_WARNING_THRESHOLDS = {
    "powerlink": {
        "rank_recent7_min_imp": 30,
        "rank_warning_drop": 1.0,
        "rank_danger_drop": 2.0,
        "cpc_min_clicks": 3,
        "cpc_warning_rise": 0.30,
        "cpc_danger_rise": 0.50,
        "click_avg_min": 3.0,
        "click_imp_keep_rate": 0.80,
        "click_warning_drop": 0.30,
        "click_danger_drop": 0.50,
        "conv_prev7_min": 2.0,
        "conv_warning_drop": 0.50,
        "waste_cost_warning": 50000,
        "waste_cost_danger": 100000,
        "no_imp_prev7_min": 30,
        "low_imp_recent7": 10,
    },
    "shopping": {
        "purchase_prev7_min": 3.0,
        "purchase_warning_drop": 0.30,
        "purchase_danger_drop": 0.50,
        "purchase_yesterday_avg_min": 1.0,
        "purchase_yesterday_warning_drop": 0.40,
        "purchase_yesterday_danger_drop": 0.70,
        "no_purchase_cost_warning": 50000,
        "no_purchase_cost_danger": 100000,
        "no_purchase_click_warning": 30,
        "no_purchase_click_danger": 50,
        "roas_recent_cost_min": 30000,
        "roas_prev_purchase_min": 3.0,
        "roas_warning_drop": 0.30,
        "roas_danger_drop": 0.50,
        "imp_prev7_min": 50,
        "imp_warning_drop": 0.50,
        "ctr_prev_click_min": 20,
        "ctr_warning_drop": 0.30,
        "click_danger_drop": 0.50,
        "imp_keep_rate": 0.80,
        "daily_purchase_base_min": 1.0,
        "daily_roas_cost_min": 10000,
        "daily_imp_base_min": 50,
        "daily_click_base_min": 10,
    },
}

SEVERITY_ORDER = {"danger": 0, "warning": 1, "info": 2}
WARNING_DISPLAY_COLS = [
    "심각도",
    "유형",
    "캠페인명",
    "광고그룹명",
    "키워드 / 검색어 / 상품명",
    "경고명",
    "경고 사유",
    "전일 수치",
    "최근 7일 평균",
    "최근 7일 합계",
    "이전 7일 합계",
    "증감률",
    "비용",
    "클릭수",
    "전환수 또는 구매완료수",
    "ROAS",
    "권장 조치",
]


def _today_kst() -> date:
    try:
        return datetime.now(ZoneInfo("Asia/Seoul")).date()
    except Exception:
        return date.today()


def _cid_text(value) -> str:
    raw = str(value or "").strip()
    if raw.endswith(".0") and raw.replace(".", "", 1).isdigit():
        raw = raw[:-2]
    return raw


def _meta_context(meta: pd.DataFrame) -> dict[str, dict]:
    if meta is None or meta.empty or "customer_id" not in meta.columns:
        return {}
    work = meta.copy()
    work["_cid"] = work["customer_id"].map(_cid_text)
    out = {}
    for _, row in work.drop_duplicates("_cid").iterrows():
        cid = str(row.get("_cid", "") or "").strip()
        if not cid:
            continue
        out[cid] = {
            "account_name": str(row.get("account_name", "") or row.get("업체명", "") or cid),
            "manager": str(row.get("manager", "") or row.get("담당자", "") or "미배정"),
        }
    return out


def _row_context(row, meta_map: dict[str, dict]) -> dict:
    cid = _cid_text(row.get("customer_id", ""))
    ctx = dict(meta_map.get(cid, {}))
    ctx.setdefault("account_name", str(row.get("account_name", "") or cid))
    ctx.setdefault("manager", str(row.get("manager", "") or "미배정"))
    ctx["customer_id"] = cid
    return ctx


def _fmt_num(value) -> str:
    try:
        return f"{int(round(float(value or 0))):,}"
    except Exception:
        return "0"


def _fmt_pct(value) -> str:
    try:
        return f"{float(value or 0):,.1f}%"
    except Exception:
        return "0.0%"


def _add_action(actions: list[dict], *, item_key: str, category: str, severity: str, title: str, body: str, context: dict, source_page: str, source_ref: str = "") -> None:
    actions.append(
        {
            "item_key": item_key,
            "category": category,
            "severity": severity,
            "title": title,
            "body": body,
            "manager": context.get("manager", ""),
            "account_name": context.get("account_name", ""),
            "customer_id": context.get("customer_id", ""),
            "source_page": source_page,
            "source_ref": source_ref,
        }
    )


def _budget_ranges(f: dict) -> tuple[date, date, date, date, date, date, date, int]:
    today = _today_kst()
    yesterday = min(f.get("end") or today - timedelta(days=1), today - timedelta(days=1))
    avg_days = 7
    avg_d2 = yesterday
    avg_d1 = avg_d2 - timedelta(days=avg_days - 1)
    month_d1 = date(yesterday.year, yesterday.month, 1)
    month_d2 = yesterday
    prev_month_d2 = month_d1 - timedelta(days=1)
    prev_month_d1 = date(prev_month_d2.year, prev_month_d2.month, 1)
    return yesterday, avg_d1, avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, avg_days


def _build_budget_actions(meta: pd.DataFrame, engine, f: dict) -> list[dict]:
    cids = tuple(f.get("customer_ids") or [])
    if not cids:
        return []
    try:
        df = query_budget_bundle(engine, cids, *_budget_ranges(f))
    except Exception:
        return []
    if df is None or df.empty:
        return []

    meta_map = _meta_context(meta)
    work = df.copy()
    work["monthly_budget_num"] = safe_numeric_col(work, "monthly_budget")
    work["month_cost_num"] = safe_numeric_col(work, "current_month_cost")
    work["avg_cost_num"] = safe_numeric_col(work, "avg_cost")
    work["bizmoney_balance_num"] = safe_numeric_col(work, "bizmoney_balance")

    today = _today_kst()
    month_end = date(today.year + int(today.month == 12), 1 if today.month == 12 else today.month + 1, 1) - timedelta(days=1)
    elapsed_days = max(1, min(today.day, month_end.day))
    month_days = month_end.day
    work["projected_cost"] = work["month_cost_num"] / elapsed_days * month_days
    budget_base = work["monthly_budget_num"].where(work["monthly_budget_num"] != 0)
    avg_cost_base = work["avg_cost_num"].where(work["avg_cost_num"] != 0)
    work["budget_use_rate"] = work["projected_cost"] / budget_base
    work["balance_cover_days"] = work["bizmoney_balance_num"] / avg_cost_base

    actions = []
    budget_hits = work[(work["monthly_budget_num"] > 0) & (work["budget_use_rate"] >= 1.05)].copy()
    for _, row in budget_hits.sort_values("budget_use_rate", ascending=False).head(20).iterrows():
        ctx = _row_context(row, meta_map)
        severity = "danger" if float(row.get("budget_use_rate") or 0) >= 1.2 else "warning"
        _add_action(
            actions,
            item_key=f"budget_projection:{ctx['customer_id']}",
            category="예산",
            severity=severity,
            title=f"월 예산 초과 예상: {ctx['account_name']}",
            body=(
                f"예상 광고비 {format_currency(row.get('projected_cost'))} / "
                f"월 예산 {format_currency(row.get('monthly_budget_num'))} "
                f"({_fmt_pct((row.get('budget_use_rate') or 0) * 100)})"
            ),
            context=ctx,
            source_page="예산 및 잔액",
            source_ref=ctx["customer_id"],
        )

    balance_hits = work[(work["avg_cost_num"] > 0) & (work["balance_cover_days"].fillna(999) <= 2.0)].copy()
    for _, row in balance_hits.sort_values("balance_cover_days", ascending=True).head(20).iterrows():
        ctx = _row_context(row, meta_map)
        cover_days = float(row.get("balance_cover_days") or 0)
        severity = "danger" if cover_days <= 1.0 else "warning"
        _add_action(
            actions,
            item_key=f"bizmoney_low:{ctx['customer_id']}",
            category="잔액",
            severity=severity,
            title=f"비즈머니 충전 필요: {ctx['account_name']}",
            body=f"잔액 {format_currency(row.get('bizmoney_balance_num'))}, 최근 평균 지출 기준 약 {cover_days:.1f}일치 남았습니다.",
            context=ctx,
            source_page="예산 및 잔액",
            source_ref=ctx["customer_id"],
        )
    return actions


def _build_campaign_actions(meta: pd.DataFrame, engine, f: dict) -> list[dict]:
    try:
        df = query_campaign_bundle(
            engine,
            f.get("start"),
            f.get("end"),
            tuple(f.get("customer_ids") or []),
            tuple(f.get("type_sel") or []),
            int(f.get("top_n_campaign", 200) or 200),
        )
    except Exception:
        return []
    if df is None or df.empty:
        return []

    meta_map = _meta_context(meta)
    work = df.copy()
    work["cost_num"] = safe_numeric_col(work, "cost")
    work["sales_num"] = safe_numeric_col(work, "sales")
    work["target_roas_num"] = safe_numeric_col(work, "target_roas")
    work["min_roas_num"] = safe_numeric_col(work, "min_roas")
    cost_base = work["cost_num"].where(work["cost_num"] != 0)
    work["roas_num"] = work["sales_num"] / cost_base * 100
    cost_floor = max(50000, float(work["cost_num"].quantile(0.75) or 0))

    target_hits = work[
        (work["cost_num"] >= cost_floor)
        & (
            ((work["min_roas_num"] > 0) & (work["roas_num"].fillna(0) < work["min_roas_num"]))
            | ((work["target_roas_num"] > 0) & (work["roas_num"].fillna(0) < work["target_roas_num"] * 0.8))
        )
    ].copy()

    actions = []
    for _, row in target_hits.sort_values("cost_num", ascending=False).head(30).iterrows():
        ctx = _row_context(row, meta_map)
        campaign_name = str(row.get("campaign_name", "") or row.get("campaign_id", "캠페인"))
        severity = "danger" if float(row.get("min_roas_num") or 0) > 0 and float(row.get("roas_num") or 0) < float(row.get("min_roas_num") or 0) else "warning"
        _add_action(
            actions,
            item_key=f"campaign_roas:{ctx['customer_id']}:{row.get('campaign_id')}",
            category="캠페인",
            severity=severity,
            title=f"목표 ROAS 미달: {campaign_name}",
            body=(
                f"광고비 {format_currency(row.get('cost_num'))}, 현재 ROAS {_fmt_pct(row.get('roas_num'))}, "
                f"목표 {_fmt_pct(row.get('target_roas_num'))}, 최소 {_fmt_pct(row.get('min_roas_num'))}"
            ),
            context=ctx,
            source_page="성과 분석 · 캠페인",
            source_ref=str(row.get("campaign_id", "")),
        )

    zero_sales = work[(work["cost_num"] >= cost_floor) & (work["sales_num"] <= 0)].copy()
    for _, row in zero_sales.sort_values("cost_num", ascending=False).head(20).iterrows():
        ctx = _row_context(row, meta_map)
        campaign_name = str(row.get("campaign_name", "") or row.get("campaign_id", "캠페인"))
        _add_action(
            actions,
            item_key=f"campaign_no_sales:{ctx['customer_id']}:{row.get('campaign_id')}",
            category="캠페인",
            severity="warning",
            title=f"매출 없는 고비용 캠페인: {campaign_name}",
            body=f"조회 기간 광고비 {format_currency(row.get('cost_num'))}이지만 구매완료 매출이 없습니다.",
            context=ctx,
            source_page="성과 분석 · 캠페인",
            source_ref=str(row.get("campaign_id", "")),
        )
    return actions


def _build_keyword_actions(meta: pd.DataFrame, engine, f: dict) -> list[dict]:
    try:
        df = query_keyword_bundle(
            engine,
            f.get("start"),
            f.get("end"),
            tuple(f.get("customer_ids") or []),
            tuple(f.get("type_sel") or []),
            int(f.get("top_n_keyword", 300) or 300),
        )
    except Exception:
        return []
    if df is None or df.empty:
        return []

    meta_map = _meta_context(meta)
    work = df.copy()
    work["cost_num"] = safe_numeric_col(work, "cost")
    work["clk_num"] = safe_numeric_col(work, "clk")
    conv_col = "tot_conv" if "tot_conv" in work.columns else "conv"
    work["conv_num"] = safe_numeric_col(work, conv_col)
    cost_floor = max(30000, float(work["cost_num"].quantile(0.8) or 0))
    hits = work[(work["cost_num"] >= cost_floor) & (work["clk_num"] >= 20) & (work["conv_num"] <= 0)].copy()

    actions = []
    for _, row in hits.sort_values("cost_num", ascending=False).head(30).iterrows():
        ctx = _row_context(row, meta_map)
        keyword = str(row.get("keyword", "") or row.get("keyword_id", "키워드"))
        _add_action(
            actions,
            item_key=f"keyword_waste:{ctx['customer_id']}:{row.get('keyword_id')}",
            category="키워드",
            severity="warning",
            title=f"전환 없는 고비용 키워드: {keyword}",
            body=f"클릭 {_fmt_num(row.get('clk_num'))}회, 광고비 {format_currency(row.get('cost_num'))}, 전환 0건입니다.",
            context=ctx,
            source_page="성과 분석 · 키워드",
            source_ref=str(row.get("keyword_id", "")),
        )
    return actions


def _build_off_log_actions(meta: pd.DataFrame, engine, f: dict) -> list[dict]:
    try:
        df = query_campaign_off_log(engine, f.get("start"), f.get("end"), tuple(f.get("customer_ids") or []))
    except Exception:
        return []
    if df is None or df.empty:
        return []
    meta_map = _meta_context(meta)
    grouped = (
        df.assign(customer_id=df["customer_id"].map(_cid_text))
        .groupby(["customer_id", "campaign_id"], dropna=False)
        .agg(off_count=("off_time", "count"), last_off_time=("off_time", "max"))
        .reset_index()
    )
    actions = []
    for _, row in grouped.sort_values("off_count", ascending=False).head(30).iterrows():
        ctx = _row_context(row, meta_map)
        _add_action(
            actions,
            item_key=f"campaign_off:{ctx['customer_id']}:{row.get('campaign_id')}",
            category="수집/운영",
            severity="info",
            title=f"캠페인 OFF 이력 확인: {row.get('campaign_id')}",
            body=f"조회 기간 OFF 기록 {_fmt_num(row.get('off_count'))}건, 최근 시간 {row.get('last_off_time')}",
            context=ctx,
            source_page="성과 분석 · 캠페인",
            source_ref=str(row.get("campaign_id", "")),
        )
    return actions


def _ops_anchor_date(f: dict) -> date:
    today = _today_kst()
    end_value = f.get("end") or today - timedelta(days=1)
    if isinstance(end_value, datetime):
        end_value = end_value.date()
    return min(end_value, today - timedelta(days=1))


def _date_window(f: dict) -> dict[str, date]:
    anchor = _ops_anchor_date(f)
    return {
        "anchor": anchor,
        "last3_start": anchor - timedelta(days=2),
        "recent7_start": anchor - timedelta(days=6),
        "recent7_end": anchor,
        "prev7_start": anchor - timedelta(days=13),
        "prev7_end": anchor - timedelta(days=7),
    }


def _to_num(value) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _safe_rate(numerator: float, denominator: float) -> float:
    denominator = _to_num(denominator)
    if denominator <= 0:
        return 0.0
    return _to_num(numerator) / denominator


def _drop_rate(current: float, base: float) -> float:
    base = _to_num(base)
    if base <= 0:
        return 0.0
    return max(0.0, (base - _to_num(current)) / base)


def _rise_rate(current: float, base: float) -> float:
    base = _to_num(base)
    if base <= 0:
        return 0.0
    return max(0.0, (_to_num(current) - base) / base)


def _fmt_metric(value, suffix: str = "") -> str:
    value = _to_num(value)
    if suffix == "%":
        return f"{value:,.1f}%"
    if suffix == "원":
        return f"{int(round(value)):,}원"
    if float(value).is_integer():
        return f"{int(round(value)):,}{suffix}"
    return f"{value:,.1f}{suffix}"


def _fmt_signed_pct(value: float, *, negative: bool = True) -> str:
    pct = _to_num(value) * 100
    sign = "-" if negative and pct > 0 else ("+" if not negative and pct > 0 else "")
    return f"{sign}{abs(pct):,.1f}%"


def _recommend_action(warning_name: str) -> str:
    mapping = {
        "CPC 급등": "입찰가 및 평균 노출순위 변동 확인, 전환 없는 경우 입찰가 조정 검토",
        "순위 급락": "경쟁 입찰 상승 여부 및 핵심 키워드 목표 순위 재점검",
        "클릭 급감": "소재 문구, 노출순위, 검색량 변화 확인",
        "전환 급감": "랜딩페이지, 전환태그, 유입 검색어 품질 확인",
        "장기 미노출": "입찰가, 예산, 광고 상태, 키워드 검색량 확인",
        "노출 저조": "입찰가, 예산, 광고 상태, 키워드 검색량 확인",
        "비용 소진 무전환": "입찰가 감액, 제외키워드 추가, 검색어 품질 점검",
        "구매완료 급감": "상품 가격, 품절 여부, 리뷰, 경쟁 상품, 노출 순위 확인",
        "전일 구매완료 급감": "상품 가격, 품절 여부, 리뷰, 경쟁 상품, 노출 순위 확인",
        "비용 소진 무구매": "고비용 저효율 검색어/상품 우선 점검, 입찰가 감액 또는 제외 검토",
        "클릭 누적 무구매": "검색어 품질, 상품 매칭, 가격/배송 조건, 제외키워드 검토",
        "ROAS 급락": "고비용 저효율 검색어/상품 우선 점검",
        "노출 급감": "예산, 캠페인/소재 상태, 상품 노출 가능 여부 확인",
        "CTR 급감": "상품명/소재 문구, 가격 경쟁력, 리뷰, 노출순위 확인",
        "수집 지연": "수집 배치 실행 상태, API 권한, 최근 오류 로그 확인",
        "캠페인 OFF 이력": "캠페인 상태 변경 사유와 자동 OFF 조건 확인",
    }
    return mapping.get(warning_name, "성과 추이와 계정 설정을 확인한 뒤 조치 여부를 판단")


def _warning_row(
    *,
    severity: str,
    warning_type: str,
    campaign: str,
    adgroup: str,
    subject: str,
    warning_name: str,
    reason: str,
    yesterday_value: str = "-",
    recent_avg: str = "-",
    recent_sum: str = "-",
    prev_sum: str = "-",
    delta_rate: str = "-",
    cost: float = 0.0,
    clicks: float = 0.0,
    conversions: float = 0.0,
    roas: float = 0.0,
    period_filter: str = "최근 7일 vs 이전 7일",
    item_key: str = "",
    has_purchase: bool = False,
) -> dict:
    severity = str(severity or "info")
    return {
        "심각도": severity,
        "유형": warning_type,
        "캠페인명": campaign or "-",
        "광고그룹명": adgroup or "-",
        "키워드 / 검색어 / 상품명": subject or "-",
        "경고명": warning_name,
        "경고 사유": reason,
        "전일 수치": yesterday_value,
        "최근 7일 평균": recent_avg,
        "최근 7일 합계": recent_sum,
        "이전 7일 합계": prev_sum,
        "증감률": delta_rate,
        "비용": float(_to_num(cost)),
        "클릭수": float(_to_num(clicks)),
        "전환수 또는 구매완료수": float(_to_num(conversions)),
        "ROAS": float(_to_num(roas)),
        "권장 조치": _recommend_action(warning_name),
        "_severity_sort": SEVERITY_ORDER.get(severity, 9),
        "_cost_sort": float(_to_num(cost)),
        "_period_filter": period_filter,
        "_item_key": item_key or f"{warning_type}:{campaign}:{adgroup}:{subject}",
        "_has_conversion": float(_to_num(conversions)) > 0,
        "_has_purchase": bool(has_purchase),
    }


def _normalize_warning_source(df: pd.DataFrame, date_col: str = "dt") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if date_col in out.columns:
        out[date_col] = pd.to_datetime(out[date_col]).dt.date
    for col in ["imp", "clk", "cost", "conv", "tot_conv", "sales", "tot_sales", "purchase_conv", "purchase_sales", "total_conv", "total_sales", "avg_rank"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    for col in ["customer_id", "campaign_id", "adgroup_id", "keyword_id", "ad_id", "campaign_name", "adgroup_name", "keyword", "query_text", "ad_name", "ad_title"]:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str)
    return out


def _prefixed_agg(df: pd.DataFrame, keys: list[str], mask: pd.Series, prefix: str, conversion_col: str, sales_col: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=keys)
    usable_keys = [k for k in keys if k in df.columns]
    part = df.loc[mask].copy()
    if part.empty or not usable_keys:
        return pd.DataFrame(columns=usable_keys)
    for col in ["imp", "clk", "cost", conversion_col, sales_col, "avg_rank", "purchase_conv", "purchase_sales", "total_conv", "total_sales"]:
        if col in part.columns:
            part[col] = pd.to_numeric(part[col], errors="coerce").fillna(0.0)
    agg_map = {}
    for col in ["imp", "clk", "cost"]:
        agg_map[col] = (col, "sum") if col in part.columns else (conversion_col, lambda _: 0.0)
    agg_map["conv"] = (conversion_col, "sum") if conversion_col in part.columns else ("imp", lambda _: 0.0)
    agg_map["sales"] = (sales_col, "sum") if sales_col in part.columns else ("imp", lambda _: 0.0)
    if "purchase_conv" in part.columns:
        agg_map["purchase_conv"] = ("purchase_conv", "sum")
    if "purchase_sales" in part.columns:
        agg_map["purchase_sales"] = ("purchase_sales", "sum")
    if "total_conv" in part.columns:
        agg_map["total_conv"] = ("total_conv", "sum")
    if "total_sales" in part.columns:
        agg_map["total_sales"] = ("total_sales", "sum")
    grouped = part.groupby(usable_keys, as_index=False, dropna=False).agg(**agg_map)
    if "avg_rank" in part.columns:
        rank_part = part.copy()
        rank_part["_rank_weight"] = rank_part["avg_rank"] * rank_part.get("imp", 0)
        rank = rank_part.groupby(usable_keys, as_index=False, dropna=False).agg(_rank_weight=("_rank_weight", "sum"), _rank_imp=("imp", "sum"))
        rank["avg_rank"] = np.where(rank["_rank_imp"] > 0, rank["_rank_weight"] / rank["_rank_imp"], np.nan)
        grouped = grouped.merge(rank[usable_keys + ["avg_rank"]], on=usable_keys, how="left")
    metric_cols = [c for c in grouped.columns if c not in usable_keys]
    return grouped.rename(columns={c: f"{prefix}_{c}" for c in metric_cols})


def _merge_periods(df: pd.DataFrame, keys: list[str], windows: dict[str, date], conversion_col: str, sales_col: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=keys)
    dt = df["dt"]
    frames = [
        _prefixed_agg(df, keys, dt == windows["anchor"], "y", conversion_col, sales_col),
        _prefixed_agg(df, keys, (dt >= windows["recent7_start"]) & (dt <= windows["recent7_end"]), "r7", conversion_col, sales_col),
        _prefixed_agg(df, keys, (dt >= windows["prev7_start"]) & (dt <= windows["prev7_end"]), "p7", conversion_col, sales_col),
        _prefixed_agg(df, keys, (dt >= windows["last3_start"]) & (dt <= windows["anchor"]), "r3", conversion_col, sales_col),
    ]
    base = df[[k for k in keys if k in df.columns]].drop_duplicates().copy()
    for frame in frames:
        if frame is not None and not frame.empty:
            base = base.merge(frame, on=[k for k in keys if k in frame.columns], how="left")
    for col in base.columns:
        if any(col.startswith(prefix) for prefix in ["y_", "r7_", "p7_", "r3_"]):
            base[col] = pd.to_numeric(base[col], errors="coerce").fillna(0.0)
    return base


def _merge_pair_periods(df: pd.DataFrame, keys: list[str], target_date: date, compare_date: date, conversion_col: str, sales_col: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=keys)
    dt = df["dt"]
    frames = [
        _prefixed_agg(df, keys, dt == target_date, "target", conversion_col, sales_col),
        _prefixed_agg(df, keys, dt == compare_date, "base", conversion_col, sales_col),
    ]
    base = df[[k for k in keys if k in df.columns]].drop_duplicates().copy()
    for frame in frames:
        if frame is not None and not frame.empty:
            base = base.merge(frame, on=[k for k in keys if k in frame.columns], how="left")
    for col in base.columns:
        if col.startswith("target_") or col.startswith("base_"):
            base[col] = pd.to_numeric(base[col], errors="coerce").fillna(0.0)
    return base


def _build_powerlink_warning_rows(meta: pd.DataFrame, engine, f: dict) -> pd.DataFrame:
    cids = tuple(f.get("customer_ids") or [])
    if not cids:
        return pd.DataFrame()
    windows = _date_window(f)
    try:
        source = query_keyword_bundle(
            engine,
            windows["prev7_start"],
            windows["recent7_end"],
            cids,
            ("파워링크",),
            topn_cost=-1,
            include_dt=True,
        )
    except Exception:
        return pd.DataFrame()
    source = _normalize_warning_source(source)
    if source.empty:
        return pd.DataFrame()

    conv_col = "tot_conv" if "tot_conv" in source.columns else "conv"
    sales_col = "tot_sales" if "tot_sales" in source.columns else "sales"
    keys = ["customer_id", "campaign_id", "adgroup_id", "keyword_id", "campaign_name", "adgroup_name", "keyword"]
    work = _merge_periods(source, keys, windows, conv_col, sales_col)
    th = OPS_WARNING_THRESHOLDS["powerlink"]
    rows = []
    for _, row in work.iterrows():
        campaign = str(row.get("campaign_name", "") or row.get("campaign_id", ""))
        adgroup = str(row.get("adgroup_name", "") or row.get("adgroup_id", ""))
        keyword = str(row.get("keyword", "") or row.get("keyword_id", ""))
        item_key = f"powerlink:{row.get('customer_id')}:{row.get('keyword_id')}"
        r7_imp, r7_clk, r7_cost, r7_conv, r7_sales = [_to_num(row.get(f"r7_{c}")) for c in ["imp", "clk", "cost", "conv", "sales"]]
        p7_imp, p7_clk, p7_conv = [_to_num(row.get(f"p7_{c}")) for c in ["imp", "clk", "conv"]]
        y_imp, y_clk, y_cost, y_conv = [_to_num(row.get(f"y_{c}")) for c in ["imp", "clk", "cost", "conv"]]
        r3_imp = _to_num(row.get("r3_imp"))
        r7_avg_imp, r7_avg_clk = r7_imp / 7.0, r7_clk / 7.0
        r7_cpc = _safe_rate(r7_cost, r7_clk)
        y_cpc = _safe_rate(y_cost, y_clk)
        r7_roas = _safe_rate(r7_sales, r7_cost) * 100

        rank_drop = _to_num(row.get("y_avg_rank")) - _to_num(row.get("r7_avg_rank"))
        if r7_imp >= th["rank_recent7_min_imp"] and _to_num(row.get("y_avg_rank")) > 0 and _to_num(row.get("r7_avg_rank")) > 0 and rank_drop >= th["rank_warning_drop"]:
            severity = "danger" if rank_drop >= th["rank_danger_drop"] else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="파워링크",
                campaign=campaign,
                adgroup=adgroup,
                subject=keyword,
                warning_name="순위 급락",
                reason=f"전일 평균순위가 최근 7일 평균 대비 {rank_drop:.1f}위 하락했습니다.",
                yesterday_value=_fmt_metric(row.get("y_avg_rank"), "위"),
                recent_avg=_fmt_metric(row.get("r7_avg_rank"), "위"),
                recent_sum=_fmt_metric(r7_imp, "회 노출"),
                prev_sum=_fmt_metric(p7_imp, "회 노출"),
                delta_rate=f"{rank_drop:+.1f}위",
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_conv,
                roas=r7_roas,
                period_filter="전일 vs 최근 7일 평균",
                item_key=item_key,
            ))

        cpc_rise = _rise_rate(y_cpc, r7_cpc)
        if y_clk >= th["cpc_min_clicks"] and r7_clk >= th["cpc_min_clicks"] and cpc_rise >= th["cpc_warning_rise"]:
            severity = "danger" if cpc_rise >= th["cpc_danger_rise"] or y_conv <= 0 else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="파워링크",
                campaign=campaign,
                adgroup=adgroup,
                subject=keyword,
                warning_name="CPC 급등",
                reason=f"전일 CPC가 최근 7일 평균 대비 {_fmt_signed_pct(cpc_rise, negative=False)} 상승했습니다.",
                yesterday_value=_fmt_metric(y_cpc, "원"),
                recent_avg=_fmt_metric(r7_cpc, "원"),
                recent_sum=_fmt_metric(r7_cost, "원"),
                prev_sum="-",
                delta_rate=_fmt_signed_pct(cpc_rise, negative=False),
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_conv,
                roas=r7_roas,
                period_filter="전일 vs 최근 7일 평균",
                item_key=item_key,
            ))

        click_drop = _drop_rate(y_clk, r7_avg_clk)
        if r7_avg_clk >= th["click_avg_min"] and y_imp >= r7_avg_imp * th["click_imp_keep_rate"] and click_drop >= th["click_warning_drop"]:
            severity = "danger" if click_drop >= th["click_danger_drop"] else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="파워링크",
                campaign=campaign,
                adgroup=adgroup,
                subject=keyword,
                warning_name="클릭 급감",
                reason=f"노출은 유지됐지만 전일 클릭수가 최근 7일 평균 대비 {_fmt_signed_pct(click_drop)} 감소했습니다.",
                yesterday_value=_fmt_metric(y_clk, "회"),
                recent_avg=_fmt_metric(r7_avg_clk, "회"),
                recent_sum=_fmt_metric(r7_clk, "회"),
                prev_sum=_fmt_metric(p7_clk, "회"),
                delta_rate=_fmt_signed_pct(click_drop),
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_conv,
                roas=r7_roas,
                period_filter="전일 vs 최근 7일 평균",
                item_key=item_key,
            ))

        conv_drop = _drop_rate(r7_conv, p7_conv)
        if p7_conv >= th["conv_prev7_min"] and (conv_drop >= th["conv_warning_drop"] or r7_conv <= 0):
            severity = "danger" if r7_conv <= 0 else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="파워링크",
                campaign=campaign,
                adgroup=adgroup,
                subject=keyword,
                warning_name="전환 급감",
                reason=f"최근 7일 전환수가 이전 7일 대비 {_fmt_signed_pct(conv_drop)} 감소했습니다.",
                yesterday_value=_fmt_metric(y_conv, "건"),
                recent_avg=_fmt_metric(r7_conv / 7.0, "건"),
                recent_sum=_fmt_metric(r7_conv, "건"),
                prev_sum=_fmt_metric(p7_conv, "건"),
                delta_rate=_fmt_signed_pct(conv_drop),
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_conv,
                roas=r7_roas,
                period_filter="최근 7일 vs 이전 7일",
                item_key=item_key,
            ))

        if r7_conv <= 0 and r7_cost >= th["waste_cost_warning"]:
            severity = "danger" if r7_cost >= th["waste_cost_danger"] else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="파워링크",
                campaign=campaign,
                adgroup=adgroup,
                subject=keyword,
                warning_name="비용 소진 무전환",
                reason=f"최근 7일 비용 {_fmt_metric(r7_cost, '원')}을 소진했지만 전환이 없습니다.",
                yesterday_value=_fmt_metric(y_cost, "원"),
                recent_avg=_fmt_metric(r7_cost / 7.0, "원"),
                recent_sum=_fmt_metric(r7_cost, "원"),
                prev_sum="-",
                delta_rate="-",
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_conv,
                roas=r7_roas,
                period_filter="최근 상태",
                item_key=item_key,
            ))

        active_before = p7_imp >= th["no_imp_prev7_min"] or p7_clk >= th["cpc_min_clicks"]
        if active_before:
            if r7_imp <= 0:
                severity, warning_name, reason = "danger", "장기 미노출", "최근 7일 노출수가 0회입니다."
            elif r3_imp <= 0:
                severity, warning_name, reason = "warning", "장기 미노출", "최근 3일 노출수가 0회입니다."
            elif r7_imp < th["low_imp_recent7"]:
                severity, warning_name, reason = "info", "노출 저조", f"최근 7일 노출수가 {_fmt_metric(r7_imp, '회')}로 낮습니다."
            else:
                severity = ""
            if severity:
                rows.append(_warning_row(
                    severity=severity,
                    warning_type="파워링크",
                    campaign=campaign,
                    adgroup=adgroup,
                    subject=keyword,
                    warning_name=warning_name,
                    reason=reason,
                    yesterday_value=_fmt_metric(y_imp, "회 노출"),
                    recent_avg=_fmt_metric(r7_avg_imp, "회"),
                    recent_sum=_fmt_metric(r7_imp, "회"),
                    prev_sum=_fmt_metric(p7_imp, "회"),
                    delta_rate="-",
                    cost=r7_cost,
                    clicks=r7_clk,
                    conversions=r7_conv,
                    roas=r7_roas,
                    period_filter="최근 상태",
                    item_key=item_key,
                ))
    return pd.DataFrame(rows)


def _shopping_subject(row) -> str:
    query_text = str(row.get("query_text", "") or "").strip()
    product = str(row.get("ad_title", "") or row.get("ad_name", "") or "").strip()
    if query_text in {"", "-", "(검색어 미제공 영역)"}:
        return product or "(검색어 미제공 영역)"
    if product and product != query_text:
        return f"{query_text} / {product}"
    return query_text


def _build_shopping_pair_warning_rows(meta: pd.DataFrame, engine, f: dict, compare_config: dict) -> pd.DataFrame:
    cids = tuple(f.get("customer_ids") or [])
    if not cids:
        return pd.DataFrame()
    target_date = compare_config.get("target_date")
    compare_date = compare_config.get("compare_date")
    if not isinstance(target_date, date) or not isinstance(compare_date, date):
        return pd.DataFrame()
    max_ready_date = _today_kst() - timedelta(days=1)
    target_date = min(target_date, max_ready_date)
    compare_date = min(compare_date, max_ready_date)
    if target_date == compare_date:
        return pd.DataFrame()
    d1, d2 = sorted([target_date, compare_date])
    try:
        source = query_shopping_warning_source(engine, d1, d2, cids)
    except Exception:
        return pd.DataFrame()
    source = _normalize_warning_source(source)
    if source.empty:
        return pd.DataFrame()

    keys = ["customer_id", "campaign_id", "adgroup_id", "ad_id", "campaign_name", "adgroup_name", "ad_name", "ad_title", "query_text"]
    work = _merge_pair_periods(source, keys, target_date, compare_date, "purchase_conv", "purchase_sales")
    th = OPS_WARNING_THRESHOLDS["shopping"]
    period_label = f"{target_date} vs {compare_date}"
    rows = []
    for _, row in work.iterrows():
        campaign = str(row.get("campaign_name", "") or row.get("campaign_id", ""))
        adgroup = str(row.get("adgroup_name", "") or row.get("adgroup_id", ""))
        subject = _shopping_subject(row)
        item_key = f"shopping_pair:{target_date}:{compare_date}:{row.get('customer_id')}:{row.get('ad_id')}:{row.get('query_text')}"
        target_imp, target_clk, target_cost = [_to_num(row.get(f"target_{c}")) for c in ["imp", "clk", "cost"]]
        base_imp, base_clk, base_cost = [_to_num(row.get(f"base_{c}")) for c in ["imp", "clk", "cost"]]
        target_purchase, base_purchase = _to_num(row.get("target_conv")), _to_num(row.get("base_conv"))
        target_sales, base_sales = _to_num(row.get("target_sales")), _to_num(row.get("base_sales"))
        target_roas = _safe_rate(target_sales, target_cost) * 100
        base_roas = _safe_rate(base_sales, base_cost) * 100

        purchase_drop = _drop_rate(target_purchase, base_purchase)
        if base_purchase >= th["daily_purchase_base_min"] and purchase_drop >= th["purchase_yesterday_warning_drop"]:
            severity = "danger" if purchase_drop >= th["purchase_yesterday_danger_drop"] else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="구매완료 급감",
                reason=f"기준일 구매완료수가 비교일 대비 {_fmt_signed_pct(purchase_drop)} 감소했습니다.",
                yesterday_value=f"{target_date}: {_fmt_metric(target_purchase, '건')}",
                recent_avg="-",
                recent_sum=f"{target_date}: {_fmt_metric(target_purchase, '건')}",
                prev_sum=f"{compare_date}: {_fmt_metric(base_purchase, '건')}",
                delta_rate=_fmt_signed_pct(purchase_drop),
                cost=target_cost,
                clicks=target_clk,
                conversions=target_purchase,
                roas=target_roas,
                period_filter=period_label,
                item_key=item_key,
                has_purchase=target_purchase > 0,
            ))

        if target_purchase <= 0 and target_cost >= th["no_purchase_cost_warning"]:
            severity = "danger" if target_cost >= th["no_purchase_cost_danger"] else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="비용 소진 무구매",
                reason=f"기준일 비용 {_fmt_metric(target_cost, '원')}을 소진했지만 구매완료가 없습니다.",
                yesterday_value=f"{target_date}: {_fmt_metric(target_cost, '원')}",
                recent_avg="-",
                recent_sum=f"{target_date}: {_fmt_metric(target_cost, '원')}",
                prev_sum=f"{compare_date}: {_fmt_metric(base_cost, '원')}",
                delta_rate="-",
                cost=target_cost,
                clicks=target_clk,
                conversions=target_purchase,
                roas=target_roas,
                period_filter=period_label,
                item_key=item_key,
                has_purchase=False,
            ))

        if target_purchase <= 0 and target_clk >= th["no_purchase_click_warning"]:
            severity = "danger" if target_clk >= th["no_purchase_click_danger"] else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="클릭 누적 무구매",
                reason=f"기준일 클릭수 {_fmt_metric(target_clk, '회')}가 누적됐지만 구매완료가 없습니다.",
                yesterday_value=f"{target_date}: {_fmt_metric(target_clk, '회')}",
                recent_avg="-",
                recent_sum=f"{target_date}: {_fmt_metric(target_clk, '회')}",
                prev_sum=f"{compare_date}: {_fmt_metric(base_clk, '회')}",
                delta_rate="-",
                cost=target_cost,
                clicks=target_clk,
                conversions=target_purchase,
                roas=target_roas,
                period_filter=period_label,
                item_key=item_key,
                has_purchase=False,
            ))

        roas_drop = _drop_rate(target_roas, base_roas)
        if base_roas > 0 and target_cost >= th["daily_roas_cost_min"] and base_purchase >= th["daily_purchase_base_min"] and roas_drop >= th["roas_warning_drop"]:
            severity = "danger" if roas_drop >= th["roas_danger_drop"] or (target_cost > base_cost and target_purchase < base_purchase) else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="ROAS 급락",
                reason=f"기준일 ROAS가 비교일 대비 {_fmt_signed_pct(roas_drop)} 하락했습니다.",
                yesterday_value=f"{target_date}: {_fmt_metric(target_roas, '%')}",
                recent_avg="-",
                recent_sum=f"{target_date}: {_fmt_metric(target_sales, '원 매출')}",
                prev_sum=f"{compare_date}: {_fmt_metric(base_sales, '원 매출')}",
                delta_rate=_fmt_signed_pct(roas_drop),
                cost=target_cost,
                clicks=target_clk,
                conversions=target_purchase,
                roas=target_roas,
                period_filter=period_label,
                item_key=item_key,
                has_purchase=target_purchase > 0,
            ))

        imp_drop = _drop_rate(target_imp, base_imp)
        if base_imp >= th["daily_imp_base_min"] and imp_drop >= th["imp_warning_drop"]:
            rows.append(_warning_row(
                severity="warning",
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="노출 급감",
                reason=f"기준일 노출수가 비교일 대비 {_fmt_signed_pct(imp_drop)} 감소했습니다.",
                yesterday_value=f"{target_date}: {_fmt_metric(target_imp, '회')}",
                recent_avg="-",
                recent_sum=f"{target_date}: {_fmt_metric(target_imp, '회')}",
                prev_sum=f"{compare_date}: {_fmt_metric(base_imp, '회')}",
                delta_rate=_fmt_signed_pct(imp_drop),
                cost=target_cost,
                clicks=target_clk,
                conversions=target_purchase,
                roas=target_roas,
                period_filter=period_label,
                item_key=item_key,
                has_purchase=target_purchase > 0,
            ))

        target_ctr = _safe_rate(target_clk, target_imp)
        base_ctr = _safe_rate(base_clk, base_imp)
        ctr_drop = _drop_rate(target_ctr, base_ctr)
        click_drop = _drop_rate(target_clk, base_clk)
        if base_clk >= th["daily_click_base_min"] and target_imp >= base_imp * th["imp_keep_rate"] and ctr_drop >= th["ctr_warning_drop"]:
            rows.append(_warning_row(
                severity="warning",
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="CTR 급감",
                reason=f"노출은 유지됐지만 기준일 CTR이 비교일 대비 {_fmt_signed_pct(ctr_drop)} 하락했습니다.",
                yesterday_value=f"{target_date}: {_fmt_metric(target_ctr * 100, '%')}",
                recent_avg="-",
                recent_sum=f"{target_date}: {_fmt_metric(target_clk, '회 클릭')}",
                prev_sum=f"{compare_date}: {_fmt_metric(base_clk, '회 클릭')}",
                delta_rate=_fmt_signed_pct(ctr_drop),
                cost=target_cost,
                clicks=target_clk,
                conversions=target_purchase,
                roas=target_roas,
                period_filter=period_label,
                item_key=item_key,
                has_purchase=target_purchase > 0,
            ))
        if base_clk >= th["daily_click_base_min"] and target_imp >= base_imp * th["imp_keep_rate"] and click_drop >= th["click_danger_drop"]:
            rows.append(_warning_row(
                severity="danger",
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="클릭 급감",
                reason=f"노출은 유지됐지만 기준일 클릭수가 비교일 대비 {_fmt_signed_pct(click_drop)} 감소했습니다.",
                yesterday_value=f"{target_date}: {_fmt_metric(target_clk, '회')}",
                recent_avg="-",
                recent_sum=f"{target_date}: {_fmt_metric(target_clk, '회')}",
                prev_sum=f"{compare_date}: {_fmt_metric(base_clk, '회')}",
                delta_rate=_fmt_signed_pct(click_drop),
                cost=target_cost,
                clicks=target_clk,
                conversions=target_purchase,
                roas=target_roas,
                period_filter=period_label,
                item_key=item_key,
                has_purchase=target_purchase > 0,
            ))
    return pd.DataFrame(rows)


def _build_shopping_warning_rows(meta: pd.DataFrame, engine, f: dict, compare_config: dict | None = None) -> pd.DataFrame:
    compare_config = compare_config or {}
    if compare_config.get("mode") in {"yesterday_vs_day_before", "custom_dates"}:
        return _build_shopping_pair_warning_rows(meta, engine, f, compare_config)

    cids = tuple(f.get("customer_ids") or [])
    if not cids:
        return pd.DataFrame()
    windows = _date_window(f)
    try:
        source = query_shopping_warning_source(engine, windows["prev7_start"], windows["recent7_end"], cids)
    except Exception:
        return pd.DataFrame()
    source = _normalize_warning_source(source)
    if source.empty:
        return pd.DataFrame()
    keys = ["customer_id", "campaign_id", "adgroup_id", "ad_id", "campaign_name", "adgroup_name", "ad_name", "ad_title", "query_text"]
    work = _merge_periods(source, keys, windows, "purchase_conv", "purchase_sales")
    th = OPS_WARNING_THRESHOLDS["shopping"]
    rows = []
    for _, row in work.iterrows():
        campaign = str(row.get("campaign_name", "") or row.get("campaign_id", ""))
        adgroup = str(row.get("adgroup_name", "") or row.get("adgroup_id", ""))
        subject = _shopping_subject(row)
        item_key = f"shopping:{row.get('customer_id')}:{row.get('ad_id')}:{row.get('query_text')}"
        r7_imp, r7_clk, r7_cost = [_to_num(row.get(f"r7_{c}")) for c in ["imp", "clk", "cost"]]
        p7_imp, p7_clk, p7_cost = [_to_num(row.get(f"p7_{c}")) for c in ["imp", "clk", "cost"]]
        y_purchase = _to_num(row.get("y_conv"))
        r7_purchase, p7_purchase = _to_num(row.get("r7_conv")), _to_num(row.get("p7_conv"))
        r7_sales, p7_sales = _to_num(row.get("r7_sales")), _to_num(row.get("p7_sales"))
        y_purchase_avg = r7_purchase / 7.0
        r7_roas = _safe_rate(r7_sales, r7_cost) * 100
        p7_roas = _safe_rate(p7_sales, p7_cost) * 100

        purchase_drop = _drop_rate(r7_purchase, p7_purchase)
        if p7_purchase >= th["purchase_prev7_min"] and purchase_drop >= th["purchase_warning_drop"]:
            severity = "danger" if purchase_drop >= th["purchase_danger_drop"] else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="구매완료 급감",
                reason=f"최근 7일 구매완료수가 이전 7일 대비 {_fmt_signed_pct(purchase_drop)} 감소했습니다.",
                yesterday_value=_fmt_metric(y_purchase, "건"),
                recent_avg=_fmt_metric(y_purchase_avg, "건"),
                recent_sum=_fmt_metric(r7_purchase, "건"),
                prev_sum=_fmt_metric(p7_purchase, "건"),
                delta_rate=_fmt_signed_pct(purchase_drop),
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_purchase,
                roas=r7_roas,
                period_filter="최근 7일 vs 이전 7일",
                item_key=item_key,
                has_purchase=r7_purchase > 0,
            ))

        y_purchase_drop = _drop_rate(y_purchase, y_purchase_avg)
        if y_purchase_avg >= th["purchase_yesterday_avg_min"] and y_purchase_drop >= th["purchase_yesterday_warning_drop"]:
            severity = "danger" if y_purchase_drop >= th["purchase_yesterday_danger_drop"] else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="전일 구매완료 급감",
                reason=f"전일 구매완료수가 최근 7일 평균 대비 {_fmt_signed_pct(y_purchase_drop)} 감소했습니다.",
                yesterday_value=_fmt_metric(y_purchase, "건"),
                recent_avg=_fmt_metric(y_purchase_avg, "건"),
                recent_sum=_fmt_metric(r7_purchase, "건"),
                prev_sum=_fmt_metric(p7_purchase, "건"),
                delta_rate=_fmt_signed_pct(y_purchase_drop),
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_purchase,
                roas=r7_roas,
                period_filter="전일 vs 최근 7일 평균",
                item_key=item_key,
                has_purchase=r7_purchase > 0,
            ))

        if r7_purchase <= 0 and r7_cost >= th["no_purchase_cost_warning"]:
            severity = "danger" if r7_cost >= th["no_purchase_cost_danger"] else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="비용 소진 무구매",
                reason=f"최근 7일 비용 {_fmt_metric(r7_cost, '원')}을 소진했지만 구매완료가 없습니다.",
                yesterday_value=_fmt_metric(_to_num(row.get("y_cost")), "원"),
                recent_avg=_fmt_metric(r7_cost / 7.0, "원"),
                recent_sum=_fmt_metric(r7_cost, "원"),
                prev_sum="-",
                delta_rate="-",
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_purchase,
                roas=r7_roas,
                period_filter="최근 상태",
                item_key=item_key,
                has_purchase=False,
            ))

        if r7_purchase <= 0 and r7_clk >= th["no_purchase_click_warning"]:
            severity = "danger" if r7_clk >= th["no_purchase_click_danger"] else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="클릭 누적 무구매",
                reason=f"최근 7일 클릭수 {_fmt_metric(r7_clk, '회')}가 누적됐지만 구매완료가 없습니다.",
                yesterday_value=_fmt_metric(_to_num(row.get("y_clk")), "회"),
                recent_avg=_fmt_metric(r7_clk / 7.0, "회"),
                recent_sum=_fmt_metric(r7_clk, "회"),
                prev_sum=_fmt_metric(p7_clk, "회"),
                delta_rate="-",
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_purchase,
                roas=r7_roas,
                period_filter="최근 상태",
                item_key=item_key,
                has_purchase=False,
            ))

        roas_drop = _drop_rate(r7_roas, p7_roas)
        cost_up = r7_cost > p7_cost
        purchase_down = r7_purchase < p7_purchase
        if p7_roas > 0 and r7_cost >= th["roas_recent_cost_min"] and p7_purchase >= th["roas_prev_purchase_min"] and roas_drop >= th["roas_warning_drop"]:
            severity = "danger" if roas_drop >= th["roas_danger_drop"] or (cost_up and purchase_down) else "warning"
            rows.append(_warning_row(
                severity=severity,
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="ROAS 급락",
                reason=f"최근 7일 ROAS가 이전 7일 대비 {_fmt_signed_pct(roas_drop)} 하락했습니다.",
                yesterday_value="-",
                recent_avg=_fmt_metric(r7_roas, "%"),
                recent_sum=_fmt_metric(r7_sales, "원 매출"),
                prev_sum=_fmt_metric(p7_sales, "원 매출"),
                delta_rate=_fmt_signed_pct(roas_drop),
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_purchase,
                roas=r7_roas,
                period_filter="최근 7일 vs 이전 7일",
                item_key=item_key,
                has_purchase=r7_purchase > 0,
            ))

        imp_drop = _drop_rate(r7_imp, p7_imp)
        if p7_imp >= th["imp_prev7_min"] and imp_drop >= th["imp_warning_drop"]:
            rows.append(_warning_row(
                severity="warning",
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="노출 급감",
                reason=f"최근 7일 노출수가 이전 7일 대비 {_fmt_signed_pct(imp_drop)} 감소했습니다.",
                yesterday_value=_fmt_metric(_to_num(row.get("y_imp")), "회"),
                recent_avg=_fmt_metric(r7_imp / 7.0, "회"),
                recent_sum=_fmt_metric(r7_imp, "회"),
                prev_sum=_fmt_metric(p7_imp, "회"),
                delta_rate=_fmt_signed_pct(imp_drop),
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_purchase,
                roas=r7_roas,
                period_filter="최근 7일 vs 이전 7일",
                item_key=item_key,
                has_purchase=r7_purchase > 0,
            ))

        r7_ctr = _safe_rate(r7_clk, r7_imp)
        p7_ctr = _safe_rate(p7_clk, p7_imp)
        ctr_drop = _drop_rate(r7_ctr, p7_ctr)
        click_drop = _drop_rate(r7_clk, p7_clk)
        if p7_clk >= th["ctr_prev_click_min"] and r7_imp >= p7_imp * th["imp_keep_rate"] and ctr_drop >= th["ctr_warning_drop"]:
            rows.append(_warning_row(
                severity="warning",
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="CTR 급감",
                reason=f"노출은 유지됐지만 CTR이 이전 7일 대비 {_fmt_signed_pct(ctr_drop)} 하락했습니다.",
                yesterday_value="-",
                recent_avg=_fmt_metric(r7_ctr * 100, "%"),
                recent_sum=_fmt_metric(r7_clk, "회 클릭"),
                prev_sum=_fmt_metric(p7_clk, "회 클릭"),
                delta_rate=_fmt_signed_pct(ctr_drop),
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_purchase,
                roas=r7_roas,
                period_filter="최근 7일 vs 이전 7일",
                item_key=item_key,
                has_purchase=r7_purchase > 0,
            ))
        if p7_clk >= th["ctr_prev_click_min"] and r7_imp >= p7_imp * th["imp_keep_rate"] and click_drop >= th["click_danger_drop"]:
            rows.append(_warning_row(
                severity="danger",
                warning_type="쇼핑검색",
                campaign=campaign,
                adgroup=adgroup,
                subject=subject,
                warning_name="클릭 급감",
                reason=f"노출은 유지됐지만 클릭수가 이전 7일 대비 {_fmt_signed_pct(click_drop)} 감소했습니다.",
                yesterday_value=_fmt_metric(_to_num(row.get("y_clk")), "회"),
                recent_avg=_fmt_metric(r7_clk / 7.0, "회"),
                recent_sum=_fmt_metric(r7_clk, "회"),
                prev_sum=_fmt_metric(p7_clk, "회"),
                delta_rate=_fmt_signed_pct(click_drop),
                cost=r7_cost,
                clicks=r7_clk,
                conversions=r7_purchase,
                roas=r7_roas,
                period_filter="최근 7일 vs 이전 7일",
                item_key=item_key,
                has_purchase=r7_purchase > 0,
            ))
    return pd.DataFrame(rows)


def _build_collection_warning_rows(meta: pd.DataFrame, engine, f: dict) -> pd.DataFrame:
    rows = []
    meta_map = _meta_context(meta)
    try:
        status_df = query_collection_status(engine, tuple(f.get("customer_ids") or []))
    except Exception:
        status_df = pd.DataFrame()
    if status_df is not None and not status_df.empty:
        work = status_df.copy()
        work["_cid"] = work["customer_id"].map(_cid_text)
        for _, row in work.iterrows():
            stale_days = int(_to_num(row.get("stale_days")))
            if stale_days < 1:
                continue
            cid = row.get("_cid", "")
            ctx = meta_map.get(cid, {})
            severity = "danger" if stale_days >= 3 else "warning"
            source_label = str(row.get("source_label", "") or "수집 소스")
            rows.append(_warning_row(
                severity=severity,
                warning_type="수집/운영",
                campaign=source_label,
                adgroup="-",
                subject=ctx.get("account_name", cid),
                warning_name="수집 지연",
                reason=f"{source_label} 최신 수집일이 {stale_days}일 지연됐습니다.",
                yesterday_value=str(row.get("latest_dt", "-")),
                recent_avg="-",
                recent_sum=_fmt_metric(row.get("row_count"), "행"),
                prev_sum="-",
                delta_rate=f"{stale_days}일 지연",
                cost=0,
                clicks=0,
                conversions=0,
                roas=0,
                period_filter="수집 상태",
                item_key=f"collection:{cid}:{source_label}",
            ))

    try:
        off_df = query_campaign_off_log(engine, f.get("start"), f.get("end"), tuple(f.get("customer_ids") or []))
    except Exception:
        off_df = pd.DataFrame()
    if off_df is not None and not off_df.empty:
        grouped = (
            off_df.assign(customer_id=off_df["customer_id"].map(_cid_text))
            .groupby(["customer_id", "campaign_id"], dropna=False)
            .agg(off_count=("off_time", "count"), last_off_time=("off_time", "max"))
            .reset_index()
        )
        for _, row in grouped.iterrows():
            cid = row.get("customer_id", "")
            ctx = meta_map.get(cid, {})
            rows.append(_warning_row(
                severity="info",
                warning_type="수집/운영",
                campaign=str(row.get("campaign_id", "") or "-"),
                adgroup="-",
                subject=ctx.get("account_name", cid),
                warning_name="캠페인 OFF 이력",
                reason=f"조회 기간 OFF 기록 {_fmt_num(row.get('off_count'))}건, 최근 시간 {row.get('last_off_time')}",
                yesterday_value="-",
                recent_avg="-",
                recent_sum=_fmt_metric(row.get("off_count"), "건"),
                prev_sum="-",
                delta_rate="-",
                cost=0,
                clicks=0,
                conversions=0,
                roas=0,
                period_filter="수집 상태",
                item_key=f"off:{cid}:{row.get('campaign_id')}",
            ))
    return pd.DataFrame(rows)


def _sort_warning_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=WARNING_DISPLAY_COLS)
    out = df.copy()
    out["_item_severity_sort"] = out.groupby("_item_key")["_severity_sort"].transform("min")
    return out.sort_values(["_item_severity_sort", "_severity_sort", "_cost_sort"], ascending=[True, True, False]).reset_index(drop=True)


def _build_all_warning_rows(meta: pd.DataFrame, engine, f: dict, shopping_compare: dict | None = None) -> pd.DataFrame:
    frames = [
        _build_powerlink_warning_rows(meta, engine, f),
        _build_shopping_warning_rows(meta, engine, f, shopping_compare),
        _build_collection_warning_rows(meta, engine, f),
    ]
    frames = [x for x in frames if x is not None and not x.empty]
    if not frames:
        return pd.DataFrame(columns=WARNING_DISPLAY_COLS)
    return _sort_warning_rows(pd.concat(frames, ignore_index=True, sort=False))


def _apply_warning_filters(df: pd.DataFrame, key_prefix: str, *, fixed_type: str = "") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=WARNING_DISPLAY_COLS)
    filtered = df.copy()
    if fixed_type:
        filtered = filtered[filtered["유형"] == fixed_type].copy()

    with st.container(border=True):
        r1c1, r1c2, r1c3 = st.columns(3)
        type_options = ["전체"] + sorted([x for x in filtered["유형"].dropna().astype(str).unique().tolist() if x])
        if fixed_type:
            type_sel = fixed_type
            r1c1.selectbox("유형", [fixed_type], key=f"{key_prefix}_type", disabled=True)
        else:
            type_sel = r1c1.selectbox("유형", type_options, key=f"{key_prefix}_type")
        severity_sel = r1c2.multiselect("심각도", ["danger", "warning", "info"], default=["danger", "warning", "info"], key=f"{key_prefix}_severity")
        period_options = ["전체"] + sorted([x for x in filtered["_period_filter"].dropna().astype(str).unique().tolist() if x])
        period_sel = r1c3.selectbox("기간", period_options, key=f"{key_prefix}_period")

        r2c1, r2c2, r2c3 = st.columns(3)
        camp_options = ["전체"] + sorted([x for x in filtered["캠페인명"].dropna().astype(str).unique().tolist() if x and x != "-"])
        camp_sel = r2c1.selectbox("캠페인명", camp_options, key=f"{key_prefix}_camp")
        grp_options = ["전체"] + sorted([x for x in filtered["광고그룹명"].dropna().astype(str).unique().tolist() if x and x != "-"])
        grp_sel = r2c2.selectbox("광고그룹명", grp_options, key=f"{key_prefix}_grp")
        warn_options = ["전체"] + sorted([x for x in filtered["경고명"].dropna().astype(str).unique().tolist() if x])
        warn_sel = r2c3.selectbox("경고명", warn_options, key=f"{key_prefix}_warn")

        r3c1, r3c2 = st.columns(2)
        conv_sel = r3c1.selectbox("전환 여부", ["전체", "전환 있음", "전환 없음"], key=f"{key_prefix}_conv")
        purchase_sel = r3c2.selectbox("구매완료 여부", ["전체", "구매완료 있음", "구매완료 없음"], key=f"{key_prefix}_purchase")

    if not fixed_type and type_sel != "전체":
        filtered = filtered[filtered["유형"] == type_sel]
    if severity_sel:
        filtered = filtered[filtered["심각도"].isin(severity_sel)]
    if period_sel != "전체":
        filtered = filtered[filtered["_period_filter"] == period_sel]
    if camp_sel != "전체":
        filtered = filtered[filtered["캠페인명"] == camp_sel]
    if grp_sel != "전체":
        filtered = filtered[filtered["광고그룹명"] == grp_sel]
    if warn_sel != "전체":
        filtered = filtered[filtered["경고명"] == warn_sel]
    if conv_sel == "전환 있음":
        filtered = filtered[filtered["_has_conversion"]]
    elif conv_sel == "전환 없음":
        filtered = filtered[~filtered["_has_conversion"]]
    if purchase_sel == "구매완료 있음":
        filtered = filtered[filtered["_has_purchase"]]
    elif purchase_sel == "구매완료 없음":
        filtered = filtered[~filtered["_has_purchase"]]
    return _sort_warning_rows(filtered)


def _warning_export_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=WARNING_DISPLAY_COLS)
    return df[[c for c in WARNING_DISPLAY_COLS if c in df.columns]].copy()


def _warning_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "warnings") -> bytes:
    buffer = io.BytesIO()
    safe_sheet = str(sheet_name or "warnings")[:31] or "warnings"
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=safe_sheet)
    return buffer.getvalue()


def _render_warning_downloads(df: pd.DataFrame, key_prefix: str, label: str) -> None:
    export_df = _warning_export_frame(df)
    if export_df.empty:
        return
    today = _today_kst().strftime("%Y%m%d")
    safe_label = str(label or "전체").replace("/", "_").replace(" ", "_")
    filename_prefix = f"검색광고_경고_{safe_label}_{today}"
    c_csv, c_xlsx = st.columns([1, 1])
    c_csv.download_button(
        "CSV 다운로드",
        data=export_df.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"{filename_prefix}.csv",
        mime="text/csv",
        key=f"{key_prefix}_download_csv_{len(export_df)}_{len(export_df.columns)}",
        use_container_width=True,
    )
    c_xlsx.download_button(
        "엑셀 다운로드",
        data=_warning_xlsx_bytes(export_df, sheet_name=safe_label),
        file_name=f"{filename_prefix}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=f"{key_prefix}_download_xlsx_{len(export_df)}_{len(export_df.columns)}",
        use_container_width=True,
    )


def _render_warning_table(df: pd.DataFrame, key_prefix: str, *, fixed_type: str = "") -> None:
    filtered = _apply_warning_filters(df, key_prefix, fixed_type=fixed_type)
    if filtered.empty:
        render_empty_state("데이터 부족 또는 판단 제외", detail="현재 필터와 최소 데이터 기준을 만족하는 경고가 없습니다.")
        return
    render_kpi_strip(
        [
            {"label": "경고", "value": f"{len(filtered):,}개", "sub": fixed_type or "전체", "accent": "blue"},
            {"label": "Danger", "value": f"{int((filtered['심각도'] == 'danger').sum()):,}개", "sub": "즉시 확인", "accent": "red"},
            {"label": "Warning", "value": f"{int((filtered['심각도'] == 'warning').sum()):,}개", "sub": "점검 필요", "accent": "amber"},
        ]
    )
    label = fixed_type or "전체"
    _render_warning_downloads(filtered, key_prefix, label)
    display = _warning_export_frame(filtered).head(700).copy()
    st.dataframe(
        display,
        key=f"{key_prefix}_table",
        use_container_width=True,
        hide_index=True,
        height=520,
        column_config={
            "비용": st.column_config.NumberColumn("비용", format="%d원"),
            "클릭수": st.column_config.NumberColumn("클릭수", format="%d"),
            "전환수 또는 구매완료수": st.column_config.NumberColumn("전환수 또는 구매완료수", format="%.1f"),
            "ROAS": st.column_config.NumberColumn("ROAS", format="%.1f%%"),
        },
    )


def _shopping_compare_controls(f: dict) -> dict:
    max_ready_date = _today_kst() - timedelta(days=1)
    anchor = min(_ops_anchor_date(f), max_ready_date)
    yesterday = max_ready_date
    day_before = yesterday - timedelta(days=1)

    with st.container(border=True):
        c1, c2, c3 = st.columns([1.5, 1, 1])
        mode_label = c1.selectbox(
            "쇼핑검색 비교 기준",
            ["기본 7일 비교", "어제 vs 엊그제", "직접 날짜 비교"],
            key="ops_shopping_compare_mode",
            help="오늘 데이터는 수집 전일 수 있어 비교 대상에서 제외합니다.",
        )

        target_date = anchor
        compare_date = anchor - timedelta(days=1)
        mode = "default"
        if mode_label == "어제 vs 엊그제":
            mode = "yesterday_vs_day_before"
            target_date = yesterday
            compare_date = day_before
            c2.date_input("기준일", value=target_date, max_value=max_ready_date, key="ops_shopping_compare_y_target", disabled=True)
            c3.date_input("비교일", value=compare_date, max_value=max_ready_date, key="ops_shopping_compare_y_base", disabled=True)
        elif mode_label == "직접 날짜 비교":
            mode = "custom_dates"
            target_date = c2.date_input("기준일", value=anchor, max_value=max_ready_date, key="ops_shopping_compare_target")
            compare_default = min(anchor - timedelta(days=1), max_ready_date)
            compare_date = c3.date_input("비교일", value=compare_default, max_value=max_ready_date, key="ops_shopping_compare_base")
            if target_date == compare_date:
                st.warning("기준일과 비교일이 같습니다. 서로 다른 날짜를 선택해야 경고를 판단합니다.")
        else:
            c2.caption(f"전일 기준: {anchor}")
            c3.caption(f"오늘({ _today_kst() })은 제외")

    return {
        "mode": mode,
        "target_date": target_date,
        "compare_date": compare_date,
    }


def _render_warning_center(meta: pd.DataFrame, engine, f: dict) -> None:
    shopping_compare = _shopping_compare_controls(f)
    with st.spinner("경고 기준을 계산하고 있습니다."):
        warning_df = _build_all_warning_rows(meta, engine, f, shopping_compare)

    tab_summary, tab_power, tab_shop, tab_ops = st.tabs(["전체 경고 요약", "파워링크 경고", "쇼핑검색 경고", "수집/운영 경고"])
    with tab_summary:
        _render_warning_table(warning_df, "ops_warn_all")
        with st.expander("기존 조치 큐 상태 관리", expanded=False):
            _render_action_queue(meta, engine, f)
    with tab_power:
        _render_warning_table(warning_df, "ops_warn_powerlink", fixed_type="파워링크")
    with tab_shop:
        _render_warning_table(warning_df, "ops_warn_shopping", fixed_type="쇼핑검색")
    with tab_ops:
        _render_warning_table(warning_df, "ops_warn_ops", fixed_type="수집/운영")


def _generate_action_items(meta: pd.DataFrame, engine, f: dict) -> int:
    builders = [
        _build_budget_actions,
        _build_campaign_actions,
        _build_keyword_actions,
        _build_off_log_actions,
    ]
    items = []
    for builder in builders:
        items.extend(builder(meta, engine, f))
    return upsert_action_items(engine, items)


def _action_status_label(value: str) -> str:
    return {
        "open": "대기",
        "in_progress": "진행 중",
        "resolved": "완료",
        "skipped": "보류",
    }.get(str(value or ""), str(value or ""))


def _render_action_queue(meta: pd.DataFrame, engine, f: dict) -> None:
    c_refresh, c_status = st.columns([1.5, 2.5])
    with c_refresh:
        if st.button("현재 조건으로 조치 항목 갱신", key="ops_refresh_actions", type="primary", use_container_width=True):
            with st.spinner("분석 결과에서 조치 항목을 만들고 있습니다."):
                written = _generate_action_items(meta, engine, f)
            st.success(f"{written:,}개 항목을 갱신했습니다.")
    with c_status:
        status_label = st.selectbox(
            "상태",
            ["대기", "진행 중", "완료", "보류", "전체"],
            key="ops_action_status_filter",
        )
        status_filter = {"대기": "open", "진행 중": "in_progress", "완료": "resolved", "보류": "skipped", "전체": ""}.get(status_label, "open")

    auto_key = f"ops_auto_generated_{f.get('start')}_{f.get('end')}_{hash(tuple(f.get('customer_ids') or []))}_{hash(tuple(f.get('type_sel') or []))}"
    if not st.session_state.get(auto_key):
        try:
            _generate_action_items(meta, engine, f)
            st.session_state[auto_key] = True
        except Exception:
            pass

    try:
        action_df = query_action_items(
            engine,
            status_filter,
            limit=500,
            customer_ids=tuple(f.get("customer_ids") or ()),
        )
    except Exception as e:
        st.warning(f"조치 큐 저장소를 준비하지 못했습니다: {e}")
        return
    if action_df is None or action_df.empty:
        render_empty_state("현재 조치 항목이 없습니다.", detail="필터를 넓히거나 조치 항목 갱신 버튼을 눌러보세요.")
        return

    open_count = int((action_df["status"] == "open").sum()) if "status" in action_df.columns else len(action_df)
    urgent_count = int(action_df["severity"].isin(["critical", "danger"]).sum()) if "severity" in action_df.columns else 0
    render_kpi_strip(
        [
            {"label": "표시 항목", "value": f"{len(action_df):,}개", "sub": _action_status_label(status_filter) if status_filter else "전체", "accent": "blue"},
            {"label": "대기", "value": f"{open_count:,}개", "sub": "바로 처리 대상", "accent": "amber" if open_count else "green"},
            {"label": "긴급", "value": f"{urgent_count:,}개", "sub": "위험도 danger 이상", "accent": "red" if urgent_count else "green"},
        ]
    )

    display = action_df.copy()
    display["상태"] = display["status"].map(_action_status_label)
    display = display.rename(
        columns={
            "id": "ID",
            "severity": "위험도",
            "category": "분류",
            "title": "제목",
            "body": "근거",
            "manager": "담당자",
            "account_name": "계정",
            "owner": "처리자",
            "note": "메모",
            "source_page": "출처",
            "last_seen_at": "최근 감지",
        }
    )
    view_cols = ["ID", "위험도", "분류", "제목", "근거", "담당자", "계정", "상태", "처리자", "메모", "출처", "최근 감지"]
    display = display[[c for c in view_cols if c in display.columns]].copy()
    edited = st.data_editor(
        display,
        key="ops_action_editor",
        hide_index=True,
        use_container_width=True,
        height=430,
        disabled=[c for c in display.columns if c not in {"상태", "처리자", "메모"}],
        column_config={
            "상태": st.column_config.SelectboxColumn("상태", options=["대기", "진행 중", "완료", "보류"]),
            "메모": st.column_config.TextColumn("메모", width="medium"),
        },
    )
    if st.button("상태/메모 저장", key="ops_save_action_changes", use_container_width=True):
        reverse_status = {"대기": "open", "진행 중": "in_progress", "완료": "resolved", "보류": "skipped"}
        original = action_df.set_index("id")
        saved = 0
        for _, row in edited.iterrows():
            item_id = int(row.get("ID"))
            before = original.loc[item_id]
            status = reverse_status.get(str(row.get("상태", "")), str(before.get("status", "open")))
            owner = str(row.get("처리자", "") or "")
            note = str(row.get("메모", "") or "")
            if status != before.get("status") or owner != str(before.get("owner", "") or "") or note != str(before.get("note", "") or ""):
                update_action_item(engine, item_id, status, owner, note)
                saved += 1
        st.success(f"{saved:,}개 변경 사항을 저장했습니다.")
        st.rerun()


def _render_collection_status(meta: pd.DataFrame, engine, f: dict) -> None:
    status_df = query_collection_status(engine, tuple(f.get("customer_ids") or []))
    if status_df is None or status_df.empty:
        render_empty_state("수집 상태를 확인할 데이터가 없습니다.", detail="수집 테이블 또는 계정 필터를 확인해주세요.")
        return

    meta_map = _meta_context(meta)
    work = status_df.copy()
    work["_cid"] = work["customer_id"].map(_cid_text)
    work["계정"] = work["_cid"].map(lambda x: meta_map.get(x, {}).get("account_name", x))
    work["담당자"] = work["_cid"].map(lambda x: meta_map.get(x, {}).get("manager", "미배정"))
    work["상태"] = work["stale_days"].map(lambda x: "지연" if int(x) >= 3 else ("확인 필요" if int(x) >= 1 else "정상"))

    stale_accounts = work[work["stale_days"] >= 1]["_cid"].nunique()
    delayed_accounts = work[work["stale_days"] >= 3]["_cid"].nunique()
    render_kpi_strip(
        [
            {"label": "수집 소스", "value": f"{work['source_label'].nunique():,}개", "sub": "fact 테이블 기준", "accent": "blue"},
            {"label": "확인 필요 계정", "value": f"{stale_accounts:,}개", "sub": "1일 이상 지연", "accent": "amber" if stale_accounts else "green"},
            {"label": "지연 계정", "value": f"{delayed_accounts:,}개", "sub": "3일 이상 지연", "accent": "red" if delayed_accounts else "green"},
        ]
    )

    summary = (
        work.groupby("source_label", dropna=False)
        .agg(최신일=("latest_dt", "max"), 지연_최대일=("stale_days", "max"), 계정수=("_cid", "nunique"), 행수=("row_count", "sum"))
        .reset_index()
        .rename(columns={"source_label": "수집 소스"})
        .sort_values(["지연_최대일", "수집 소스"], ascending=[False, True])
    )
    st.dataframe(summary, use_container_width=True, hide_index=True, height=220)

    detail = work.rename(
        columns={
            "source_label": "수집 소스",
            "latest_dt": "최신 수집일",
            "stale_days": "지연일",
            "row_count": "행수",
        }
    )
    detail_cols = ["상태", "담당자", "계정", "customer_id", "수집 소스", "최신 수집일", "지연일", "행수"]
    st.dataframe(detail[[c for c in detail_cols if c in detail.columns]], use_container_width=True, hide_index=True, height=430)


def _render_audit_log(engine) -> None:
    try:
        audit = query_dashboard_audit_log(engine, 300)
    except Exception as e:
        st.warning(f"변경 이력 저장소를 준비하지 못했습니다: {e}")
        return
    if audit is None or audit.empty:
        render_empty_state("아직 변경 이력이 없습니다.", detail="예산, 목표 ROAS, 연결 정보, 조치 상태를 바꾸면 이곳에 남습니다.")
        return
    work = audit.copy()
    work = work.rename(
        columns={
            "event_time": "시간",
            "actor": "사용자",
            "action_type": "작업",
            "target_type": "대상",
            "target_id": "대상 ID",
            "summary": "내용",
        }
    )
    cols = ["시간", "사용자", "작업", "대상", "대상 ID", "내용"]
    st.dataframe(work[[c for c in cols if c in work.columns]], use_container_width=True, hide_index=True, height=520)


def page_ops_center(meta: pd.DataFrame, engine, f: dict) -> None:
    render_toolbar(
        "오늘 처리할 운영 항목",
        "분석 화면에서 발견되는 위험 신호를 조치 큐로 모으고, 수집 상태와 변경 이력을 한 곳에서 확인합니다.",
        chips=[
            {"label": f"{f.get('start')} ~ {f.get('end')}", "tone": "primary"},
            {"label": f.get("scope_label", "전체 계정"), "tone": "info"},
        ],
    )

    tab_queue, tab_status, tab_audit = st.tabs(["경고 확인", "수집 상태", "변경 이력"])
    with tab_queue:
        _render_warning_center(meta, engine, f)
    with tab_status:
        _render_collection_status(meta, engine, f)
    with tab_audit:
        _render_audit_log(engine)
