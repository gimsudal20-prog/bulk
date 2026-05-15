# -*- coding: utf-8 -*-
"""view_overview.py - Overview page view (Toggle renamed & controls both % and abs)."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import streamlit_compat  # noqa: F401
import io
from typing import Dict
from datetime import date

from data import *
from ui import render_echarts_dual_axis, render_kpi_strip, render_ops_cards, render_toolbar, safe_numeric_col, safe_numeric_series
from page_helpers import get_dynamic_cmp_options, period_compare_range


def _inject_overview_css():
    st.markdown("""
    <style>
    .ov-chip { background: transparent; color: var(--nv-text); border: 1px solid var(--nv-line); border-radius: 5px; padding: 5px 10px; font-size: 12px; font-weight: 700; line-height: 1.2; }
    .ov-chip.primary { background: var(--nv-primary-soft); color: var(--nv-primary); border-color: transparent; }
    .ov-chip.muted { color: var(--nv-muted); background: var(--nv-surface); }
    .ov-kpi-grid { display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-bottom: 18px; }
    .ov-kpi-panel { background: var(--nv-bg); border: 1px solid var(--nv-line); border-radius: var(--nv-radius-lg); padding: 14px; box-shadow: var(--nv-shadow-soft); }
    .ov-kpi-title { font-size: 13px; font-weight: 700; color: var(--nv-text); margin-bottom: 12px; }
    .ov-kpi-cells { display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
    .ov-kpi-cell { background: var(--nv-surface); border-radius: 6px; padding: 12px; min-width: 0; border: 1px solid var(--nv-line); }
    .ov-kpi-label { font-size: 12px; color: var(--nv-muted); margin-bottom: 6px; }
    .ov-kpi-value { font-size: 20px; font-weight: 800; color: var(--nv-text); line-height: 1.15; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .ov-kpi-delta { margin-top: 8px; font-size: 11px; font-weight: 700; display:inline-flex; padding: 4px 8px; border-radius: 5px; }
    .ov-kpi-delta.pos { background: var(--nv-success-soft); color: var(--nv-success); }
    .ov-kpi-delta.neg { background: var(--nv-danger-soft); color: var(--nv-danger); }
    .ov-kpi-delta.neu { background: var(--nv-surface); color: var(--nv-muted); border:1px solid var(--nv-line); }
    .ov-chart-shell { background: var(--nv-bg); border: 1px solid var(--nv-line); border-radius: var(--nv-radius-lg); padding: 16px; box-shadow: var(--nv-shadow-soft); }
    @media (max-width: 1100px) {
      .ov-kpi-grid { grid-template-columns: 1fr; }
    }
    </style>
    """, unsafe_allow_html=True)


def _diag_add(diag: list | None, step: str, status: str = "ok", rows=None, source: str = "", note: str = "") -> None:
    if diag is None:
        return
    row_txt = "-" if rows is None else str(rows)
    diag.append({
        "step": str(step),
        "status": str(status),
        "rows": row_txt,
        "source": str(source or "-"),
        "note": str(note or "-")[:300],
    })


def _render_diag_panel(diag: list | None, enabled: bool = False) -> None:
    if (not enabled) or (not diag):
        return
    df = pd.DataFrame(diag)
    if df.empty:
        return
    status_order = {"error": 0, "zero_data": 1, "warn": 2, "ok": 3}
    if "status" in df.columns:
        df["_ord"] = df["status"].map(status_order).fillna(9)
        df = df.sort_values(["_ord", "step"], ascending=[True, True]).drop(columns=["_ord"])
    rename_map = {"step": "단계", "status": "상태", "rows": "건수", "source": "원천", "note": "메모"}
    df = df.rename(columns=rename_map)
    with st.expander("조회 진단", expanded=False):
        st.caption("개요 화면에서 어떤 조회 단계가 비었거나 실패했는지 확인하는 용도입니다.")
        st.dataframe(df, width="stretch", hide_index=True)

def _format_report_line(label: str, value: str) -> str:
    return f"{label} : {value}"


def _format_report_count(value) -> str:
    try:
        if pd.isna(value):
            return "0"
        return f"{float(value):,.0f}"
    except Exception:
        return "0"


def _format_avg_rank(value) -> str:
    try:
        num = pd.to_numeric(value, errors="coerce")
        if pd.isna(num) or num <= 0:
            return "미수집"
        return f"{num:.0f}위"
    except Exception:
        return "미수집"


def _weighted_avg_rank_by_group(df: pd.DataFrame, group_col: str, rank_col: str = "avg_rank", imp_col: str = "imp") -> pd.DataFrame:
    if df is None or df.empty or group_col not in df.columns or rank_col not in df.columns:
        return pd.DataFrame(columns=[group_col, rank_col])
    tmp = df[[group_col]].copy()
    tmp[imp_col] = safe_numeric_col(df, imp_col, default=0.0) if imp_col in df.columns else pd.Series([0.0] * len(df.index))
    tmp[rank_col] = pd.to_numeric(df[rank_col], errors="coerce")
    tmp["_rank_imp"] = tmp[rank_col].fillna(0.0) * tmp[imp_col]
    grp = tmp.groupby(group_col, as_index=False, dropna=False)[["_rank_imp", imp_col]].sum()
    grp[rank_col] = np.where(grp[imp_col] > 0, grp["_rank_imp"] / grp[imp_col], np.nan)
    return grp[[group_col, rank_col]]


def _sticky_cfg(first_col: str):
    return {
        first_col: st.column_config.TextColumn(first_col, pinned=True, width="medium")
    }


def _auto_table_height(data_obj, default_height: int = 420, min_height: int = 72, max_height: int = 560) -> int:
    try:
        df = data_obj.data if hasattr(data_obj, "data") else data_obj
        rows = len(df.index)
        if rows <= 0: return min_height
        if rows == 1: return 72
        if rows == 2: return 106
        calc = 36 + (rows * 34)
        return max(min_height, min(calc, max_height))
    except Exception:
        return default_height

def _render_overview_sticky_table(styler_or_df, first_col: str, height: int = 420, hide_index: bool = False):
    real_height = _auto_table_height(styler_or_df, default_height=height, max_height=height)
    st.dataframe(styler_or_df, width="stretch", height=real_height, hide_index=hide_index, column_config=_sticky_cfg(first_col))


def _selected_type_label(type_sel: tuple) -> str:
    if not type_sel: return "전체 유형"
    if len(type_sel) == 1: return type_sel[0]
    return ", ".join(type_sel)


@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def _cached_campaign_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try: return query_campaign_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=1500)
    except Exception: return pd.DataFrame()


@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def _cached_keyword_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try: return query_keyword_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=300)
    except Exception: return pd.DataFrame()


@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def _cached_campaign_timeseries(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try: return query_campaign_timeseries(_engine, start_dt, end_dt, cids, type_sel)
    except Exception: return pd.DataFrame()


def _attach_account_names(df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    out = df.copy()
    if not meta.empty and 'customer_id' in meta.columns and 'account_name' in meta.columns:
        mapping = dict(zip(meta['customer_id'].astype(str), meta['account_name']))
        out['account_name'] = out['customer_id'].astype(str).map(mapping).fillna(out['customer_id'].astype(str))
    else:
        out['account_name'] = out['customer_id'].astype(str)
    return out


def _build_overview_campaign_frames(cur_camp: pd.DataFrame, base_camp: pd.DataFrame, meta: pd.DataFrame):
    df_display, df_type_display, camp_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    cur_camp = _attach_account_names(cur_camp, meta)
    base_camp = _attach_account_names(base_camp, meta)
    if cur_camp.empty and base_camp.empty:
        return df_display, df_type_display, camp_disp
    df_display = _build_comparison_df(cur_camp, base_camp, 'account_name', '계정명')
    type_kor_map = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENTS": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
    type_col = 'campaign_tp' if 'campaign_tp' in cur_camp.columns else ('campaign_type' if 'campaign_type' in cur_camp.columns else None)
    if type_col:
        df_type_display = _build_comparison_df(cur_camp, base_camp, type_col, '캠페인 유형', type_kor_map)
    camp_col = 'campaign_name' if 'campaign_name' in cur_camp.columns else None
    if camp_col:
        camp_disp = _build_comparison_df(cur_camp, base_camp, camp_col, '캠페인명')
    return df_display, df_type_display, camp_disp

def _build_overview_keyword_frames(cur_kw: pd.DataFrame, base_kw: pd.DataFrame):
    kw_disp = pd.DataFrame()
    if cur_kw.empty and base_kw.empty:
        return kw_disp
    
    kw_col = 'keyword' if 'keyword' in cur_kw.columns else None
    if kw_col:
        kw_disp = _build_comparison_df(cur_kw, base_kw, kw_col, '키워드')
    return kw_disp

def _build_overview_timeseries_frames(daily_ts: pd.DataFrame, base_daily_ts: pd.DataFrame):
    daily_disp, dow_disp, weekly_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    if daily_ts is None or daily_ts.empty:
        return daily_disp, dow_disp, weekly_disp
    daily_copy = daily_ts.copy()
    base_daily_copy = base_daily_ts.copy() if base_daily_ts is not None and not base_daily_ts.empty else pd.DataFrame()
    daily_copy['일자'] = daily_copy['dt'].dt.strftime('%Y-%m-%d')
    if not base_daily_copy.empty:
        base_daily_copy['일자'] = base_daily_copy['dt'].dt.strftime('%Y-%m-%d')
    daily_disp = _build_ts_compare_df(daily_copy, base_daily_copy, '일자', '일자', align_mode="sequence").sort_values('일자', ascending=False)
    daily_copy['요일'] = daily_copy['dt'].dt.dayofweek
    if not base_daily_copy.empty:
        base_daily_copy['요일'] = base_daily_copy['dt'].dt.dayofweek
    dow_disp = _build_ts_compare_df(daily_copy, base_daily_copy, '요일', '요일', align_mode="label").sort_values('요일')
    dow_map = {0: '월요일', 1: '화요일', 2: '수요일', 3: '목요일', 4: '금요일', 5: '토요일', 6: '일요일'}
    dow_disp['요일명'] = dow_disp['요일'].map(dow_map)
    daily_copy['주차'] = daily_copy['dt'].dt.to_period('W').apply(lambda r: f"{r.start_time.strftime('%Y-%m-%d')} ~ {r.end_time.strftime('%Y-%m-%d')}")
    if not base_daily_copy.empty:
        base_daily_copy['주차'] = base_daily_copy['dt'].dt.to_period('W').apply(lambda r: f"{r.start_time.strftime('%Y-%m-%d')} ~ {r.end_time.strftime('%Y-%m-%d')}")
    weekly_disp = _build_ts_compare_df(daily_copy, base_daily_copy, '주차', '주차', align_mode="sequence").sort_values('주차', ascending=False)
    return daily_disp, dow_disp, weekly_disp


def _get_top_keyword_report_text(kw_bundle: pd.DataFrame) -> str:
    top_kw_str = "없음"
    if kw_bundle is None or kw_bundle.empty:
        return top_kw_str
    keyword_col = 'keyword' if 'keyword' in kw_bundle.columns else None
    if keyword_col is None:
        return top_kw_str
    metric_col = 'clk' if 'clk' in kw_bundle.columns else None
    if metric_col is None:
        return top_kw_str
    kw = kw_bundle.copy()
    try:
        kw = kw[(kw[keyword_col].notna()) & (kw[keyword_col].astype(str).str.strip() != "")]
        if kw.empty:
            return top_kw_str
        grouped = kw.groupby(keyword_col, dropna=False)[metric_col].sum().sort_values(ascending=False).head(5)
        if grouped.empty:
            return top_kw_str
        top_kw_str = ", ".join([f"{str(k)}({int(v):,}회)" for k, v in grouped.items()])
    except Exception:
        return "없음"
    return top_kw_str


def _get_campaign_top_keyword_map(kw_bundle: pd.DataFrame, top_n: int = 5) -> dict[str, str]:
    out: dict[str, str] = {}
    if kw_bundle is None or kw_bundle.empty:
        return out
    if not {'campaign_name', 'keyword', 'clk'}.issubset(set(kw_bundle.columns)):
        return out
    kw = kw_bundle.copy()
    kw = kw[(kw['campaign_name'].notna()) & (kw['keyword'].notna())]
    if kw.empty:
        return out
    kw['campaign_name'] = kw['campaign_name'].astype(str).str.strip()
    kw['keyword'] = kw['keyword'].astype(str).str.strip()
    kw = kw[(kw['campaign_name'] != '') & (kw['keyword'] != '')]
    if kw.empty:
        return out
    grouped = (
        kw.groupby(['campaign_name', 'keyword'], dropna=False)['clk']
        .sum()
        .reset_index()
        .sort_values(['campaign_name', 'clk'], ascending=[True, False])
    )
    for campaign_name, sub in grouped.groupby('campaign_name', sort=False):
        top_rows = sub.head(top_n)
        if top_rows.empty:
            continue
        out[str(campaign_name)] = ", ".join(
            [f"{str(r['keyword'])}({int(r['clk']):,}회)" for _, r in top_rows.iterrows()]
        )
    return out


def _get_campaign_top_shopping_query_map(shop_terms: pd.DataFrame, top_n: int = 3) -> dict[str, str]:
    out: dict[str, str] = {}
    if shop_terms is None or shop_terms.empty:
        return out
    required = {'campaign_name', 'query_text'}
    if not required.issubset(set(shop_terms.columns)):
        return out
    metric_col = 'purchase_conv' if 'purchase_conv' in shop_terms.columns else ('total_conv' if 'total_conv' in shop_terms.columns else None)
    if metric_col is None:
        return out
    qdf = shop_terms.copy()
    qdf = qdf[(qdf['campaign_name'].notna()) & (qdf['query_text'].notna())]
    if qdf.empty:
        return out
    qdf['campaign_name'] = qdf['campaign_name'].astype(str).str.strip()
    qdf['query_text'] = qdf['query_text'].astype(str).str.strip()
    qdf = qdf[(qdf['campaign_name'] != '') & (qdf['query_text'] != '')]
    if qdf.empty:
        return out
    grouped = (
        qdf.groupby(['campaign_name', 'query_text'], dropna=False)[metric_col]
        .sum()
        .reset_index()
        .sort_values(['campaign_name', metric_col], ascending=[True, False])
    )
    grouped = grouped[grouped[metric_col] > 0]
    if grouped.empty:
        return out
    for campaign_name, sub in grouped.groupby('campaign_name', sort=False):
        top_rows = sub.head(top_n)
        if top_rows.empty:
            continue
        out[str(campaign_name)] = ", ".join(
            [f"{str(r['query_text'])}({int(r[metric_col]):,}회)" for _, r in top_rows.iterrows()]
        )
    return out


def _build_campaign_report_text(
    cur_camp: pd.DataFrame,
    selected_type_label: str,
    is_shopping_only: bool,
    combined_toggle: bool,
    kpi_mode: str,
    campaign_top_keyword_map: dict[str, str] | None = None,
    campaign_top_shopping_query_map: dict[str, str] | None = None,
) -> str:
    if cur_camp is None or cur_camp.empty or 'campaign_name' not in cur_camp.columns:
        return ''

    campaign_top_keyword_map = campaign_top_keyword_map or {}
    campaign_top_shopping_query_map = campaign_top_shopping_query_map or {}

    cols = ['campaign_name', 'imp', 'clk', 'cost', 'conv', 'sales']
    work = cur_camp.copy()
    for col in cols[1:]:
        if col not in work.columns:
            work[col] = 0.0

    grouped = work.groupby('campaign_name', dropna=False)[cols[1:]].sum().reset_index()
    if 'tot_conv' in work.columns:
        grouped['tot_conv'] = work.groupby('campaign_name', dropna=False)['tot_conv'].sum().values
    else:
        grouped['tot_conv'] = grouped['conv']
    if 'tot_sales' in work.columns:
        grouped['tot_sales'] = work.groupby('campaign_name', dropna=False)['tot_sales'].sum().values
    else:
        grouped['tot_sales'] = grouped['sales']
    grouped = grouped.sort_values(['cost', 'clk', 'imp'], ascending=[False, False, False])

    sections: list[str] = []
    for _, row in grouped.iterrows():
        campaign_name = str(row.get('campaign_name', '') or '').strip()
        if not campaign_name:
            continue
        if is_shopping_only:
            keyword_text = campaign_top_shopping_query_map.get(campaign_name, '없음')
            section = "\n".join([
                f"[ {campaign_name} 성과 요약 ]",
                _format_report_line("노출수", f"{int(float(row.get('imp', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(row.get('clk', 0))):,}"),
                _format_report_line("클릭률", f"{float(_safe_div(float(row.get('clk', 0)), float(row.get('imp', 0)), 100.0)):.1f}%"),
                _format_report_line("광고 소진비용", f"{int(float(row.get('cost', 0))):,}원"),
                _format_report_line("구매완료수", _format_report_count(row.get('conv', 0.0))),
                _format_report_line("구매완료 매출", f"{int(float(row.get('sales', 0))):,}원"),
                _format_report_line("구매 ROAS", f"{float(_safe_div(float(row.get('sales', 0)), float(row.get('cost', 0)), 100.0)):.1f}%"),
                _format_report_line("주요 전환 키워드", keyword_text),
            ])
        else:
            if combined_toggle or kpi_mode != 'shopping_purchase':
                c_conv_val = row.get('tot_conv', 0)
                c_sales_val = row.get('tot_sales', 0)
            else:
                c_conv_val = row.get('conv', 0)
                c_sales_val = row.get('sales', 0)
            c_roas_val = _safe_div(float(c_sales_val), float(row.get('cost', 0)), 100.0)
            keyword_text = campaign_top_keyword_map.get(campaign_name, '없음')
            section = "\n".join([
                f"[ {campaign_name} 성과 요약 ]",
                _format_report_line("노출수", f"{int(float(row.get('imp', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(row.get('clk', 0))):,}"),
                _format_report_line("클릭률", f"{float(_safe_div(float(row.get('clk', 0)), float(row.get('imp', 0)), 100.0)):.1f}%"),
                _format_report_line("광고 소진비용", f"{int(float(row.get('cost', 0))):,}원"),
                _format_report_line("전환수", f"{float(c_conv_val):.1f}"),
                _format_report_line("총전환매출", f"{int(float(c_sales_val)):,}원"),
                _format_report_line("ROAS", f"{float(c_roas_val):.1f}%"),
                _format_report_line("주요 유입 키워드", keyword_text),
            ])
        sections.append(section)

    if not sections:
        return ''

    title = f"[ 캠페인별 성과 요약 | {selected_type_label} ]"
    return "\n\n".join([title, *sections])



def _is_powerlink_type_label(value) -> bool:
    s = str(value or "").strip()
    su = s.upper()
    return s == "파워링크" or su in {"WEB_SITE", "POWER_LINK", "POWERLINK"}


def _is_shopping_type_label(value) -> bool:
    s = str(value or "").strip()
    su = s.upper()
    return ("쇼핑" in s) or su in {"SHOPPING", "SSA", "SHOPPING_SEARCH"}


def _keyword_report_type_candidates(type_sel: tuple) -> list[tuple]:
    vals = tuple(v for v in (type_sel or ()) if v not in (None, ""))
    if not vals:
        return [("파워링크",), ("WEB_SITE",), ("POWER_LINK",)]
    powerlink_vals = tuple(v for v in vals if _is_powerlink_type_label(v))
    if powerlink_vals:
        candidates = [powerlink_vals]
    elif vals and all(_is_shopping_type_label(v) for v in vals):
        return []
    else:
        candidates = []
    for cand in (("파워링크",), ("WEB_SITE",), ("POWER_LINK",)):
        if cand not in candidates:
            candidates.append(cand)
    return candidates


def _load_report_keyword_bundle(engine, start_dt, end_dt, cids: tuple, type_sel: tuple, state_key: str, force_refresh: bool = False) -> pd.DataFrame:
    if force_refresh or state_key not in st.session_state:
        st.session_state[state_key] = _cached_keyword_bundle(engine, start_dt, end_dt, cids, type_sel)
    bundle = st.session_state.get(state_key)
    return bundle if isinstance(bundle, pd.DataFrame) else pd.DataFrame()


def _resolve_overview_report_top_keywords(engine, start_dt, end_dt, cids: tuple, type_sel: tuple, selected_type_label: str, diag: list | None = None, force_refresh: bool = False) -> str:
    is_shopping_only = ("쇼핑" in selected_type_label and "파워링크" not in selected_type_label and selected_type_label != "전체 유형")
    if is_shopping_only:
        return "없음"

    generic_key = f"overview_text_kw::{start_dt}::{end_dt}::{','.join(map(str, cids))}::{','.join(map(str, type_sel))}"
    bundle = _load_report_keyword_bundle(engine, start_dt, end_dt, cids, type_sel, generic_key, force_refresh=force_refresh)
    top_kw_str = _get_top_keyword_report_text(bundle)
    if top_kw_str != "없음":
        if diag is not None:
            _diag_add(diag, "키워드 번들", "ok", 0 if bundle is None else len(bundle.index), "query_keyword_bundle", "보고서용 기본 키워드 번들 사용")
        return top_kw_str

    candidates = _keyword_report_type_candidates(type_sel)
    last_rows = 0
    for cand in candidates:
        state_key = f"overview_text_kw_powerlink::{start_dt}::{end_dt}::{','.join(map(str, cids))}::{','.join(map(str, cand))}"
        cand_bundle = _load_report_keyword_bundle(engine, start_dt, end_dt, cids, cand, state_key, force_refresh=force_refresh)
        last_rows = 0 if cand_bundle is None else len(cand_bundle.index)
        top_kw_str = _get_top_keyword_report_text(cand_bundle)
        if top_kw_str != "없음":
            if diag is not None:
                _diag_add(diag, "키워드 번들", "ok", last_rows, "query_keyword_bundle", f"보고서용 파워링크 fallback 사용 | type={','.join(map(str, cand))}")
            return top_kw_str

    if diag is not None:
        _diag_add(diag, "키워드 번들", "zero_data", last_rows, "query_keyword_bundle", "보고서용 파워링크 키워드 미검출")
    return "없음"


def format_for_csv(df):
    out_df = df.copy()
    for col in out_df.columns:
        if out_df[col].dtype in ['float64', 'int64']:
            if col == "순위 변화":
                out_df[col] = out_df[col].apply(lambda x: f"{x:+.0f}" if pd.notnull(x) else "-")
            elif col in ["노출수", "클릭수", "구매완료수", "총 전환수"]:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
            elif col in ["광고비", "구매완료 매출", "총 전환매출", "CPC"]:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.0f}원" if pd.notnull(x) else "0원")
            elif "차이" in col:
                if "광고비" in col or "매출" in col or "CPC" in col: out_df[col] = out_df[col].apply(lambda x: f"{x:+,.0f}원" if pd.notnull(x) and x != 0 else "0원")
                elif "노출" in col or "클릭" in col: out_df[col] = out_df[col].apply(lambda x: f"{x:+,.0f}" if pd.notnull(x) and x != 0 else "0")
                else: out_df[col] = out_df[col].apply(lambda x: f"{x:+,.1f}" if pd.notnull(x) and x != 0 else "0.0")
            elif "증감" in col:
                if "ROAS" in col or "전환율" in col or "클릭률" in col:
                    out_df[col] = out_df[col].apply(lambda x: f"{x:+.2f}%" if pd.notnull(x) and x != 0 else "0.00%")
                else:
                    out_df[col] = out_df[col].apply(lambda x: f"{x:+.1f}%" if pd.notnull(x) and x != 0 else "0.0%")
            elif "ROAS" in col or "전환율" in col or "클릭률" in col:
                out_df[col] = out_df[col].apply(lambda x: f"{x:,.2f}%" if pd.notnull(x) else "0.00%")
    return out_df


def _style_delta_numeric(val):
    try: v = float(val)
    except Exception: return ''
    if pd.isna(v) or v == 0: return ''
    return 'color: #1A73E8; font-weight: 700;' if v > 0 else 'color: #EA4335; font-weight: 700;'


def _style_delta_numeric_neg(val):
    try: v = float(val)
    except Exception: return ''
    if pd.isna(v) or v == 0: return ''
    return 'color: #EA4335; font-weight: 700;' if v > 0 else 'color: #1A73E8; font-weight: 700;'


def _apply_overview_delta_styles(styler, df: pd.DataFrame):
    positive_cols = [
        '노출 증감', '노출 차이', '클릭 증감', '클릭 차이', '클릭률 증감',
        '구매완료 증감', '구매완료 차이', '구매 전환율 증감', '구매완료 매출 증감', '구매완료 매출 차이', '구매완료 ROAS 증감',
        '총 전환 증감', '총 전환 차이', '총 전환율 증감', '총 매출 증감', '총 매출 차이', '통합 ROAS 증감'
    ]
    negative_cols = ['광고비 증감', '광고비 차이', 'CPC 증감', 'CPC 차이', '순위 변화']

    pos_subset = [c for c in positive_cols if c in df.columns]
    neg_subset = [c for c in negative_cols if c in df.columns]

    try:
        if pos_subset: styler = styler.map(_style_delta_numeric, subset=pos_subset)
        if neg_subset: styler = styler.map(_style_delta_numeric_neg, subset=neg_subset)
    except AttributeError:
        if pos_subset: styler = styler.applymap(_style_delta_numeric, subset=pos_subset)
        if neg_subset: styler = styler.applymap(_style_delta_numeric_neg, subset=neg_subset)
    return styler

def _safe_div(n, d, mult=1.0):
    sd = np.where(d == 0, 1, d)
    return np.where(d > 0, (n / sd) * mult, 0.0)

def _build_comparison_df(cur_df, base_df, group_col, group_label, type_kor_map=None):
    if cur_df.empty and base_df.empty: return pd.DataFrame()

    cur_has_rank = not cur_df.empty and 'avg_rank' in cur_df.columns
    base_has_rank = not base_df.empty and 'avg_rank' in base_df.columns
    base_cols = [group_col, 'imp', 'clk', 'cost', 'conv', 'sales']
    for c in base_cols[1:]:
        if not cur_df.empty and c not in cur_df.columns: cur_df[c] = 0.0
        if not base_df.empty and c not in base_df.columns: base_df[c] = 0.0

    cur_grp = cur_df.groupby(group_col)[base_cols[1:]].sum().reset_index() if not cur_df.empty else pd.DataFrame(columns=base_cols)
    base_grp = base_df.groupby(group_col)[base_cols[1:]].sum().reset_index() if not base_df.empty else pd.DataFrame(columns=base_cols)
    
    if not cur_df.empty:
        cur_grp['tot_conv'] = cur_df.groupby(group_col)['tot_conv'].sum().values if 'tot_conv' in cur_df.columns else cur_grp['conv']
        cur_grp['tot_sales'] = cur_df.groupby(group_col)['tot_sales'].sum().values if 'tot_sales' in cur_df.columns else cur_grp['sales']
        cur_rank = _weighted_avg_rank_by_group(cur_df, group_col)
        if not cur_rank.empty:
            cur_grp = cur_grp.merge(cur_rank, on=group_col, how="left")
    
    if not base_df.empty:
        base_grp['tot_conv'] = base_df.groupby(group_col)['tot_conv'].sum().values if 'tot_conv' in base_df.columns else base_grp['conv']
        base_grp['tot_sales'] = base_df.groupby(group_col)['tot_sales'].sum().values if 'tot_sales' in base_df.columns else base_grp['sales']
        base_rank = _weighted_avg_rank_by_group(base_df, group_col)
        if not base_rank.empty:
            base_grp = base_grp.merge(base_rank, on=group_col, how="left")

    merged = pd.merge(cur_grp, base_grp, on=group_col, how='outer', suffixes=('_cur', '_base')).fillna(0)

    c_imp, b_imp = merged.get('imp_cur', 0), merged.get('imp_base', 0)
    c_clk, b_clk = merged.get('clk_cur', 0), merged.get('clk_base', 0)
    c_cost, b_cost = merged.get('cost_cur', 0), merged.get('cost_base', 0)
    c_conv, b_conv = merged.get('conv_cur', 0), merged.get('conv_base', 0)
    c_sales, b_sales = merged.get('sales_cur', 0), merged.get('sales_base', 0)
    c_tot_conv, b_tot_conv = merged.get('tot_conv_cur', 0), merged.get('tot_conv_base', 0)
    c_tot_sales, b_tot_sales = merged.get('tot_sales_cur', 0), merged.get('tot_sales_base', 0)
    cur_rank_col = 'avg_rank_cur' if (cur_has_rank and base_has_rank) else ('avg_rank' if cur_has_rank else None)
    base_rank_col = 'avg_rank_base' if (cur_has_rank and base_has_rank) else ('avg_rank' if base_has_rank and not cur_has_rank else None)
    c_rank = pd.to_numeric(merged.get(cur_rank_col), errors="coerce") if cur_rank_col else pd.Series(np.nan, index=merged.index)
    b_rank = pd.to_numeric(merged.get(base_rank_col), errors="coerce") if base_rank_col else pd.Series(np.nan, index=merged.index)

    c_cpc = _safe_div(c_cost, c_clk)
    b_cpc = _safe_div(b_cost, b_clk)

    out = pd.DataFrame()
    out[group_label] = merged[group_col].astype(str).str.upper().map(type_kor_map).fillna(merged[group_col]) if type_kor_map else merged[group_col]

    out['노출수'] = c_imp
    out['클릭수'] = c_clk
    out['클릭률(%)'] = _safe_div(c_clk, c_imp, 100.0)
    out['광고비'] = c_cost
    out['CPC'] = c_cpc
    if cur_rank_col:
        out['avg_rank'] = c_rank
        out['평균순위'] = c_rank.apply(_format_avg_rank)
    
    out['구매완료수'] = c_conv
    out['구매 전환율(%)'] = _safe_div(c_conv, c_clk, 100.0)
    out['구매완료 매출'] = c_sales
    out['구매완료 ROAS(%)'] = _safe_div(c_sales, c_cost, 100.0)
    
    out['총 전환수'] = c_tot_conv
    out['총 전환율(%)'] = _safe_div(c_tot_conv, c_clk, 100.0)
    out['총 전환매출'] = c_tot_sales
    out['통합 ROAS(%)'] = _safe_div(c_tot_sales, c_cost, 100.0)

    # Base values for deltas
    b_ctr = _safe_div(b_clk, b_imp, 100.0)
    b_cvr = _safe_div(b_conv, b_clk, 100.0)
    b_roas = _safe_div(b_sales, b_cost, 100.0)
    
    b_tcvr = _safe_div(b_tot_conv, b_clk, 100.0)
    b_troas = _safe_div(b_tot_sales, b_cost, 100.0)

    def _apply_pct_diff(c, b, pct_col, abs_col):
        diff = c - b
        safe_b = np.where(b == 0, 1, b)
        pct = np.where(b == 0, np.where(c > 0, 100.0, 0.0), (diff / safe_b) * 100.0)
        out[pct_col] = pct
        out[abs_col] = diff

    _apply_pct_diff(c_imp, b_imp, '노출 증감', '노출 차이')
    _apply_pct_diff(c_clk, b_clk, '클릭 증감', '클릭 차이')
    _apply_pct_diff(c_cost, b_cost, '광고비 증감', '광고비 차이')
    _apply_pct_diff(c_cpc, b_cpc, 'CPC 증감', 'CPC 차이')
    _apply_pct_diff(c_conv, b_conv, '구매완료 증감', '구매완료 차이')
    _apply_pct_diff(c_sales, b_sales, '구매완료 매출 증감', '구매완료 매출 차이')
    _apply_pct_diff(c_tot_conv, b_tot_conv, '총 전환 증감', '총 전환 차이')
    _apply_pct_diff(c_tot_sales, b_tot_sales, '총 매출 증감', '총 매출 차이')

    # Rates diffs (percentage points removed for cleaner view)
    out['클릭률 증감'] = out['클릭률(%)'] - b_ctr
    out['구매 전환율 증감'] = out['구매 전환율(%)'] - b_cvr
    out['구매완료 ROAS 증감'] = out['구매완료 ROAS(%)'] - b_roas
    out['총 전환율 증감'] = out['총 전환율(%)'] - b_tcvr
    out['통합 ROAS 증감'] = out['통합 ROAS(%)'] - b_troas
    if cur_rank_col:
        out['순위 변화'] = np.where((c_rank > 0) & (b_rank > 0), c_rank - b_rank, np.nan)

    return out.sort_values("광고비", ascending=False).reset_index(drop=True)


def _build_ts_df(df, group_col, group_label):
    if df is None or df.empty: return pd.DataFrame()

    grp_cols = ['imp', 'clk', 'cost', 'conv', 'sales']
    has_tot = 'tot_conv' in df.columns
    if has_tot: grp_cols.extend(['tot_conv', 'tot_sales'])

    for c in grp_cols:
        if c not in df.columns: df[c] = 0.0

    grp = df.groupby(group_col)[grp_cols].sum().reset_index()
    rank_grp = _weighted_avg_rank_by_group(df, group_col)
    if not rank_grp.empty:
        grp = grp.merge(rank_grp, on=group_col, how="left")

    out = pd.DataFrame()
    out[group_label] = grp[group_col]
    out['노출수'] = grp['imp']
    out['클릭수'] = grp['clk']
    out['클릭률(%)'] = _safe_div(grp['clk'], grp['imp'], 100.0)
    out['광고비'] = grp['cost']
    out['CPC'] = _safe_div(grp['cost'], grp['clk'])
    if 'avg_rank' in grp.columns:
        out['avg_rank'] = grp['avg_rank']
        out['평균순위'] = grp['avg_rank'].apply(_format_avg_rank)
    
    out['구매완료수'] = grp['conv']
    out['구매 전환율(%)'] = _safe_div(grp['conv'], grp['clk'], 100.0)
    out['구매완료 매출'] = grp['sales']
    out['구매완료 ROAS(%)'] = _safe_div(grp['sales'], grp['cost'], 100.0)
    
    out['총 전환수'] = grp['tot_conv'] if has_tot else grp['conv']
    out['총 전환율(%)'] = _safe_div(out['총 전환수'], grp['clk'], 100.0)
    out['총 전환매출'] = grp['tot_sales'] if has_tot else grp['sales']
    out['통합 ROAS(%)'] = _safe_div(out['총 전환매출'], grp['cost'], 100.0)

    return out


def _build_ts_compare_df(cur_df, base_df, group_col, group_label, align_mode="label"):
    cur_view = _build_ts_df(cur_df, group_col, group_label)
    if cur_view.empty: return pd.DataFrame()

    base_view = _build_ts_df(base_df, group_col, group_label) if base_df is not None and not base_df.empty else pd.DataFrame()

    if align_mode == "sequence":
        cur_view = cur_view.reset_index(drop=True).copy()
        base_view = base_view.reset_index(drop=True).copy() if not base_view.empty else base_view
        cur_view["_seq"] = range(len(cur_view))
        if not base_view.empty: base_view["_seq"] = range(len(base_view))
        merge_key = "_seq"
    else: merge_key = group_label

    if not base_view.empty: merged = pd.merge(cur_view, base_view, on=merge_key, how="left", suffixes=("", "_base"))
    else:
        merged = cur_view.copy()
        for c in cur_view.columns:
            if c != merge_key: merged[f"{c}_base"] = 0

    diff_pairs = [
        ("노출수", "노출 증감", "노출 차이"),
        ("클릭수", "클릭 증감", "클릭 차이"),
        ("광고비", "광고비 증감", "광고비 차이"),
        ("CPC", "CPC 증감", "CPC 차이"),
        ("구매완료수", "구매완료 증감", "구매완료 차이"),
        ("구매완료 매출", "구매완료 매출 증감", "구매완료 매출 차이"),
        ("총 전환수", "총 전환 증감", "총 전환 차이"),
        ("총 전환매출", "총 매출 증감", "총 매출 차이"),
    ]
    
    for cur_col, pct_col, abs_col in diff_pairs:
        if cur_col in merged.columns:
            base_col = f"{cur_col}_base"
            c_val = pd.to_numeric(merged[cur_col], errors="coerce").fillna(0)
            b_val = safe_numeric_series(merged.get(base_col), length=len(merged.index), default=0)
            
            diff = c_val - b_val
            safe_b = np.where(b_val == 0, 1, b_val)
            pct = np.where(b_val == 0, np.where(c_val > 0, 100.0, 0.0), (diff / safe_b) * 100.0)
            
            merged[pct_col] = pct
            merged[abs_col] = diff

    rate_diff_pairs = [
        ("클릭률(%)", "클릭률 증감"),
        ("구매 전환율(%)", "구매 전환율 증감"),
        ("총 전환율(%)", "총 전환율 증감"),
        ("구매완료 ROAS(%)", "구매완료 ROAS 증감"),
        ("통합 ROAS(%)", "통합 ROAS 증감")
    ]
    for cur_col, diff_col in rate_diff_pairs:
        if cur_col in merged.columns:
            base_col = f"{cur_col}_base"
            c_val = pd.to_numeric(merged[cur_col], errors="coerce").fillna(0)
            b_val = safe_numeric_series(merged.get(base_col), length=len(merged.index), default=0)
            merged[diff_col] = c_val - b_val

    if "avg_rank" in merged.columns:
        c_rank = pd.to_numeric(merged["avg_rank"], errors="coerce")
        b_rank = safe_numeric_series(merged.get("avg_rank_base"), length=len(merged.index), default=np.nan)
        merged["평균순위"] = c_rank.apply(_format_avg_rank)
        merged["순위 변화"] = np.where((c_rank > 0) & (b_rank > 0), c_rank - b_rank, np.nan)

    if align_mode == "sequence" and "_seq" in merged.columns:
        merged = merged.drop(columns=["_seq"])

    return merged


def _delta_chip(cur_val, base_val, improve_when_up=True):
    diff = pct_change(float(cur_val or 0), float(base_val or 0)) if base_val is not None else 0.0
    if abs(diff) < 5: return "neu", f"유지 ({diff:+.1f}%)"
    improved = diff > 0 if improve_when_up else diff < 0
    return "pos" if improved else "neg", pct_to_arrow(diff)

def _render_kpi_group(title: str, items: list[dict]) -> str:
    cells = []
    for item in items:
        cls, text = _delta_chip(item["cur"], item["base"], item.get("improve_when_up", True))
        cells.append(
            f"<div class='ov-kpi-cell'>"
            f"<div class='ov-kpi-label'>{item['label']}</div>"
            f"<div class='ov-kpi-value' title='{item['value']}'>{item['value']}</div>"
            f"<div class='ov-kpi-delta {cls}'>{text}</div>"
            f"</div>"
        )
    return f"<div class='ov-kpi-panel'><div class='ov-kpi-title'>{title}</div><div class='ov-kpi-cells'>{''.join(cells)}</div></div>"


def _normalize_type_label(val) -> str:
    s = str(val or "").strip().upper()
    if not s: return ""
    if "쇼핑" in s or "SHOPPING" in s: return "쇼핑검색"
    if "파워링크" in s or "WEB_SITE" in s: return "파워링크"
    if "브랜드" in s or "BRAND" in s: return "브랜드검색"
    if "POWER_CONTENTS" in s or "파워컨텐츠" in s: return "파워컨텐츠"
    if "PLACE" in s or "플레이스" in s: return "플레이스"
    return str(val).strip()


def _infer_kpi_mode(type_sel: tuple, cur_camp: pd.DataFrame, is_split_only: bool) -> str:
    labels = {_normalize_type_label(x) for x in type_sel if str(x).strip()}
    if not labels and cur_camp is not None and not cur_camp.empty:
        for col in ["campaign_type_label", "campaign_type", "campaign_tp", "캠페인유형"]:
            if col in cur_camp.columns:
                vals = cur_camp[col].dropna().astype(str).tolist()
                labels = {_normalize_type_label(v) for v in vals if str(v).strip()}
                if labels: break
    labels = {x for x in labels if x}
    if is_split_only and labels and labels == {"쇼핑검색"}: return "shopping_purchase"
    return "generic_conversion"


def _format_compact_currency(value: float) -> str:
    try: v = float(value or 0)
    except Exception: return "0원"
    abs_v = abs(v)
    if abs_v >= 100000000: return f"{v / 100000000:.2f}억"
    if abs_v >= 10000: return f"{v / 10000:.1f}만원"
    return f"{int(round(v)):,}원"


@st.fragment
def page_overview(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f: return

    _inject_overview_css()

    diag: list[dict] = []
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    opts = get_dynamic_cmp_options(f["start"], f["end"])
    cmp_mode = opts[1] if len(opts) > 1 else "이전 같은 기간 대비"
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
    _diag_add(diag, "필터", "ok", len(cids), "filters", f"기간={f['start']}~{f['end']} | 비교={cmp_mode} | 유형={', '.join(type_sel) if type_sel else '전체'}")

    with st.spinner("데이터를 집계 중입니다... (최적화 모드)"):
        try:
            cur_summary = get_entity_totals(engine, "campaign", f["start"], f["end"], cids, type_sel)
            _diag_add(diag, "요약(현재)", "ok" if cur_summary else "zero_data", 1 if cur_summary else 0, "get_entity_totals", "현재 기간 캠페인 합계")
        except Exception as e:
            cur_summary = {}
            _diag_add(diag, "요약(현재)", "error", 0, "get_entity_totals", f"{type(e).__name__}: {e}")
        try:
            base_summary = get_entity_totals(engine, "campaign", b1, b2, cids, type_sel)
            _diag_add(diag, "요약(비교)", "ok" if base_summary else "zero_data", 1 if base_summary else 0, "get_entity_totals", f"비교 기간 {b1}~{b2}")
        except Exception as e:
            base_summary = {}
            _diag_add(diag, "요약(비교)", "error", 0, "get_entity_totals", f"{type(e).__name__}: {e}")
        try:
            cur_camp = _cached_campaign_bundle(engine, f["start"], f["end"], cids, type_sel)
            _diag_add(diag, "캠페인 번들(현재)", "ok" if cur_camp is not None and not cur_camp.empty else "zero_data", 0 if cur_camp is None else len(cur_camp.index), "query_campaign_bundle", "현재 기간 캠페인 상세")
        except Exception as e:
            cur_camp = pd.DataFrame()
            _diag_add(diag, "캠페인 번들(현재)", "error", 0, "query_campaign_bundle", f"{type(e).__name__}: {e}")
            
        try:
            cur_kw = _cached_keyword_bundle(engine, f["start"], f["end"], cids, type_sel)
            _diag_add(diag, "키워드 번들(현재)", "ok" if cur_kw is not None and not cur_kw.empty else "zero_data", 0 if cur_kw is None else len(cur_kw.index), "query_keyword_bundle", "현재 기간 키워드 상세")
        except Exception as e:
            cur_kw = pd.DataFrame()
            _diag_add(diag, "키워드 번들(현재)", "error", 0, "query_keyword_bundle", f"{type(e).__name__}: {e}")
            
        base_camp = pd.DataFrame()
        base_kw = pd.DataFrame()
        kw_bundle = None
        _diag_add(diag, "캠페인 번들(비교)", "warn", 0, "query_campaign_bundle", f"비교 기간 {b1}~{b2} | 상세 패널 필요 시 지연 조회")
        _diag_add(diag, "키워드 번들(비교)", "warn", 0, "query_keyword_bundle", f"비교 기간 {b1}~{b2} | 상세 패널 필요 시 지연 조회")
        
        try:
            daily_ts = _cached_campaign_timeseries(engine, f["start"], f["end"], cids, type_sel)
            _diag_add(diag, "일자 추이(현재)", "ok" if daily_ts is not None and not daily_ts.empty else "zero_data", 0 if daily_ts is None else len(daily_ts.index), "query_campaign_timeseries", "현재 기간 시계열")
        except Exception as e:
            daily_ts = pd.DataFrame()
            _diag_add(diag, "일자 추이(현재)", "error", 0, "query_campaign_timeseries", f"{type(e).__name__}: {e}")
            
        base_daily_ts = pd.DataFrame()
        _diag_add(diag, "일자 추이(비교)", "warn", 0, "query_campaign_timeseries", f"비교 기간 {b1}~{b2} | 기간별 상세 필요 시 지연 조회")

    account_name = "전체 계정"
    if cids and not meta.empty:
        acc_names = meta[meta['customer_id'].isin(cids)]['account_name'].dropna().unique()
        if len(acc_names) == 1: account_name = f"{acc_names[0]}"
        elif len(acc_names) > 1: account_name = f"{acc_names[0]} 외 {len(acc_names)-1}개"

    selected_type_label = _selected_type_label(type_sel)

    st.markdown(f"<div class='nv-sec-title'>{account_name} 종합 성과 요약</div>", unsafe_allow_html=True)
    patch_date = date(2026, 3, 11)
    is_legacy_only = f["end"] < patch_date
    is_split_only = f["start"] >= patch_date
    is_mixed_period = (f["start"] < patch_date <= f["end"])
    combined_toggle = not is_split_only
    auto_kpi_mode = _infer_kpi_mode(type_sel, cur_camp, is_split_only)
    can_use_purchase_toggle = (f["end"] >= patch_date)

    head_col_meta, empty_col, head_col_toggle = st.columns([5, 1, 3])
    with head_col_meta:
        st.markdown(
            f"<div style='display:flex; flex-wrap:wrap; gap:8px; align-items:center; padding-top:4px; margin-bottom: 12px;'>"
            f"<div class='ov-chip primary'>{selected_type_label}</div>"
            f"<div class='ov-chip muted'>{f['start']} ~ {f['end']}</div>"
            f"<div class='ov-chip muted'>{cmp_mode} · {b1} ~ {b2}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
    with head_col_toggle:
        purchase_view = st.toggle(
            "구매완료 데이터로 보기",
            value=(auto_kpi_mode == "shopping_purchase"),
            key="overview_purchase_view_toggle",
            disabled=not can_use_purchase_toggle,
        )

    if is_mixed_period:
        st.info("안내: 3월 11일 이전 및 이후 데이터가 혼재되어 있어, 상단 성과 지표와 추이 그래프는 '총 전환' 기준으로 표시됩니다.")
    elif is_legacy_only:
        st.info("안내: 3월 11일 이전 데이터 조회 시, 상단 성과 지표와 추이 그래프는 '총 전환' 기준으로 표시됩니다.")

    cur = cur_summary or {}
    base = base_summary or {}

    cur['tot_conv'] = cur.get('tot_conv', cur.get('conv', 0))
    cur['tot_sales'] = cur.get('tot_sales', cur.get('sales', 0))
    cur['tot_roas'] = (cur['tot_sales'] / cur['cost'] * 100) if cur.get('cost', 0) > 0 else 0
    cur['cpm'] = (cur.get('cost', 0) / cur.get('imp', 0) * 1000) if cur.get('imp', 0) > 0 else 0
    cur['tot_cvr'] = (cur['tot_conv'] / cur['clk'] * 100) if cur.get('clk', 0) > 0 else 0
    cur['tot_cpa'] = (cur['cost'] / cur['tot_conv']) if cur.get('tot_conv', 0) > 0 else 0

    base['tot_conv'] = base.get('tot_conv', base.get('conv', 0))
    base['tot_sales'] = base.get('tot_sales', base.get('sales', 0))
    base['tot_roas'] = (base['tot_sales'] / base['cost'] * 100) if base.get('cost', 0) > 0 else 0
    base['cpm'] = (base.get('cost', 0) / base.get('imp', 0) * 1000) if base.get('imp', 0) > 0 else 0
    base['tot_cvr'] = (base['tot_conv'] / base['clk'] * 100) if base.get('clk', 0) > 0 else 0
    base['tot_cpa'] = (base['cost'] / base['tot_conv']) if base.get('tot_conv', 0) > 0 else 0

    kpi_mode = "shopping_purchase" if (purchase_view and can_use_purchase_toggle) else "generic_conversion"

    inflow_items = [
        {"label": "노출수", "value": format_number_commas(cur.get("imp", 0.0)), "cur": cur.get("imp", 0), "base": base.get("imp", 0)},
        {"label": "클릭수", "value": format_number_commas(cur.get("clk", 0.0)), "cur": cur.get("clk", 0), "base": base.get("clk", 0)},
        {"label": "클릭률", "value": f"{float(cur.get('ctr', 0.0) or 0.0):.1f}%", "cur": cur.get("ctr", 0), "base": base.get("ctr", 0)},
    ]
    cost_items = [
        {"label": "광고비", "value": _format_compact_currency(cur.get("cost", 0.0)), "cur": cur.get("cost", 0), "base": base.get("cost", 0), "improve_when_up": False},
        {"label": "CPC", "value": format_currency(cur.get("cpc", 0.0)), "cur": cur.get("cpc", 0), "base": base.get("cpc", 0), "improve_when_up": False},
        {"label": "CPM", "value": format_currency(cur.get("cpm", 0.0)), "cur": cur.get("cpm", 0), "base": base.get("cpm", 0), "improve_when_up": False},
    ]
    if kpi_mode == "shopping_purchase":
        perf_items = [
            {"label": "구매완료 ROAS", "value": f"{float(cur.get('roas', 0.0) or 0.0):.1f}%", "cur": cur.get("roas", 0), "base": base.get("roas", 0)},
            {"label": "구매완료수", "value": f"{float(cur.get('conv', 0.0)):.0f}", "cur": cur.get("conv", 0), "base": base.get("conv", 0)},
            {"label": "구매완료 매출", "value": _format_compact_currency(cur.get("sales", 0.0)), "cur": cur.get("sales", 0), "base": base.get("sales", 0)},
        ]
    else:
        perf_items = [
            {"label": "통합 ROAS", "value": f"{float(cur.get('tot_roas', 0.0) or 0.0):.1f}%", "cur": cur.get("tot_roas", 0), "base": base.get("tot_roas", 0)},
            {"label": "총 전환수", "value": f"{float(cur.get('tot_conv', 0.0)):.0f}", "cur": cur.get("tot_conv", 0), "base": base.get("tot_conv", 0)},
            {"label": "총 전환매출", "value": _format_compact_currency(cur.get("tot_sales", 0.0)), "cur": cur.get("tot_sales", 0), "base": base.get("tot_sales", 0)},
        ]

    primary_kpis = [
        {"label": "광고비", "value": _format_compact_currency(cur.get("cost", 0.0)), "cur": cur.get("cost", 0), "base": base.get("cost", 0), "improve_when_up": False, "accent": "blue"},
        {"label": "클릭", "value": format_number_commas(cur.get("clk", 0.0)), "cur": cur.get("clk", 0), "base": base.get("clk", 0), "accent": "cyan"},
        {"label": "CPC", "value": format_currency(cur.get("cpc", 0.0)), "cur": cur.get("cpc", 0), "base": base.get("cpc", 0), "improve_when_up": False, "accent": "amber"},
        {"label": perf_items[1]["label"], "value": perf_items[1]["value"], "cur": perf_items[1]["cur"], "base": perf_items[1]["base"], "accent": "green"},
        {"label": perf_items[2]["label"], "value": perf_items[2]["value"], "cur": perf_items[2]["cur"], "base": perf_items[2]["base"], "accent": "green"},
        {"label": perf_items[0]["label"], "value": perf_items[0]["value"], "cur": perf_items[0]["cur"], "base": perf_items[0]["base"], "accent": "blue"},
    ]
    render_kpi_strip(primary_kpis)

    campaign_count = 0 if cur_camp is None or cur_camp.empty else int(cur_camp.get("campaign_name", pd.Series(dtype=object)).nunique())
    keyword_count = 0 if cur_kw is None or cur_kw.empty else int(cur_kw.get("keyword", pd.Series(dtype=object)).nunique()) if "keyword" in cur_kw.columns else len(cur_kw.index)
    roas_now = float((perf_items[0].get("cur") or 0))
    roas_base = float((perf_items[0].get("base") or 0))
    cost_now = float(cur.get("cost", 0) or 0)
    cost_base = float(base.get("cost", 0) or 0)
    cost_diff_txt = "비교비 없음"
    if cost_base > 0:
        cost_diff_txt = f"{((cost_now - cost_base) / cost_base) * 100:+.1f}%"
    roas_gap_txt = "비교비 없음"
    if roas_base > 0:
        roas_gap_txt = f"{roas_now - roas_base:+.1f}p"
    render_ops_cards([
        {"title": "분석 대상", "value": f"{campaign_count:,}개 캠페인", "note": f"키워드/소재 후보 {keyword_count:,}건", "tone": "info", "icon": "01"},
        {"title": "비용 변화", "value": cost_diff_txt, "note": f"비교 기간 {b1} ~ {b2}", "tone": "warning" if cost_now > cost_base and cost_base > 0 else "success", "icon": "↕"},
        {"title": "ROAS 변화", "value": roas_gap_txt, "note": "성과 지표 기준 전기 대비", "tone": "success" if roas_now >= roas_base else "danger", "icon": "%"},
    ])

    with st.container(border=True):
        render_toolbar(
            "일자별 성과 추이",
            "광고비와 매출, 유입 지표를 빠르게 전환해 확인합니다.",
            [{"label": selected_type_label, "tone": "primary", "icon": "채널 "}, {"label": cmp_mode, "tone": "info", "icon": "비교 "}],
        )
        if daily_ts is not None and not daily_ts.empty:
            expected_cols = ['imp', 'clk', 'cost', 'conv', 'sales', 'tot_sales', 'tot_conv']
            for c in expected_cols:
                if c not in daily_ts.columns:
                    daily_ts[c] = 0.0
            daily_ts_chart = daily_ts.groupby('dt')[expected_cols].sum().reset_index()
            trend_view = st.segmented_control(
                "추이 보기",
                ["비용 및 매출 추이", "유입 지표 추이"],
                default="비용 및 매출 추이",
                key="overview_trend_view",
                label_visibility="collapsed",
            )
            if trend_view == "유입 지표 추이":
                render_echarts_dual_axis("노출 및 클릭 추이", daily_ts_chart, "dt", "imp", "노출수", "clk", "클릭수", height=320)
            else:
                if combined_toggle:
                    render_echarts_dual_axis("비용 및 총 전환 매출 추이", daily_ts_chart, "dt", "cost", "광고비", "tot_sales", "매출", height=320)
                else:
                    render_echarts_dual_axis("비용 및 구매 완료 매출 추이", daily_ts_chart, "dt", "cost", "광고비", "sales", "매출", height=320)
        else:
            st.info("선택한 기간의 일자별 트렌드 데이터가 존재하지 않습니다.")

    with st.expander("캠페인별 목표 달성 현황", expanded=False):
        st.markdown("<div style='font-size:13px; color:var(--nv-muted); margin-bottom:12px;'>캠페인별 설정된 목표 ROAS 대비 현재 달성 상태를 확인합니다.</div>", unsafe_allow_html=True)
        
        if not cur_camp.empty and "target_roas" in cur_camp.columns and "min_roas" in cur_camp.columns:
            only_miss = st.toggle("목표 미달만 보기", value=False, key="ov_target_only_miss")
            target_df = cur_camp.copy()
            target_df["target_roas"] = pd.to_numeric(target_df["target_roas"], errors="coerce").fillna(0.0)
            target_df["min_roas"] = pd.to_numeric(target_df["min_roas"], errors="coerce").fillna(0.0)
            target_df = target_df[(target_df["target_roas"] > 0) | (target_df["min_roas"] > 0)]
            
            if not target_df.empty:
                target_df["cost"] = safe_numeric_col(target_df, "cost")
                target_df["sales"] = safe_numeric_col(target_df, "sales")
                target_df["conv"] = safe_numeric_col(target_df, "conv")

                target_df["base_roas"] = np.where(target_df["target_roas"] > 0, target_df["target_roas"], target_df["min_roas"])
                target_df["c_roas_purch"] = _safe_div(target_df["sales"], target_df["cost"], 100.0)
                target_df["achieve_raw"] = _safe_div(target_df["c_roas_purch"], target_df["base_roas"], 100.0)
                target_df["achieve"] = target_df["achieve_raw"].clip(upper=100.0)
                
                target_df["status"] = np.where(
                    (target_df["target_roas"] > 0) & (target_df["c_roas_purch"] > target_df["target_roas"]), "초과 달성",
                    np.where(
                        (target_df["target_roas"] > 0) & (target_df["c_roas_purch"] == target_df["target_roas"]), "목표 달성",
                        np.where(
                            (target_df["min_roas"] > 0) & (target_df["c_roas_purch"] >= target_df["min_roas"]), "최소 달성",
                            "미달"
                        )
                    )
                )
                if only_miss: target_df = target_df[target_df["status"] == "미달"]
                target_df = target_df.sort_values(by="cost", ascending=False).head(200)

                if not target_df.empty:
                    disp_target = target_df.rename(columns={
                        "campaign_name": "캠페인명", "achieve": "달성률(%)", "status": "달성 상태",
                        "c_roas_purch": "구매완료 ROAS(%)", "target_roas": "목표 ROAS(%)", "min_roas": "최소 ROAS(%)", "cost": "광고비",
                        "conv": "구매완료수"
                    })
                    
                    disp_cols = ["캠페인명", "달성 상태", "달성률(%)", "구매완료수", "구매완료 ROAS(%)", "최소 ROAS(%)", "목표 ROAS(%)", "광고비"]
                    st.dataframe(
                        disp_target[disp_cols],
                        width="stretch", hide_index=True,
                        column_config={
                            "달성 상태": st.column_config.TextColumn("상태", width="small"),
                            "달성률(%)": st.column_config.ProgressColumn("달성률", format="%.1f%%", min_value=0, max_value=100),
                            "구매완료수": st.column_config.NumberColumn("구매완료수", format="%d"),
                            "구매완료 ROAS(%)": st.column_config.NumberColumn("구매완료 ROAS(%)", format="%.1f%%"),
                            "최소 ROAS(%)": st.column_config.NumberColumn("최소 ROAS(%)", format="%d%%"),
                            "목표 ROAS(%)": st.column_config.NumberColumn("목표 ROAS(%)", format="%d%%"),
                            "광고비": st.column_config.NumberColumn("광고비", format="%d 원")
                        }
                    )
                else: st.info("조건에 맞는 캠페인이 없습니다.")
            else: st.info("안내: 최소/목표 ROAS가 설정된 캠페인이 없습니다. 설정 메뉴에서 계정별 목표를 지정해주세요.")
        else: st.info("안내: 최소/목표 ROAS가 설정된 캠페인이 없습니다. 설정 메뉴에서 계정별 목표를 지정해주세요.")

    # ----------------------------------------------------
    # 상세 데이터 전처리 (지연 조회 전 기본값만 준비)
    # ----------------------------------------------------
    df_display, df_type_display, camp_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    daily_disp, dow_disp, weekly_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    kw_disp = pd.DataFrame() 

    fmt_dict_standard = {
        "노출수": "{:,.0f}", "노출 증감": "{:+.1f}%", "노출 차이": "{:+,.0f}",
        "클릭수": "{:,.0f}", "클릭 증감": "{:+.1f}%", "클릭 차이": "{:+,.0f}",
        "클릭률(%)": "{:,.2f}%", "클릭률 증감": "{:+.2f}%",
        "광고비": "{:,.0f}원", "광고비 증감": "{:+.1f}%", "광고비 차이": "{:+,.0f}원",
        "CPC": "{:,.0f}원", "CPC 증감": "{:+.1f}%", "CPC 차이": "{:+,.0f}원",
        "구매완료수": "{:,.0f}", "구매완료 증감": "{:+.1f}%", "구매완료 차이": "{:+,.0f}",
        "구매 전환율(%)": "{:,.2f}%", "구매 전환율 증감": "{:+.2f}%",
        "구매완료 매출": "{:,.0f}원", "구매완료 매출 증감": "{:+.1f}%", "구매완료 매출 차이": "{:+,.0f}원",
        "구매완료 ROAS(%)": "{:,.1f}%", "구매완료 ROAS 증감": "{:+.1f}%",
        "총 전환수": "{:,.0f}", "총 전환 증감": "{:+.1f}%", "총 전환 차이": "{:+,.0f}",
        "총 전환율(%)": "{:,.2f}%", "총 전환율 증감": "{:+.2f}%",
        "총 전환매출": "{:,.0f}원", "총 매출 증감": "{:+.1f}%", "총 매출 차이": "{:+,.0f}원",
        "통합 ROAS(%)": "{:,.1f}%", "통합 ROAS 증감": "{:+.1f}%",
        "순위 변화": lambda x: f"{x:+.0f}" if pd.notna(x) else "-"
    }

    # ====================================================
    # 퍼널 뷰 배치 및 증감/절대값 토글 로직
    # ====================================================
    st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)
    
    render_toolbar(
        "세부 성과 표",
        "업체, 유형, 기간, 캠페인, 키워드 단위로 같은 KPI 묶음을 비교합니다.",
        [{"label": "절대값/증감 전환", "tone": "primary", "icon": "표시 "}, {"label": f"최대 {f.get('top_n_campaign', 200):,}행", "tone": "info", "icon": "행 "}],
    )
    show_deltas = st.toggle("증감율 보기", value=False, key="ov_abs_toggle")

    def get_funnel_cols(show_deltas):
        cols = []
        cols.extend(["노출수", "노출 증감", "노출 차이"] if show_deltas else ["노출수"])
        cols.extend(["클릭수", "클릭 증감", "클릭 차이"] if show_deltas else ["클릭수"])
        cols.extend(["클릭률(%)", "클릭률 증감"] if show_deltas else ["클릭률(%)"])
        cols.extend(["광고비", "광고비 증감", "광고비 차이"] if show_deltas else ["광고비"])
        cols.extend(["CPC", "CPC 증감", "CPC 차이"] if show_deltas else ["CPC"])
        cols.extend(["평균순위", "순위 변화"] if show_deltas else ["평균순위"])
        
        cols.extend(["구매완료수", "구매완료 증감", "구매완료 차이"] if show_deltas else ["구매완료수"])
        cols.extend(["구매 전환율(%)", "구매 전환율 증감"] if show_deltas else ["구매 전환율(%)"])
        cols.extend(["구매완료 매출", "구매완료 매출 증감", "구매완료 매출 차이"] if show_deltas else ["구매완료 매출"])
        cols.extend(["구매완료 ROAS(%)", "구매완료 ROAS 증감"] if show_deltas else ["구매완료 ROAS(%)"])
        
        cols.extend(["총 전환수", "총 전환 증감", "총 전환 차이"] if show_deltas else ["총 전환수"])
        cols.extend(["총 전환율(%)", "총 전환율 증감"] if show_deltas else ["총 전환율(%)"])
        cols.extend(["총 전환매출", "총 매출 증감", "총 매출 차이"] if show_deltas else ["총 전환매출"])
        cols.extend(["통합 ROAS(%)", "통합 ROAS 증감"] if show_deltas else ["통합 ROAS(%)"])
        return cols

    detail_panel = st.segmented_control(
        "세부 성과 보기",
        ["업체별 요약", "매체·유형별 요약", "기간별 상세", "캠페인 상세 분석", "키워드 상세 분석"],
        default="업체별 요약",
        key="overview_detail_panel",
        label_visibility="collapsed",
    )

    # 탭별 화면 렌더링에 필요한 데이터만 지연 로드 (UI 응답성 최적화)
    if detail_panel in {"업체별 요약", "매체·유형별 요약", "캠페인 상세 분석"}:
        if base_camp is None or base_camp.empty:
            try: base_camp = _cached_campaign_bundle(engine, b1, b2, cids, type_sel)
            except Exception: base_camp = pd.DataFrame()
        df_display, df_type_display, camp_disp = _build_overview_campaign_frames(cur_camp, base_camp, meta)

    if detail_panel == "키워드 상세 분석":
        if base_kw is None or base_kw.empty:
            try: base_kw = _cached_keyword_bundle(engine, b1, b2, cids, type_sel)
            except Exception: base_kw = pd.DataFrame()
        kw_disp = _build_overview_keyword_frames(cur_kw, base_kw)

    if detail_panel == "기간별 상세":
        if base_daily_ts is None or base_daily_ts.empty:
            try: base_daily_ts = _cached_campaign_timeseries(engine, b1, b2, cids, type_sel)
            except Exception: base_daily_ts = pd.DataFrame()
        daily_disp, dow_disp, weekly_disp = _build_overview_timeseries_frames(daily_ts, base_daily_ts)

    # 렌더링 블록
    if detail_panel == "업체별 요약":
        if not df_display.empty:
            view_cols = ["계정명"] + [c for c in get_funnel_cols(show_deltas) if c in df_display.columns]
            disp_df = df_display[view_cols].copy()
            styled_df = disp_df.style.format(fmt_dict_standard)
            styled_df = _apply_overview_delta_styles(styled_df, disp_df)
            _render_overview_sticky_table(styled_df, "계정명", height=420, hide_index=True)
        else:
            st.info("조건에 맞는 데이터가 없습니다.")

    elif detail_panel == "매체·유형별 요약":
        if not df_type_display.empty:
            view_cols = ["캠페인 유형"] + [c for c in get_funnel_cols(show_deltas) if c in df_type_display.columns]
            disp_type_df = df_type_display[view_cols].copy()
            styled_type_df = disp_type_df.style.format(fmt_dict_standard)
            styled_type_df = _apply_overview_delta_styles(styled_type_df, disp_type_df)
            _render_overview_sticky_table(styled_type_df, "캠페인 유형", height=420, hide_index=True)
        else:
            st.info("조건에 맞는 데이터가 없습니다.")

    elif detail_panel == "기간별 상세":
        if any(not df.empty for df in [daily_disp, dow_disp, weekly_disp]):
            period_panel = st.segmented_control(
                "기간 세부 보기",
                ["일자별", "주차별", "요일별"],
                default="일자별",
                key="overview_period_panel",
                label_visibility="collapsed",
            )

            def _display_ts_tab(df, col_name):
                if df.empty:
                    st.info("조건에 맞는 데이터가 없습니다.")
                    return
                v_cols = [col_name] + [c for c in get_funnel_cols(show_deltas) if c in df.columns]
                d_df = df[v_cols].copy()
                s_df = d_df.style.format(fmt_dict_standard)
                s_df = _apply_overview_delta_styles(s_df, d_df)
                _render_overview_sticky_table(s_df, col_name, height=420, hide_index=True)

            if period_panel == "주차별": _display_ts_tab(weekly_disp, "주차")
            elif period_panel == "요일별": _display_ts_tab(dow_disp, "요일명")
            else: _display_ts_tab(daily_disp, "일자")
        else:
            st.info("조건에 맞는 데이터가 없습니다.")

    elif detail_panel == "캠페인 상세 분석":
        if not camp_disp.empty:
            camp_disp_top = camp_disp.head(200)
            view_cols = ["캠페인명"] + [c for c in get_funnel_cols(show_deltas) if c in camp_disp_top.columns]
            disp_camp = camp_disp_top[view_cols].copy()
            styled_camp_df = disp_camp.style.format(fmt_dict_standard)
            styled_camp_df = _apply_overview_delta_styles(styled_camp_df, disp_camp)
            _render_overview_sticky_table(styled_camp_df, "캠페인명", height=460, hide_index=True)
        else:
            st.info("조건에 맞는 데이터가 없습니다.")

    elif detail_panel == "키워드 상세 분석":
        if not kw_disp.empty:
            kw_disp_top = kw_disp.head(200)
            view_cols = ["키워드"] + [c for c in get_funnel_cols(show_deltas) if c in kw_disp_top.columns]
            disp_kw = kw_disp_top[view_cols].copy()
            styled_kw_df = disp_kw.style.format(fmt_dict_standard)
            styled_kw_df = _apply_overview_delta_styles(styled_kw_df, disp_kw)
            _render_overview_sticky_table(styled_kw_df, "키워드", height=460, hide_index=True)
        else:
            st.info("조건에 맞는 데이터가 없습니다.")


    # ----------------------------------------------------
    # 엑셀 다운로드 (화면에 표시되지 않은 탭의 데이터도 모두 강제로 로드하여 시트별로 저장)
    # ----------------------------------------------------
    st.markdown("<div style='height: 24px;'></div>", unsafe_allow_html=True)
    
    # 누락 방지를 위한 전체 데이터 프레임 강제 동기화 
    if base_camp is None or base_camp.empty:
        try: base_camp = _cached_campaign_bundle(engine, b1, b2, cids, type_sel)
        except Exception: base_camp = pd.DataFrame()
    if df_display.empty or camp_disp.empty:
        df_display, df_type_display, camp_disp = _build_overview_campaign_frames(cur_camp, base_camp, meta)

    if base_kw is None or base_kw.empty:
        try: base_kw = _cached_keyword_bundle(engine, b1, b2, cids, type_sel)
        except Exception: base_kw = pd.DataFrame()
    if kw_disp.empty:
        kw_disp = _build_overview_keyword_frames(cur_kw, base_kw)

    if base_daily_ts is None or base_daily_ts.empty:
        try: base_daily_ts = _cached_campaign_timeseries(engine, b1, b2, cids, type_sel)
        except Exception: base_daily_ts = pd.DataFrame()
    if daily_disp.empty:
        daily_disp, dow_disp, weekly_disp = _build_overview_timeseries_frames(daily_ts, base_daily_ts)

    has_data_to_export = any([not df_display.empty, not df_type_display.empty, not camp_disp.empty, not daily_disp.empty, not kw_disp.empty])
    if has_data_to_export:
        with st.container(border=True):
            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:8px;'>엑셀 데이터 일괄 다운로드</div>", unsafe_allow_html=True)
            st.markdown("<div style='font-size:12px; color:var(--nv-muted); margin-bottom:10px;'>계정/유형/캠페인/키워드/일자/요일별 상세 데이터를 하나의 엑셀 파일로 내려받습니다.</div>", unsafe_allow_html=True)
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer) as writer:
                if not df_display.empty: format_for_csv(df_display).to_excel(writer, sheet_name='계정별_성과상세', index=False)
                if not df_type_display.empty: format_for_csv(df_type_display).to_excel(writer, sheet_name='유형별_성과상세', index=False)
                if not camp_disp.empty: format_for_csv(camp_disp).to_excel(writer, sheet_name='캠페인별_성과상세', index=False)
                if not kw_disp.empty: format_for_csv(kw_disp).to_excel(writer, sheet_name='키워드별_성과상세', index=False)
                if not daily_disp.empty: format_for_csv(daily_disp).to_excel(writer, sheet_name='일자별_성과상세', index=False)
                if not dow_disp.empty:
                    dow_export = dow_disp.drop(columns=['요일']) if '요일' in dow_disp.columns else dow_disp
                    format_for_csv(dow_export).to_excel(writer, sheet_name='요일별_성과상세', index=False)
                if not weekly_disp.empty: format_for_csv(weekly_disp).to_excel(writer, sheet_name='주간_성과상세', index=False)
            st.download_button("통합 엑셀 다운로드", data=excel_buffer.getvalue(), file_name=f"통합_상세_성과보고서_{f['start']}_{f['end']}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width="stretch")

    with st.expander("텍스트 보고서 생성", expanded=False):
        include_campaign_breakdown = st.checkbox(
            "캠페인별 성과도 함께 생성",
            key="overview_include_campaign_text_report",
            value=False,
            help="기존 요약 아래에 캠페인별 성과 요약을 동일한 형식으로 추가합니다.",
        )
        generate_text_report = st.button("텍스트 보고서 생성", key="overview_generate_text_report", use_container_width=True)
        top_kw_str = "없음"
        try:
            top_kw_str = _resolve_overview_report_top_keywords(
                engine,
                f["start"],
                f["end"],
                tuple(cids),
                tuple(type_sel),
                selected_type_label,
                diag=diag,
                force_refresh=generate_text_report,
            )
        except Exception as e:
            _diag_add(diag, "키워드 번들", "error", 0, "query_keyword_bundle", f"{type(e).__name__}: {e}")
            top_kw_str = "없음"

        shop_kw_str = "없음"
        campaign_top_shopping_query_map: dict[str, str] = {}
        if cids:
            try:
                shop_terms_df = query_shopping_search_terms(engine, f["start"], f["end"], tuple(cids))
                if shop_terms_df is not None and not shop_terms_df.empty:
                    metric_col = "purchase_conv" if "purchase_conv" in shop_terms_df.columns else ("total_conv" if "total_conv" in shop_terms_df.columns else None)
                    if metric_col is not None:
                        top_shop_terms = (
                            shop_terms_df.groupby("query_text", dropna=False)[metric_col]
                            .sum()
                            .reset_index()
                        )
                        top_shop_terms = top_shop_terms[top_shop_terms[metric_col] > 0].sort_values(metric_col, ascending=False).head(3)
                        if not top_shop_terms.empty:
                            shop_kw_str = ", ".join([f"{r['query_text']}({int(r[metric_col]):,}회)" for _, r in top_shop_terms.iterrows()])
                    campaign_top_shopping_query_map = _get_campaign_top_shopping_query_map(shop_terms_df, top_n=3)
            except Exception:
                pass

        campaign_top_keyword_map = _get_campaign_top_keyword_map(cur_kw, top_n=5)
        is_shopping_only = ("쇼핑" in selected_type_label and "파워링크" not in selected_type_label and selected_type_label != "전체 유형")

        if is_shopping_only:
            report_text = "\n".join([
                f"[ {selected_type_label} 성과 요약 ]",
                _format_report_line("노출수", f"{int(float(cur.get('imp', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(cur.get('clk', 0))):,}"),
                _format_report_line("클릭률", f"{float(cur.get('ctr', 0)):.1f}%"),
                _format_report_line("광고 소진비용", f"{int(float(cur.get('cost', 0))):,}원"),
                _format_report_line("구매완료수", _format_report_count(cur.get('conv', 0.0))),
                _format_report_line("구매완료 매출", f"{int(float(cur.get('sales', 0))):,}원"),
                _format_report_line("구매 ROAS", f"{float(cur.get('roas', 0)):.1f}%"),
                _format_report_line("주요 전환 키워드", shop_kw_str)
            ])
        else:
            if combined_toggle or kpi_mode != "shopping_purchase":
                c_conv_val = cur.get('tot_conv', 0)
                c_sales_val = cur.get('tot_sales', 0)
                c_roas_val = cur.get('tot_roas', 0)
            else:
                c_conv_val = cur.get('conv', 0)
                c_sales_val = cur.get('sales', 0)
                c_roas_val = cur.get('roas', 0)

            report_text = "\n".join([
                f"[ {selected_type_label} 성과 요약 ]",
                _format_report_line("노출수", f"{int(float(cur.get('imp', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(cur.get('clk', 0))):,}"),
                _format_report_line("클릭률", f"{float(cur.get('ctr', 0)):.1f}%"),
                _format_report_line("광고 소진비용", f"{int(float(cur.get('cost', 0))):,}원"),
                _format_report_line("전환수", f"{float(c_conv_val):.1f}"),
                _format_report_line("총전환매출", f"{int(float(c_sales_val)):,}원"),
                _format_report_line("ROAS", f"{float(c_roas_val):.1f}%"),
                _format_report_line("주요 유입 키워드", top_kw_str)
            ])
            
        if include_campaign_breakdown:
            campaign_report_text = _build_campaign_report_text(
                cur_camp=cur_camp,
                selected_type_label=selected_type_label,
                is_shopping_only=is_shopping_only,
                combined_toggle=combined_toggle,
                kpi_mode=kpi_mode,
                campaign_top_keyword_map=campaign_top_keyword_map,
                campaign_top_shopping_query_map=campaign_top_shopping_query_map,
            )
            if campaign_report_text:
                report_text = f"{report_text}\n\n{campaign_report_text}"

        st.code(report_text, language="text")

    _render_diag_panel(diag, enabled=bool(f.get("show_diagnostics", False)))
