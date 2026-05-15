# -*- coding: utf-8 -*-
"""view_campaign.py - Campaign performance page view (Minimal Device Share UI & Robust SQL)."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import streamlit_compat  # noqa: F401
from typing import Dict
from datetime import date
from html import escape

from data import (
    query_campaign_bundle,
    query_keyword_bundle,
    query_ad_bundle,
    query_campaign_off_log,
    load_dim_campaign,
    sql_read,
    table_exists,
    get_table_columns,
    _sql_in_str_list,
    format_currency,
)
from page_helpers import get_dynamic_cmp_options, period_compare_range, _perf_common_merge_meta
from ui import render_kpi_strip, render_toolbar, safe_numeric_col

def _campaign_fetch_limit(top_n: int) -> int:
    try:
        top_n = int(top_n or 0)
    except Exception:
        top_n = 0
    top_n = max(top_n, 1)
    return min(max(top_n * 3, 800), 1800)


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
        st.caption("조회 이상값이 보일 때, 어떤 원천/단계에서 비거나 실패했는지 확인하는 용도입니다.")
        st.dataframe(df, width="stretch", hide_index=True)


FMT_DICT = {
    "노출": "{:,.0f}", "노출 증감": "{:+.0f}%", "노출 차이": "{:+,.0f}",
    "클릭": "{:,.0f}", "클릭 증감": "{:+.0f}%", "클릭 차이": "{:+,.0f}",
    "CTR(%)": "{:,.2f}%", 
    "광고비": "{:,.0f}원", "광고비 증감": "{:+.0f}%", "광고비 차이": "{:+,.0f}원",
    "CPC(원)": "{:,.0f}원", "CPC 증감": "{:+.0f}%", "CPC 차이": "{:+,.0f}원",
    "구매완료수": "{:,.0f}", "구매 증감": "{:+.0f}%", "구매 차이": "{:+,.0f}",
    "구매완료 매출": "{:,.0f}원", "구매 매출 증감": "{:+.0f}%", "구매 매출 차이": "{:+,.0f}원",
    "구매 ROAS(%)": "{:,.0f}%", "구매 ROAS 증감": "{:+.0f}%",
    "장바구니수": "{:,.0f}", "장바구니 증감": "{:+.0f}%", "장바구니 차이": "{:+,.0f}",
    "장바구니 매출액": "{:,.0f}원", "장바구니 ROAS(%)": "{:,.0f}%",
    "위시리스트수": "{:,.0f}", "위시리스트 증감": "{:+.0f}%", "위시리스트 차이": "{:+,.0f}",
    "위시리스트 매출액": "{:,.0f}원", "위시리스트 ROAS(%)": "{:,.0f}%",
    "총 전환수": "{:,.0f}", "총 전환 증감": "{:+.0f}%", "총 전환 차이": "{:+,.0f}",
    "총 전환매출": "{:,.0f}원", "총 매출 증감": "{:+.0f}%", "총 매출 차이": "{:+,.0f}원",
    "통합 ROAS(%)": "{:,.0f}%", "통합 ROAS 증감": "{:+.0f}%",
    "순위 변화": lambda x: f"{x:+.0f}" if pd.notna(x) else "-"
}

def _style_delta_numeric(val):
    try: v = float(val)
    except: return ''
    if pd.isna(v) or v == 0: return ''
    return 'color: #1A73E8; font-weight: 700;' if v > 0 else 'color: #EA4335; font-weight: 700;'

def _style_delta_numeric_neg(val):
    try: v = float(val)
    except: return ''
    if pd.isna(v) or v == 0: return ''
    return 'color: #EA4335; font-weight: 700;' if v > 0 else 'color: #1A73E8; font-weight: 700;'

def _apply_delta_styles(styler, df: pd.DataFrame):
    pos_cols = [c for c in ['노출 증감', '노출 차이', '클릭 증감', '클릭 차이', '장바구니 증감', '장바구니 차이', '위시리스트 증감', '위시리스트 차이', '구매 증감', '구매 차이', '구매 매출 증감', '구매 매출 차이', '구매 ROAS 증감', '총 전환 증감', '총 전환 차이', '총 매출 증감', '총 매출 차이', '통합 ROAS 증감'] if c in df.columns]
    neg_cols = [c for c in ['광고비 증감', '광고비 차이', 'CPC 증감', 'CPC 차이', '순위 변화'] if c in df.columns]
    try:
        if pos_cols: styler = styler.map(_style_delta_numeric, subset=pos_cols)
        if neg_cols: styler = styler.map(_style_delta_numeric_neg, subset=neg_cols)
    except AttributeError:
        if pos_cols: styler = styler.applymap(_style_delta_numeric, subset=pos_cols)
        if neg_cols: styler = styler.applymap(_style_delta_numeric_neg, subset=neg_cols)
    return styler



def _campaign_fast_col_config(df: pd.DataFrame, first_col: str | None = None) -> dict:
    cfg: dict = {}
    if first_col and first_col in df.columns:
        cfg[first_col] = st.column_config.TextColumn(first_col, pinned=True, width="medium")
    pct_cols = {"구매 ROAS(%)", "장바구니 ROAS(%)", "위시리스트 ROAS(%)", "통합 ROAS(%)"}
    diff_pct_cols = {c for c in df.columns if "증감" in c and "차이" not in c}
    currency_cols = {"광고비", "CPC(원)", "구매완료 매출", "장바구니 매출액", "위시리스트 매출액", "총 전환매출"}
    currency_diff_cols = {c for c in df.columns if c.endswith("차이") and ("매출" in c or "광고비" in c or "CPC" in c)}
    count_cols = {"노출", "클릭", "구매완료수", "장바구니수", "위시리스트수", "총 전환수"}
    count_diff_cols = {c for c in df.columns if c.endswith("차이") and c not in currency_diff_cols}
    for c in df.columns:
        if c in cfg:
            continue
        if c == "CTR(%)":
            cfg[c] = st.column_config.NumberColumn(c, format="%,.2f %%")
        elif c in pct_cols or c in diff_pct_cols:
            cfg[c] = st.column_config.NumberColumn(c, format="%,.0f %%")
        elif c in currency_cols or c in currency_diff_cols:
            cfg[c] = st.column_config.NumberColumn(c, format="%,.0f 원")
        elif c in count_cols or c in count_diff_cols or c == "순위 변화":
            cfg[c] = st.column_config.NumberColumn(c, format="%,.0f")
        elif c == "평균순위":
            cfg[c] = st.column_config.TextColumn(c)
    if "지출 비중(%)" in df.columns:
        cfg["지출 비중(%)"] = st.column_config.ProgressColumn("지출 비중(%)", format="%,.0f %%", min_value=0, max_value=100)
    return cfg


def _campaign_sticky_cfg(first_col: str | None = None) -> dict:
    if not first_col:
        return {}
    return {first_col: st.column_config.TextColumn(first_col, pinned=True, width="medium")}


def _render_campaign_sticky_table(df: pd.DataFrame, first_col: str, apply_delta_styles: bool = False):
    fmt_dict = {k: v for k, v in FMT_DICT.items() if k in df.columns}
    styled = df.style.format(fmt_dict)
    if apply_delta_styles:
        styled = _apply_delta_styles(styled, df)
    st.dataframe(styled, width="stretch", hide_index=True, column_config=_campaign_sticky_cfg(first_col))

def _format_avg_rank(value):
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num) or num <= 0: return "미수집"
    return f"{num:.0f}위"


def _weighted_avg_rank_by_keys(df: pd.DataFrame, keys: list[str], imp_col: str = "imp", rank_col: str = "avg_rank") -> pd.DataFrame:
    if df is None or df.empty or rank_col not in df.columns or not keys:
        return pd.DataFrame(columns=keys + [rank_col])
    usable_keys = [k for k in keys if k in df.columns]
    if not usable_keys:
        return pd.DataFrame(columns=keys + [rank_col])
    tmp = df.copy()
    tmp[imp_col] = safe_numeric_col(tmp, imp_col, default=0.0) if imp_col in tmp.columns else pd.Series([0.0] * len(tmp.index))
    tmp[rank_col] = pd.to_numeric(tmp[rank_col], errors="coerce")
    tmp["_rank_imp"] = tmp[rank_col].fillna(0.0) * tmp[imp_col]
    grp = tmp.groupby(usable_keys, as_index=False, dropna=False)[["_rank_imp", imp_col]].sum()
    grp[rank_col] = np.where(grp[imp_col] > 0, grp["_rank_imp"] / grp[imp_col], np.nan)
    return grp[usable_keys + [rank_col]]


def _add_perf_metrics(view: pd.DataFrame) -> pd.DataFrame:
    has_total_cols = {"tot_conv", "tot_sales"}.issubset(set(view.columns))
    for c in ["광고비", "구매완료 매출", "장바구니 매출액", "위시리스트 매출액", "노출", "클릭", "구매완료수", "장바구니수", "위시리스트수", "tot_conv", "tot_sales"]:
        view[c] = safe_numeric_col(view, c) if c in view.columns else pd.Series([0.0] * len(view.index))

    if has_total_cols:
        view["총 전환수"] = view["tot_conv"]
        view["총 전환매출"] = view["tot_sales"]
    else:
        view["총 전환수"] = safe_numeric_col(view, "구매완료수") + safe_numeric_col(view, "장바구니수") + safe_numeric_col(view, "위시리스트수")
        view["총 전환매출"] = safe_numeric_col(view, "구매완료 매출") + safe_numeric_col(view, "장바구니 매출액") + safe_numeric_col(view, "위시리스트 매출액")

    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    
    view["구매 ROAS(%)"] = np.where(view["광고비"] > 0, (safe_numeric_col(view, "구매완료 매출") / view["광고비"]) * 100, 0.0)
    view["장바구니 ROAS(%)"] = np.where(view["광고비"] > 0, (safe_numeric_col(view, "장바구니 매출액") / view["광고비"]) * 100, 0.0)
    view["위시리스트 ROAS(%)"] = np.where(view["광고비"] > 0, (safe_numeric_col(view, "위시리스트 매출액") / view["광고비"]) * 100, 0.0)
    view["통합 ROAS(%)"] = np.where(view["광고비"] > 0, (view["총 전환매출"] / view["광고비"]) * 100, 0.0)
    return view

def _apply_comparison_metrics(view_df: pd.DataFrame, base_df: pd.DataFrame, merge_keys: list) -> pd.DataFrame:
    if view_df.empty: return view_df

    for k in merge_keys:
        if k in view_df.columns: view_df[k] = view_df[k].astype(str)
        if k in base_df.columns: base_df[k] = base_df[k].astype(str)

    val_cols = ['imp', 'clk', 'cost', 'cart_conv', 'cart_sales', 'wishlist_conv', 'wishlist_sales', 'conv', 'sales', 'tot_conv', 'tot_sales']
    for c in val_cols:
        if c in base_df.columns: base_df[c] = pd.to_numeric(base_df[c], errors='coerce').fillna(0)

    agg_dict = {c: 'sum' for c in val_cols if c in base_df.columns}

    if not base_df.empty and merge_keys:
        base_agg = base_df.groupby(merge_keys).agg(agg_dict).reset_index()
        if 'avg_rank' in base_df.columns:
            rank_agg = _weighted_avg_rank_by_keys(base_df, merge_keys)
            if not rank_agg.empty:
                base_agg = base_agg.merge(rank_agg, on=merge_keys, how="left")
                agg_dict['avg_rank'] = 'weighted'
        base_agg = base_agg.rename(columns={c: f"b_{c}" for c in agg_dict.keys()})
        merged = pd.merge(view_df, base_agg, on=merge_keys, how='left')
    else:
        merged = view_df.copy()

    for c in val_cols:
        bc = f"b_{c}"
        if bc not in merged.columns: merged[bc] = 0
        merged[bc] = pd.to_numeric(merged[bc], errors='coerce').fillna(0)

    if 'b_avg_rank' not in merged.columns: merged['b_avg_rank'] = np.nan

    def _vec_pct_diff(c, b):
        diff = c - b
        safe_b = np.where(b == 0, 1, b)
        pct = np.where(b == 0, np.where(c > 0, 100.0, 0.0), (diff / safe_b) * 100.0)
        return pct, diff

    c_imp, b_imp = merged.get('노출', 0), merged.get('b_imp', 0)
    c_clk, b_clk = merged.get('클릭', 0), merged.get('b_clk', 0)
    c_cost, b_cost = merged.get('광고비', 0), merged.get('b_cost', 0)
    c_cpc = np.where(c_clk > 0, c_cost / c_clk, 0)
    b_cpc = np.where(b_clk > 0, b_cost / b_clk, 0)

    merged['노출 증감'], merged['노출 차이'] = _vec_pct_diff(c_imp, b_imp)
    merged['클릭 증감'], merged['클릭 차이'] = _vec_pct_diff(c_clk, b_clk)
    merged['광고비 증감'], merged['광고비 차이'] = _vec_pct_diff(c_cost, b_cost)
    merged['CPC 증감'], merged['CPC 차이'] = _vec_pct_diff(c_cpc, b_cpc)

    c_cart, b_cart = merged.get('장바구니수', 0), merged.get('b_cart_conv', 0)
    c_wish, b_wish = merged.get('위시리스트수', 0), merged.get('b_wishlist_conv', 0)
    c_conv, b_conv = merged.get('구매완료수', 0), merged.get('b_conv', 0)
    c_sales, b_sales = merged.get('구매완료 매출', 0), merged.get('b_sales', 0)
    c_tconv, b_tconv = merged.get('총 전환수', 0), merged.get('b_tot_conv', merged.get('b_conv', 0) + merged.get('b_cart_conv', 0) + merged.get('b_wishlist_conv', 0))
    c_tsales, b_tsales = merged.get('총 전환매출', 0), merged.get('b_tot_sales', merged.get('b_sales', 0) + merged.get('b_cart_sales', 0) + merged.get('b_wishlist_sales', 0))

    merged['장바구니 증감'], merged['장바구니 차이'] = _vec_pct_diff(c_cart, b_cart)
    merged['위시리스트 증감'], merged['위시리스트 차이'] = _vec_pct_diff(c_wish, b_wish)
    merged['구매 증감'], merged['구매 차이'] = _vec_pct_diff(c_conv, b_conv)
    merged['구매 매출 증감'], merged['구매 매출 차이'] = _vec_pct_diff(c_sales, b_sales)
    merged['총 전환 증감'], merged['총 전환 차이'] = _vec_pct_diff(c_tconv, b_tconv)
    merged['총 매출 증감'], merged['총 매출 차이'] = _vec_pct_diff(c_tsales, b_tsales)

    c_roas = np.where(c_cost > 0, (c_sales / c_cost) * 100, 0)
    b_roas = np.where(b_cost > 0, (b_sales / b_cost) * 100, 0)
    merged['구매 ROAS 증감'] = c_roas - b_roas

    c_troas = np.where(c_cost > 0, (c_tsales / c_cost) * 100, 0)
    b_troas = np.where(b_cost > 0, (b_tsales / b_cost) * 100, 0)
    merged['통합 ROAS 증감'] = c_troas - b_troas

    if "avg_rank" in merged.columns:
        if "평균순위" not in merged.columns: merged['평균순위'] = merged['avg_rank'].apply(_format_avg_rank)
        merged['순위 변화'] = np.where((merged['b_avg_rank'] > 0) & (merged['avg_rank'] > 0), merged['avg_rank'] - merged['b_avg_rank'], np.nan)

    return merged

def _normalize_merge_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    out = df.copy()
    for k in keys:
        if k in out.columns: out[k] = out[k].astype(str)
    return out

def _keyword_rank_by_keys(detail_bundle: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    return _weighted_avg_rank_by_keys(detail_bundle, keys)

def _compact_df_height(df: pd.DataFrame, min_height: int = 72, max_height: int = 260) -> int:
    try:
        rows = len(df.index)
        if rows <= 0: return min_height
        if rows == 1: return 74
        if rows == 2: return 108
        return max(min_height, min(40 + rows * 34, max_height))
    except: return min_height

def _normalize_device_label(v: str) -> str:
    s = str(v or '').strip().upper()
    if s in {'M', 'MO', 'MOBILE', '모바일'} or 'MOBILE' in s or '모바일' in s:
        return 'MO'
    if s in {'P', 'PC'} or 'PC' in s:
        return 'PC'
    return '기타'

def _expand_campaign_type_values(type_sel: tuple) -> list[str]:
    mapping = {
        "파워링크": ["파워링크", "WEB_SITE"],
        "쇼핑검색": ["쇼핑검색", "SHOPPING"],
        "파워컨텐츠": ["파워컨텐츠", "POWER_CONTENTS"],
        "브랜드검색": ["브랜드검색", "BRAND_SEARCH"],
        "플레이스": ["플레이스", "PLACE"],
        "WEB_SITE": ["WEB_SITE", "파워링크"],
        "SHOPPING": ["SHOPPING", "쇼핑검색"],
        "POWER_CONTENTS": ["POWER_CONTENTS", "파워컨텐츠"],
        "BRAND_SEARCH": ["BRAND_SEARCH", "브랜드검색"],
        "PLACE": ["PLACE", "플레이스"],
    }
    out: list[str] = []
    for v in type_sel or ():
        s = str(v).strip()
        if not s:
            continue
        out.append(s)
        out.extend(mapping.get(s, []))
    seen = set()
    deduped = []
    for x in out:
        if x not in seen:
            seen.add(x)
            deduped.append(x)
    return deduped

def _query_device_breakdown(engine, d1, d2, cids: tuple, type_sel: tuple, diag: list | None = None) -> pd.DataFrame:
    params = {'d1': str(d1), 'd2': str(d2)}
    type_vals = _expand_campaign_type_values(type_sel)

    if table_exists(engine, 'fact_campaign_device_daily'):
        where_cid = f"AND CAST(f.customer_id AS TEXT) IN ({_sql_in_str_list(list(cids))})" if cids else ''
        join_sql = ''
        type_filter = ''
        if type_vals:
            cp_col = _campaign_type_column(engine)
            join_sql = ' LEFT JOIN dim_campaign c ON CAST(f.customer_id AS TEXT) = CAST(c.customer_id AS TEXT) AND CAST(f.campaign_id AS TEXT) = CAST(c.campaign_id AS TEXT) '
            type_list = _sql_in_str_list(type_vals)
            type_filter = f"""
                AND (
                    COALESCE(CAST(c.{cp_col} AS TEXT), '') IN ({type_list})
                    OR (
                        CASE
                            WHEN COALESCE(CAST(c.{cp_col} AS TEXT), '') = 'WEB_SITE' THEN '파워링크'
                            WHEN COALESCE(CAST(c.{cp_col} AS TEXT), '') = 'SHOPPING' THEN '쇼핑검색'
                            WHEN COALESCE(CAST(c.{cp_col} AS TEXT), '') = 'POWER_CONTENTS' THEN '파워컨텐츠'
                            WHEN COALESCE(CAST(c.{cp_col} AS TEXT), '') = 'BRAND_SEARCH' THEN '브랜드검색'
                            WHEN COALESCE(CAST(c.{cp_col} AS TEXT), '') = 'PLACE' THEN '플레이스'
                            ELSE COALESCE(CAST(c.{cp_col} AS TEXT), '')
                        END
                    ) IN ({type_list})
                )
            """
        sql = f"""
            SELECT COALESCE(NULLIF(TRIM(f.device_name), ''), '기타') AS device_name,
                   SUM(CAST(COALESCE(f.cost,0) AS NUMERIC)) AS cost
            FROM fact_campaign_device_daily f
            {join_sql}
            WHERE f.dt BETWEEN :d1 AND :d2 {where_cid} {type_filter}
            GROUP BY COALESCE(NULLIF(TRIM(f.device_name), ''), '기타')
            HAVING SUM(CAST(COALESCE(f.cost,0) AS NUMERIC)) > 0
            ORDER BY SUM(CAST(COALESCE(f.cost,0) AS NUMERIC)) DESC
        """
        try:
            df = sql_read(engine, sql, params)
            if not df.empty:
                df['device_name'] = df['device_name'].apply(_normalize_device_label)
                df['cost'] = pd.to_numeric(df['cost'], errors='coerce').fillna(0)
                df = df.groupby('device_name', as_index=False)['cost'].sum().sort_values('cost', ascending=False)
                out = df[df['cost'] > 0]
                _diag_add(diag, '기기비중', 'ok' if not out.empty else 'zero_data', len(out.index), 'fact_campaign_device_daily', '기기별 원천 사용')
                return out
        except Exception as e:
            _diag_add(diag, '기기비중', 'error', 0, 'fact_campaign_device_daily', f"{type(e).__name__}: {e}")
            pass

    if table_exists(engine, 'fact_media_daily'):
        cols = get_table_columns(engine, 'fact_media_daily')
        if 'device_name' in cols:
            where_cid = f"AND CAST(customer_id AS TEXT) IN ({_sql_in_str_list(list(cids))})" if cids else ''
            
            cp_col = "campaign_type"
            if "campaign_tp" in cols: cp_col = "campaign_tp"
            elif "campaign_type_label" in cols: cp_col = "campaign_type_label"
                
            type_filter = f"AND CAST({cp_col} AS TEXT) IN ({_sql_in_str_list(type_vals)})" if type_vals and cp_col in cols else ''
            sql = f"SELECT COALESCE(NULLIF(TRIM(device_name), ''), '기타') AS device_name, SUM(CAST(COALESCE(cost,0) AS NUMERIC)) AS cost FROM fact_media_daily WHERE dt BETWEEN :d1 AND :d2 {where_cid} {type_filter} GROUP BY COALESCE(NULLIF(TRIM(device_name), ''), '기타') HAVING SUM(CAST(COALESCE(cost,0) AS NUMERIC)) > 0 ORDER BY SUM(CAST(COALESCE(cost,0) AS NUMERIC)) DESC"
            try:
                df = sql_read(engine, sql, params)
                if not df.empty:
                    df['device_name'] = df['device_name'].apply(_normalize_device_label)
                    df['cost'] = pd.to_numeric(df['cost'], errors='coerce').fillna(0)
                    df = df.groupby('device_name', as_index=False)['cost'].sum().sort_values('cost', ascending=False)
                    out = df[df['cost'] > 0]
                    _diag_add(diag, '기기비중', 'ok' if not out.empty else 'zero_data', len(out.index), 'fact_media_daily', 'fact_campaign_device_daily fallback')
                    return out
            except Exception as e:
                _diag_add(diag, '기기비중', 'error', 0, 'fact_media_daily', f"{type(e).__name__}: {e}")
                pass
    _diag_add(diag, '기기비중', 'zero_data', 0, 'none', '기기별 광고비 데이터 없음')
    return pd.DataFrame()


def _render_device_share_panel(device_df: pd.DataFrame) -> None:
    if device_df is None or device_df.empty:
        st.info('기기별 데이터가 없어 지출 비중을 표시할 수 없습니다.')
        return

    df = device_df.copy()
    df['cost'] = pd.to_numeric(df['cost'], errors='coerce').fillna(0)
    df = df.groupby('device_name', as_index=False)['cost'].sum()
    total = float(df['cost'].sum())
    if total <= 0:
        st.info('기기별 데이터가 없어 지출 비중을 표시할 수 없습니다.')
        return

    df['share'] = (df['cost'] / total) * 100.0
    order = ['PC', 'MO', '기타']
    color_map = {'PC': '#3B82F6', 'MO': '#93C5FD', '기타': '#E5E7EB'}
    df['ord'] = df['device_name'].map({k: i for i, k in enumerate(order)}).fillna(99)
    df = df.sort_values(['ord', 'cost'], ascending=[True, False]).reset_index(drop=True)

    top = df.sort_values('cost', ascending=False).iloc[0]
    dominant = str(top['device_name'])
    dominant_share = float(top['share'])

    legends = []
    for _, row in df.iterrows():
        name = str(row['device_name'])
        color = color_map.get(name, '#E5E7EB')
        legends.append(
            "<div style='display:flex; align-items:center; gap:6px;'>"
            f"<div style='width:8px; height:8px; border-radius:50%; background-color:{color};'></div>"
            f"<div style='font-size:13px; font-weight:500; color:#4B5563;'>{escape(name)} <span style='font-weight:700; color:#111827; margin-left:4px;'>{row['share']:.1f}%</span></div>"
            "</div>"
        )

    bar_segments = ''.join(
        f"<div style='height:100%; background-color:{color_map.get(str(row['device_name']), '#E5E7EB')}; width:{max(float(row['share']), 0):.4f}%;'></div>"
        for _, row in df.iterrows()
    )

    html_str = (
        "<div style='display: flex; flex-direction: column; gap: 14px; padding: 4px 0;'>"
        "<div style='display: flex; justify-content: space-between; align-items: flex-end;'>"
        "<div>"
        "<div style='font-size: 12px; font-weight: 500; color: #6B7280; margin-bottom: 4px;'>총 광고비</div>"
        f"<div style='font-size: 20px; font-weight: 700; color: #111827; line-height: 1;'>{int(total):,}원</div>"
        "</div>"
        "<div style='text-align: right;'>"
        "<div style='font-size: 12px; font-weight: 500; color: #6B7280; margin-bottom: 4px;'>우세 기기</div>"
        f"<div style='font-size: 14px; font-weight: 600; color: #111827; line-height: 1;'>{escape(dominant)}</div>"
        "</div>"
        "</div>"
        "<div style='width: 100%; background-color: #F3F4F6; border-radius: 999px; overflow: hidden; display: flex; height: 8px;'>"
        f"{bar_segments}"
        "</div>"
        "<div style='display: flex; gap: 16px; margin-top: 4px;'>"
        f"{''.join(legends)}"
        "</div>"
        "</div>"
    )

    st.markdown(html_str, unsafe_allow_html=True)


def _campaign_type_column(engine) -> str:
    cols = get_table_columns(engine, "dim_campaign")
    return "campaign_tp" if "campaign_tp" in cols else ("campaign_type_label" if "campaign_type_label" in cols else "campaign_type")


def _query_keyword_detail_for_campaign(engine, d1, d2, customer_id: str, campaign_id: str) -> pd.DataFrame:
    if not table_exists(engine, "fact_keyword_daily"):
        return pd.DataFrame()
    cp_col = _campaign_type_column(engine)
    kw_fact_cols = get_table_columns(engine, "fact_keyword_daily")
    expr = {
        "purchase_conv_expr": "COALESCE(conv,0)",
        "purchase_sales_expr": "COALESCE(sales,0)",
        "total_conv_expr": "COALESCE(tot_conv, COALESCE(conv,0)+COALESCE(cart_conv,0)+COALESCE(wishlist_conv,0))",
        "total_sales_expr": "COALESCE(tot_sales, COALESCE(sales,0)+COALESCE(cart_sales,0)+COALESCE(wishlist_sales,0))",
        "cart_conv_expr": "COALESCE(cart_conv,0)",
        "cart_sales_expr": "COALESCE(cart_sales,0)",
        "wish_conv_expr": "COALESCE(wishlist_conv,0)",
        "wish_sales_expr": "COALESCE(wishlist_sales,0)",
    }
    try:
        from data import _strict_conv_selects
        expr = _strict_conv_selects(kw_fact_cols)
    except Exception:
        pass
    rank_col = next((c for c in ["avg_rank", "avg_rnk", "averageposition", "average_position", "avgrnk"] if c in kw_fact_cols), None)
    rank_agg_sql = f", CASE WHEN SUM(imp) > 0 THEN SUM(COALESCE({rank_col}, 0) * imp) / SUM(imp) ELSE NULL END as avg_rank" if rank_col else ""
    rank_select_sql = ", agg.avg_rank" if rank_col else ""
    sql = f"""
        WITH agg AS (
            SELECT customer_id, keyword_id,
                   SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost,
                   SUM({expr['purchase_conv_expr']}) as conv,
                   SUM({expr['purchase_sales_expr']}) as sales,
                   SUM({expr['total_conv_expr']}) as tot_conv,
                   SUM({expr['total_sales_expr']}) as tot_sales,
                   SUM({expr['cart_conv_expr']}) as cart_conv,
                   SUM({expr['cart_sales_expr']}) as cart_sales,
                   SUM({expr['wish_conv_expr']}) as wishlist_conv,
                   SUM({expr['wish_sales_expr']}) as wishlist_sales
                   {rank_agg_sql}
            FROM fact_keyword_daily
            WHERE dt BETWEEN :d1 AND :d2 AND customer_id = :cid
            GROUP BY customer_id, keyword_id
        )
        SELECT
            agg.customer_id, a.campaign_id, k.adgroup_id, agg.keyword_id,
            c.campaign_name, c.{cp_col} as campaign_type_label,
            a.adgroup_name, k.keyword,
            agg.imp, agg.clk, agg.cost, agg.conv, agg.sales, agg.tot_conv, agg.tot_sales,
            agg.cart_conv, agg.cart_sales, agg.wishlist_conv, agg.wishlist_sales{rank_select_sql}
        FROM agg
        JOIN dim_keyword k ON agg.keyword_id = k.keyword_id AND agg.customer_id = k.customer_id
        JOIN dim_adgroup a ON k.adgroup_id = a.adgroup_id AND agg.customer_id = a.customer_id
        JOIN dim_campaign c ON a.campaign_id = c.campaign_id AND agg.customer_id = c.customer_id
        WHERE agg.customer_id = :cid AND a.campaign_id = :camp_id
    """
    df = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2), "cid": str(customer_id), "camp_id": str(campaign_id)})
    if not df.empty and "campaign_type_label" in df.columns:
        mapping = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENTS": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
        df["campaign_type_label"] = df["campaign_type_label"].map(lambda x: mapping.get(x, x))
    return df


def _query_ad_detail_for_campaign(engine, d1, d2, customer_id: str, campaign_id: str) -> pd.DataFrame:
    if not table_exists(engine, "fact_ad_daily"):
        return pd.DataFrame()
    cp_col = _campaign_type_column(engine)
    ad_cols = get_table_columns(engine, "dim_ad")
    title_select = "ad.ad_title" if "ad_title" in ad_cols else "ad.ad_name as ad_title"
    image_select = "ad.image_url" if "image_url" in ad_cols else "'' as image_url"
    url_select = "ad.pc_landing_url as landing_url" if "pc_landing_url" in ad_cols else "'' as landing_url"
    ad_fact_cols = get_table_columns(engine, "fact_ad_daily")
    expr = {
        "purchase_conv_expr": "COALESCE(conv,0)",
        "purchase_sales_expr": "COALESCE(sales,0)",
        "total_conv_expr": "COALESCE(tot_conv, COALESCE(conv,0)+COALESCE(cart_conv,0)+COALESCE(wishlist_conv,0))",
        "total_sales_expr": "COALESCE(tot_sales, COALESCE(sales,0)+COALESCE(cart_sales,0)+COALESCE(wishlist_sales,0))",
        "cart_conv_expr": "COALESCE(cart_conv,0)",
        "cart_sales_expr": "COALESCE(cart_sales,0)",
        "wish_conv_expr": "COALESCE(wishlist_conv,0)",
        "wish_sales_expr": "COALESCE(wishlist_sales,0)",
    }
    try:
        from data import _strict_conv_selects
        expr = _strict_conv_selects(ad_fact_cols)
    except Exception:
        pass
    rank_col = next((c for c in ["avg_rank", "avg_rnk", "averageposition", "average_position", "avgrnk"] if c in ad_fact_cols), None)
    rank_agg_sql = f", CASE WHEN SUM(imp) > 0 THEN SUM(COALESCE({rank_col}, 0) * imp) / SUM(imp) ELSE NULL END as avg_rank" if rank_col else ""
    rank_select_sql = ", agg.avg_rank" if rank_col else ""
    sql = f"""
        WITH agg AS (
            SELECT customer_id, ad_id,
                   SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost,
                   SUM({expr['purchase_conv_expr']}) as conv,
                   SUM({expr['purchase_sales_expr']}) as sales,
                   SUM({expr['total_conv_expr']}) as tot_conv,
                   SUM({expr['total_sales_expr']}) as tot_sales,
                   SUM({expr['cart_conv_expr']}) as cart_conv,
                   SUM({expr['cart_sales_expr']}) as cart_sales,
                   SUM({expr['wish_conv_expr']}) as wishlist_conv,
                   SUM({expr['wish_sales_expr']}) as wishlist_sales
                   {rank_agg_sql}
            FROM fact_ad_daily
            WHERE dt BETWEEN :d1 AND :d2 AND customer_id = :cid
            GROUP BY customer_id, ad_id
        )
        SELECT
            agg.customer_id, a.campaign_id, ad.adgroup_id, agg.ad_id,
            c.campaign_name, c.{cp_col} as campaign_type_label,
            a.adgroup_name, ad.ad_name, {title_select}, {image_select}, {url_select},
            agg.imp, agg.clk, agg.cost, agg.conv, agg.sales, agg.tot_conv, agg.tot_sales,
            agg.cart_conv, agg.cart_sales, agg.wishlist_conv, agg.wishlist_sales{rank_select_sql}
        FROM agg
        JOIN dim_ad ad ON agg.ad_id = ad.ad_id AND agg.customer_id = ad.customer_id
        JOIN dim_adgroup a ON ad.adgroup_id = a.adgroup_id AND agg.customer_id = a.customer_id
        JOIN dim_campaign c ON a.campaign_id = c.campaign_id AND agg.customer_id = c.customer_id
        WHERE agg.customer_id = :cid AND a.campaign_id = :camp_id
    """
    df = sql_read(engine, sql, {"d1": str(d1), "d2": str(d2), "cid": str(customer_id), "camp_id": str(campaign_id)})
    if not df.empty and "campaign_type_label" in df.columns:
        mapping = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENTS": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
        df["campaign_type_label"] = df["campaign_type_label"].map(lambda x: mapping.get(x, x))
    return df


def _is_shopping_campaign_type(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype=bool)
    return series.astype(str).str.contains(r"쇼핑|SHOPPING", case=False, na=False)


def _prefer_detail_source_by_campaign(kw_df: pd.DataFrame, ad_df: pd.DataFrame) -> pd.DataFrame:
    kw_df = kw_df.copy() if kw_df is not None and not kw_df.empty else pd.DataFrame()
    ad_df = ad_df.copy() if ad_df is not None and not ad_df.empty else pd.DataFrame()

    if kw_df.empty and ad_df.empty:
        return pd.DataFrame()

    if not ad_df.empty and "item_name" in ad_df.columns:
        ad_df = ad_df[~ad_df["item_name"].astype(str).str.contains("확장소재", na=False)]

    if kw_df.empty:
        return ad_df.reset_index(drop=True)
    if ad_df.empty:
        return kw_df.reset_index(drop=True)

    for df in (kw_df, ad_df):
        if "campaign_id" in df.columns:
            df["campaign_id"] = df["campaign_id"].astype(str)

    pref = {}
    campaign_ids = set()
    if "campaign_id" in kw_df.columns:
        campaign_ids.update(kw_df["campaign_id"].dropna().astype(str).unique().tolist())
    if "campaign_id" in ad_df.columns:
        campaign_ids.update(ad_df["campaign_id"].dropna().astype(str).unique().tolist())

    for cid in campaign_ids:
        kw_rows = kw_df[kw_df["campaign_id"] == cid] if "campaign_id" in kw_df.columns else pd.DataFrame()
        ad_rows = ad_df[ad_df["campaign_id"] == cid] if "campaign_id" in ad_df.columns else pd.DataFrame()

        camp_type = ""
        if not kw_rows.empty and "campaign_type_label" in kw_rows.columns:
            camp_type = str(kw_rows["campaign_type_label"].iloc[0]).upper()
        elif not ad_rows.empty and "campaign_type_label" in ad_rows.columns:
            camp_type = str(ad_rows["campaign_type_label"].iloc[0]).upper()

        if any(x in camp_type for x in ["쇼핑", "SHOPPING", "브랜드", "BRAND", "플레이스", "PLACE"]):
            pref[cid] = "ad"
        elif any(x in camp_type for x in ["파워링크", "WEB_SITE", "파워컨텐츠", "POWER"]):
            pref[cid] = "kw"
        else:
            if not kw_rows.empty:
                pref[cid] = "kw"
            elif not ad_rows.empty:
                pref[cid] = "ad"

    kept = []
    if not kw_df.empty and "campaign_id" in kw_df.columns:
        kw_keep = kw_df[kw_df["campaign_id"].map(lambda x: pref.get(str(x), "kw") == "kw")].copy()
        if not kw_keep.empty:
            kept.append(kw_keep)
    if not ad_df.empty and "campaign_id" in ad_df.columns:
        ad_keep = ad_df[ad_df["campaign_id"].map(lambda x: pref.get(str(x), "ad") == "ad")].copy()
        if not ad_keep.empty:
            kept.append(ad_keep)

    if kept:
        return pd.concat(kept, ignore_index=True)
    return kw_df.reset_index(drop=True) if not kw_df.empty else ad_df.reset_index(drop=True)

def _query_detail_bundles_for_campaign(engine, d1, d2, customer_id: str, campaign_id: str, diag: list | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    kw_bundle = _query_keyword_detail_for_campaign(engine, d1, d2, customer_id, campaign_id)
    ad_bundle = _query_ad_detail_for_campaign(engine, d1, d2, customer_id, campaign_id)
    _diag_add(diag, '상세-키워드', 'ok' if not kw_bundle.empty else 'zero_data', len(kw_bundle.index), 'fact_keyword_daily', f'customer_id={customer_id} campaign_id={campaign_id}')
    _diag_add(diag, '상세-소재', 'ok' if not ad_bundle.empty else 'zero_data', len(ad_bundle.index), 'fact_ad_daily', f'customer_id={customer_id} campaign_id={campaign_id}')
    
    kw_tmp = kw_bundle.rename(columns={"keyword": "item_name"}) if not kw_bundle.empty else pd.DataFrame()
    
    ext_ads = pd.DataFrame()
    if not ad_bundle.empty:
        ad_tmp = ad_bundle.copy()
        if "ad_title" in ad_tmp.columns:
            ad_tmp["final_ad_name"] = ad_tmp["ad_title"].fillna("").astype(str).str.strip()
            mask_empty = ad_tmp["final_ad_name"].isin(["", "nan", "None"])
            ad_tmp.loc[mask_empty, "final_ad_name"] = ad_tmp.loc[mask_empty, "ad_name"].astype(str)
        else:
            ad_tmp["final_ad_name"] = ad_tmp["ad_name"].astype(str)
        ad_tmp = ad_tmp.rename(columns={"final_ad_name": "item_name"})
        
        # 확장소재만 별도로 추출
        ext_ads = ad_tmp[ad_tmp["item_name"].astype(str).str.contains("확장소재", na=False)].copy()
    else:
        ad_tmp = pd.DataFrame()
        
    regular_detail = _prefer_detail_source_by_campaign(kw_tmp, ad_tmp)
    _diag_add(diag, '상세-분리결과', 'ok' if (not regular_detail.empty or not ext_ads.empty) else 'zero_data', len(regular_detail.index) + len(ext_ads.index), 'detail_split', f'일반={len(regular_detail.index)} 확장소재={len(ext_ads.index)}')
    return regular_detail, ext_ads



def _summary_metric_config(has_pre_patch_cur: bool) -> tuple[list[str], str, str]:
    if has_pre_patch_cur:
        return ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "총 전환수", "총 전환매출", "통합 ROAS(%)"], "통합 ROAS(%)", "총 전환매출"
    return [
        "노출", "클릭", "CTR(%)", "CPC(원)", "광고비",
        "장바구니수", "장바구니 매출액", "장바구니 ROAS(%)",
        "구매완료수", "구매완료 매출", "구매 ROAS(%)",
        "총 전환수", "총 전환매출", "통합 ROAS(%)",
    ], "구매 ROAS(%)", "구매완료 매출"


def _render_campaign_type_device_summary(disp_main: pd.DataFrame, engine, f: Dict, diag: list[dict], roas_col: str, sales_col: str) -> None:
    st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)
    with st.container(border=True):
        st.markdown("<div style='font-size:14px;font-weight:700;color:#1F2937;margin-bottom:10px;'>캠페인 유형 · 기기 요약</div>", unsafe_allow_html=True)
        col_type, col_device = st.columns([1.45, 1], gap="medium")
        with col_type:
            st.markdown("<div style='font-size:13px;color:#4B5563;margin-bottom:8px;'>캠페인 유형별 데이터</div>", unsafe_allow_html=True)
            type_grp = disp_main.groupby("캠페인유형").agg({"광고비": "sum", sales_col: "sum"}).reset_index()
            total_cost = type_grp["광고비"].sum()
            type_grp["지출 비중(%)"] = np.where(total_cost > 0, (type_grp["광고비"] / total_cost) * 100, 0.0)
            type_grp[roas_col] = np.where(type_grp["광고비"] > 0, (type_grp[sales_col] / type_grp["광고비"]) * 100, 0.0)
            type_grp = type_grp.sort_values("광고비", ascending=False)
            st.dataframe(
                type_grp,
                width="stretch",
                height=_compact_df_height(type_grp, min_height=74, max_height=220),
                hide_index=True,
                column_config=_campaign_fast_col_config(type_grp),
            )
        with col_device:
            st.markdown("<div style='font-size:13px;color:#4B5563;margin-bottom:8px;'>기기별 광고비 지출 비중</div>", unsafe_allow_html=True)
            device_df = _query_device_breakdown(engine, f["start"], f["end"], tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), diag=diag)
            _render_device_share_panel(device_df)
    st.markdown("<div style='height:18px;'></div>", unsafe_allow_html=True)


def _normalize_detail_metric_frame(df: pd.DataFrame, item_col: str) -> pd.DataFrame:
    if df.empty:
        return df
    for c in ["cart_sales", "cart_conv", "wishlist_sales", "wishlist_conv"]:
        if c not in df.columns:
            df[c] = 0
    renamed = df.rename(columns={
        "adgroup_name": "광고그룹",
        item_col: "항목명",
        "imp": "노출",
        "클릭": "클릭",
        "cost": "광고비",
        "cart_conv": "장바구니수",
        "cart_sales": "장바구니 매출액",
        "wishlist_conv": "위시리스트수",
        "wishlist_sales": "위시리스트 매출액",
        "conv": "구매완료수",
        "sales": "구매완료 매출",
    }).copy()

    if "광고그룹" not in renamed.columns:
        for cand in ["group_name", "adgroup", "adgroup_nm"]:
            if cand in df.columns:
                renamed["광고그룹"] = df[cand]
                break
    if "광고그룹" not in renamed.columns:
        renamed["광고그룹"] = "미분류"

    if "항목명" not in renamed.columns:
        for cand in [item_col, "item_name", "final_ad_name", "ad_name", "ad_title", "keyword", "keyword_name", "extension_name", "소재명", "키워드"]:
            if cand in renamed.columns:
                renamed["항목명"] = renamed[cand]
                break
            if cand in df.columns:
                renamed["항목명"] = df[cand]
                break
    if "항목명" not in renamed.columns:
        renamed["항목명"] = "미분류"

    renamed["광고그룹"] = renamed["광고그룹"].fillna("미분류").replace("", "미분류")
    renamed["항목명"] = renamed["항목명"].fillna("미분류").replace("", "미분류")
    group_value_cols = [c for c in [
        "노출", "클릭", "광고비", "장바구니수", "장바구니 매출액", "위시리스트수", "위시리스트 매출액", "구매완료수", "구매완료 매출", "총 전환수", "총 전환매출"
    ] if c in renamed.columns]
    grouped = renamed.groupby(["광고그룹", "항목명"], as_index=False)[group_value_cols].sum()
    return _add_perf_metrics(grouped)


def _detail_display_columns(has_pre_patch_cur: bool, item_label: str) -> list[str]:
    if has_pre_patch_cur:
        return ["광고그룹", item_label, "노출", "클릭", "CTR(%)", "광고비", "총 전환수", "총 전환매출", "통합 ROAS(%)"]
    return [
        "광고그룹", item_label, "노출", "클릭", "CTR(%)", "광고비",
        "장바구니수", "장바구니 매출액", "장바구니 ROAS(%)",
        "구매완료수", "구매완료 매출", "구매 ROAS(%)",
        "총 전환수", "총 전환매출", "통합 ROAS(%)",
    ]


def _render_campaign_detail_table(title: str, df: pd.DataFrame, item_label: str, has_pre_patch_cur: bool) -> bool:
    if df.empty:
        return False
    st.markdown(f"<div style='font-size:14px;font-weight:700;color:#374151;margin-bottom:8px;'> {escape(title)}</div>", unsafe_allow_html=True)
    norm = _normalize_detail_metric_frame(df.copy(), item_label)
    norm = norm.rename(columns={"항목명": item_label})
    cols = [c for c in _detail_display_columns(has_pre_patch_cur, item_label) if c in norm.columns]
    disp = norm[cols].sort_values("광고비", ascending=False).head(100)
    st.dataframe(disp, width="stretch", hide_index=True, column_config=_campaign_fast_col_config(disp, item_label))
    return True


def _render_campaign_detail_section(selected_campaign: str, engine, f: Dict, selected_customer_id: str, selected_campaign_id: str, diag: list[dict], has_pre_patch_cur: bool) -> None:
    with st.spinner("🔄 선택한 캠페인의 하위 키워드/소재 성과를 불러오는 중입니다..."):
        try:
            kw_detail, ext_ads = _query_detail_bundles_for_campaign(engine, f["start"], f["end"], selected_customer_id, selected_campaign_id, diag=diag)
        except Exception as e:
            _diag_add(diag, '상세조회', 'error', 0, 'campaign_detail', f"{type(e).__name__}: {e}")
            kw_detail, ext_ads = pd.DataFrame(), pd.DataFrame()
            st.warning("상세 데이터를 불러오는 중 오류가 발생했습니다. 아래 조회 진단을 확인해 주세요.")
    st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)
    with st.container(border=True):
        st.markdown(f"<h5 style='color: #335CFF; margin-bottom: 12px;'>[{escape(str(selected_campaign))}] 상세 분석</h5>", unsafe_allow_html=True)
        has_data = False
        has_data = _render_campaign_detail_table("확장소재 성과", ext_ads, "확장소재명", has_pre_patch_cur) or has_data
        if not ext_ads.empty:
            st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)
        has_data = _render_campaign_detail_table("하위 그룹 / 소재 성과", kw_detail, "키워드/상품명", has_pre_patch_cur) or has_data
        if not has_data:
            st.info("해당 캠페인에 등록된 하위 키워드/소재 및 확장소재 데이터가 없습니다.")


def _render_campaign_summary_tab(view: pd.DataFrame, engine, f: Dict, diag: list[dict], has_pre_patch_cur: bool, top_n: int) -> None:
    camps_main = ["전체"] + sorted([str(x) for x in view["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view.columns else ["전체"]
    sel_camp_main = st.selectbox("캠페인 검색", camps_main, key="camp_name_filter_main")
    disp_main = view.copy()
    if sel_camp_main != "전체":
        disp_main = disp_main[disp_main["캠페인"] == sel_camp_main]

    base_cols = ["업체명", "담당자", "캠페인유형", "캠페인"]
    if "평균순위" in disp_main.columns:
        base_cols.append("평균순위")
    all_metrics_cols, roas_col, sales_col = _summary_metric_config(has_pre_patch_cur)
    _render_campaign_type_device_summary(disp_main, engine, f, diag, roas_col, sales_col)

    final_cols = [c for c in base_cols + all_metrics_cols if c in disp_main.columns]
    disp_main_src = disp_main.sort_values("광고비", ascending=False).head(top_n).reset_index(drop=True)
    disp_main_show = disp_main_src[final_cols].copy()
    render_toolbar(
        "캠페인별 성과",
        "행을 선택하면 하위 그룹, 키워드, 소재 상세를 아래에서 확인할 수 있습니다.",
        [{"label": f"{len(disp_main_show):,}행 표시", "tone": "info"}, {"label": "선택 상세", "tone": "primary"}],
    )
    event = st.dataframe(disp_main_show, width="stretch", hide_index=True, selection_mode="single-row", on_select="rerun", column_config=_campaign_fast_col_config(disp_main_show, "캠페인"))
    selected_rows = event.selection.rows
    if not selected_rows:
        return
    selected_idx = selected_rows[0]
    selected_campaign = disp_main_src.iloc[selected_idx]["캠페인"]
    selected_customer_id = str(disp_main_src.iloc[selected_idx].get("customer_id", ""))
    selected_campaign_id = str(disp_main_src.iloc[selected_idx].get("campaign_id", ""))
    _render_campaign_detail_section(selected_campaign, engine, f, selected_customer_id, selected_campaign_id, diag, has_pre_patch_cur)


def _group_mode_columns(show_deltas_grp: bool, show_mode: str) -> list[str]:
    if show_deltas_grp:
        metrics_cols_grp = ["노출", "노출 증감", "노출 차이", "클릭", "클릭 증감", "클릭 차이", "광고비", "광고비 증감", "광고비 차이", "CPC(원)", "CPC 증감", "CPC 차이"]
        if show_mode == "integrated_only":
            metrics_cols_grp.extend(["총 전환수", "총 전환 증감", "총 전환 차이", "총 전환매출", "총 매출 증감", "총 매출 차이", "통합 ROAS(%)", "통합 ROAS 증감"])
        else:
            metrics_cols_grp.extend(["구매완료수", "구매 증감", "구매 차이", "구매완료 매출", "구매 매출 증감", "구매 매출 차이", "구매 ROAS(%)", "구매 ROAS 증감"])
        return metrics_cols_grp
    if show_mode == "integrated_only":
        return ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "총 전환수", "총 전환매출", "통합 ROAS(%)"]
    return ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "장바구니수", "장바구니 매출액", "장바구니 ROAS(%)", "구매완료수", "구매완료 매출", "구매 ROAS(%)", "총 전환수", "총 전환매출", "통합 ROAS(%)"]


def _build_detail_source(kw_bundle: pd.DataFrame, ad_bundle: pd.DataFrame) -> pd.DataFrame:
    kw_tmp = kw_bundle.rename(columns={"keyword": "item_name"}) if not kw_bundle.empty else pd.DataFrame()
    if not ad_bundle.empty:
        ad_tmp = ad_bundle.copy()
        if "ad_title" in ad_tmp.columns:
            ad_tmp["final_ad_name"] = ad_tmp["ad_title"].fillna("").astype(str).str.strip()
            mask_empty = ad_tmp["final_ad_name"].isin(["", "nan", "None"])
            ad_tmp.loc[mask_empty, "final_ad_name"] = ad_tmp.loc[mask_empty, "ad_name"].astype(str)
        else:
            ad_tmp["final_ad_name"] = ad_tmp["ad_name"].astype(str)
        ad_tmp = ad_tmp.rename(columns={"final_ad_name": "item_name"})
    else:
        ad_tmp = pd.DataFrame()
    return _prefer_detail_source_by_campaign(kw_tmp, ad_tmp)


def _render_campaign_group_tab(meta: pd.DataFrame, engine, f: Dict, cids: tuple, type_sel: tuple, top_n: int, patch_date: date, has_pre_patch_cur: bool) -> None:
    st.markdown("<div style='display:flex; justify-content:flex-end; margin-bottom:8px;'>", unsafe_allow_html=True)
    show_deltas_grp = st.toggle(" 증감율 보기", value=False, key="grp_abs_toggle")
    st.markdown("</div>", unsafe_allow_html=True)
    cmp_mode_grp = None
    b1_grp, b2_grp = None, None
    if show_deltas_grp:
        opts = get_dynamic_cmp_options(f["start"], f["end"])
        cmp_opts = [o for o in opts if o != "비교 안함"]
        cmp_mode_grp = st.radio("비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="grp_cmp_mode")
        b1_grp, b2_grp = period_compare_range(f["start"], f["end"], cmp_mode_grp)
    with st.spinner("🔄 광고그룹 성과를 불러오는 중입니다..."):
        kw_bundle_grp = query_keyword_bundle(engine, f["start"], f["end"], list(cids), type_sel, topn_cost=_campaign_fetch_limit(top_n))
        ad_bundle_grp = query_ad_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=_campaign_fetch_limit(top_n), top_k=50)
        base_detail_bundle_grp = pd.DataFrame()
        if show_deltas_grp and b1_grp and b2_grp:
            b_kw_bundle_grp = query_keyword_bundle(engine, b1_grp, b2_grp, list(cids), type_sel, topn_cost=_campaign_fetch_limit(top_n))
            b_ad_bundle_grp = query_ad_bundle(engine, b1_grp, b2_grp, cids, type_sel, topn_cost=_campaign_fetch_limit(top_n), top_k=50)
            base_detail_bundle_grp = _build_detail_source(b_kw_bundle_grp, b_ad_bundle_grp)
        detail_bundle_grp = _build_detail_source(kw_bundle_grp, ad_bundle_grp)
    if detail_bundle_grp is None or detail_bundle_grp.empty:
        st.info("광고그룹 성과 데이터가 없습니다.")
        return
    grp_cols = [c for c in ["customer_id", "campaign_id", "adgroup_id", "campaign_type_label", "campaign_name", "adgroup_name"] if c in detail_bundle_grp.columns]
    val_cols = [c for c in ["imp", "clk", "cost", "cart_conv", "cart_sales", "wishlist_conv", "wishlist_sales", "conv", "sales", "tot_conv", "tot_sales"] if c in detail_bundle_grp.columns]
    if not grp_cols or not val_cols:
        st.info("광고그룹 성과 데이터가 없습니다.")
        return
    grp = detail_bundle_grp.groupby(grp_cols, as_index=False)[val_cols].sum()
    rank_grp = _keyword_rank_by_keys(detail_bundle_grp, grp_cols)
    if not rank_grp.empty:
        grp = grp.merge(rank_grp, on=grp_cols, how="left")
    grp = _perf_common_merge_meta(grp, meta)
    grouped = grp.rename(columns={
        "account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹",
        "imp": "노출", "clk": "클릭", "cost": "광고비", "cart_conv": "장바구니수", "cart_sales": "장바구니 매출액",
        "wishlist_conv": "위시리스트수", "wishlist_sales": "위시리스트 매출액", "conv": "구매완료수", "sales": "구매완료 매출",
    }).copy()
    grouped = _add_perf_metrics(grouped)
    if "avg_rank" in grouped.columns:
        grouped["평균순위"] = grouped["avg_rank"].apply(_format_avg_rank)
    valid_keys_grp = [k for k in ["customer_id", "campaign_id", "adgroup_id"] if k in grouped.columns]
    if show_deltas_grp:
        if not base_detail_bundle_grp.empty and valid_keys_grp:
            b_grp_cols = [c for c in valid_keys_grp if c in base_detail_bundle_grp.columns]
            b_grp = base_detail_bundle_grp.groupby(b_grp_cols, as_index=False)[val_cols].sum()
            b_rank_grp = _keyword_rank_by_keys(base_detail_bundle_grp, b_grp_cols)
            if not b_rank_grp.empty:
                b_grp = b_grp.merge(b_rank_grp, on=b_grp_cols, how="left")
            grouped = _apply_comparison_metrics(grouped, b_grp, valid_keys_grp)
        else:
            grouped = _apply_comparison_metrics(grouped, pd.DataFrame(), valid_keys_grp)
    camps = ["전체"] + sorted([str(x) for x in grouped["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in grouped.columns else ["전체"]
    sel_camp = st.selectbox("캠페인 필터", camps, key="camp_group_filter")
    if sel_camp != "전체":
        grouped = grouped[grouped["캠페인"] == sel_camp]
    has_pre_patch_base = (b1_grp < patch_date) if (show_deltas_grp and b1_grp) else False
    show_mode = "integrated_only" if (has_pre_patch_base or has_pre_patch_cur) else "purchase_default"
    if show_deltas_grp and show_mode == "integrated_only":
        st.warning("⚠️ 비교 기간에 3월 11일 이전(네이버 퍼널 분리 패치 전) 데이터가 포함되어 '통합 전환' 기준으로 표시합니다.")
    metrics_cols_grp = _group_mode_columns(show_deltas_grp, show_mode)
    base_cols_grp = ["업체명", "담당자", "캠페인유형", "캠페인", "광고그룹"]
    if "avg_rank" in grouped.columns or "평균순위" in grouped.columns:
        base_cols_grp.append("평균순위")
        if show_deltas_grp:
            metrics_cols_grp.append("순위 변화")
    cols_grp = [c for c in base_cols_grp + metrics_cols_grp if c in grouped.columns]
    disp_grp = grouped[cols_grp].sort_values("광고비", ascending=False).head(top_n).copy()
    _render_campaign_sticky_table(disp_grp, "광고그룹", apply_delta_styles=show_deltas_grp)


def _compare_mode_columns(show_deltas: bool, show_mode: str) -> list[str]:
    metrics_cols_cmp = []
    metrics_cols_cmp.extend(["노출", "노출 증감", "노출 차이"] if show_deltas else ["노출"])
    metrics_cols_cmp.extend(["클릭", "클릭 증감", "클릭 차이"] if show_deltas else ["클릭"])
    metrics_cols_cmp.extend(["광고비", "광고비 증감", "광고비 차이"] if show_deltas else ["광고비"])
    metrics_cols_cmp.extend(["CPC(원)", "CPC 증감", "CPC 차이"] if show_deltas else ["CPC(원)"])
    if show_mode == "integrated_only":
        metrics_cols_cmp.extend(["총 전환수", "총 전환 증감", "총 전환 차이"] if show_deltas else ["총 전환수"])
        metrics_cols_cmp.extend(["총 전환매출", "총 매출 증감", "총 매출 차이"] if show_deltas else ["총 전환매출"])
        metrics_cols_cmp.extend(["통합 ROAS(%)", "통합 ROAS 증감"] if show_deltas else ["통합 ROAS(%)"])
    else:
        metrics_cols_cmp.extend(["구매완료수", "구매 증감", "구매 차이"] if show_deltas else ["구매완료수"])
        metrics_cols_cmp.extend(["구매완료 매출", "구매 매출 증감", "구매 매출 차이"] if show_deltas else ["구매완료 매출"])
        metrics_cols_cmp.extend(["구매 ROAS(%)", "구매 ROAS 증감"] if show_deltas else ["구매 ROAS(%)"])
    return metrics_cols_cmp


def _render_campaign_compare_tab(view: pd.DataFrame, engine, f: Dict, cids: tuple, type_sel: tuple, top_n: int, patch_date: date, has_pre_patch_cur: bool) -> None:
    st.markdown("<div style='display:flex; justify-content:flex-end; margin-bottom:8px;'>", unsafe_allow_html=True)
    show_deltas = st.toggle(" 증감율 보기", value=False, key="camp_abs_toggle")
    st.markdown("</div>", unsafe_allow_html=True)
    opts = get_dynamic_cmp_options(f["start"], f["end"])
    cmp_opts = [o for o in opts if o != "비교 안함"]
    cmp_mode = st.radio("비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="camp_cmp_mode")
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
    with st.spinner("🔄 이전 기간의 데이터를 불러오는 중입니다..."):
        base_bundle = query_campaign_bundle(engine, b1, b2, cids, type_sel, topn_cost=_campaign_fetch_limit(top_n))
    view_cmp = view.copy()
    valid_keys = [k for k in ["customer_id", "campaign_id"] if k in view_cmp.columns and k in base_bundle.columns]
    if not base_bundle.empty and valid_keys:
        view_cmp = _apply_comparison_metrics(view_cmp, base_bundle, valid_keys)
    else:
        view_cmp = _apply_comparison_metrics(view_cmp, pd.DataFrame(), [k for k in ["customer_id", "campaign_id"] if k in view_cmp.columns])
    has_pre_patch_base = (b1 < patch_date) if b1 else False
    show_mode = "integrated_only" if (has_pre_patch_base or has_pre_patch_cur) else "purchase_default"
    if show_mode == "integrated_only":
        st.warning("⚠️ 비교 기간에 3월 11일 이전(네이버 퍼널 분리 패치 전) 데이터가 포함되어 '통합 전환' 기준으로 표시합니다.")
    metrics_cols_cmp = _compare_mode_columns(show_deltas, show_mode)
    base_cols_cmp = ["업체명", "담당자", "캠페인유형", "캠페인"]
    if "avg_rank" in view_cmp.columns or "평균순위" in view_cmp.columns:
        base_cols_cmp.append("평균순위")
        if show_deltas:
            metrics_cols_cmp.append("순위 변화")
    final_cols_cmp = [c for c in base_cols_cmp + metrics_cols_cmp if c in view_cmp.columns]
    disp_cmp = view_cmp[final_cols_cmp].sort_values("광고비", ascending=False).head(top_n).copy()
    st.dataframe(disp_cmp, width="stretch", height=560, hide_index=True, column_config=_campaign_fast_col_config(disp_cmp, "캠페인"))


def _render_campaign_off_tab(view: pd.DataFrame, meta: pd.DataFrame, engine, f: Dict, cids: tuple) -> None:
    st.info("이 지면에서는 상세 퍼널보다 안정적인 광고 운영 여부가 중요합니다.")
    try:
        days_diff = (pd.to_datetime(f["end"]) - pd.to_datetime(f["start"])).days + 1
        if days_diff < 3:
            st.warning("단기 데이터(3일 미만) 기반 예산 증액 주의: 일시적인 효율 상승일 수 있습니다.")
    except Exception:
        pass
    off_log = query_campaign_off_log(engine, f["start"], f["end"], cids)
    required_cols = {"dt", "customer_id", "campaign_id", "off_time"}
    if off_log.empty or not required_cols.issubset(set(off_log.columns)):
        st.info("조회 기간 동안 예산 부족으로 꺼진 기록이 없습니다.")
        return
    dim_camp = load_dim_campaign(engine)
    if not dim_camp.empty:
        dim_camp["campaign_id"], off_log["campaign_id"] = dim_camp["campaign_id"].astype(str), off_log["campaign_id"].astype(str)
        off_log = off_log.merge(dim_camp[["campaign_id", "campaign_name"]], on="campaign_id", how="left")
    else:
        off_log["campaign_name"] = off_log["campaign_id"]
    if not meta.empty:
        meta_copy = meta.copy()
        meta_copy["customer_id"], off_log["customer_id"] = meta_copy["customer_id"].astype(str), off_log["customer_id"].astype(str)
        off_log = off_log.merge(meta_copy[["customer_id", "account_name"]], on="customer_id", how="left")
    else:
        off_log["account_name"] = off_log["customer_id"]
    off_log["dt_str"] = pd.to_datetime(off_log["dt"]).dt.strftime("%m/%d")
    pivot_df = off_log.pivot_table(index=["account_name", "campaign_name"], columns="dt_str", values="off_time", aggfunc='first').reset_index()
    pivot_df = pivot_df.rename(columns={"account_name": "업체명", "campaign_name": "캠페인"}).fillna("-")
    if not view.empty and "통합 ROAS(%)" in view.columns:
        roas_df = view[["업체명", "캠페인", "통합 ROAS(%)"]].drop_duplicates()
        pivot_df = pivot_df.merge(roas_df, on=["업체명", "캠페인"], how="left")
        cols = pivot_df.columns.tolist()
        cols.insert(2, cols.pop(cols.index('통합 ROAS(%)')))
        pivot_df = pivot_df[cols]
    st.dataframe(pivot_df, width="stretch", hide_index=True)


@st.fragment
def page_perf_campaign(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        return
    render_toolbar(
        "캠페인 운영 테이블",
        "캠페인을 선택하면 하위 키워드와 소재 성과까지 같은 화면에서 이어서 확인합니다.",
        [{"label": f"{f['start']} ~ {f['end']}", "tone": "primary"}, {"label": f"Top {int(f.get('top_n_campaign', 200)):,}", "tone": "info"}],
    )
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_campaign", 200))
    patch_date = date(2026, 3, 11)
    has_pre_patch_cur = (f["start"] < patch_date)
    diag: list[dict] = []
    if has_pre_patch_cur:
        st.info("💡 3월 11일 이전 데이터가 포함되어 있어 '통합 전환' 기준으로 성과가 표시됩니다.")
    selected_tab = st.pills("분석 탭 선택", ["종합 성과", "그룹 성과", "기간 비교", "꺼짐 기록"], default="종합 성과")

    view = pd.DataFrame()
    needs_summary_bundle = selected_tab in ("종합 성과", "기간 비교", "꺼짐 기록")
    if needs_summary_bundle:
        with st.spinner("🔄 최신 필터 조건에 맞추어 데이터를 실시간으로 집계하고 있습니다..."):
            bundle = query_campaign_bundle(engine, f["start"], f["end"], cids, type_sel, topn_cost=_campaign_fetch_limit(top_n))
            _diag_add(diag, '캠페인집계', 'ok' if bundle is not None and not bundle.empty else 'zero_data', 0 if bundle is None else len(bundle.index), 'query_campaign_bundle', f'기간={f["start"]}~{f["end"]} 고객수={len(cids)} 유형수={len(type_sel)}')
            if bundle is None or bundle.empty:
                _render_diag_panel(diag, enabled=bool(f.get("show_diagnostics", False)))
                return
            df = _perf_common_merge_meta(bundle, meta)
            _diag_add(diag, '메타병합', 'ok' if not df.empty else 'zero_data', len(df.index), 'meta_merge', 'campaign bundle + account meta')
            view = df.rename(columns={
                "account_name": "업체명", "manager": "담당자", "campaign_type": "캠페인유형", "campaign_name": "캠페인",
                "imp": "노출", "clk": "클릭", "cost": "광고비", "cart_conv": "장바구니수", "cart_sales": "장바구니 매출액",
                "wishlist_conv": "위시리스트수", "wishlist_sales": "위시리스트 매출액", "conv": "구매완료수", "sales": "구매완료 매출",
            }).copy()
            view = _add_perf_metrics(view)
            if "avg_rank" in view.columns:
                view["평균순위"] = view["avg_rank"].apply(_format_avg_rank)
            total_cost = float(safe_numeric_col(view, "광고비").sum())
            total_clk = float(safe_numeric_col(view, "클릭").sum())
            total_sales_col = "총 전환매출" if "총 전환매출" in view.columns else "구매완료 매출"
            total_conv_col = "총 전환수" if "총 전환수" in view.columns else "구매완료수"
            total_sales = float(safe_numeric_col(view, total_sales_col).sum())
            total_conv = float(safe_numeric_col(view, total_conv_col).sum())
            total_roas = (total_sales / total_cost * 100.0) if total_cost > 0 else 0.0
            total_cpc = (total_cost / total_clk) if total_clk > 0 else 0.0
            render_kpi_strip([
                {"label": "캠페인", "value": f"{view['캠페인'].nunique():,}개", "sub": "현재 필터", "tone": "neu"},
                {"label": "광고비", "value": format_currency(total_cost), "sub": "집행 합계", "tone": "neu"},
                {"label": "클릭", "value": f"{total_clk:,.0f}", "sub": "유입 합계", "tone": "neu"},
                {"label": "CPC", "value": format_currency(total_cpc), "sub": "평균 비용", "tone": "neu"},
                {"label": "총 전환", "value": f"{total_conv:,.0f}", "sub": "전환 합계", "tone": "neu"},
                {"label": "통합 ROAS", "value": f"{total_roas:,.1f}%", "sub": "수익성", "tone": "neu"},
            ])
    else:
        _diag_add(diag, '캠페인집계', 'warn', 0, 'lazy_skip', '선택 탭에서는 요약 bundle 조회를 생략했습니다.')

    if selected_tab == "종합 성과":
        _render_campaign_summary_tab(view, engine, f, diag, has_pre_patch_cur, top_n)
    elif selected_tab == "그룹 성과":
        _render_campaign_group_tab(meta, engine, f, cids, type_sel, top_n, patch_date, has_pre_patch_cur)
    elif selected_tab == "기간 비교":
        _render_campaign_compare_tab(view, engine, f, cids, type_sel, top_n, patch_date, has_pre_patch_cur)
    elif selected_tab == "꺼짐 기록":
        _render_campaign_off_tab(view, meta, engine, f, cids)
    _render_diag_panel(diag, enabled=bool(f.get("show_diagnostics", False)))
