# -*- coding: utf-8 -*-
"""view_ad.py - Ad performance & A/B Testing page view (Toggle renamed & controls both % and abs)."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import streamlit_compat  # noqa: F401
from typing import Dict
from datetime import date

from data import *
from ui import *
from page_helpers import *
from page_helpers import _perf_common_merge_meta, _render_ab_test_sbs, render_item_comparison_search

FMT_DICT = {
    "노출": "{:,.0f}", "노출 증감": "{:+.1f}%", "노출 차이": "{:+,.0f}",
    "클릭": "{:,.0f}", "클릭 증감": "{:+.1f}%", "클릭 차이": "{:+,.0f}",
    "CTR(%)": "{:,.2f}%", "CTR 증감": "{:+.2f}%",
    "광고비": "{:,.0f}원", "광고비 증감": "{:+.1f}%", "광고비 차이": "{:+,.0f}원",
    "CPC(원)": "{:,.0f}원", "CPC 증감": "{:+.1f}%", "CPC 차이": "{:+,.0f}원",
    "전환": "{:,.0f}", "전환 증감": "{:+.1f}%", "전환 차이": "{:+,.0f}",
    "CVR(%)": "{:,.2f}%", "CVR 증감": "{:+.2f}%",
    "CPA(원)": "{:,.0f}원",
    "전환매출": "{:,.0f}원", "전환매출 증감": "{:+.1f}%", "전환매출 차이": "{:+,.0f}원",
    "ROAS(%)": "{:,.1f}%", "ROAS 증감": "{:+.1f}%"
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
    pos_cols = [c for c in ['노출 증감', '노출 차이', '클릭 증감', '클릭 차이', 'CTR 증감', '전환 증감', '전환 차이', 'CVR 증감', '전환매출 증감', '전환매출 차이', 'ROAS 증감'] if c in df.columns]
    neg_cols = [c for c in ['광고비 증감', '광고비 차이', 'CPC 증감', 'CPC 차이'] if c in df.columns]
    try:
        if pos_cols: styler = styler.map(_style_delta_numeric, subset=pos_cols)
        if neg_cols: styler = styler.map(_style_delta_numeric_neg, subset=neg_cols)
    except AttributeError:
        if pos_cols: styler = styler.applymap(_style_delta_numeric, subset=pos_cols)
        if neg_cols: styler = styler.applymap(_style_delta_numeric_neg, subset=neg_cols)
    return styler

def _inject_ad_css():
    st.markdown("""
    <style>
    .ad-toolbar { background: var(--nv-surface); border: 1px solid var(--nv-line); border-radius: 12px; padding: 14px 16px 10px 16px; margin-bottom: 16px; }
    .ad-toolbar-title { font-size: 13px; font-weight: 700; color: var(--nv-text); margin-bottom: 10px; }
    .ad-section-sub { font-size: 12px; color: var(--nv-muted); margin-top: -2px; margin-bottom: 12px; }
    </style>
    """, unsafe_allow_html=True)

def _build_material_name(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if "ad_title" in work.columns:
        work["final_ad_name"] = work["ad_title"].fillna("").astype(str).str.strip()
        mask_empty = work["final_ad_name"].isin(["", "nan", "None"])
        if "ad_name" in work.columns:
            work.loc[mask_empty, "final_ad_name"] = work.loc[mask_empty, "ad_name"].fillna("").astype(str).str.strip()
    elif "ad_name" in work.columns:
        work["final_ad_name"] = work["ad_name"].fillna("").astype(str).str.strip()
    else: work["final_ad_name"] = ""
    return work

def _filter_by_ad_kind(df: pd.DataFrame, kind: str) -> pd.DataFrame:
    if df is None or df.empty or "소재내용" not in df.columns:
        return pd.DataFrame() if df is None else df
    work = df.copy()
    s = work["소재내용"].fillna("").astype(str)
    non_talk = ~s.str.contains("TALK", na=False, case=False)
    is_ext = s.str.contains("확장소재", na=False)
    
    if kind == "확장소재":
        return work[is_ext & non_talk].copy()
    else:
        return work[~is_ext & non_talk].copy()

def _apply_comparison_metrics(view_df: pd.DataFrame, base_df: pd.DataFrame, merge_keys: list) -> pd.DataFrame:
    if view_df.empty: return view_df
    for k in merge_keys:
        if k in view_df.columns: view_df[k] = view_df[k].astype(str)
        if k in base_df.columns: base_df[k] = base_df[k].astype(str)
            
    agg_dict = {'imp': 'sum', 'clk': 'sum', 'cost': 'sum', 'conv': 'sum', 'sales': 'sum'}
    if not base_df.empty:
        base_agg = base_df.groupby(merge_keys).agg(agg_dict).reset_index()
        base_agg = base_agg.rename(columns={'imp': 'b_imp', 'clk': 'b_clk', 'cost': 'b_cost', 'conv': 'b_conv', 'sales': 'b_sales'})
        merged = pd.merge(view_df, base_agg, on=merge_keys, how='left')
    else: merged = view_df.copy()
        
    for c in ['b_imp', 'b_clk', 'b_cost', 'b_conv', 'b_sales']:
        if c not in merged.columns: merged[c] = 0
        merged[c] = merged[c].fillna(0)

    def _vec_pct_diff(c, b):
        diff = c - b
        safe_b = np.where(b == 0, 1, b)
        pct = np.where(b == 0, np.where(c > 0, 100.0, 0.0), (diff / safe_b) * 100.0)
        return pct, diff

    c_imp, b_imp = merged.get('노출', 0), merged.get('b_imp', 0)
    c_clk, b_clk = merged.get('클릭', 0), merged.get('b_clk', 0)
    c_cost, b_cost = merged.get('광고비', 0), merged.get('b_cost', 0)
    c_conv, b_conv = merged.get('전환', 0), merged.get('b_conv', 0)
    c_sales, b_sales = merged.get('전환매출', 0), merged.get('b_sales', 0)

    c_ctr = np.where(c_imp > 0, (c_clk / c_imp) * 100, 0)
    b_ctr = np.where(b_imp > 0, (b_clk / b_imp) * 100, 0)
    c_cpc = np.where(c_clk > 0, c_cost / c_clk, 0)
    b_cpc = np.where(b_clk > 0, b_cost / b_clk, 0)
    c_cvr = np.where(c_clk > 0, (c_conv / c_clk) * 100, 0)
    b_cvr = np.where(b_clk > 0, (b_conv / b_clk) * 100, 0)
    c_roas = np.where(c_cost > 0, (c_sales / c_cost) * 100, 0)
    b_roas = np.where(b_cost > 0, (b_sales / b_cost) * 100, 0)

    merged['CTR(%)'] = c_ctr
    merged['CPC(원)'] = c_cpc
    merged['CVR(%)'] = c_cvr
    merged['ROAS(%)'] = c_roas

    merged['노출 증감'], merged['노출 차이'] = _vec_pct_diff(c_imp, b_imp)
    merged['클릭 증감'], merged['클릭 차이'] = _vec_pct_diff(c_clk, b_clk)
    merged['CTR 증감'] = c_ctr - b_ctr
    merged['광고비 증감'], merged['광고비 차이'] = _vec_pct_diff(c_cost, b_cost)
    merged['CPC 증감'], merged['CPC 차이'] = _vec_pct_diff(c_cpc, b_cpc)
    merged['전환 증감'], merged['전환 차이'] = _vec_pct_diff(c_conv, b_conv)
    merged['CVR 증감'] = c_cvr - b_cvr
    merged['전환매출 증감'], merged['전환매출 차이'] = _vec_pct_diff(c_sales, b_sales)
    merged['ROAS 증감'] = c_roas - b_roas
        
    return merged

@st.cache_data(show_spinner=False, max_entries=20, ttl=300)
def compute_ad_view(bundle, meta):
    if bundle is None or bundle.empty: return pd.DataFrame()
    df = _perf_common_merge_meta(bundle, meta)
    df = _build_material_name(df)
    view = df.rename(columns={
        "account_name": "업체명", "manager": "담당자",
        "campaign_type": "캠페인유형", "campaign_type_label": "캠페인유형",
        "campaign_name": "캠페인", "adgroup_name": "광고그룹", "final_ad_name": "소재내용",
        "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"
    }).copy()
    if "캠페인유형" not in view.columns and "campaign_type" in view.columns: view["캠페인유형"] = view["campaign_type"]
    if "소재내용" in view.columns:
        view["_clean_ad"] = view["소재내용"].astype(str).str.replace("|", "").str.strip()
        view = view[view["_clean_ad"] != ""]
        view = view.drop(columns=["_clean_ad"])
    if view.empty: return pd.DataFrame()
    for c in ["노출", "클릭", "광고비", "전환", "전환매출"]:
        if c in view.columns: view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)
        else: view[c] = 0
    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CVR(%)"] = np.where(view["클릭"] > 0, (view["전환"] / view["클릭"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
    view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)
    return view



def _ad_cmp_fast_col_config(df: pd.DataFrame) -> dict:
    cfg = dict(FAST_AD_CONFIG)
    if "소재내용" in df.columns:
        cfg["소재내용"] = st.column_config.TextColumn("소재내용", pinned=True, width="medium")
    for c in df.columns:
        if c in cfg:
            continue
        if ("증감" in c and "차이" not in c) or c in {"CTR(%)", "CVR(%)", "ROAS(%)"}:
            cfg[c] = st.column_config.NumberColumn(c, format="%.1f %%")
        elif c.endswith("차이") and ("광고비" in c or "매출" in c or "CPC" in c):
            cfg[c] = st.column_config.NumberColumn(c, format="%d 원")
        elif c.endswith("차이") or c == "전환":
            cfg[c] = st.column_config.NumberColumn(c, format="%d")
    return cfg

FAST_AD_CONFIG = {
    "노출": st.column_config.NumberColumn("노출", format="%d"),
    "클릭": st.column_config.NumberColumn("클릭", format="%d"),
    "CTR(%)": st.column_config.NumberColumn("CTR(%)", format="%.2f %%"),
    "CPC(원)": st.column_config.NumberColumn("CPC(원)", format="%d 원"),
    "광고비": st.column_config.NumberColumn("광고비", format="%d 원"),
    "전환": st.column_config.NumberColumn("전환", format="%.1f"),
    "CPA(원)": st.column_config.NumberColumn("CPA(원)", format="%d 원"),
    "전환매출": st.column_config.NumberColumn("전환매출", format="%d 원"),
    "ROAS(%)": st.column_config.NumberColumn("ROAS(%)", format="%.2f %%"),
    "CVR(%)": st.column_config.NumberColumn("CVR(%)", format="%.2f %%"),
}


def _ad_fetch_limit(top_n: int, compare: bool = False) -> int:
    try:
        top_n = int(top_n or 0)
    except Exception:
        top_n = 0
    top_n = max(top_n, 1)
    if compare:
        return min(max(top_n * 4, 400), 1200)
    return min(max(top_n * 3, 300), 1000)

def _render_ad_sticky_table(df, first_col: str, height: int = 500, col_config: dict = None):
    try:
        rows = len(df.index)
        if rows <= 0: calc_height = 100
        elif rows == 1: calc_height = 80
        else: calc_height = min(height, max(100, 40 + (rows * 36)))
    except: calc_height = height
    cfg = col_config.copy() if col_config else {}
    cfg[first_col] = st.column_config.TextColumn(first_col, pinned=True, width="medium")
    if "랜딩페이지 URL" in df.columns: cfg["랜딩페이지 URL"] = st.column_config.LinkColumn("랜딩페이지 URL", display_text="링크 열기", pinned=True)
    st.dataframe(df, use_container_width=True, height=calc_height, hide_index=True, column_config=cfg)

@st.fragment
def _render_ad_tab(df_tab: pd.DataFrame, ad_type_name: str, top_n: int, start_dt, end_dt):
    if df_tab.empty:
        st.info(f"해당 기간의 {ad_type_name} 데이터가 없습니다.")
        return
    st.markdown("<div class='ad-toolbar'><div class='ad-toolbar-title'>조회 조건</div>", unsafe_allow_html=True)
    c1, c2 = st.columns([1, 1])
    with c1:
        camps = ["전체"] + sorted([str(x) for x in df_tab["캠페인"].unique() if str(x).strip()])
        sel_camp = st.selectbox("캠페인 필터", camps, key=f"ad_tab_f1_{ad_type_name}")
    with c2:
        if sel_camp != "전체":
            filtered_grp = df_tab[df_tab["캠페인"] == sel_camp]
            grps = ["전체"] + sorted([str(x) for x in filtered_grp["광고그룹"].unique() if str(x).strip()])
            sel_grp = st.selectbox("광고그룹 필터", grps, key=f"ad_tab_f2_{ad_type_name}")
        else:
            sel_grp = "전체"
            st.selectbox("광고그룹 필터", ["전체"], disabled=True, key=f"ad_tab_f2_dis_{ad_type_name}")
    st.markdown("</div>", unsafe_allow_html=True)

    if sel_camp != "전체":
        df_tab = df_tab[df_tab["캠페인"] == sel_camp]
        if sel_grp != "전체":
            df_tab = df_tab[df_tab["광고그룹"] == sel_grp]
            with st.container(border=True):
                st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:8px;'>A/B 비교</div>", unsafe_allow_html=True)
                _render_ab_test_sbs(df_tab, start_dt, end_dt)

    cols = ["업체명", "담당자", "캠페인", "광고그룹", "소재내용", "노출", "클릭", "CTR(%)", "광고비", "CPC(원)", "전환", "CVR(%)", "CPA(원)", "전환매출", "ROAS(%)"]
    disp = df_tab[[c for c in cols if c in df_tab.columns]].copy()
    disp = disp.sort_values("광고비", ascending=False).head(top_n)

    with st.container(border=True):
        st.markdown(f"<div style='font-size:14px; font-weight:700; margin-bottom:8px;'>{ad_type_name} 성과 표</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='ad-section-sub'>표시 행 수: {len(disp):,}개</div>", unsafe_allow_html=True)
        _render_ad_sticky_table(disp, "소재내용", height=500, col_config=FAST_AD_CONFIG)

@st.fragment
def render_landing_tab(view):
    if "landing_url" not in view.columns:
        st.info("랜딩페이지 URL 컬럼이 없습니다.")
        return
    df_lp = view[view["landing_url"].astype(str) != ""].copy()
    if df_lp.empty:
        st.info("수집된 URL 데이터가 없습니다.")
        return
    lp_grp = df_lp.groupby("landing_url", as_index=False)[["노출", "클릭", "광고비", "전환", "전환매출"]].sum()
    lp_grp["CTR(%)"] = np.where(lp_grp["노출"] > 0, (lp_grp["클릭"] / lp_grp["노출"]) * 100, 0)
    lp_grp["CVR(%)"] = np.where(lp_grp["클릭"] > 0, (lp_grp["전환"] / lp_grp["클릭"]) * 100, 0)
    lp_grp["ROAS(%)"] = np.where(lp_grp["광고비"] > 0, (lp_grp["전환매출"] / lp_grp["광고비"]) * 100, 0)
    lp_grp = lp_grp.rename(columns={"landing_url": "랜딩페이지 URL"}).sort_values("광고비", ascending=False)
    with st.container(border=True):
        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:8px;'>랜딩페이지(URL) 효율</div>", unsafe_allow_html=True)
        st.markdown("<div class='ad-section-sub'>소재별 URL 수집값이 있는 경우만 표시됩니다.</div>", unsafe_allow_html=True)
        _render_ad_sticky_table(lp_grp, "랜딩페이지 URL", height=500, col_config=FAST_AD_CONFIG)

@st.fragment
def render_ad_cmp_tab(view, engine, cids, type_sel, top_n, start_dt, end_dt):
    st.markdown("<div style='display:flex; justify-content:flex-start; margin-bottom:8px;'>", unsafe_allow_html=True)
    show_deltas = st.toggle("증감률 보기", value=False, key="ad_abs_toggle")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='ad-toolbar'><div class='ad-toolbar-title'>기간 비교 설정</div>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1: cmp_sub_mode = st.radio("매체 대상", ["파워링크", "쇼핑검색"], horizontal=True, key="ad_cmp_sub")
    with c2: ad_kind_cmp = st.radio("소재 유형", ["일반 소재", "확장소재"], horizontal=True, key="ad_cmp_kind")
    with c3:
        opts = get_dynamic_cmp_options(start_dt, end_dt)
        cmp_mode = st.radio("비교 기준", [o for o in opts if o != "비교 안함"], horizontal=True, key="ad_cmp_base")
    st.markdown("</div>", unsafe_allow_html=True)

    b1, b2 = period_compare_range(start_dt, end_dt, cmp_mode)
    base_ad_bundle = query_ad_bundle(engine, b1, b2, cids, type_sel, topn_cost=_ad_fetch_limit(top_n, compare=True), top_k=50)

    df_target = view[view["캠페인유형"] == "파워링크"].copy() if "파워링크" in cmp_sub_mode else view[view["캠페인유형"] == "쇼핑검색"].copy()
    df_target = _filter_by_ad_kind(df_target, ad_kind_cmp)

    if df_target.empty:
        st.info("비교할 데이터가 없습니다.")
        return

    if not base_ad_bundle.empty:
        valid_keys = [k for k in ['customer_id', 'ad_id'] if k in df_target.columns and k in base_ad_bundle.columns]
        if valid_keys: df_target = _apply_comparison_metrics(df_target, base_ad_bundle, valid_keys)

    st.markdown("<div class='ad-toolbar'><div class='ad-toolbar-title'>비교 필터</div>", unsafe_allow_html=True)
    c_f1, _ = st.columns(2)
    with c_f1: sel_c = st.selectbox("캠페인 필터", ["전체"] + sorted(df_target["캠페인"].unique().tolist()), key="ad_cmp_f1")
    st.markdown("</div>", unsafe_allow_html=True)

    if sel_c != "전체": df_target = df_target[df_target["캠페인"] == sel_c]

    if not base_ad_bundle.empty:
        base_ad_bundle = _build_material_name(base_ad_bundle)
        base_for_search = base_ad_bundle.rename(columns={"final_ad_name": "소재내용"})
        base_for_search = _filter_by_ad_kind(base_for_search, ad_kind_cmp)
    else: base_for_search = pd.DataFrame()
        
    with st.container(border=True):
        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:8px;'>개별 소재 비교</div>", unsafe_allow_html=True)
        render_item_comparison_search("소재", df_target, base_for_search, "소재내용", start_dt, end_dt, b1, b2)

    metrics_cols_cmp = []
    metrics_cols_cmp.extend(["노출", "노출 증감", "노출 차이"] if show_deltas else ["노출"])
    metrics_cols_cmp.extend(["클릭", "클릭 증감", "클릭 차이"] if show_deltas else ["클릭"])
    metrics_cols_cmp.extend(["CTR(%)", "CTR 증감"] if show_deltas else ["CTR(%)"])
    metrics_cols_cmp.extend(["광고비", "광고비 증감", "광고비 차이"] if show_deltas else ["광고비"])
    metrics_cols_cmp.extend(["CPC(원)", "CPC 증감", "CPC 차이"] if show_deltas else ["CPC(원)"])
    metrics_cols_cmp.extend(["전환", "전환 증감", "전환 차이"] if show_deltas else ["전환"])
    metrics_cols_cmp.extend(["CVR(%)", "CVR 증감"] if show_deltas else ["CVR(%)"])
    metrics_cols_cmp.extend(["CPA(원)"])
    metrics_cols_cmp.extend(["전환매출", "전환매출 증감", "전환매출 차이"] if show_deltas else ["전환매출"])
    metrics_cols_cmp.extend(["ROAS(%)", "ROAS 증감"] if show_deltas else ["ROAS(%)"])

    base_cols_cmp = ["업체명", "캠페인", "광고그룹", "소재내용"]
    cols_cmp = [c for c in base_cols_cmp + metrics_cols_cmp if c in df_target.columns]
    disp_c = df_target[cols_cmp].sort_values("광고비", ascending=False).head(top_n)

    with st.container(border=True):
        st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:8px;'>소재 기간 비교 데이터</div>", unsafe_allow_html=True)
        st.dataframe(disp_c, use_container_width=True, height=500, hide_index=True, column_config=_ad_cmp_fast_col_config(disp_c))

def page_perf_ad(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    _inject_ad_css()
    render_toolbar(
        "광고 소재 및 랜딩페이지 분석",
        "소재 유형, 캠페인, 랜딩 URL을 기준으로 성과와 비교 데이터를 점검합니다.",
        [{"label": f"{f['start']} ~ {f['end']}", "tone": "primary"}, {"label": f"Top {int(f.get('top_n_ad', 100)):,}", "tone": "info"}],
    )

    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_ad", 100))
    selected_tab = st.pills("분석 탭 선택", ["파워링크", "쇼핑검색", "랜딩페이지 효율", "기간 비교"], default="파워링크")

    bundle = query_ad_bundle(
        engine,
        f["start"],
        f["end"],
        cids,
        type_sel,
        topn_cost=_ad_fetch_limit(top_n, compare=(selected_tab == "기간 비교")),
        top_k=50,
    )

    view = compute_ad_view(bundle, meta)
    if view.empty:
        st.info("해당 기간에 분석할 유효한 광고 소재(카피) 데이터가 없습니다.")
        return

    if selected_tab == "파워링크":
        df_pl = view[view["캠페인유형"] == "파워링크"].copy()
        ad_kind = st.segmented_control("소재 유형 필터", ["일반 소재", "확장소재"], default="일반 소재", key="pl_ad_kind")
        if ad_kind:
            df_pl = _filter_by_ad_kind(df_pl, ad_kind)
            _render_ad_tab(df_pl, f"파워링크 ({ad_kind})", top_n, f["start"], f["end"])

    elif selected_tab == "쇼핑검색":
        df_shop = view[view["캠페인유형"] == "쇼핑검색"].copy()
        ad_kind = st.segmented_control("소재 유형 필터", ["일반 소재", "확장소재"], default="확장소재", key="shop_ad_kind")
        if ad_kind:
            df_shop = _filter_by_ad_kind(df_shop, ad_kind)
            _render_ad_tab(df_shop, f"쇼핑검색 ({ad_kind})", top_n, f["start"], f["end"])

    elif selected_tab == "랜딩페이지 효율":
        render_landing_tab(view)

    elif selected_tab == "기간 비교":
        render_ad_cmp_tab(view, engine, cids, type_sel, top_n, f["start"], f["end"])
