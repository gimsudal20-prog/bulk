# -*- coding: utf-8 -*-
"""view_shopping_query.py - Shopping Search Term performance view (unified UI/UX)."""

from __future__ import annotations
import io
from datetime import date
from typing import Dict

import numpy as np
import pandas as pd
import streamlit as st
import streamlit_compat  # noqa: F401

from data import query_shopping_placement_performance, query_shopping_search_terms
from page_helpers import _perf_common_merge_meta, period_compare_range
from ui import render_toolbar, safe_numeric_col, safe_numeric_series


# ✨ 숫자 콤마 및 기호 포맷팅 딕셔너리
FMT_DICT = {
    "구매완료수": "{:,.0f}",
    "구매완료 매출": "{:,.0f}원",
    "장바구니수": "{:,.0f}",
    "장바구니 매출액": "{:,.0f}원",
    "총 전환수": "{:,.0f}",
    "총 전환매출": "{:,.0f}원",
    "구매완료수 증감": "{:+.1f}%",
    "구매완료 매출 증감": "{:+.1f}%",
    "총 전환수 증감": "{:+.1f}%",
    "총 전환매출 증감": "{:+.1f}%",
}

def _style_delta_numeric(val):
    try: v = float(val)
    except: return ''
    if pd.isna(v) or v == 0: return ''
    return 'color: #1A73E8; font-weight: 700;' if v > 0 else 'color: #EA4335; font-weight: 700;'

def _apply_delta_styles(styler, df: pd.DataFrame):
    pos_cols = [c for c in ['구매완료수 증감', '구매완료 매출 증감', '총 전환수 증감', '총 전환매출 증감', '광고비 증감', '클릭수 증감'] if c in df.columns]
    try:
        if pos_cols: styler = styler.map(_style_delta_numeric, subset=pos_cols)
    except AttributeError:
        if pos_cols: styler = styler.applymap(_style_delta_numeric, subset=pos_cols)
    return styler


@st.cache_data(show_spinner=False, ttl=300)
def _cached_sq_terms(_engine, d1, d2, cids: tuple):
    return query_shopping_search_terms(_engine, d1, d2, cids)


@st.cache_data(show_spinner=False, ttl=300)
def _cached_sq_placement(_engine, d1, d2, cids: tuple):
    return query_shopping_placement_performance(_engine, d1, d2, cids)


def _build_sq_excel_bytes(df: pd.DataFrame) -> bytes:
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer) as writer:
        df.to_excel(writer, sheet_name="쇼핑_검색어_성과", index=False)
    return excel_buffer.getvalue()


def _build_placement_excel_bytes(df: pd.DataFrame) -> bytes:
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer) as writer:
        df.to_excel(writer, sheet_name="검색콘텐츠_지면성과", index=False)
    return excel_buffer.getvalue()

def _empty_notice(message: str):
    st.info(message)


def _to_num(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)
    return out


def _pct_change(cur, base):
    length = len(cur.index) if isinstance(cur, pd.Series) else (len(base.index) if isinstance(base, pd.Series) else None)
    cur = safe_numeric_series(cur, length=length, default=0)
    base = safe_numeric_series(base, length=length, default=0)
    diff = cur - base
    safe_base = np.where(base == 0, 1, base)
    pct = np.where(base == 0, np.where(cur > 0, 100.0, 0.0), (diff / safe_base) * 100.0)
    return pct, diff


def _merge_compare(cur: pd.DataFrame, prev: pd.DataFrame) -> pd.DataFrame:
    keys = [c for c in ["customer_id", "campaign_name", "adgroup_name", "query_text"] if c in cur.columns and c in prev.columns]
    if not keys:
        return cur.copy()

    c = cur.copy()
    p = prev.copy()
    for k in keys:
        c[k] = c[k].astype(str)
        p[k] = p[k].astype(str)

    val_cols = [
        "purchase_conv", "purchase_sales", "cart_conv", "cart_sales",
        "total_conv", "total_sales",
    ]
    p = _to_num(p, val_cols)
    p = p[keys + [x for x in val_cols if x in p.columns]].copy()
    p = p.rename(columns={x: f"b_{x}" for x in val_cols if x in p.columns})
    out = c.merge(p, on=keys, how="left")
    for x in val_cols:
        bx = f"b_{x}"
        if bx not in out.columns:
            out[bx] = 0
        out[bx] = pd.to_numeric(out[bx], errors="coerce").fillna(0)

    out["구매완료수 증감"], out["구매완료수 차이"] = _pct_change(safe_numeric_col(out, "purchase_conv"), safe_numeric_col(out, "b_purchase_conv"))
    out["구매완료 매출 증감"], out["구매완료 매출 차이"] = _pct_change(safe_numeric_col(out, "purchase_sales"), safe_numeric_col(out, "b_purchase_sales"))
    out["총 전환수 증감"], out["총 전환수 차이"] = _pct_change(safe_numeric_col(out, "total_conv"), safe_numeric_col(out, "b_total_conv"))
    out["총 전환매출 증감"], out["총 전환매출 차이"] = _pct_change(safe_numeric_col(out, "total_sales"), safe_numeric_col(out, "b_total_sales"))

    return out


def _render_top_cards(view: pd.DataFrame, cmp_mode: str):
    q_cnt = int(len(view))
    purchase_cnt = int((safe_numeric_col(view, "구매완료수") > 0).sum())
    cart_cnt = int((safe_numeric_col(view, "장바구니수") > 0).sum())

    with st.container(border=True):
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("검색어 수", f"{q_cnt:,}개", help="조회 기간 내 실제 검색어")
        with c2: st.metric("구매 발생", f"{purchase_cnt:,}개", help=f"{cmp_mode} 기준 증감 태그 포함")
        with c3: st.metric("장바구니 발생", f"{cart_cnt:,}개")


def _render_filter_panel(view: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, object]]:
    with st.container(border=True):
        st.markdown("<div style='font-size:15px;font-weight:700;color:#1F2937;margin-bottom:12px;'>쇼핑 검색어 필터</div>", unsafe_allow_html=True)
        filtered = view.copy()

        r1c1, r1c2 = st.columns(2)
        camps = ["전체"] + sorted([str(x) for x in filtered["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in filtered.columns else ["전체"]
        sel_camp = r1c1.selectbox("캠페인", camps, key="sq_camp_filter_unified")
        if sel_camp != "전체":
            filtered = filtered[filtered["캠페인"] == sel_camp]

        grps = ["전체"] + sorted([str(x) for x in filtered["광고그룹"].dropna().unique() if str(x).strip()]) if "광고그룹" in filtered.columns else ["전체"]
        sel_grp = r1c2.selectbox("광고그룹", grps, key="sq_grp_filter_unified")
        if sel_grp != "전체":
            filtered = filtered[filtered["광고그룹"] == sel_grp]

        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
        r2c1, r2c2, r2c3 = st.columns(3)
        only_purchase = r2c1.checkbox("구매 발생만", key="sq_only_purchase_unified")
        only_cart = r2c2.checkbox("장바구니 발생만", key="sq_only_cart_unified")
        q_text = r2c3.text_input("검색어 포함", value="", key="sq_query_contains_unified", placeholder="예: 의자")

        r3c1, r3c2 = st.columns(2)
        min_purchase_sales = r3c1.number_input("최소 구매매출", min_value=0, value=0, step=10000, key="sq_min_purchase_sales_unified")
        min_total_conv = r3c2.number_input("최소 총 전환수", min_value=0, value=0, step=1, key="sq_min_total_conv_unified")

        if only_purchase:
            filtered = filtered[pd.to_numeric(filtered["구매완료수"], errors="coerce").fillna(0) > 0]
        if only_cart:
            filtered = filtered[pd.to_numeric(filtered["장바구니수"], errors="coerce").fillna(0) > 0]
        if q_text.strip():
            filtered = filtered[filtered["실제 검색어"].astype(str).str.contains(q_text.strip(), case=False, na=False)]
        if min_purchase_sales > 0:
            filtered = filtered[pd.to_numeric(filtered["구매완료 매출"], errors="coerce").fillna(0) >= float(min_purchase_sales)]
        if min_total_conv > 0:
            filtered = filtered[pd.to_numeric(filtered["총 전환수"], errors="coerce").fillna(0) >= float(min_total_conv)]

    filter_state = {
        "sel_camp": sel_camp,
        "sel_grp": sel_grp,
        "q_text": q_text.strip(),
        "min_purchase_sales": float(min_purchase_sales),
        "min_total_conv": float(min_total_conv),
        "only_purchase": bool(only_purchase),
        "only_cart": bool(only_cart),
    }
    return filtered, filter_state


def _render_shopping_search_terms_tab(meta: pd.DataFrame, engine, f: Dict, cids: tuple, cmp_mode: str, b1: date, b2: date) -> None:
    with st.spinner("🔄 쇼핑 검색어 데이터를 불러오는 중입니다..."):
        df_cur = _cached_sq_terms(engine, f["start"], f["end"], cids)
        if df_cur.empty:
            _empty_notice("해당 기간에 수집된 쇼핑 검색어 전환 데이터가 없습니다.")
            return
        df_prev = _cached_sq_terms(engine, b1, b2, cids)
        df_cur = _perf_common_merge_meta(df_cur, meta)
        if not df_prev.empty:
            df_prev = _perf_common_merge_meta(df_prev, meta)
        df = _merge_compare(df_cur, df_prev)

    view = df.rename(columns={
        "account_name": "업체명",
        "manager": "담당자",
        "campaign_name": "캠페인",
        "adgroup_name": "광고그룹",
        "query_text": "실제 검색어",
        "purchase_conv": "구매완료수",
        "purchase_sales": "구매완료 매출",
        "cart_conv": "장바구니수",
        "cart_sales": "장바구니 매출액",
        "total_conv": "총 전환수",
        "total_sales": "총 전환매출",
    }).copy()
    raw_query = view["실제 검색어"].astype(str).str.strip() if "실제 검색어" in view.columns else pd.Series([], dtype=str)
    if "query_provided" in view.columns:
        provided_mask = view["query_provided"].fillna(True).astype(bool)
    else:
        provided_mask = ~raw_query.isin(["", "-", "(검색어 미제공)", "(검색어 미제공 영역)"])
    view.loc[~provided_mask, "실제 검색어"] = "(검색어 미제공 영역)"
    view["전환출처"] = np.where(provided_mask, "검색어 상세", "검색어 미제공(리포트 -)")

    numeric_cols = [
        "구매완료수", "구매완료 매출", "장바구니수", "장바구니 매출액",
        "총 전환수", "총 전환매출",
        "구매완료수 증감", "구매완료 매출 증감", "총 전환수 증감", "총 전환매출 증감",
    ]
    view = _to_num(view, numeric_cols)

    _render_top_cards(view, cmp_mode)
    filtered, filter_state = _render_filter_panel(view)

    display_cols = [
        "업체명", "캠페인", "광고그룹", "실제 검색어", "전환출처",
        "구매완료수", "구매완료 매출", "장바구니수", "총 전환수", "총 전환매출",
        "구매완료수 증감", "구매완료 매출 증감", "총 전환수 증감",
    ]
    disp = filtered[[c for c in display_cols if c in filtered.columns]].sort_values(["구매완료 매출", "총 전환매출"], ascending=False).head(500).copy()

    with st.container(border=True):
        st.markdown("<div style='font-size:15px;font-weight:700;margin-bottom:12px;'>검색어별 퍼널 성과 (상위 500개)</div>", unsafe_allow_html=True)
        if disp.empty:
            _empty_notice("조건에 맞는 검색어가 없습니다.")
        else:
            # ✨ 세 자리 콤마 포맷팅 & 증감률 컬러 스타일링 적용
            safe_fmt = {k: v for k, v in FMT_DICT.items() if k in disp.columns}
            styled_disp = disp.style.format(safe_fmt)
            styled_disp = _apply_delta_styles(styled_disp, disp)
            st.dataframe(styled_disp, use_container_width=True, hide_index=True)

    st.markdown("<div style='margin-bottom:16px;'></div>", unsafe_allow_html=True)
    
    with st.container(border=True):
        st.markdown("<div style='font-size:15px;font-weight:700;margin-bottom:8px;'>리포트 다운로드</div><div style='font-size:13px;color:#6B7280;margin-bottom:12px;'>현재 필터 기준 결과를 엑셀로 내려받습니다.</div>", unsafe_allow_html=True)
        cache_key = (
            f"sq_excel::{f['start']}::{f['end']}::{cmp_mode}::"
            f"{filter_state['sel_camp']}::{filter_state['sel_grp']}::{filter_state['q_text']}::"
            f"{int(filter_state['min_purchase_sales'])}::{int(filter_state['min_total_conv'])}::"
            f"{int(filter_state['only_purchase'])}::{int(filter_state['only_cart'])}"
        )
        if st.button("엑셀 파일 준비", key="sq_prepare_excel_btn", use_container_width=True):
            st.session_state[cache_key] = _build_sq_excel_bytes(disp)
        excel_bytes = st.session_state.get(cache_key)
        if excel_bytes:
            st.download_button(
                label="검색어 리포트 다운로드 (Excel)",
                data=excel_bytes,
                file_name=f"쇼핑_검색어_리포트_{f['start']}_{f['end']}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="sq_download_excel_btn",
            )


def _placement_label(value) -> str:
    raw = str(value or "").strip().upper()
    if raw == "SEARCH":
        return "검색"
    if raw == "CONTENT":
        return "콘텐츠"
    return str(value or "미분류")


def _device_label(value) -> str:
    raw = str(value or "").strip().upper()
    if raw == "MOBILE":
        return "모바일"
    if raw == "PC":
        return "PC"
    if raw == "UNSEGMENTED":
        return "미분리"
    return str(value or "미분류")


def _add_rate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = _to_num(df, ["imp", "clk", "cost", "conv", "sales", "purchase_conv", "purchase_sales"])
    out["CTR(%)"] = np.where(safe_numeric_col(out, "imp") > 0, safe_numeric_col(out, "clk") / safe_numeric_col(out, "imp") * 100, 0.0)
    out["CPC"] = np.where(safe_numeric_col(out, "clk") > 0, safe_numeric_col(out, "cost") / safe_numeric_col(out, "clk"), 0.0)
    out["구매완료 ROAS(%)"] = np.where(safe_numeric_col(out, "cost") > 0, safe_numeric_col(out, "purchase_sales") / safe_numeric_col(out, "cost") * 100, 0.0)
    out["총 ROAS(%)"] = np.where(safe_numeric_col(out, "cost") > 0, safe_numeric_col(out, "sales") / safe_numeric_col(out, "cost") * 100, 0.0)
    return out


def _merge_placement_compare(cur: pd.DataFrame, prev: pd.DataFrame) -> pd.DataFrame:
    keys = [c for c in ["customer_id", "campaign_id", "adgroup_id", "device_name", "placement_type"] if c in cur.columns and c in prev.columns]
    if not keys:
        return cur.copy()
    c = cur.copy()
    p = prev.copy()
    for key in keys:
        c[key] = c[key].astype(str)
        p[key] = p[key].astype(str)
    val_cols = ["imp", "clk", "cost", "purchase_conv", "purchase_sales", "conv", "sales"]
    p = _to_num(p, val_cols)
    p = p[keys + [x for x in val_cols if x in p.columns]].rename(columns={x: f"b_{x}" for x in val_cols if x in p.columns})
    out = c.merge(p, on=keys, how="left")
    for col in val_cols:
        b_col = f"b_{col}"
        if b_col not in out.columns:
            out[b_col] = 0
        out[b_col] = pd.to_numeric(out[b_col], errors="coerce").fillna(0)
    out["광고비 증감"], _ = _pct_change(safe_numeric_col(out, "cost"), safe_numeric_col(out, "b_cost"))
    out["구매완료수 증감"], _ = _pct_change(safe_numeric_col(out, "purchase_conv"), safe_numeric_col(out, "b_purchase_conv"))
    out["클릭수 증감"], _ = _pct_change(safe_numeric_col(out, "clk"), safe_numeric_col(out, "b_clk"))
    return out


def _render_placement_cards(view: pd.DataFrame) -> None:
    total_cost = safe_numeric_col(view, "광고비").sum()
    total_purchase = safe_numeric_col(view, "구매완료수").sum()
    total_sales = safe_numeric_col(view, "구매완료 매출").sum()
    roas = (total_sales / total_cost * 100) if total_cost else 0.0
    search_cost = safe_numeric_col(view[view["지면"] == "검색"], "광고비").sum() if "지면" in view.columns else 0.0
    content_cost = safe_numeric_col(view[view["지면"] == "콘텐츠"], "광고비").sum() if "지면" in view.columns else 0.0
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("지면 행", f"{len(view.index):,}개")
        c2.metric("광고비", f"{total_cost:,.0f}원")
        c3.metric("구매완료", f"{total_purchase:,.0f}건")
        c4.metric("구매완료 ROAS", f"{roas:,.0f}%")
        st.caption(f"검색 광고비 {search_cost:,.0f}원 · 콘텐츠 광고비 {content_cost:,.0f}원")


def _render_placement_filter_panel(view: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, object]]:
    with st.container(border=True):
        st.markdown("<div style='font-size:15px;font-weight:700;color:#1F2937;margin-bottom:12px;'>검색/콘텐츠 지면 필터</div>", unsafe_allow_html=True)
        filtered = view.copy()
        r1c1, r1c2, r1c3, r1c4 = st.columns(4)
        types = ["전체"] + sorted([str(x) for x in filtered.get("캠페인유형", pd.Series(dtype=str)).dropna().unique() if str(x).strip()])
        placements = ["전체"] + sorted([str(x) for x in filtered.get("지면", pd.Series(dtype=str)).dropna().unique() if str(x).strip()])
        devices = ["전체"] + sorted([str(x) for x in filtered.get("기기", pd.Series(dtype=str)).dropna().unique() if str(x).strip()])
        camps = ["전체"] + sorted([str(x) for x in filtered.get("캠페인", pd.Series(dtype=str)).dropna().unique() if str(x).strip()])
        sel_type = r1c1.selectbox("캠페인유형", types, key="sq_place_type_filter")
        sel_place = r1c2.selectbox("지면", placements, key="sq_place_filter")
        sel_device = r1c3.selectbox("기기", devices, key="sq_place_device_filter")
        sel_camp = r1c4.selectbox("캠페인", camps, key="sq_place_camp_filter")
        if sel_type != "전체":
            filtered = filtered[filtered["캠페인유형"] == sel_type]
        if sel_place != "전체":
            filtered = filtered[filtered["지면"] == sel_place]
        if sel_device != "전체":
            filtered = filtered[filtered["기기"] == sel_device]
        if sel_camp != "전체":
            filtered = filtered[filtered["캠페인"] == sel_camp]

        r2c1, r2c2, r2c3 = st.columns(3)
        grps = ["전체"] + sorted([str(x) for x in filtered.get("광고그룹", pd.Series(dtype=str)).dropna().unique() if str(x).strip()])
        sel_grp = r2c1.selectbox("광고그룹", grps, key="sq_place_grp_filter")
        min_cost = r2c2.number_input("최소 광고비", min_value=0, value=0, step=10000, key="sq_place_min_cost")
        only_purchase = r2c3.checkbox("구매 발생만", key="sq_place_only_purchase")
        if sel_grp != "전체":
            filtered = filtered[filtered["광고그룹"] == sel_grp]
        if min_cost > 0:
            filtered = filtered[pd.to_numeric(filtered["광고비"], errors="coerce").fillna(0) >= float(min_cost)]
        if only_purchase:
            filtered = filtered[pd.to_numeric(filtered["구매완료수"], errors="coerce").fillna(0) > 0]

    return filtered, {
        "campaign_type": sel_type,
        "placement": sel_place,
        "device": sel_device,
        "campaign": sel_camp,
        "adgroup": sel_grp,
        "min_cost": int(min_cost),
        "only_purchase": bool(only_purchase),
    }


def _render_shopping_placement_tab(meta: pd.DataFrame, engine, f: Dict, cids: tuple, cmp_mode: str, b1: date, b2: date) -> None:
    with st.spinner("검색/콘텐츠 지면 데이터를 불러오는 중입니다..."):
        df_cur = _cached_sq_placement(engine, f["start"], f["end"], cids)
        if df_cur.empty:
            _empty_notice("해당 기간에 수집된 검색/콘텐츠 지면 데이터가 없습니다.")
            return
        df_prev = _cached_sq_placement(engine, b1, b2, cids)
        df_cur = _perf_common_merge_meta(df_cur, meta)
        if not df_prev.empty:
            df_prev = _perf_common_merge_meta(df_prev, meta)
        df = _merge_placement_compare(_add_rate_metrics(df_cur), _add_rate_metrics(df_prev))

    view = df.rename(columns={
        "account_name": "업체명",
        "manager": "담당자",
        "campaign_type_label": "캠페인유형",
        "campaign_name": "캠페인",
        "adgroup_name": "광고그룹",
        "imp": "노출수",
        "clk": "클릭수",
        "cost": "광고비",
        "purchase_conv": "구매완료수",
        "purchase_sales": "구매완료 매출",
        "conv": "총 전환수",
        "sales": "총 전환매출",
    }).copy()
    view["지면"] = df["placement_type"].apply(_placement_label) if "placement_type" in df.columns else "미분류"
    view["기기"] = df["device_name"].apply(_device_label) if "device_name" in df.columns else "미분류"
    view = _to_num(view, ["노출수", "클릭수", "광고비", "구매완료수", "구매완료 매출", "총 전환수", "총 전환매출", "CTR(%)", "CPC", "구매완료 ROAS(%)", "총 ROAS(%)", "광고비 증감", "구매완료수 증감", "클릭수 증감"])

    _render_placement_cards(view)
    filtered, filter_state = _render_placement_filter_panel(view)
    display_cols = [
        "업체명", "캠페인유형", "캠페인", "광고그룹", "지면", "기기",
        "노출수", "클릭수", "CTR(%)", "CPC", "광고비",
        "구매완료수", "구매완료 매출", "구매완료 ROAS(%)",
        "총 전환수", "총 전환매출", "총 ROAS(%)",
        "광고비 증감", "구매완료수 증감", "클릭수 증감",
    ]
    disp = filtered[[c for c in display_cols if c in filtered.columns]].sort_values(["광고비", "클릭수"], ascending=False).head(800).copy()
    with st.container(border=True):
        st.markdown("<div style='font-size:15px;font-weight:700;margin-bottom:12px;'>검색/콘텐츠 지면별 성과</div>", unsafe_allow_html=True)
        if disp.empty:
            _empty_notice("조건에 맞는 지면 데이터가 없습니다.")
        else:
            fmt = {
                "노출수": "{:,.0f}", "클릭수": "{:,.0f}", "CTR(%)": "{:,.2f}%",
                "CPC": "{:,.0f}원", "광고비": "{:,.0f}원", "구매완료수": "{:,.0f}",
                "구매완료 매출": "{:,.0f}원", "구매완료 ROAS(%)": "{:,.0f}%",
                "총 전환수": "{:,.0f}", "총 전환매출": "{:,.0f}원", "총 ROAS(%)": "{:,.0f}%",
                "광고비 증감": "{:+.1f}%", "구매완료수 증감": "{:+.1f}%", "클릭수 증감": "{:+.1f}%",
            }
            styled = disp.style.format({k: v for k, v in fmt.items() if k in disp.columns})
            styled = _apply_delta_styles(styled, disp)
            st.dataframe(styled, use_container_width=True, hide_index=True)

    with st.container(border=True):
        st.markdown("<div style='font-size:15px;font-weight:700;margin-bottom:8px;'>지면 리포트 다운로드</div><div style='font-size:13px;color:#6B7280;margin-bottom:12px;'>현재 필터 기준 결과를 엑셀로 내려받습니다.</div>", unsafe_allow_html=True)
        cache_key = (
            f"sq_place_excel::{f['start']}::{f['end']}::{cmp_mode}::"
            f"{filter_state['campaign_type']}::{filter_state['placement']}::{filter_state['device']}::{filter_state['campaign']}::"
            f"{filter_state['adgroup']}::{filter_state['min_cost']}::{int(filter_state['only_purchase'])}"
        )
        if st.button("지면 엑셀 파일 준비", key="sq_place_prepare_excel_btn", use_container_width=True):
            st.session_state[cache_key] = _build_placement_excel_bytes(disp)
        excel_bytes = st.session_state.get(cache_key)
        if excel_bytes:
            st.download_button(
                label="지면 리포트 다운로드 (Excel)",
                data=excel_bytes,
                file_name=f"쇼핑_검색콘텐츠_지면_리포트_{f['start']}_{f['end']}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="sq_place_download_excel_btn",
            )


@st.fragment
def page_perf_shopping_query(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False):
        return

    render_toolbar(
        "쇼핑 검색어 분석",
        "실제 검색어, 검색어 미제공 영역, 검색/콘텐츠 지면 기준으로 구매와 전환 퍼널 성과를 확인합니다.",
        [{"label": f"{f['start']} ~ {f['end']}", "tone": "primary"}, {"label": f.get("media_label", "전체 매체"), "tone": "success"}, {"label": "검색어 · 지면", "tone": "info"}],
    )

    cids = tuple(f.get("selected_customer_ids", []))
    patch_date = date(2026, 3, 11)
    if f["start"] < patch_date:
        st.info("3월 11일 이전 데이터가 포함되어 있어 퍼널 분리값 일부가 비어 있을 수 있습니다.")

    st.markdown("<div style='display:flex; justify-content:flex-end; margin-bottom:8px;'>", unsafe_allow_html=True)
    cmp_mode = st.radio("비교 기준", ["이전 같은 기간 대비", "전주대비", "전일대비"], horizontal=True, key="sq_cmp_mode_unified")
    st.markdown("</div>", unsafe_allow_html=True)
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)

    tab_terms, tab_place = st.tabs(["검색어 성과", "검색/콘텐츠 지면"])
    with tab_terms:
        _render_shopping_search_terms_tab(meta, engine, f, cids, cmp_mode, b1, b2)
    with tab_place:
        _render_shopping_placement_tab(meta, engine, f, cids, cmp_mode, b1, b2)
