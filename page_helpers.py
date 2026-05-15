# -*- coding: utf-8 -*-
"""page_helpers.py - Shared UI helpers, filters, and rendering logic for pages."""

from __future__ import annotations

import os
import re
import textwrap
import numpy as np
import pandas as pd
import streamlit as st
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List

from data import *
from ui import *
from data import pct_change, pct_to_arrow

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return default


BUILD_TAG = os.getenv("APP_BUILD", "")
TOPUP_STATIC_THRESHOLD = _env_int("TOPUP_STATIC_THRESHOLD", 50000)
TOPUP_AVG_DAYS = _env_int("TOPUP_AVG_DAYS", 3)
TOPUP_DAYS_COVER = _env_int("TOPUP_DAYS_COVER", 2)


def _normalize_customer_id_value_for_page(value) -> str:
    """Return a clean customer_id string for UI joins and filters.

    Handles values read as 3469289.0 from Excel/DB and safely drops blank/NaN
    values so pandas never tries to cast NaN directly to int64.
    """
    if pd.isna(value):
        return ""
    value_str = str(value).strip()
    if not value_str or value_str.lower() in {"nan", "none", "nat", "<na>"}:
        return ""
    if re.fullmatch(r"\d+\.0+", value_str):
        return value_str.split(".", 1)[0]
    if re.fullmatch(r"\d+(?:\.\d+)?[eE]\+\d+", value_str):
        try:
            as_float = float(value_str)
            if as_float.is_integer():
                return str(int(as_float))
        except Exception:
            pass
    return value_str


def _normalize_customer_id_series_for_page(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype="string")
    return series.map(_normalize_customer_id_value_for_page).astype("string")


def _customer_id_int_series_for_page(series: pd.Series) -> pd.Series:
    """Normalize customer_id then return valid int64 IDs only."""
    normalized = _normalize_customer_id_series_for_page(series)
    numeric = pd.to_numeric(normalized.replace("", pd.NA), errors="coerce").dropna()
    if numeric.empty:
        return pd.Series(dtype="int64")
    return numeric.astype("int64")

def _all_customer_ids(meta: pd.DataFrame) -> list:
    if meta is None or meta.empty or "customer_id" not in meta.columns:
        return []
    s = _customer_id_int_series_for_page(meta["customer_id"])
    return sorted(s.drop_duplicates().tolist())

@st.cache_data(ttl=43200, max_entries=8, show_spinner=False)
def _build_filter_maps(meta: pd.DataFrame) -> Dict:
    if meta is None or meta.empty:
        return {
            "managers": [],
            "accounts": [],
            "manager_to_accounts": {},
            "manager_to_cids": {},
            "account_to_cids": {},
        }

    work = meta.copy()
    if "manager" in work.columns:
        work["manager"] = work["manager"].astype(str).str.strip()
    else:
        work["manager"] = ""
    if "account_name" in work.columns:
        work["account_name"] = work["account_name"].astype(str).str.strip()
    else:
        work["account_name"] = ""
    if "customer_id" in work.columns:
        work["customer_id"] = _customer_id_int_series_for_page(work["customer_id"])
        work = work.dropna(subset=["customer_id"]).copy()
        work["customer_id"] = work["customer_id"].astype("int64")
    else:
        work["customer_id"] = pd.Series(dtype="int64")

    managers = sorted([x for x in work["manager"].dropna().unique().tolist() if str(x).strip()])
    accounts = sorted([x for x in work["account_name"].dropna().unique().tolist() if str(x).strip()])

    manager_to_accounts: Dict[str, List[str]] = {}
    manager_to_cids: Dict[str, List[int]] = {}
    account_to_cids: Dict[str, List[int]] = {}

    if not work.empty:
        if "manager" in work.columns and "account_name" in work.columns:
            for manager, grp in work.groupby("manager", dropna=False):
                key = str(manager).strip()
                if not key:
                    continue
                manager_to_accounts[key] = sorted([x for x in grp["account_name"].dropna().unique().tolist() if str(x).strip()])
                manager_to_cids[key] = sorted(_customer_id_int_series_for_page(grp["customer_id"]).drop_duplicates().tolist())
        if "account_name" in work.columns:
            for account, grp in work.groupby("account_name", dropna=False):
                key = str(account).strip()
                if not key:
                    continue
                account_to_cids[key] = sorted(_customer_id_int_series_for_page(grp["customer_id"]).drop_duplicates().tolist())

    return {
        "managers": managers,
        "accounts": accounts,
        "manager_to_accounts": manager_to_accounts,
        "manager_to_cids": manager_to_cids,
        "account_to_cids": account_to_cids,
    }


def resolve_customer_ids(meta: pd.DataFrame, manager_sel: list, account_sel: list) -> list:
    if meta is None or meta.empty:
        return []

    manager_sel = [str(x).strip() for x in (manager_sel or []) if str(x).strip()]
    account_sel = [str(x).strip() for x in (account_sel or []) if str(x).strip()]

    # 아무 것도 선택하지 않았으면 전체 계정을 반환한다.
    if not manager_sel and not account_sel:
        return _all_customer_ids(meta)

    df = meta.copy()
    if manager_sel and "manager" in df.columns:
        df = df[df["manager"].astype(str).str.strip().isin(manager_sel)]
    if account_sel and "account_name" in df.columns:
        df = df[df["account_name"].astype(str).str.strip().isin(account_sel)]
    if "customer_id" not in df.columns:
        return []
    s = _customer_id_int_series_for_page(df["customer_id"])
    return sorted(s.drop_duplicates().tolist())

def ui_multiselect(col, label: str, options, default=None, *, key: str, placeholder: str = "선택"):
    try: return col.multiselect(label, options, default=default, key=key, placeholder=placeholder)
    except Exception: return col.multiselect(label, options, default=default, key=key)

def get_dynamic_cmp_options(d1: date, d2: date) -> List[str]:
    delta = (d2 - d1).days + 1
    if delta == 1: return ["비교 안함", "전일대비"]
    elif delta == 7: return ["비교 안함", "전주대비"]
    elif 28 <= delta <= 31: return ["비교 안함", "전월대비"]
    else: return ["비교 안함", "이전 같은 기간 대비"]

def period_compare_range(d1: date, d2: date, cmp_mode: str):
    delta = (d2 - d1).days + 1
    if cmp_mode == "전일대비":
        return d1 - timedelta(days=1), d2 - timedelta(days=1)
    elif cmp_mode == "전주대비":
        return d1 - timedelta(days=7), d2 - timedelta(days=7)
    else:
        return d1 - timedelta(days=delta), d1 - timedelta(days=1)

def _shift_period(direction: str):
    """현재 선택된 기간의 일수(delta)만큼 날짜를 앞/뒤로 이동시키는 콜백 함수"""
    if "filters_v8" not in st.session_state: return
    sv = st.session_state["filters_v8"]
    d1 = sv.get("d1")
    d2 = sv.get("d2")
    
    if not d1 or not d2: return
    
    delta = (d2 - d1).days + 1
    if direction == "prev":
        new_d2 = d1 - timedelta(days=1)
        new_d1 = new_d2 - timedelta(days=delta - 1)
    else:
        new_d1 = d2 + timedelta(days=1)
        new_d2 = new_d1 + timedelta(days=delta - 1)
        
    # 상태값 업데이트
    sv["d1"], sv["d2"] = new_d1, new_d2
    sv["period_mode"] = "직접 선택"
    
    # UI 위젯 State 강제 업데이트 (화면에 즉시 반영)
    st.session_state["f_period_mode"] = "직접 선택"
    st.session_state["f_d1"] = new_d1
    st.session_state["f_d2"] = new_d2

def _today_kst() -> date:
    try:
        return datetime.now(ZoneInfo("Asia/Seoul")).date()
    except Exception:
        return date.today()


def _normalize_filter_state(values: Dict) -> Dict:
    out = dict(values)
    out["q"] = str(out.get("q", "") or "")
    out["manager"] = list(out.get("manager", []) or [])
    out["account"] = list(out.get("account", []) or [])
    out["type_sel"] = list(out.get("type_sel", []) or [])
    out["top_n_campaign"] = int(out.get("top_n_campaign", 200) or 200)
    out["top_n_keyword"] = int(out.get("top_n_keyword", 300) or 300)
    out["top_n_ad"] = int(out.get("top_n_ad", 200) or 200)
    return out

def build_filters(meta: pd.DataFrame, type_opts: List[str], engine=None) -> Dict:
    today = _today_kst()
    default_end = today - timedelta(days=1)
    default_start = default_end

    if "filters_v8" not in st.session_state:
        st.session_state["filters_v8"] = {
            "q": "", "manager": [], "account": [], "type_sel": [],
            "period_mode": "어제", "d1": default_start, "d2": default_end,
            "top_n_campaign": 200, "top_n_keyword": 300, "top_n_ad": 200, "prefetch_warm": True, "show_diagnostics": False,
        }
    sv = st.session_state["filters_v8"]

    filter_maps = _build_filter_maps(meta)
    managers = filter_maps.get("managers", [])
    accounts = filter_maps.get("accounts", [])

    with st.sidebar:
        st.markdown("<div class='nav-sidebar-title'>Global Filters</div>", unsafe_allow_html=True)

        st.markdown("<div style='font-size:13px; font-weight:600; color:var(--nv-muted); margin-bottom:8px;'>기간 선택</div>", unsafe_allow_html=True)
        
        period_options = ["어제", "오늘", "최근 7일", "최근 30일", "이번 달", "지난 주", "지난 달", "직접 선택"]
        sv_period = sv.get("period_mode", "어제")
        if sv_period not in period_options:
            sv_period = "어제"
            
        # ✨ 위젯 경고 해결: session_state가 없을 때만 초기값 세팅
        if "f_period_mode" not in st.session_state:
            st.session_state["f_period_mode"] = sv_period

        c_prev, c_sel, c_next = st.columns([1.2, 4.6, 1.2])
        with c_prev:
            st.button("◀", key="f_btn_prev", on_click=_shift_period, args=("prev",), use_container_width=True)
        with c_sel:
            period_mode = st.selectbox(
                "기간 간편 선택",
                period_options,
                # ✨ 핵심: index 값을 강제로 넣지 않고 key(session_state)에 온전히 제어권을 넘김
                key="f_period_mode", 
                label_visibility="collapsed"
            )
        with c_next:
            st.button("▶", key="f_btn_next", on_click=_shift_period, args=("next",), use_container_width=True)

        if period_mode == "직접 선택":
            c1, c2 = st.columns(2)
            
            # ✨ 날짜도 마찬가지로 session_state 초기화 후 key로만 제어
            if "f_d1" not in st.session_state:
                st.session_state["f_d1"] = sv.get("d1", default_start)
            if "f_d2" not in st.session_state:
                st.session_state["f_d2"] = sv.get("d2", default_end)
                
            d1 = c1.date_input("시작일", key="f_d1", label_visibility="collapsed")
            d2 = c2.date_input("종료일", key="f_d2", label_visibility="collapsed")
        else:
            if period_mode == "오늘": d2 = d1 = today
            elif period_mode == "어제": d2 = d1 = today - timedelta(days=1)
            elif period_mode == "최근 7일": d2 = today - timedelta(days=1); d1 = d2 - timedelta(days=6)
            elif period_mode == "최근 30일": d2 = today - timedelta(days=1); d1 = d2 - timedelta(days=29)
            elif period_mode == "이번 달": d2 = today; d1 = date(today.year, today.month, 1)
            elif period_mode == "지난 주": d2 = today - timedelta(days=today.weekday() + 1); d1 = today - timedelta(days=today.weekday() + 7)
            elif period_mode == "지난 달": d2 = date(today.year, today.month, 1) - timedelta(days=1); d1 = date(d2.year, d2.month, 1)
            else: d2 = sv.get("d2", default_end); d1 = sv.get("d1", default_start)
            
            # ✨ 다른 모드를 선택했을 때도 세션의 날짜 상태를 갱신해두어야
            # 나중에 '직접 선택'을 눌렀을 때 엉뚱한 날짜가 표시되지 않음
            st.session_state["f_d1"] = d1
            st.session_state["f_d2"] = d2

        st.divider()

        st.markdown("<div style='font-size:13px; font-weight:600; color:var(--nv-muted); margin-bottom:8px;'>담당자 및 계정</div>", unsafe_allow_html=True)
        manager_sel = ui_multiselect(st, "담당자", managers, default=sv.get("manager", []), key="f_manager", placeholder="전체 담당자")

        accounts_by_mgr = accounts
        if manager_sel:
            selected_accounts = []
            manager_to_accounts = filter_maps.get("manager_to_accounts", {})
            for manager in manager_sel:
                selected_accounts.extend(manager_to_accounts.get(str(manager).strip(), []))
            accounts_by_mgr = sorted(set(selected_accounts))

        prev_acc = [a for a in (sv.get("account", []) or []) if a in accounts_by_mgr]
        account_sel = ui_multiselect(st, "광고주(계정)", accounts_by_mgr, default=prev_acc, key="f_account", placeholder="전체 계정")

        selected_scope = "전체 계정"
        if account_sel:
            selected_scope = f"계정 {len(account_sel):,}개 선택"
        elif manager_sel:
            selected_scope = f"담당자 {len(manager_sel):,}명 선택"
        st.markdown(
            f"<div style='display:flex; flex-direction:column; gap:6px; padding:10px 12px; border:1px solid var(--nv-line); border-radius:10px; background:var(--nv-bg); margin-top:12px;'>"
            f"<div style='font-size:11px; color:var(--nv-muted); font-weight:800;'>현재 범위</div>"
            f"<div style='font-size:13px; color:var(--nv-text); font-weight:800;'>{selected_scope}</div>"
            f"<div style='font-size:12px; color:var(--nv-muted);'>{d1} ~ {d2}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

        st.divider()
        
        with st.expander("상세 설정 (검색, 표시 제한)", expanded=False):
            q = st.text_input("텍스트 검색", sv.get("q", ""), key="f_q", placeholder="키워드/캠페인명 입력")
            type_sel = ui_multiselect(st, "광고 유형", type_opts, default=sv.get("type_sel", []), key="f_type_sel", placeholder="전체 광고 유형")
            
            st.markdown("<div style='margin-top:12px; margin-bottom:4px; font-size:12px; font-weight:500; color:var(--nv-muted);'>표시 데이터 수 제한</div>", unsafe_allow_html=True)
            top_n_campaign = st.number_input("캠페인 한도", min_value=10, max_value=2000, value=int(sv.get("top_n_campaign", 200)), step=50, key="f_top_n_campaign")
            top_n_keyword = st.number_input("키워드 한도", min_value=10, max_value=2000, value=int(sv.get("top_n_keyword", 300)), step=50, key="f_top_n_keyword")
            top_n_ad = st.number_input("소재 한도", min_value=10, max_value=2000, value=int(sv.get("top_n_ad", 200)), step=50, key="f_top_n_ad")
            st.markdown("<div style='margin-top:12px; margin-bottom:4px; font-size:12px; font-weight:500; color:var(--nv-muted);'>관리자 옵션</div>", unsafe_allow_html=True)
            show_diagnostics = st.checkbox("조회 진단 보기", value=bool(sv.get("show_diagnostics", False)), key="f_show_diagnostics")

        st.caption("필터 변경 시 즉시 반영됩니다.")

    updated_filters = _normalize_filter_state({
        "q": q or "", "manager": manager_sel or [], "account": account_sel or [],
        "type_sel": type_sel or [], "period_mode": period_mode, "d1": d1, "d2": d2,
        "top_n_campaign": top_n_campaign, "top_n_keyword": top_n_keyword, "top_n_ad": top_n_ad,
        "prefetch_warm": sv.get("prefetch_warm", True), "show_diagnostics": bool(show_diagnostics),
    })
    current_filters = _normalize_filter_state(sv)
    if current_filters != updated_filters:
        st.session_state["filters_v8"] = dict(updated_filters)
        sv = st.session_state["filters_v8"]

    cids = resolve_customer_ids(meta, manager_sel, account_sel)
    if not cids and not (manager_sel or account_sel):
        cids = _all_customer_ids(meta)

    return {
        "q": sv["q"], "manager": sv["manager"], "account": sv["account"], "type_sel": tuple(sv["type_sel"]) if sv["type_sel"] else tuple(),
        "start": d1, "end": d2, "period_mode": period_mode, "customer_ids": cids, "selected_customer_ids": cids,
        "top_n_keyword": int(sv.get("top_n_keyword", 300)), "top_n_ad": int(sv.get("top_n_ad", 200)), "top_n_campaign": int(sv.get("top_n_campaign", 200)),
        "show_diagnostics": bool(sv.get("show_diagnostics", False)),
        "ready": True,
    }

def _perf_common_merge_meta(df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or meta is None or meta.empty:
        return df
    if "customer_id" not in df.columns or "customer_id" not in meta.columns:
        return df

    out = df.copy()
    out["_customer_id_key"] = _normalize_customer_id_series_for_page(out["customer_id"])
    out = out[out["_customer_id_key"].astype(str).str.strip() != ""].copy()

    # Keep customer_id numeric for existing campaign/keyword/ad page logic,
    # but do the metadata join on a normalized string key to avoid 3469289 vs 3469289.0 mismatches.
    out["customer_id"] = pd.to_numeric(out["_customer_id_key"], errors="coerce").astype("Int64")
    out = out.dropna(subset=["customer_id"]).copy()
    out["customer_id"] = out["customer_id"].astype("int64")

    meta_copy = meta.copy()
    meta_copy["_customer_id_key"] = _normalize_customer_id_series_for_page(meta_copy["customer_id"])
    meta_copy = meta_copy[meta_copy["_customer_id_key"].astype(str).str.strip() != ""].copy()
    if meta_copy.empty:
        return out.drop(columns=["_customer_id_key"], errors="ignore")

    if "account_name" not in meta_copy.columns:
        meta_copy["account_name"] = ""
    if "manager" not in meta_copy.columns:
        meta_copy["manager"] = ""

    meta_view = (
        meta_copy[["_customer_id_key", "account_name", "manager"]]
        .drop_duplicates(subset=["_customer_id_key"], keep="last")
    )
    merged = out.merge(meta_view, on="_customer_id_key", how="left")
    return merged.drop(columns=["_customer_id_key"], errors="ignore")
def append_comparison_data(df_cur: pd.DataFrame, df_prev: pd.DataFrame, join_keys: list) -> pd.DataFrame:
    if df_prev is None or df_prev.empty or df_cur is None or df_cur.empty: return df_cur.copy()
    valid_join_keys = [k for k in join_keys if k in df_cur.columns and k in df_prev.columns]
    if not valid_join_keys: return df_cur.copy()
    
    df_c = df_cur.copy()
    for k in valid_join_keys:
        df_c[k] = df_c[k].astype(str)
        df_prev[k] = df_prev[k].astype(str)
        
    num_cols = df_prev.select_dtypes(include=[np.number]).columns.tolist()
    agg_dict = {c: 'sum' for c in num_cols if c not in valid_join_keys}
    if 'avg_rank' in agg_dict: agg_dict['avg_rank'] = 'mean'
    
    base_agg = df_prev.groupby(valid_join_keys, as_index=False).agg(agg_dict)
    base_agg.rename(columns={c: f"{c}_base" for c in agg_dict.keys()}, inplace=True)
    
    return df_c.merge(base_agg, on=valid_join_keys, how='left').fillna(0)

def style_table_deltas(val):
    return style_table_deltas_positive(val)

def style_table_deltas_positive(val):
    if pd.isna(val) or val == "-" or val == "": return ""
    color_up = "color: #0528F2; font-weight: 600;"    
    color_down = "color: #F04438; font-weight: 600;"  
    
    if isinstance(val, str):
        if "▲" in val: return color_up
        if "▼" in val: return color_down
        val_clean = val.replace(",", "").replace("%", "").strip()
        if val_clean.startswith("+") and len(val_clean) > 1:
            try:
                if float(val_clean) > 0: return color_up
            except ValueError: pass
        if val_clean.startswith("-") and len(val_clean) > 1:
            try:
                if float(val_clean) < 0: return color_down
            except ValueError: pass
    elif isinstance(val, (int, float)):
        if val > 0: return color_up
        elif val < 0: return color_down
    return ""

def style_table_deltas_negative(val):
    if pd.isna(val) or val == "-" or val == "": return ""
    color_up = "color: #F04438; font-weight: 600;"    
    color_down = "color: #0528F2; font-weight: 600;"  
    
    if isinstance(val, str):
        if "▲" in val: return color_up
        if "▼" in val: return color_down
        val_clean = val.replace(",", "").replace("%", "").strip()
        if val_clean.startswith("+") and len(val_clean) > 1:
            try:
                if float(val_clean) > 0: return color_up
            except ValueError: pass
        if val_clean.startswith("-") and len(val_clean) > 1:
            try:
                if float(val_clean) < 0: return color_down
            except ValueError: pass
    elif isinstance(val, (int, float)):
        if val > 0: return color_up
        elif val < 0: return color_down
    return ""

def _render_ab_test_sbs(df_grp: pd.DataFrame, d1: date, d2: date):
    st.markdown("<div class='nv-sec-title'>소재 A/B 비교 (상위 2개)</div>", unsafe_allow_html=True)
    st.caption(f"조회 기간: {d1} ~ {d2}")
    
    valid_ads = df_grp.sort_values(by=['노출', '광고비'], ascending=[False, False])
    if len(valid_ads) < 2:
        st.info("해당 그룹에 비교 가능한 소재가 2개 이상 없습니다.")
        st.divider()
        return
        
    ad1, ad2 = valid_ads.iloc[0], valid_ads.iloc[1]
    c1, c2 = st.columns(2)
    
    def _card(row, label):
        return f"""
        <div style='background:#FFFFFF; padding:24px; border-radius:12px; border:1px solid var(--nv-line); box-shadow: 0 2px 8px rgba(0,0,0,0.02);'>
            <div style='text-align:center; font-size:12px; font-weight:600; color:var(--nv-muted); margin-bottom:8px; text-transform:uppercase;'>{label}</div>
            <h4 style='text-align:center; margin-top:0; margin-bottom:20px; color:var(--nv-text); font-size:15px; font-weight:700;'>{row['소재내용']}</h4>
            <div style='display:flex; justify-content:space-between; margin-bottom:8px;'>
                <span style='color:var(--nv-muted); font-weight:500; font-size:13px;'>광고비</span>
                <span style='font-weight:600; color:var(--nv-text); font-size:13px;'>{format_currency(row.get('광고비',0))}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:12px;'>
                <span style='color:var(--nv-muted); font-weight:500; font-size:13px;'>전환매출</span>
                <span style='font-weight:600; color:var(--nv-text); font-size:13px;'>{format_currency(row.get('전환매출',0))}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:16px; padding-bottom:16px; border-bottom:1px solid var(--nv-line);'>
                <span style='color:var(--nv-muted); font-weight:500; font-size:13px;'>ROAS</span>
                <span style='font-weight:700; color:var(--nv-primary); font-size:16px;'>{row.get('ROAS(%)',0):.1f}%</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:8px;'>
                <span style='color:var(--nv-muted-light); font-size:12px;'>노출수</span>
                <span style='color:var(--nv-muted); font-size:12px; font-weight:600;'>{format_number_commas(row.get('노출',0))}</span>
            </div>
            <div style='display:flex; justify-content:space-between; margin-bottom:8px;'>
                <span style='color:var(--nv-muted-light); font-size:12px;'>클릭수</span>
                <span style='color:var(--nv-muted); font-size:12px; font-weight:600;'>{format_number_commas(row.get('클릭',0))}</span>
            </div>
            <div style='display:flex; justify-content:space-between;'>
                <span style='color:var(--nv-muted-light); font-size:12px;'>전환수</span>
                <span style='color:var(--nv-muted); font-size:12px; font-weight:600;'>{row.get('전환',0):.1f}</span>
            </div>
        </div>
        """
    
    with c1: st.markdown(_card(ad1, "소재 A"), unsafe_allow_html=True)
    with c2: st.markdown(_card(ad2, "소재 B"), unsafe_allow_html=True)
    st.divider()

def render_item_comparison_search(entity_label: str, df_cur: pd.DataFrame, df_base: pd.DataFrame, name_col: str, d1: date, d2: date, b1: date, b2: date):
    items_cur = set(df_cur[name_col].dropna().astype(str).unique()) if not df_cur.empty and name_col in df_cur.columns else set()
    items_base = set(df_base[name_col].dropna().astype(str).unique()) if not df_base.empty and name_col in df_base.columns else set()
    all_items = sorted([x for x in list(items_cur | items_base) if str(x).strip() != ""])

    if not all_items: return

    st.markdown(f"<div class='nv-sec-title'>개별 상세 비교 ({entity_label})</div>", unsafe_allow_html=True)

    query = st.text_input("항목 검색", key=f"search_detail_query_{entity_label}_{name_col}", placeholder="이름을 입력해 빠르게 찾기")
    filtered_items = [item for item in all_items if query.lower() in item.lower()] if query else all_items
    options = ["선택 안 함"] + filtered_items

    if len(options) == 1:
        st.info("검색 결과가 없습니다.")
        return

    selected = st.selectbox("분석 항목 선택", options, key=f"search_detail_{entity_label}_{name_col}")

    if selected == "선택 안 함": return

    c_df = df_cur[df_cur[name_col] == selected] if not df_cur.empty else pd.DataFrame()
    b_df = df_base[df_base[name_col] == selected] if not df_base.empty else pd.DataFrame()

    def _sum_col(df: pd.DataFrame, candidates: list[str]) -> float:
        if df is None or df.empty: return 0.0
        for col in candidates:
            if col in df.columns: return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
        return 0.0

    def _cur_val(candidates_kr: list[str], candidates_en: list[str]) -> float:
        return _sum_col(c_df, candidates_kr + candidates_en)

    def _base_val(base_cols: list[str], prev_cols: list[str]) -> float:
        val = _sum_col(b_df, base_cols)
        if val > 0: return val
        return _sum_col(c_df, prev_cols)

    patch_date = date(2026, 3, 11)
    has_pre_patch = (d1 < patch_date) or (b1 < patch_date if b1 else False)

    c_cost = _cur_val(["광고비"], ["cost"])
    b_cost = _base_val(["이전 광고비", "광고비", "cost", "b_cost"], ["p_cost", "cost_base"])

    if has_pre_patch:
        c_sales = _cur_val(["총 전환매출", "전환매출"], ["tot_sales"])
        c_conv = _cur_val(["총 전환수", "전환수"], ["tot_conv"])
        b_sales = _base_val(["이전 총 전환매출", "전환매출", "tot_sales", "b_tot_sales"], ["p_sales", "tot_sales_base"])
        b_conv = _base_val(["이전 총 전환수", "전환수", "tot_conv", "b_tot_conv"], ["p_conv", "tot_conv_base"])
        roas_label = "통합 ROAS"
        conv_label = "총 전환수"
        sales_label = "총 전환매출"
    else:
        c_sales = _cur_val(["구매완료 매출", "전환매출"], ["sales"])
        c_conv = _cur_val(["구매완료수", "전환수"], ["conv"])
        b_sales = _base_val(["이전 구매완료 매출", "전환매출", "sales", "b_sales"], ["p_sales", "sales_base"])
        b_conv = _base_val(["이전 구매완료수", "전환수", "conv", "b_conv"], ["p_conv", "conv_base"])
        roas_label = "구매 ROAS"
        conv_label = "구매완료수"
        sales_label = "구매완료 매출"

    c_clk = _cur_val(["클릭", "클릭수"], ["clk"])
    c_imp = _cur_val(["노출", "노출수"], ["imp"])
    b_clk = _base_val(["이전 클릭", "클릭", "클릭수", "clk", "b_clk"], ["p_clk", "clk_base"])
    b_imp = _base_val(["이전 노출", "노출", "노출수", "imp", "b_imp"], ["p_imp", "imp_base"])

    c_roas = (c_sales / c_cost * 100) if c_cost > 0 else 0
    b_roas = (b_sales / b_cost * 100) if b_cost > 0 else 0

    def fmt_krw(v): return f"{int(v):,}원"
    def fmt_num(v): return f"{int(v):,}"
    def fmt_pct(v): return f"{v:.1f}%"
    def fmt_float(v): return f"{v:,.1f}"

    def calc_detail_delta(c, b, is_currency=False, is_pct=False):
        diff = c - b
        if b == 0 and c > 0: return "신규"
        if diff == 0: return "변동 없음"
        sign = "▲" if diff > 0 else "▼"
        if is_currency: abs_val = f"{int(abs(diff)):,}원"
        elif is_pct: abs_val = f"{abs(diff):.1f}%p"
        else: abs_val = f"{int(abs(diff)):,}" if float(diff).is_integer() else f"{abs(diff):.1f}"
        word = "증가" if diff > 0 else "감소"
        return f"{sign} {abs_val} {word}"

    def calc_delta_rate(c, b):
        if b == 0 and c > 0: return "신규"
        if b == 0: return "0.0%"
        return f"{((c - b) / b) * 100:+.1f}%"

    def delta_chip_text(delta_text: str, is_negative_metric=False):
        if delta_text == "변동 없음": return "<span class='delta-chip delta-flat'>변동 없음</span>"
        if delta_text == "신규": return "<span class='delta-chip delta-up'>▲ 신규</span>"
        
        is_up = delta_text.startswith("▲")
        if is_negative_metric:
            cls = "delta-down" if is_up else "delta-up" 
        else:
            cls = "delta-up" if is_up else "delta-down"
            
        return f"<span class='delta-chip {cls}'>{delta_text}</span>"

    rows = [
        {"label": "광고비", "base": fmt_krw(b_cost), "curr": fmt_krw(c_cost), "delta": calc_detail_delta(c_cost, b_cost, is_currency=True), "rate": calc_delta_rate(c_cost, b_cost), "emphasis": False, "is_neg": True},
        {"label": sales_label, "base": fmt_krw(b_sales), "curr": fmt_krw(c_sales), "delta": calc_detail_delta(c_sales, b_sales, is_currency=True), "rate": calc_delta_rate(c_sales, b_sales), "emphasis": False, "is_neg": False},
        {"label": roas_label, "base": fmt_pct(b_roas), "curr": fmt_pct(c_roas), "delta": calc_detail_delta(c_roas, b_roas, is_pct=True), "rate": calc_delta_rate(c_roas, b_roas), "emphasis": True, "is_neg": False},
        {"label": "노출수", "base": fmt_num(b_imp), "curr": fmt_num(c_imp), "delta": calc_detail_delta(c_imp, b_imp), "rate": calc_delta_rate(c_imp, b_imp), "emphasis": False, "is_neg": False},
        {"label": "클릭수", "base": fmt_num(b_clk), "curr": fmt_num(c_clk), "delta": calc_detail_delta(c_clk, b_clk), "rate": calc_delta_rate(c_clk, b_clk), "emphasis": False, "is_neg": False},
        {"label": conv_label, "base": fmt_float(b_conv), "curr": fmt_float(c_conv), "delta": calc_detail_delta(c_conv, b_conv), "rate": calc_delta_rate(c_conv, b_conv), "emphasis": False, "is_neg": False},
    ]

    def _board_rows(items: list[dict], is_right: bool = False) -> str:
        html_rows = ""
        for r in items:
            curr_cls = "cmp-value emphasize" if r.get("emphasis", False) else "cmp-value"
            sub_row = (
                f"<div class='cmp-sub'>{delta_chip_text(r['delta'], r.get('is_neg', False))}<span class='rate'>({r['rate']})</span></div>"
                if is_right else "<div class='cmp-sub cmp-sub-muted'>기준 값</div>"
            )
            value = r["curr"] if is_right else r["base"]
            html_rows += f"""
            <div class='cmp-row'>
                <div class='cmp-top'>
                    <span class='cmp-label'>{r['label']}</span>
                    <span class='{curr_cls}'>{value}</span>
                </div>
                {sub_row}
            </div>
            """
        return html_rows

    left_rows = _board_rows(rows, is_right=False)
    right_rows = _board_rows(rows, is_right=True)

    html = textwrap.dedent(f"""    <div class='cmp-wrapper'>
        <div class='cmp-boards'>
            <div class='cmp-board'>
                <div class='cmp-board-head'>이전 기간 ({b1} ~ {b2})</div>
                {left_rows}
            </div>
            <div class='cmp-board cmp-board-right'>
                <div class='cmp-board-head'>선택 기간 ({d1} ~ {d2})</div>
                {right_rows}
            </div>
        </div>
    </div>
    <style>
        .cmp-wrapper {{
            background: var(--nv-bg);
            border: 1px solid var(--nv-line);
            border-radius: 12px;
            padding: 20px;
            margin-top: 10px;
            margin-bottom: 24px;
        }}
        .cmp-boards {{
            display:grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 16px;
            align-items:stretch;
        }}
        .cmp-board {{
            border-radius: 8px;
            padding: 16px;
            border: 1px solid var(--nv-line);
            background: var(--nv-surface);
        }}
        .cmp-board-right {{
            background: var(--nv-bg);
            border-color: var(--nv-primary-soft);
            box-shadow: 0 2px 8px rgba(5, 40, 242, 0.05);
        }}
        .cmp-board-head {{
            font-size: 13px;
            font-weight: 600;
            text-align: center;
            margin-bottom: 12px;
            padding-bottom: 12px;
            border-bottom: 1px solid var(--nv-line);
            color: var(--nv-muted);
        }}

        .cmp-row {{
            border-top: 1px dashed var(--nv-line);
            padding: 12px 0;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }}
        .cmp-row:first-child {{ border-top:none; }}
        .cmp-top {{
            display:flex;
            justify-content:space-between;
            align-items:center;
        }}
        .cmp-label {{ font-weight:500; font-size:13px; color:var(--nv-muted); }}
        .cmp-value {{ color:var(--nv-text); font-weight:700; font-size:16px; }}
        .cmp-value.emphasize {{ color:var(--nv-primary); }}
        .cmp-sub {{
            margin-top:6px;
            display:flex;
            justify-content:flex-end;
            gap:6px;
            align-items:center;
            font-size:11px;
        }}
        .cmp-sub-muted {{ color:var(--nv-muted-light); font-weight:500; }}
        .delta-chip {{ font-size:11px; font-weight:600; border-radius:4px; padding:3px 8px; display:inline-block; }}
        
        .delta-up { background:var(--nv-primary-soft); color:var(--nv-primary); } 
        .delta-down { background:#FEE4E2; color:#F04438; } 
        .delta-flat { background:var(--nv-line); color:var(--nv-muted); } 
        .rate { color:var(--nv-muted-light); font-weight:500; }
    </style>
    """).strip()
    comp_height = 200 + (len(rows) * 60)
    st.components.v1.html(html, height=max(460, comp_height), scrolling=False)
