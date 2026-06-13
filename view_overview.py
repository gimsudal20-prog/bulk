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
from ui import render_echarts_dual_axis, render_kpi_strip, render_ops_cards, render_toolbar, render_inline_notice, safe_numeric_col, safe_numeric_series, numeric_column_config
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
        st.dataframe(df, width="stretch", hide_index=True, column_config=numeric_column_config(df))

def _format_report_line(label: str, value: str) -> str:
    return f"{label} : {value}"


def _format_report_count(value) -> str:
    try:
        if pd.isna(value):
            return "0"
        return f"{float(value):,.0f}"
    except Exception:
        return "0"


def _report_float(value, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(str(value).replace(",", "").replace("원", "").replace("%", "").strip())
    except Exception:
        return default


def _format_report_pct_delta(cur_value, base_value, digits: int = 1) -> str:
    cur = _report_float(cur_value)
    base = _report_float(base_value)
    if base == 0:
        if cur == 0:
            return "0.0%"
        return "신규"
    return f"{((cur - base) / base) * 100:+.{digits}f}%"


def _format_report_point_delta(cur_value, base_value, digits: int = 1) -> str:
    cur = _report_float(cur_value)
    base = _report_float(base_value)
    return f"{cur - base:+.{digits}f}p"


def _build_report_delta_lines(cur: dict, base: dict, report_uses_purchase: bool) -> list[str]:
    if not base:
        return [_format_report_line("증감 기준", "비교 기간 데이터 없음")]

    if report_uses_purchase:
        conv_label, conv_key = "구매완료수 증감", "conv"
        sales_label, sales_key = "구매완료 매출 증감", "sales"
        roas_label, roas_key = "구매 ROAS 증감", "roas"
    else:
        conv_label, conv_key = "전환수 증감", "tot_conv"
        sales_label, sales_key = "총전환매출 증감", "tot_sales"
        roas_label, roas_key = "ROAS 증감", "tot_roas"

    return [
        _format_report_line("노출수 증감", _format_report_pct_delta(cur.get("imp", 0), base.get("imp", 0))),
        _format_report_line("클릭수 증감", _format_report_pct_delta(cur.get("clk", 0), base.get("clk", 0))),
        _format_report_line("클릭률 증감", _format_report_point_delta(cur.get("ctr", 0), base.get("ctr", 0))),
        _format_report_line("광고비 증감", _format_report_pct_delta(cur.get("cost", 0), base.get("cost", 0))),
        _format_report_line(conv_label, _format_report_pct_delta(cur.get(conv_key, 0), base.get(conv_key, 0))),
        _format_report_line(sales_label, _format_report_pct_delta(cur.get(sales_key, 0), base.get(sales_key, 0))),
        _format_report_line(roas_label, _format_report_point_delta(cur.get(roas_key, 0), base.get(roas_key, 0))),
    ]


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


def _frame_for_column_config(data_obj) -> pd.DataFrame:
    try:
        df = data_obj.data if hasattr(data_obj, "data") else data_obj
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _sticky_cfg(first_col: str, df: pd.DataFrame | None = None):
    base = {
        first_col: st.column_config.TextColumn(first_col, pinned=True, width="medium")
    }
    return numeric_column_config(df, base=base) if df is not None and not df.empty else base


def _auto_table_height(data_obj, default_height: int = 420, min_height: int = 72, max_height: int = 560) -> int:
    try:
        df = _frame_for_column_config(data_obj)
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
    cfg_df = _frame_for_column_config(styler_or_df)
    st.dataframe(styler_or_df, width="stretch", height=real_height, hide_index=hide_index, column_config=_sticky_cfg(first_col, cfg_df))


def _selected_type_label(type_sel: tuple) -> str:
    if not type_sel: return "전체 유형"
    if len(type_sel) == 1: return type_sel[0]
    return ", ".join(type_sel)

def _overview_type_allows_shopping(type_sel: tuple) -> bool:
    if not type_sel:
        return True
    labels = {str(x or "").strip() for x in type_sel}
    for label in labels:
        up = label.upper()
        if "쇼핑" in label or "SHOPPING" in up or up in {"네이버", "NAVER"}:
            return True
    return False


def _normalize_overview_type_label(value) -> str:
    raw = str(value or "").strip()
    up = raw.upper()
    if not raw:
        return ""
    if "쇼핑" in raw or "SHOPPING" in up:
        return "쇼핑검색"
    if "파워" in raw or up in {"WEB_SITE", "POWERLINK", "SA"}:
        return "파워링크"
    if "META" in up or "메타" in raw:
        return "메타"
    if "GOOGLE" in up or "구글" in raw:
        return "구글"
    if up in {"NAVER", "네이버"}:
        return "네이버"
    return raw


def _overview_is_shopping_only_context(type_sel: tuple, cur_camp: pd.DataFrame | None = None) -> bool:
    labels = {_normalize_overview_type_label(x) for x in type_sel if str(x or "").strip()}
    labels = {x for x in labels if x and x != "네이버"}
    if labels:
        return labels == {"쇼핑검색"}
    if cur_camp is None or cur_camp.empty:
        return False
    camp_labels: set[str] = set()
    for col in ["campaign_type_label", "campaign_type", "campaign_tp", "캠페인유형"]:
        if col in cur_camp.columns:
            camp_labels.update(_normalize_overview_type_label(v) for v in cur_camp[col].dropna().astype(str).tolist())
    camp_labels = {x for x in camp_labels if x and x != "네이버"}
    return bool(camp_labels) and camp_labels == {"쇼핑검색"}


def _overview_is_shopping_series(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype=bool)
    return series.astype(str).str.strip().str.upper().isin({"SHOPPING", "쇼핑검색"})




# 쇼핑검색 오버뷰의 구매완료는 캠페인/일자 단위 split 수집값을 기준으로 둔다.
# 검색어 상세 테이블은 검색어 미제공·미매핑 버킷이 섞일 수 있어 오버뷰 합계 대체 원천으로 사용하지 않는다.

@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def _cached_campaign_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try: return query_campaign_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=1500)
    except Exception: return pd.DataFrame()


@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def _cached_keyword_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    cache_version = 5  # 쇼핑검색 오버뷰/키워드 구매완료 strict split 기준 캐시 갱신
    # 오버뷰 최초 로딩/리포트용 경량 번들입니다.
    # 화면 상세 표는 아래 _cached_keyword_full_bundle()을 별도로 사용해 정렬 누락을 막습니다.
    try:
        bundle = query_keyword_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=300)
        return _filter_overview_keyword_rows(bundle)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=43200, max_entries=6, show_spinner=False)
def _cached_keyword_full_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    cache_version = 5  # 쇼핑검색 오버뷰/키워드 구매완료 strict split 기준 캐시 갱신
    # st.dataframe의 정렬은 브라우저에 전달된 행 안에서만 동작합니다.
    # 따라서 키워드 상세/엑셀용 데이터는 광고비 상위 제한을 걸지 않고 전체를 가져옵니다.
    try:
        bundle = query_keyword_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=-1)
        return _filter_overview_keyword_rows(bundle)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=43200, max_entries=6, show_spinner=False)
def _cached_ad_full_bundle(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try:
        return query_ad_bundle(_engine, start_dt, end_dt, cids, type_sel, topn_cost=-1, top_k=0)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def _cached_campaign_timeseries(_engine, start_dt, end_dt, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    try: return query_campaign_timeseries(_engine, start_dt, end_dt, cids, type_sel)
    except Exception: return pd.DataFrame()


PLATFORM_LABELS = {
    "naver": "네이버",
    "meta": "메타",
    "meta_ads": "메타",
    "facebook": "메타",
    "facebook_ads": "메타",
    "instagram": "메타",
    "google": "구글",
    "google_ads": "구글",
    "googleads": "구글",
    "performance_max": "구글",
    "pmax": "구글",
    "p_max": "구글",
}

CAMPAIGN_PLATFORM_LABELS = {
    "WEB_SITE": "네이버",
    "SHOPPING": "네이버",
    "POWER_CONTENT": "네이버",
    "POWER_CONTENTS": "네이버",
    "BRAND_SEARCH": "네이버",
    "PLACE": "네이버",
    "파워링크": "네이버",
    "쇼핑검색": "네이버",
    "파워컨텐츠": "네이버",
    "브랜드검색": "네이버",
    "플레이스": "네이버",
    "META": "메타",
    "FACEBOOK": "메타",
    "FACEBOOK_ADS": "메타",
    "INSTAGRAM": "메타",
    "메타": "메타",
    "GOOGLE": "구글",
    "GOOGLE_ADS": "구글",
    "PERFORMANCE_MAX": "구글",
    "PMAX": "구글",
    "P_MAX": "구글",
    "구글": "구글",
}


def _clean_overview_customer_id(value) -> str:
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    raw = str(value or "").strip()
    if not raw or raw.lower() in {"nan", "none", "nat", "<na>"}:
        return ""
    if raw.startswith("act_"):
        raw = raw[4:]
    if raw.endswith(".0") and raw[:-2].isdigit():
        raw = raw[:-2]
    compact = raw.replace("-", "").replace(" ", "")
    return compact if compact.isdigit() else raw


def _platform_label(value) -> str:
    return PLATFORM_LABELS.get(str(value or "").strip().lower(), "")


def _campaign_platform_label(value) -> str:
    raw = str(value or "").strip()
    if not raw or raw.lower() in {"nan", "none"}:
        return ""
    return CAMPAIGN_PLATFORM_LABELS.get(raw, CAMPAIGN_PLATFORM_LABELS.get(raw.upper(), ""))


def _account_platform_display(account_name: str, platform_label: str) -> str:
    label = str(account_name or "").strip()
    platform = str(platform_label or "").strip()
    if not label:
        return platform
    if not platform or platform in label:
        return label
    return f"{label} · {platform}"


def _overview_account_maps(meta: pd.DataFrame, engine=None) -> tuple[dict[str, str], dict[str, str]]:
    base_map: dict[str, str] = {}
    platform_map: dict[str, str] = {}

    if meta is not None and not meta.empty and "customer_id" in meta.columns and "account_name" in meta.columns:
        meta_view = meta[["customer_id", "account_name"]].copy()
        meta_view["_cid"] = meta_view["customer_id"].map(_clean_overview_customer_id)
        meta_view["_name"] = meta_view["account_name"].fillna("").astype(str).str.strip()
        for _, row in meta_view.iterrows():
            cid = str(row.get("_cid", "") or "").strip()
            name = str(row.get("_name", "") or "").strip()
            if cid and name:
                base_map[cid] = name

    if engine is not None:
        try:
            conn_df = get_platform_credentials(engine)
        except Exception:
            conn_df = pd.DataFrame()
        if conn_df is not None and not conn_df.empty:
            work = conn_df.copy()
            if "is_active" in work.columns:
                work = work[work["is_active"].fillna(False).astype(bool)].copy()
            for _, row in work.iterrows():
                platform = _platform_label(row.get("platform", ""))
                account_name = str(row.get("account_label", "") or "").strip()
                if not account_name:
                    account_name = base_map.get(_clean_overview_customer_id(row.get("customer_id", "")), "")
                display_name = _account_platform_display(account_name, platform)
                if not display_name:
                    continue
                account_id = _clean_overview_customer_id(row.get("account_id", ""))
                customer_id = _clean_overview_customer_id(row.get("customer_id", ""))
                if platform == "네이버":
                    for cid in [customer_id, account_id]:
                        if cid:
                            platform_map[cid] = display_name
                else:
                    # Non-Naver rows usually keep the dashboard customer_id separately and store the real platform ID in account_id.
                    # Mapping the dashboard ID here can make Naver data show as Meta/Google, so only fall back to customer_id
                    # when it is not already a known dashboard account.
                    if account_id:
                        platform_map[account_id] = display_name
                    elif customer_id and customer_id not in base_map:
                        platform_map[customer_id] = display_name

    return base_map, platform_map



def _ordered_unique_customer_ids(values) -> tuple:
    seen: set[str] = set()
    out: list[str] = []
    for value in values or []:
        cid = _clean_overview_customer_id(value)
        if cid and cid not in seen:
            seen.add(cid)
            out.append(cid)
    return tuple(out)


def _expand_overview_customer_ids(meta: pd.DataFrame, engine, cids: tuple) -> tuple:
    """Include linked platform account IDs for an account selected in the global filter.

    Meta/Google rows are stored with the platform ad account ID as customer_id, while the
    global account filter often starts from the Naver dashboard customer_id.  Without this
    expansion, selecting a client such as 핵이득마켓 can filter out its linked Meta rows.
    """
    selected = list(_ordered_unique_customer_ids(cids))
    if not selected or engine is None:
        return tuple(selected)

    try:
        base_map, _ = _overview_account_maps(meta, engine)
    except Exception:
        base_map = {}

    selected_set = set(selected)
    selected_names = {str(base_map.get(cid, "") or "").strip() for cid in selected_set}
    selected_names = {name for name in selected_names if name}

    try:
        conn_df = get_platform_credentials(engine)
    except Exception:
        return tuple(selected)
    if conn_df is None or conn_df.empty:
        return tuple(selected)

    work = conn_df.copy()
    if "is_active" in work.columns:
        try:
            work = work[work["is_active"].fillna(False).astype(bool)].copy()
        except Exception:
            pass

    expanded = list(selected)
    seen = set(selected)

    for _, row in work.iterrows():
        dashboard_cid = _clean_overview_customer_id(row.get("customer_id", ""))
        platform_cid = _clean_overview_customer_id(row.get("account_id", ""))
        account_label = str(row.get("account_label", "") or "").strip()
        linked_ids = [cid for cid in [dashboard_cid, platform_cid] if cid]
        linked_names = {account_label, str(base_map.get(dashboard_cid, "") or "").strip()}
        linked_names = {name for name in linked_names if name}

        should_link = bool(selected_set.intersection(linked_ids)) or bool(selected_names.intersection(linked_names))
        if not should_link:
            continue

        for cid in linked_ids:
            if cid not in seen:
                seen.add(cid)
                expanded.append(cid)

    return tuple(expanded)


def _attach_account_names(df: pd.DataFrame, meta: pd.DataFrame, engine=None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    out = df.copy()
    if "customer_id" not in out.columns:
        out["account_name"] = ""
        return out

    base_map, platform_map = _overview_account_maps(meta, engine)
    type_col = next((col for col in ["campaign_type", "campaign_type_label", "campaign_tp"] if col in out.columns), None)

    def resolve_display(row) -> str:
        cid = _clean_overview_customer_id(row.get("customer_id", ""))
        if cid in platform_map:
            return platform_map[cid]
        base_name = base_map.get(cid, cid)
        campaign_platform = _campaign_platform_label(row.get(type_col, "")) if type_col else ""
        return _account_platform_display(base_name, campaign_platform) if campaign_platform else base_name

    out["account_name"] = out.apply(resolve_display, axis=1)
    return out


def _selected_account_title(meta: pd.DataFrame, engine, cids: tuple, cur_camp: pd.DataFrame) -> str:
    if not cids:
        return "전체 계정"

    names: list[str] = []
    try:
        if cur_camp is not None and not cur_camp.empty:
            attached = _attach_account_names(cur_camp, meta, engine)
            if "account_name" in attached.columns:
                names = [str(x).strip() for x in attached["account_name"].dropna().unique().tolist() if str(x).strip()]
    except Exception:
        names = []

    if not names:
        base_map, platform_map = _overview_account_maps(meta, engine)
        for cid in cids:
            cid_key = _clean_overview_customer_id(cid)
            label = platform_map.get(cid_key) or base_map.get(cid_key) or cid_key
            if label:
                names.append(label)

    names = list(dict.fromkeys(names))
    if not names:
        return "전체 계정"
    if len(names) == 1:
        return names[0]
    return f"{names[0]} 외 {len(names) - 1}개"


@st.cache_data(ttl=43200, max_entries=24, show_spinner=False)
def _build_overview_campaign_frames(cur_camp: pd.DataFrame, base_camp: pd.DataFrame, meta: pd.DataFrame, _engine=None):
    df_display, df_type_display, camp_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    cur_camp = _attach_account_names(cur_camp, meta, _engine)
    base_camp = _attach_account_names(base_camp, meta, _engine)
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


_UNMAPPED_KEYWORD_LABEL = "(키워드 미매핑 전환)"


def _overview_is_naver_keyword_scope(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    labels = {
        "WEB_SITE", "POWER_CONTENT", "POWER_CONTENTS", "BRAND_SEARCH", "PLACE",
        "파워링크", "파워컨텐츠", "브랜드검색", "플레이스",
    }
    normalized_labels = {x.upper() for x in labels}
    mask = pd.Series([False] * len(df.index), index=df.index)
    for col in ["campaign_type_label", "campaign_type", "campaign_tp"]:
        if col in df.columns:
            mask = mask | df[col].astype(str).str.strip().str.upper().isin(normalized_labels)
    return mask


def _filter_overview_keyword_rows(kw_df: pd.DataFrame) -> pd.DataFrame:
    if kw_df is None or kw_df.empty:
        return pd.DataFrame() if kw_df is None else kw_df
    out = kw_df.copy()
    if "keyword" in out.columns:
        out = out[out["keyword"].astype(str) != _UNMAPPED_KEYWORD_LABEL].copy()
    type_col = next((c for c in ["campaign_type_label", "campaign_type", "campaign_tp"] if c in out.columns), None)
    if type_col:
        out = out[~_overview_is_shopping_series(out[type_col])].copy()
    return out


def _filter_keyword_scope_for_campaigns(kw_df: pd.DataFrame, camp_scope: pd.DataFrame) -> pd.DataFrame:
    if kw_df is None or kw_df.empty:
        return pd.DataFrame()
    out = _filter_overview_keyword_rows(kw_df)
    if out.empty:
        return out

    if camp_scope is not None and not camp_scope.empty and {"customer_id", "campaign_id"}.issubset(out.columns) and {"customer_id", "campaign_id"}.issubset(camp_scope.columns):
        pairs = set(
            zip(
                camp_scope["customer_id"].astype(str),
                camp_scope["campaign_id"].astype(str),
            )
        )
        pair_mask = [
            (str(row.customer_id), str(row.campaign_id)) in pairs
            for row in out[["customer_id", "campaign_id"]].itertuples(index=False)
        ]
        scoped = out[pair_mask].copy()
        if not scoped.empty:
            return scoped

    if camp_scope is not None and not camp_scope.empty and "campaign_id" in out.columns and "campaign_id" in camp_scope.columns:
        campaign_ids = set(camp_scope["campaign_id"].dropna().astype(str))
        scoped = out[out["campaign_id"].astype(str).isin(campaign_ids)].copy()
        if not scoped.empty:
            return scoped

    scoped = out[_overview_is_naver_keyword_scope(out)].copy()
    return scoped if not scoped.empty else out


def _sum_numeric_metric(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


def _append_unmapped_keyword_conversion_row(kw_df: pd.DataFrame, camp_df: pd.DataFrame) -> pd.DataFrame:
    """Keep keyword detail analysis limited to actually mapped keyword rows."""
    return _filter_overview_keyword_rows(kw_df)


@st.cache_data(ttl=43200, max_entries=16, show_spinner=False)
def _build_overview_keyword_frames(cur_kw: pd.DataFrame, base_kw: pd.DataFrame, cur_camp: pd.DataFrame | None = None, base_camp: pd.DataFrame | None = None):
    kw_disp = pd.DataFrame()
    cur_kw = _append_unmapped_keyword_conversion_row(cur_kw, cur_camp)
    base_kw = _append_unmapped_keyword_conversion_row(base_kw, base_camp)
    if cur_kw.empty and base_kw.empty:
        return kw_disp
    
    kw_col = 'keyword' if 'keyword' in cur_kw.columns else None
    if kw_col:
        kw_disp = _build_comparison_df(cur_kw, base_kw, kw_col, '키워드')
    return kw_disp


def _overview_prepare_ad_source(ad_bundle: pd.DataFrame) -> pd.DataFrame:
    if ad_bundle is None or ad_bundle.empty:
        return pd.DataFrame()
    out = ad_bundle.copy()
    if "ad_title" in out.columns:
        out["item_name"] = out["ad_title"].fillna("").astype(str).str.strip()
        empty = out["item_name"].isin(["", "nan", "None"])
        fallback = out["ad_name"].astype(str) if "ad_name" in out.columns else pd.Series([""] * len(out.index), index=out.index)
        out.loc[empty, "item_name"] = fallback.loc[empty]
    elif "ad_name" in out.columns:
        out["item_name"] = out["ad_name"].astype(str)
    else:
        out["item_name"] = "소재"
    out = out[~out["item_name"].astype(str).str.contains("확장소재", na=False)].copy()
    return out


def _overview_build_detail_source(kw_bundle: pd.DataFrame, ad_bundle: pd.DataFrame) -> pd.DataFrame:
    kw_df = _filter_overview_keyword_rows(kw_bundle).rename(columns={"keyword": "item_name"}) if kw_bundle is not None and not kw_bundle.empty else pd.DataFrame()
    ad_df = _overview_prepare_ad_source(ad_bundle)

    if kw_df.empty:
        return ad_df.reset_index(drop=True)
    if ad_df.empty:
        return kw_df.reset_index(drop=True)

    for df in (kw_df, ad_df):
        if "campaign_id" in df.columns:
            df["campaign_id"] = df["campaign_id"].astype(str)

    campaign_ids = set()
    if "campaign_id" in kw_df.columns:
        campaign_ids.update(kw_df["campaign_id"].dropna().astype(str).unique().tolist())
    if "campaign_id" in ad_df.columns:
        campaign_ids.update(ad_df["campaign_id"].dropna().astype(str).unique().tolist())

    preferred: dict[str, str] = {}
    for campaign_id in campaign_ids:
        kw_rows = kw_df[kw_df["campaign_id"] == campaign_id] if "campaign_id" in kw_df.columns else pd.DataFrame()
        ad_rows = ad_df[ad_df["campaign_id"] == campaign_id] if "campaign_id" in ad_df.columns else pd.DataFrame()
        campaign_type = ""
        if not kw_rows.empty and "campaign_type_label" in kw_rows.columns:
            campaign_type = str(kw_rows["campaign_type_label"].iloc[0]).upper()
        elif not ad_rows.empty and "campaign_type_label" in ad_rows.columns:
            campaign_type = str(ad_rows["campaign_type_label"].iloc[0]).upper()

        if any(token in campaign_type for token in ["쇼핑", "SHOPPING", "브랜드", "BRAND", "플레이스", "PLACE", "META", "GOOGLE"]):
            preferred[campaign_id] = "ad"
        elif any(token in campaign_type for token in ["파워링크", "WEB_SITE", "파워컨텐츠", "POWER"]):
            preferred[campaign_id] = "kw"
        else:
            preferred[campaign_id] = "kw" if not kw_rows.empty else "ad"

    kept = []
    if "campaign_id" in kw_df.columns:
        kw_keep = kw_df[kw_df["campaign_id"].map(lambda x: preferred.get(str(x), "kw") == "kw")].copy()
        if not kw_keep.empty:
            kept.append(kw_keep)
    if "campaign_id" in ad_df.columns:
        ad_keep = ad_df[ad_df["campaign_id"].map(lambda x: preferred.get(str(x), "ad") == "ad")].copy()
        if not ad_keep.empty:
            kept.append(ad_keep)
    if kept:
        return pd.concat(kept, ignore_index=True)
    return kw_df.reset_index(drop=True) if not kw_df.empty else ad_df.reset_index(drop=True)


def _weighted_avg_rank_by_keys(df: pd.DataFrame, keys: list[str], rank_col: str = "avg_rank", imp_col: str = "imp") -> pd.DataFrame:
    usable_keys = [k for k in keys if k in df.columns]
    if df is None or df.empty or not usable_keys or rank_col not in df.columns:
        return pd.DataFrame(columns=usable_keys + [rank_col])
    tmp = df[usable_keys].copy()
    tmp[imp_col] = safe_numeric_col(df, imp_col, default=0.0) if imp_col in df.columns else pd.Series([0.0] * len(df.index), index=df.index)
    tmp[rank_col] = pd.to_numeric(df[rank_col], errors="coerce")
    tmp["_rank_imp"] = tmp[rank_col].fillna(0.0) * tmp[imp_col]
    grp = tmp.groupby(usable_keys, as_index=False, dropna=False)[["_rank_imp", imp_col]].sum()
    grp[rank_col] = np.where(grp[imp_col] > 0, grp["_rank_imp"] / grp[imp_col], np.nan)
    return grp[usable_keys + [rank_col]]


def _aggregate_overview_group_source(detail_df: pd.DataFrame, meta: pd.DataFrame, _engine=None) -> pd.DataFrame:
    if detail_df is None or detail_df.empty:
        return pd.DataFrame()
    work = _attach_account_names(detail_df, meta, _engine)
    if "adgroup_name" not in work.columns and "adgroup_id" in work.columns:
        work["adgroup_name"] = work["adgroup_id"].astype(str)
    group_keys = [
        c for c in ["customer_id", "campaign_id", "adgroup_id", "account_name", "campaign_type_label", "campaign_name", "adgroup_name"]
        if c in work.columns
    ]
    if not group_keys:
        return pd.DataFrame()
    metric_cols = [c for c in ["imp", "clk", "cost", "conv", "sales", "tot_conv", "tot_sales", "cart_conv", "cart_sales", "wishlist_conv", "wishlist_sales"] if c in work.columns]
    if not metric_cols:
        return pd.DataFrame()
    for c in metric_cols:
        work[c] = pd.to_numeric(work[c], errors="coerce").fillna(0.0)
    grouped = work.groupby(group_keys, as_index=False, dropna=False)[metric_cols].sum()
    rank_grp = _weighted_avg_rank_by_keys(work, group_keys)
    if not rank_grp.empty:
        grouped = grouped.merge(rank_grp, on=group_keys, how="left")
    return grouped


def _base_group_for_merge(base_group: pd.DataFrame, merge_keys: list[str]) -> pd.DataFrame:
    if base_group is None or base_group.empty or not merge_keys:
        return pd.DataFrame(columns=merge_keys)
    metric_cols = [c for c in ["imp", "clk", "cost", "conv", "sales", "tot_conv", "tot_sales", "cart_conv", "cart_sales", "wishlist_conv", "wishlist_sales"] if c in base_group.columns]
    if not metric_cols:
        return pd.DataFrame(columns=merge_keys)
    out = base_group.groupby(merge_keys, as_index=False, dropna=False)[metric_cols].sum()
    rank_grp = _weighted_avg_rank_by_keys(base_group, merge_keys)
    if not rank_grp.empty:
        out = out.merge(rank_grp, on=merge_keys, how="left")
    return out


def _build_overview_group_frames(cur_detail: pd.DataFrame, base_detail: pd.DataFrame, meta: pd.DataFrame, _engine=None) -> pd.DataFrame:
    cur_group = _aggregate_overview_group_source(cur_detail, meta, _engine)
    base_group = _aggregate_overview_group_source(base_detail, meta, _engine)
    if cur_group.empty:
        return pd.DataFrame()

    stable_keys = [k for k in ["customer_id", "campaign_id", "adgroup_id"] if k in cur_group.columns and k in base_group.columns]
    if not stable_keys:
        stable_keys = [k for k in ["account_name", "campaign_name", "adgroup_name"] if k in cur_group.columns and k in base_group.columns]

    if stable_keys and not base_group.empty:
        base_merge = _base_group_for_merge(base_group, stable_keys).rename(columns={
            c: f"{c}_base" for c in base_group.columns if c not in stable_keys
        })
        merged = cur_group.merge(base_merge, on=stable_keys, how="left")
    else:
        merged = cur_group.copy()

    for c in ["imp", "clk", "cost", "conv", "sales", "tot_conv", "tot_sales", "cart_conv", "cart_sales", "wishlist_conv", "wishlist_sales"]:
        if c not in merged.columns:
            merged[c] = 0.0
        if f"{c}_base" not in merged.columns:
            merged[f"{c}_base"] = 0.0
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0.0)
        merged[f"{c}_base"] = pd.to_numeric(merged[f"{c}_base"], errors="coerce").fillna(0.0)
    if "avg_rank_base" not in merged.columns:
        merged["avg_rank_base"] = np.nan

    out = pd.DataFrame(index=merged.index)
    out["계정명"] = merged.get("account_name", "")
    out["캠페인유형"] = merged.get("campaign_type_label", "")
    out["캠페인명"] = merged.get("campaign_name", "")
    out["광고그룹"] = merged.get("adgroup_name", merged.get("adgroup_id", "미분류"))
    out["노출수"] = merged["imp"]
    out["클릭수"] = merged["clk"]
    out["클릭률(%)"] = _safe_div(merged["clk"], merged["imp"], 100.0)
    out["광고비"] = merged["cost"]
    out["CPC"] = _safe_div(merged["cost"], merged["clk"])

    if "avg_rank" in merged.columns:
        out["avg_rank"] = pd.to_numeric(merged["avg_rank"], errors="coerce")
        out["평균순위"] = out["avg_rank"].apply(_format_avg_rank)

    out["구매완료수"] = merged["conv"]
    out["구매 전환율(%)"] = _safe_div(merged["conv"], merged["clk"], 100.0)
    out["구매완료 매출"] = merged["sales"]
    out["구매완료 ROAS(%)"] = _safe_div(merged["sales"], merged["cost"], 100.0)

    out["총 전환수"] = merged["tot_conv"]
    out["총 전환율(%)"] = _safe_div(merged["tot_conv"], merged["clk"], 100.0)
    out["총 전환매출"] = merged["tot_sales"]
    out["통합 ROAS(%)"] = _safe_div(merged["tot_sales"], merged["cost"], 100.0)

    base_imp = merged["imp_base"]
    base_clk = merged["clk_base"]
    base_cost = merged["cost_base"]
    base_conv = merged["conv_base"]
    base_sales = merged["sales_base"]
    base_tot_conv = merged["tot_conv_base"]
    base_tot_sales = merged["tot_sales_base"]
    base_cpc = _safe_div(base_cost, base_clk)

    def _apply_pct_diff(cur_val, base_val, pct_col, abs_col):
        diff = cur_val - base_val
        safe_base = np.where(base_val == 0, 1, base_val)
        out[pct_col] = np.where(base_val == 0, np.where(cur_val > 0, 100.0, 0.0), (diff / safe_base) * 100.0)
        out[abs_col] = diff

    _apply_pct_diff(merged["imp"], base_imp, "노출 증감", "노출 차이")
    _apply_pct_diff(merged["clk"], base_clk, "클릭 증감", "클릭 차이")
    _apply_pct_diff(merged["cost"], base_cost, "광고비 증감", "광고비 차이")
    _apply_pct_diff(out["CPC"], base_cpc, "CPC 증감", "CPC 차이")
    _apply_pct_diff(merged["conv"], base_conv, "구매완료 증감", "구매완료 차이")
    _apply_pct_diff(merged["sales"], base_sales, "구매완료 매출 증감", "구매완료 매출 차이")
    _apply_pct_diff(merged["tot_conv"], base_tot_conv, "총 전환 증감", "총 전환 차이")
    _apply_pct_diff(merged["tot_sales"], base_tot_sales, "총 매출 증감", "총 매출 차이")

    out["클릭률 증감"] = out["클릭률(%)"] - _safe_div(base_clk, base_imp, 100.0)
    out["구매 전환율 증감"] = out["구매 전환율(%)"] - _safe_div(base_conv, base_clk, 100.0)
    out["구매완료 ROAS 증감"] = out["구매완료 ROAS(%)"] - _safe_div(base_sales, base_cost, 100.0)
    out["총 전환율 증감"] = out["총 전환율(%)"] - _safe_div(base_tot_conv, base_clk, 100.0)
    out["통합 ROAS 증감"] = out["통합 ROAS(%)"] - _safe_div(base_tot_sales, base_cost, 100.0)

    if "avg_rank" in merged.columns:
        cur_rank = pd.to_numeric(merged["avg_rank"], errors="coerce")
        base_rank = pd.to_numeric(merged["avg_rank_base"], errors="coerce")
        out["순위 변화"] = np.where((cur_rank > 0) & (base_rank > 0), cur_rank - base_rank, np.nan)
        out["b_avg_rank"] = base_rank

    out["b_imp"] = base_imp
    out["b_clk"] = base_clk
    out["b_cost"] = base_cost
    total_cost = float(pd.to_numeric(out["광고비"], errors="coerce").fillna(0.0).sum())
    out["지출 비중(%)"] = np.where(total_cost > 0, (out["광고비"] / total_cost) * 100.0, 0.0)
    out = _add_overview_group_status(out)
    return out.sort_values("광고비", ascending=False).reset_index(drop=True)

@st.cache_data(ttl=43200, max_entries=16, show_spinner=False)
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


def _pick_shopping_query_metric_col(shop_terms: pd.DataFrame, prefer_purchase: bool) -> str | None:
    if shop_terms is None or shop_terms.empty:
        return None
    cols = set(shop_terms.columns)
    preferred = ["purchase_conv", "total_conv"] if prefer_purchase else ["total_conv", "purchase_conv"]
    return next((col for col in preferred if col in cols), None)


def _get_campaign_top_shopping_query_map(shop_terms: pd.DataFrame, top_n: int = 3, prefer_purchase: bool = True) -> dict[str, str]:
    out: dict[str, str] = {}
    if shop_terms is None or shop_terms.empty:
        return out
    required = {'campaign_name', 'query_text'}
    if not required.issubset(set(shop_terms.columns)):
        return out
    metric_col = _pick_shopping_query_metric_col(shop_terms, prefer_purchase)
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


def _campaign_type_column_from_frame(df: pd.DataFrame) -> str | None:
    if df is None or df.empty:
        return None
    return next((col for col in ["campaign_type", "campaign_type_label", "campaign_tp", "캠페인유형"] if col in df.columns), None)


def _type_summary_sort_key(label: str) -> tuple[int, str]:
    order = {
        "파워링크": 0,
        "쇼핑검색": 1,
        "파워컨텐츠": 2,
        "브랜드검색": 3,
        "플레이스": 4,
        "메타": 5,
        "구글": 6,
    }
    text = str(label or "").strip()
    return (order.get(text, 99), text)


def _aggregate_type_performance_frame(src: pd.DataFrame) -> pd.DataFrame:
    metric_cols = ["imp", "clk", "cost", "conv", "sales", "tot_conv", "tot_sales"]
    if src is None or src.empty:
        return pd.DataFrame(columns=["_type_label", *metric_cols])

    type_col = _campaign_type_column_from_frame(src)
    if not type_col:
        return pd.DataFrame(columns=["_type_label", *metric_cols])

    work = src.copy()
    work["_type_label"] = work[type_col].map(_normalize_type_label).replace("", "미분류")
    for col in ["imp", "clk", "cost", "conv", "sales"]:
        if col not in work.columns:
            work[col] = 0.0
        work[col] = safe_numeric_col(work, col, default=0.0)
    if "tot_conv" not in work.columns:
        work["tot_conv"] = work["conv"]
    else:
        work["tot_conv"] = safe_numeric_col(work, "tot_conv", default=0.0)
    if "tot_sales" not in work.columns:
        work["tot_sales"] = work["sales"]
    else:
        work["tot_sales"] = safe_numeric_col(work, "tot_sales", default=0.0)

    return work.groupby("_type_label", as_index=False, dropna=False)[metric_cols].sum()


def _build_type_performance_summary(cur_camp: pd.DataFrame, base_camp: pd.DataFrame | None = None) -> pd.DataFrame:
    columns = [
        "캠페인 유형", "노출수", "클릭수", "클릭률(%)", "광고비", "CPC",
        "구매완료수", "구매완료 매출", "구매완료 ROAS(%)",
        "총 전환수", "총 전환매출", "통합 ROAS(%)",
        "클릭수 증감", "광고비 증감", "전환수 증감", "ROAS 증감",
    ]

    cur_grouped = _aggregate_type_performance_frame(cur_camp)
    if cur_grouped.empty:
        return pd.DataFrame(columns=columns)
    base_grouped = _aggregate_type_performance_frame(base_camp) if base_camp is not None else pd.DataFrame()
    grouped = cur_grouped.merge(base_grouped, on="_type_label", how="left", suffixes=("", "_base")).fillna(0)

    out = pd.DataFrame()
    out["캠페인 유형"] = grouped["_type_label"].fillna("미분류").astype(str)
    out["노출수"] = grouped["imp"]
    out["클릭수"] = grouped["clk"]
    out["클릭률(%)"] = _safe_div(grouped["clk"], grouped["imp"], 100.0)
    out["광고비"] = grouped["cost"]
    out["CPC"] = _safe_div(grouped["cost"], grouped["clk"])
    out["구매완료수"] = grouped["conv"]
    out["구매완료 매출"] = grouped["sales"]
    out["구매완료 ROAS(%)"] = _safe_div(grouped["sales"], grouped["cost"], 100.0)
    out["총 전환수"] = grouped["tot_conv"]
    out["총 전환매출"] = grouped["tot_sales"]
    out["통합 ROAS(%)"] = _safe_div(grouped["tot_sales"], grouped["cost"], 100.0)

    b_clk = grouped["clk_base"] if "clk_base" in grouped.columns else pd.Series([0.0] * len(grouped.index))
    b_cost = grouped["cost_base"] if "cost_base" in grouped.columns else pd.Series([0.0] * len(grouped.index))
    b_tot_conv = grouped["tot_conv_base"] if "tot_conv_base" in grouped.columns else pd.Series([0.0] * len(grouped.index))
    b_tot_sales = grouped["tot_sales_base"] if "tot_sales_base" in grouped.columns else pd.Series([0.0] * len(grouped.index))
    b_troas = _safe_div(b_tot_sales, b_cost, 100.0)
    out["클릭수 증감"] = np.where(b_clk > 0, (grouped["clk"] - b_clk) / b_clk * 100.0, np.nan)
    out["광고비 증감"] = np.where(b_cost > 0, (grouped["cost"] - b_cost) / b_cost * 100.0, np.nan)
    out["전환수 증감"] = np.where(b_tot_conv > 0, (grouped["tot_conv"] - b_tot_conv) / b_tot_conv * 100.0, np.nan)
    out["ROAS 증감"] = out["통합 ROAS(%)"] - b_troas
    out["_ord"] = out["캠페인 유형"].map(_type_summary_sort_key)
    out = out.sort_values(["_ord", "광고비"], ascending=[True, False]).drop(columns=["_ord"])
    return out[columns]


def _render_type_performance_snapshot(type_summary: pd.DataFrame) -> None:
    if type_summary is None or type_summary.empty:
        st.info("조건에 맞는 유형별 성과 데이터가 없습니다.")
        return
    view_cols = [
        "캠페인 유형", "광고비", "클릭수", "구매완료수",
        "총 전환수", "총 전환매출", "통합 ROAS(%)",
        "클릭수 증감", "광고비 증감", "전환수 증감", "ROAS 증감",
    ]
    disp = type_summary[[c for c in view_cols if c in type_summary.columns]].copy()
    st.dataframe(
        disp,
        width="stretch",
        height=_auto_table_height(disp, default_height=160, max_height=280),
        hide_index=True,
        column_config=numeric_column_config(disp, base={
            "캠페인 유형": st.column_config.TextColumn("캠페인 유형", pinned=True, width="medium"),
            "광고비": st.column_config.NumberColumn("광고비", format="%,.0f 원"),
            "클릭수": st.column_config.NumberColumn("클릭수", format="%,.0f"),
            "구매완료수": st.column_config.NumberColumn("구매완료수", format="%,.0f"),
            "총 전환수": st.column_config.NumberColumn("총 전환수", format="%,.0f"),
            "총 전환매출": st.column_config.NumberColumn("총 전환매출", format="%,.0f 원"),
            "통합 ROAS(%)": st.column_config.NumberColumn("통합 ROAS(%)", format="%,.1f%%"),
            "클릭수 증감": st.column_config.NumberColumn("클릭수 증감", format="%+,.1f%%"),
            "광고비 증감": st.column_config.NumberColumn("광고비 증감", format="%+,.1f%%"),
            "전환수 증감": st.column_config.NumberColumn("전환수 증감", format="%+,.1f%%"),
            "ROAS 증감": st.column_config.NumberColumn("ROAS 증감", format="%+,.1fp"),
        }),
    )


def _get_type_top_keyword_map(kw_bundle: pd.DataFrame, top_n: int = 5) -> dict[str, str]:
    out: dict[str, str] = {}
    if kw_bundle is None or kw_bundle.empty:
        return out
    type_col = _campaign_type_column_from_frame(kw_bundle)
    if not type_col or not {"keyword", "clk"}.issubset(set(kw_bundle.columns)):
        return out
    kw = kw_bundle.copy()
    kw["_type_label"] = kw[type_col].map(_normalize_type_label).replace("", "미분류")
    kw["keyword"] = kw["keyword"].fillna("").astype(str).str.strip()
    kw["clk"] = safe_numeric_col(kw, "clk", default=0.0)
    kw = kw[(kw["keyword"] != "") & (kw["clk"] > 0)]
    if kw.empty:
        return out
    grouped = (
        kw.groupby(["_type_label", "keyword"], dropna=False)["clk"]
        .sum()
        .reset_index()
        .sort_values(["_type_label", "clk"], ascending=[True, False])
    )
    for type_label, sub in grouped.groupby("_type_label", sort=False):
        top_rows = sub.head(top_n)
        if top_rows.empty:
            continue
        out[str(type_label)] = ", ".join(
            [f"{str(r['keyword'])}({int(r['clk']):,}회)" for _, r in top_rows.iterrows()]
        )
    return out


def _build_type_report_text(
    type_summary: pd.DataFrame,
    report_uses_purchase: bool,
    type_top_keyword_map: dict[str, str] | None = None,
    shopping_keyword_text: str = "없음",
) -> str:
    if type_summary is None or type_summary.empty:
        return ""
    type_top_keyword_map = type_top_keyword_map or {}
    sections: list[str] = []
    for _, row in type_summary.iterrows():
        type_label = str(row.get("캠페인 유형", "") or "").strip()
        if not type_label:
            continue
        keyword_text = shopping_keyword_text if _is_shopping_type_label(type_label) else type_top_keyword_map.get(type_label, "없음")
        if report_uses_purchase:
            section = "\n".join([
                f"[ {type_label} 성과 요약 ]",
                _format_report_line("노출수", f"{int(float(row.get('노출수', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(row.get('클릭수', 0))):,}"),
                _format_report_line("클릭률", f"{float(row.get('클릭률(%)', 0)):.1f}%"),
                _format_report_line("광고 소진비용", f"{int(float(row.get('광고비', 0))):,}원"),
                _format_report_line("구매완료수", _format_report_count(row.get("구매완료수", 0))),
                _format_report_line("구매완료 매출", f"{int(float(row.get('구매완료 매출', 0))):,}원"),
                _format_report_line("구매 ROAS", f"{float(row.get('구매완료 ROAS(%)', 0)):.1f}%"),
                _format_report_line("주요 전환 키워드", keyword_text),
            ])
        else:
            section = "\n".join([
                f"[ {type_label} 성과 요약 ]",
                _format_report_line("노출수", f"{int(float(row.get('노출수', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(row.get('클릭수', 0))):,}"),
                _format_report_line("클릭률", f"{float(row.get('클릭률(%)', 0)):.1f}%"),
                _format_report_line("광고 소진비용", f"{int(float(row.get('광고비', 0))):,}원"),
                _format_report_line("전환수", _format_report_count(row.get("총 전환수", 0))),
                _format_report_line("총전환매출", f"{int(float(row.get('총 전환매출', 0))):,}원"),
                _format_report_line("ROAS", f"{float(row.get('통합 ROAS(%)', 0)):.1f}%"),
                _format_report_line("주요 유입 키워드", keyword_text),
            ])
        sections.append(section)
    if not sections:
        return ""
    metric_label = "구매완료 데이터" if report_uses_purchase else "보고서 전환"
    return "\n\n".join([f"[ 유형별 성과 요약 | {metric_label} ]", *sections])


def _build_campaign_report_text(
    cur_camp: pd.DataFrame,
    selected_type_label: str,
    is_shopping_only: bool,
    combined_toggle: bool,
    kpi_mode: str,
    report_uses_purchase: bool = False,
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
        use_purchase_metrics = bool(report_uses_purchase)
        if use_purchase_metrics:
            keyword_text = campaign_top_shopping_query_map.get(campaign_name) if is_shopping_only else None
            if not keyword_text:
                keyword_text = campaign_top_keyword_map.get(campaign_name, '없음')
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
            c_conv_val = row.get('tot_conv', 0)
            c_sales_val = row.get('tot_sales', 0)
            c_roas_val = _safe_div(float(c_sales_val), float(row.get('cost', 0)), 100.0)
            keyword_text = campaign_top_shopping_query_map.get(campaign_name) if is_shopping_only else None
            if not keyword_text:
                keyword_text = campaign_top_keyword_map.get(campaign_name, '없음')
            section = "\n".join([
                f"[ {campaign_name} 성과 요약 ]",
                _format_report_line("노출수", f"{int(float(row.get('imp', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(row.get('clk', 0))):,}"),
                _format_report_line("클릭률", f"{float(_safe_div(float(row.get('clk', 0)), float(row.get('imp', 0)), 100.0)):.1f}%"),
                _format_report_line("광고 소진비용", f"{int(float(row.get('cost', 0))):,}원"),
                _format_report_line("전환수", _format_report_count(c_conv_val)),
                _format_report_line("총전환매출", f"{int(float(c_sales_val)):,}원"),
                _format_report_line("ROAS", f"{float(c_roas_val):.1f}%"),
                _format_report_line("주요 유입 키워드", keyword_text),
            ])
        sections.append(section)

    if not sections:
        return ''

    metric_label = "구매완료 데이터" if report_uses_purchase else "보고서 전환"
    title = f"[ 캠페인별 성과 요약 | {selected_type_label} | {metric_label} ]"
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

    cached_text = "없음" if force_refresh else _cached_report_source_text(engine, "powerlink_keyword", start_dt, end_dt, cids, type_sel, 5)
    if cached_text != "없음":
        if diag is not None:
            _diag_add(diag, "키워드 번들", "ok", 5, "overview_report_source_cache", "보고서용 상위 키워드 캐시 사용")
        return cached_text

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


def _join_report_source_rows(df: pd.DataFrame, text_col: str = "source_text", metric_col: str = "metric_value", top_n: int = 5) -> str:
    if df is None or df.empty or text_col not in df.columns:
        return "없음"
    work = df.copy()
    work[text_col] = work[text_col].fillna("").astype(str).str.strip()
    work[metric_col] = safe_numeric_col(work, metric_col, default=0.0) if metric_col in work.columns else 0.0
    work = work[(work[text_col] != "") & (work[metric_col] > 0)]
    if work.empty:
        return "없음"
    work = work.sort_values(metric_col, ascending=False).head(max(1, int(top_n or 5)))
    return ", ".join([f"{str(r[text_col])}({int(r[metric_col]):,}회)" for _, r in work.iterrows()])


@st.cache_data(ttl=3600, max_entries=120, show_spinner=False)
def _cached_report_source_text(_engine, source_kind: str, start_dt, end_dt, cids: tuple, type_sel: tuple, top_n: int = 5) -> str:
    try:
        source_df = query_overview_report_source_cache(_engine, source_kind, start_dt, end_dt, cids, type_sel, top_n)
        return _join_report_source_rows(source_df, top_n=top_n)
    except Exception:
        return "없음"


@st.cache_data(ttl=3600, max_entries=80, show_spinner=False)
def _cached_type_top_source_map(_engine, source_kind: str, start_dt, end_dt, cids: tuple, type_sel: tuple, top_n: int = 5) -> dict[str, str]:
    try:
        source_df = query_overview_report_source_cache_by_type(_engine, source_kind, start_dt, end_dt, cids, type_sel, top_n)
    except Exception:
        return {}
    if source_df is None or source_df.empty or not {"campaign_type", "source_text"}.issubset(source_df.columns):
        return {}
    out: dict[str, str] = {}
    work = source_df.copy()
    work["_type_label"] = work["campaign_type"].map(_normalize_type_label).replace("", "미분류")
    for type_label, sub in work.groupby("_type_label", sort=False):
        text = _join_report_source_rows(sub, top_n=top_n)
        if text != "없음":
            out[str(type_label)] = text
    return out


@st.cache_data(ttl=300, max_entries=80, show_spinner=False)
def _cached_shopping_top_terms_text(_engine, start_dt, end_dt, cids: tuple, report_uses_purchase: bool, top_n: int = 3) -> str:
    try:
        terms_df = query_shopping_top_search_terms(_engine, start_dt, end_dt, cids, bool(report_uses_purchase), top_n)
        return _join_report_source_rows(terms_df, text_col="query_text", top_n=top_n)
    except Exception:
        return "없음"


@st.cache_data(ttl=43200, max_entries=12, show_spinner=False)
def _build_overview_excel_bytes(
    df_display: pd.DataFrame,
    df_type_display: pd.DataFrame,
    camp_disp: pd.DataFrame,
    group_disp: pd.DataFrame,
    kw_disp: pd.DataFrame,
    daily_disp: pd.DataFrame,
    dow_disp: pd.DataFrame,
    weekly_disp: pd.DataFrame,
) -> bytes:
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer) as writer:
        if not df_display.empty:
            format_for_csv(df_display).to_excel(writer, sheet_name='계정별_성과상세', index=False)
        if not df_type_display.empty:
            format_for_csv(df_type_display).to_excel(writer, sheet_name='유형별_성과상세', index=False)
        if not camp_disp.empty:
            format_for_csv(camp_disp).to_excel(writer, sheet_name='캠페인별_성과상세', index=False)
        if group_disp is not None and not group_disp.empty:
            format_for_csv(_overview_export_cols(group_disp)).to_excel(writer, sheet_name='그룹별_성과상세', index=False)
        if not kw_disp.empty:
            format_for_csv(kw_disp).to_excel(writer, sheet_name='키워드별_성과상세', index=False)
        if not daily_disp.empty:
            format_for_csv(daily_disp).to_excel(writer, sheet_name='일자별_성과상세', index=False)
        if not dow_disp.empty:
            dow_export = dow_disp.drop(columns=['요일']) if '요일' in dow_disp.columns else dow_disp
            format_for_csv(dow_export).to_excel(writer, sheet_name='요일별_성과상세', index=False)
        if not weekly_disp.empty:
            format_for_csv(weekly_disp).to_excel(writer, sheet_name='주간_성과상세', index=False)
    return excel_buffer.getvalue()


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


def _sort_overview_detail_frame(df: pd.DataFrame, sort_col: str, descending: bool = True) -> pd.DataFrame:
    if df is None or df.empty or sort_col not in df.columns:
        return pd.DataFrame() if df is None else df
    work = df.copy()
    numeric_key = pd.to_numeric(work[sort_col], errors="coerce")
    if numeric_key.notna().any():
        work["_sort_key"] = numeric_key.fillna(0)
    else:
        work["_sort_key"] = work[sort_col].astype(str)
    tie_cols = [c for c in ["광고비", "총 전환수", "구매완료수"] if c in work.columns and c != sort_col]
    sort_by = ["_sort_key"] + tie_cols
    ascending = [not descending] + [False] * len(tie_cols)
    return work.sort_values(sort_by, ascending=ascending, kind="mergesort").drop(columns=["_sort_key"]).reset_index(drop=True)


def _render_overview_keyword_sort_controls(df: pd.DataFrame, visible_cols: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    preferred = [
        "광고비", "총 전환수", "구매완료수", "총 전환매출", "구매완료 매출",
        "클릭수", "노출수", "클릭률(%)", "총 전환율(%)", "구매 전환율(%)",
        "CPC", "통합 ROAS(%)", "구매완료 ROAS(%)",
    ]
    sort_options = [c for c in preferred if c in visible_cols and c in df.columns]
    sort_options.extend([c for c in visible_cols if c in df.columns and c not in sort_options and c not in {"키워드", "점검"}])
    if not sort_options:
        return df

    sort_col_a, sort_dir_a = st.columns([2, 1], gap="small")
    with sort_col_a:
        default_idx = sort_options.index("광고비") if "광고비" in sort_options else 0
        sort_col = st.selectbox("전체 기준 정렬", sort_options, index=default_idx, key="overview_keyword_sort_col")
    with sort_dir_a:
        sort_dir = st.segmented_control(
            "정렬 방향",
            ["내림차순", "오름차순"],
            default="내림차순",
            key="overview_keyword_sort_dir",
        )
    return _sort_overview_detail_frame(df, sort_col, descending=(sort_dir == "내림차순"))


def _overview_keyword_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    return safe_numeric_series(df.get(col), length=len(df.index), default=0.0)


def _overview_keyword_cutoff(series: pd.Series, quantile: float = 0.75) -> float:
    positive = pd.to_numeric(series, errors="coerce").fillna(0)
    positive = positive[positive > 0]
    if positive.empty:
        return 0.0
    return float(positive.quantile(quantile))


def _filter_overview_keyword_workbench(df: pd.DataFrame, preset: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    work = df.copy()
    cost = _overview_keyword_numeric(work, "광고비")
    clicks = _overview_keyword_numeric(work, "클릭수")
    purchase = _overview_keyword_numeric(work, "구매완료수")
    total_conv = _overview_keyword_numeric(work, "총 전환수")
    roas = _overview_keyword_numeric(work, "구매완료 ROAS(%)")

    if preset == "구매 발생":
        return work[purchase > 0].copy()
    if preset == "비용 발생·구매 0":
        return work[(cost > 0) & (purchase <= 0)].copy()
    if preset == "클릭 많고 구매 0":
        click_cutoff = max(1.0, _overview_keyword_cutoff(clicks, 0.75))
        return work[(clicks >= click_cutoff) & (purchase <= 0)].copy()
    if preset == "ROAS 우수":
        return work[(purchase > 0) & (roas >= 300)].copy()
    if preset == "총전환 있음":
        return work[total_conv > 0].copy()
    return work


def _add_overview_keyword_status(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    work = df.copy()
    if "점검" in work.columns:
        return work
    cost = _overview_keyword_numeric(work, "광고비")
    clicks = _overview_keyword_numeric(work, "클릭수")
    purchase = _overview_keyword_numeric(work, "구매완료수")
    total_conv = _overview_keyword_numeric(work, "총 전환수")
    roas = _overview_keyword_numeric(work, "구매완료 ROAS(%)")
    cost_cutoff = max(1.0, _overview_keyword_cutoff(cost, 0.75))
    click_cutoff = max(1.0, _overview_keyword_cutoff(clicks, 0.75))

    status = np.select(
        [
            (purchase > 0) & (roas >= 300),
            purchase > 0,
            (cost >= cost_cutoff) & (purchase <= 0),
            (clicks >= click_cutoff) & (purchase <= 0),
            total_conv > 0,
        ],
        ["ROAS 우수", "구매 발생", "비용 점검", "클릭 점검", "총전환 있음"],
        default="관찰",
    )
    insert_at = 1 if "키워드" in work.columns else 0
    work.insert(insert_at, "점검", status)
    return work


def _render_overview_keyword_workbench_cards(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    cost = _overview_keyword_numeric(df, "광고비")
    purchase = _overview_keyword_numeric(df, "구매완료수")
    total_conv = _overview_keyword_numeric(df, "총 전환수")
    no_purchase_cost = (cost > 0) & (purchase <= 0)
    purchase_rows = int((purchase > 0).sum())
    total_rows = int(len(df.index))
    total_purchase = float(purchase.sum())
    total_conversion = float(total_conv.sum())
    max_no_purchase_cost = float(cost[no_purchase_cost].max()) if no_purchase_cost.any() else 0.0
    render_ops_cards([
        {"title": "표시 키워드", "value": f"{total_rows:,}개", "note": f"구매 발생 {purchase_rows:,}개", "tone": "info", "icon": "KW"},
        {"title": "비용 발생·구매 0", "value": f"{int(no_purchase_cost.sum()):,}개", "note": f"최대 비용 {format_currency(max_no_purchase_cost)}", "tone": "danger" if no_purchase_cost.any() else "success", "icon": "0"},
        {"title": "전환 합계", "value": f"{total_conversion:,.0f}건", "note": f"구매완료 {total_purchase:,.0f}건", "tone": "success" if total_purchase > 0 else "warning", "icon": "CV"},
    ])


def _overview_keyword_visible_cols(df: pd.DataFrame, show_deltas: bool, mode: str, funnel_cols: list[str]) -> list[str]:
    if df is None or df.empty:
        return []
    if mode == "전체 지표":
        cols = ["키워드", "점검"] + [c for c in funnel_cols if c in df.columns]
    elif mode == "전환 상세":
        cols = [
            "키워드", "점검", "광고비", "클릭수", "CPC", "구매완료수", "구매 전환율(%)",
            "구매완료 매출", "구매완료 ROAS(%)", "총 전환수", "총 전환율(%)",
            "총 전환매출", "통합 ROAS(%)",
        ]
        if show_deltas:
            cols += ["구매완료 증감", "구매완료 차이", "총 전환 증감", "총 전환 차이", "통합 ROAS 증감"]
    else:
        cols = [
            "키워드", "점검", "광고비", "클릭수", "CPC", "구매완료수",
            "구매완료 매출", "구매완료 ROAS(%)", "총 전환수", "통합 ROAS(%)",
        ]
        if show_deltas:
            cols += ["광고비 증감", "구매완료 증감", "구매완료 ROAS 증감"]
    seen = []
    for col in cols:
        if col in df.columns and col not in seen:
            seen.append(col)
    return seen


def _overview_group_signal_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    if df is None or df.empty:
        empty = pd.Series(dtype=bool)
        return {
            "cpc_up": empty, "roas_down": empty, "rank_down": empty, "cost_high": empty, "click_low": empty, "imp_low": empty,
            "cost_leak": empty, "efficiency_down": empty, "exposure_weak": empty,
            "growth_candidate": empty, "data_sparse": empty, "normal": empty,
        }

    idx = df.index
    imp = safe_numeric_col(df, "노출수")
    clk = safe_numeric_col(df, "클릭수")
    cost = safe_numeric_col(df, "광고비")
    total_conv = safe_numeric_col(df, "총 전환수")
    purchase_conv = safe_numeric_col(df, "구매완료수")
    total_roas = safe_numeric_col(df, "통합 ROAS(%)")
    purchase_roas = safe_numeric_col(df, "구매완료 ROAS(%)")
    cost_share = safe_numeric_col(df, "지출 비중(%)")
    base_clk = safe_numeric_col(df, "b_clk")
    base_cost = safe_numeric_col(df, "b_cost")
    base_imp = safe_numeric_col(df, "b_imp")
    base_cpc = np.where(base_clk > 0, base_cost / base_clk, 0.0)
    cost_rank = cost.rank(method="first", ascending=False)
    high_cost_cut = min(len(df.index), max(3, int(np.ceil(len(df.index) * 0.1))))

    cpc_diff = safe_numeric_col(df, "CPC 차이")
    cpc_pct = safe_numeric_col(df, "CPC 증감")
    rank_change = safe_numeric_col(df, "순위 변화", default=np.nan)
    click_diff = safe_numeric_col(df, "클릭 차이")
    click_pct = safe_numeric_col(df, "클릭 증감")
    imp_diff = safe_numeric_col(df, "노출 차이")
    imp_pct = safe_numeric_col(df, "노출 증감")
    roas_delta = safe_numeric_col(df, "통합 ROAS 증감", default=np.nan)
    purchase_roas_delta = safe_numeric_col(df, "구매완료 ROAS 증감", default=np.nan)

    enough_cost = cost >= 1000
    enough_click_base = base_clk >= 5
    enough_imp_base = base_imp >= 50
    median_cost = float(cost.median()) if pd.notna(cost.median()) else 0.0
    cpc_up_mask = enough_cost & enough_click_base & (clk >= 3) & (base_cpc > 0) & (cpc_diff > 0) & ((cpc_pct >= 20) | (cpc_diff >= 100))
    roas_down_mask = enough_cost & ((roas_delta <= -30) | (purchase_roas_delta <= -30))
    rank_down_mask = (rank_change >= 1) & (safe_numeric_col(df, "b_avg_rank", default=np.nan) > 0) & (enough_imp_base | enough_cost)
    click_low_mask = enough_click_base & (click_diff <= -3) & (click_pct <= -20)
    imp_low_mask = enough_imp_base & (imp_diff <= -50) & (imp_pct <= -20)
    masks = {
        "cpc_up": pd.Series(cpc_up_mask, index=idx).fillna(False),
        "roas_down": pd.Series(roas_down_mask, index=idx).fillna(False),
        "rank_down": pd.Series(rank_down_mask, index=idx).fillna(False),
        "cost_high": pd.Series((cost > 0) & (cost_rank <= high_cost_cut), index=idx).fillna(False),
        "click_low": pd.Series(click_low_mask, index=idx).fillna(False),
        "imp_low": pd.Series(imp_low_mask, index=idx).fillna(False),
        "cost_leak": pd.Series(enough_cost & (clk >= 3) & (total_conv <= 0) & (purchase_conv <= 0), index=idx).fillna(False),
        "efficiency_down": pd.Series(cpc_up_mask | roas_down_mask, index=idx).fillna(False),
        "exposure_weak": pd.Series(rank_down_mask | imp_low_mask, index=idx).fillna(False),
        "growth_candidate": pd.Series((total_conv > 0) & ((total_roas >= 300) | (purchase_roas >= 300)) & ((cost_share <= 5) | (cost < median_cost)), index=idx).fillna(False),
        "data_sparse": pd.Series((cost > 0) & ((imp < 50) | (clk < 3) | (cost < 1000)), index=idx).fillna(False),
    }
    category_cols = ["cost_leak", "efficiency_down", "exposure_weak", "growth_candidate", "data_sparse"]
    has_category = pd.Series(False, index=idx)
    for key in category_cols:
        has_category = has_category | masks[key].reindex(idx, fill_value=False)
    masks["normal"] = (~has_category).fillna(False)
    return masks


def _format_overview_signed_pct(value) -> str:
    try:
        if pd.isna(value):
            return "-"
        return f"{float(value):+,.1f}%"
    except Exception:
        return "-"


def _format_overview_signed_rank(value) -> str:
    try:
        if pd.isna(value):
            return "-"
        return f"{float(value):+,.0f}위"
    except Exception:
        return "-"


def _overview_top_group_note(df: pd.DataFrame, mask: pd.Series, sort_col: str, value_col: str, formatter, *, ascending: bool = False) -> str:
    if df is None or df.empty or sort_col not in df.columns:
        return "해당 그룹 없음"
    work = df[mask.reindex(df.index, fill_value=False)].copy()
    if work.empty:
        return "해당 그룹 없음"
    work["_sort"] = pd.to_numeric(work[sort_col], errors="coerce")
    work = work.dropna(subset=["_sort"]).sort_values("_sort", ascending=ascending)
    if work.empty:
        return "해당 그룹 없음"
    row = work.iloc[0]
    group_name = str(row.get("광고그룹", "-") or "-")
    if len(group_name) > 22:
        group_name = f"{group_name[:22]}..."
    return f"{group_name} · {formatter(row.get(value_col))}"


def _format_overview_signed_currency(value) -> str:
    try:
        if pd.isna(value):
            return "-"
        return f"{float(value):+,.0f}원"
    except Exception:
        return "-"


def _format_overview_signed_count(value) -> str:
    try:
        if pd.isna(value):
            return "-"
        return f"{float(value):+,.0f}"
    except Exception:
        return "-"


def _overview_group_signal_evidence(row: pd.Series, judgment: str, active: list[str]) -> str:
    if judgment == "비용 누수":
        return (
            f"광고비 {format_currency(float(row.get('광고비', 0) or 0))} · "
            f"클릭 {float(row.get('클릭수', 0) or 0):,.0f} · "
            f"전환 {float(row.get('총 전환수', 0) or 0):,.0f}"
        )
    if judgment == "확장 후보":
        roas = float(row.get("통합 ROAS(%)", 0) or row.get("구매완료 ROAS(%)", 0) or 0)
        return (
            f"ROAS {roas:,.1f}% · "
            f"전환 {float(row.get('총 전환수', 0) or 0):,.0f} · "
            f"지출 비중 {float(row.get('지출 비중(%)', 0) or 0):.1f}%"
        )
    if judgment == "데이터 부족":
        return (
            f"노출 {float(row.get('노출수', 0) or 0):,.0f} · "
            f"클릭 {float(row.get('클릭수', 0) or 0):,.0f} · "
            f"광고비 {format_currency(float(row.get('광고비', 0) or 0))}"
        )
    if not active:
        return "특이 변화 없음"
    parts: list[str] = []
    for signal in active:
        if signal == "CPC 상승":
            parts.append(f"CPC {_format_overview_signed_currency(row.get('CPC 차이'))} ({_format_overview_signed_pct(row.get('CPC 증감'))})")
        elif signal == "순위 하락":
            parts.append(f"순위 {_format_overview_signed_rank(row.get('순위 변화'))}")
        elif signal == "클릭 저조":
            parts.append(f"클릭 {_format_overview_signed_count(row.get('클릭 차이'))} ({_format_overview_signed_pct(row.get('클릭 증감'))})")
        elif signal == "노출 저조":
            parts.append(f"노출 {_format_overview_signed_count(row.get('노출 차이'))} ({_format_overview_signed_pct(row.get('노출 증감'))})")
        elif signal == "비용 상위":
            parts.append(f"지출 비중 {float(row.get('지출 비중(%)', 0) or 0):.1f}%")
        elif signal == "ROAS 하락":
            parts.append(f"ROAS {_format_overview_signed_pct(row.get('통합 ROAS 증감'))}")
    return " · ".join([p for p in parts if p][:3]) or "특이 변화 없음"


def _overview_group_check_target(judgment: str, active: list[str]) -> str:
    if judgment == "비용 누수":
        return "검색어·소재·랜딩"
    if judgment == "효율 악화":
        return "입찰가·CPC·ROAS"
    if judgment == "노출 약화":
        return "예산·순위·노출"
    if judgment == "확장 후보":
        return "예산 증액 후보"
    if judgment == "데이터 부족":
        return "기간 확대 후 판단"
    if judgment == "정상":
        return "유지"
    action_map = {
        "CPC 상승": "입찰가·품질요인",
        "순위 하락": "입찰·경쟁 변화",
        "클릭 저조": "소재·상품명·검색어",
        "노출 저조": "예산·노출 제한",
        "비용 상위": "예산 배분",
    }
    return action_map.get(active[0], "확인") if active else "유지"


def _overview_group_judgment(masks: dict[str, pd.Series], idx) -> tuple[str, int]:
    if bool(masks["cost_leak"].get(idx, False)):
        return "비용 누수", 90
    if bool(masks["efficiency_down"].get(idx, False)):
        return "효율 악화", 75
    if bool(masks["exposure_weak"].get(idx, False)):
        return "노출 약화", 60
    if bool(masks["growth_candidate"].get(idx, False)):
        return "확장 후보", 45
    if bool(masks["data_sparse"].get(idx, False)):
        return "데이터 부족", 15
    return "정상", 0


def _add_overview_group_status(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    work = df.copy()
    if {"운영 판단", "판단 근거", "확인 대상"}.issubset(set(work.columns)):
        return work
    signal_cols = ["신호 등급", "우선순위", "검토 상태", "운영 판단", "핵심 신호", "업무 신호", "판단 근거", "다음 확인", "확인 대상", "_신호 점수"]
    work = work.drop(columns=[c for c in signal_cols if c in work.columns])
    masks = _overview_group_signal_masks(work)
    signal_labels = [
        ("CPC 상승", masks["cpc_up"]),
        ("ROAS 하락", masks["roas_down"]),
        ("순위 하락", masks["rank_down"]),
        ("클릭 저조", masks["click_low"]),
        ("노출 저조", masks["imp_low"]),
        ("비용 상위", masks["cost_high"]),
    ]
    judgments = []
    primary_signals = []
    signals = []
    evidences = []
    check_targets = []
    scores = []
    for idx in work.index:
        active = [label for label, mask in signal_labels if bool(mask.get(idx, False))]
        judgment, score = _overview_group_judgment(masks, idx)
        judgments.append(judgment)
        primary_signals.append(active[0] if active else judgment)
        signals.append(" · ".join(active) if active else "정상")
        evidences.append(_overview_group_signal_evidence(work.loc[idx], judgment, active))
        check_targets.append(_overview_group_check_target(judgment, active))
        scores.append(score)
    insert_at = 1 if "광고그룹" in work.columns else 0
    work.insert(insert_at, "운영 판단", judgments)
    work.insert(insert_at + 1, "핵심 신호", primary_signals)
    work.insert(insert_at + 2, "판단 근거", evidences)
    work.insert(insert_at + 3, "확인 대상", check_targets)
    work.insert(insert_at + 4, "업무 신호", signals)
    work["_신호 점수"] = scores
    return work


def _filter_overview_group_workbench(df: pd.DataFrame, preset: str) -> pd.DataFrame:
    if df is None or df.empty or preset == "전체":
        return pd.DataFrame() if df is None else df
    if preset in {"비용 누수", "효율 악화", "노출 약화", "확장 후보", "데이터 부족", "정상"} and "운영 판단" in df.columns:
        return df[df["운영 판단"].astype(str) == str(preset)].copy()
    masks = _overview_group_signal_masks(df)
    key_map = {
        "CPC 상승": "cpc_up",
        "순위 하락": "rank_down",
        "비용 상위": "cost_high",
        "클릭 저조": "click_low",
        "노출 저조": "imp_low",
    }
    key = key_map.get(str(preset))
    if not key:
        return df
    return df[masks[key]].copy()


def _sort_overview_group_workbench(df: pd.DataFrame, preset: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    if preset in {"전체", "비용 누수", "효율 악화", "노출 약화", "확장 후보", "데이터 부족", "정상"} and "_신호 점수" in df.columns:
        sort_cols = ["_신호 점수"] + (["광고비"] if "광고비" in df.columns else [])
        return df.sort_values(sort_cols, ascending=[False] * len(sort_cols)).reset_index(drop=True)
    sort_map = {
        "CPC 상승": ("CPC 차이", False),
        "순위 하락": ("순위 변화", False),
        "비용 상위": ("광고비", False),
        "클릭 저조": ("클릭 증감", True),
        "노출 저조": ("노출 증감", True),
    }
    sort_col, ascending = sort_map.get(str(preset), ("광고비", False))
    if sort_col not in df.columns:
        sort_col, ascending = "광고비", False
    return df.sort_values(sort_col, ascending=ascending).reset_index(drop=True)


def _overview_top_operational_note(df: pd.DataFrame, mask: pd.Series, sort_col: str = "광고비") -> str:
    if df is None or df.empty:
        return "해당 그룹 없음"
    work = df[mask.reindex(df.index, fill_value=False)].copy()
    if work.empty:
        return "해당 그룹 없음"
    if sort_col not in work.columns:
        sort_col = "광고비" if "광고비" in work.columns else work.columns[0]
    work["_sort"] = pd.to_numeric(work[sort_col], errors="coerce").fillna(0)
    row = work.sort_values("_sort", ascending=False).iloc[0]
    group_name = str(row.get("광고그룹", "-") or "-")
    if len(group_name) > 22:
        group_name = f"{group_name[:22]}..."
    evidence = str(row.get("판단 근거", "") or "")
    return f"{group_name} · {evidence}" if evidence else group_name


def _render_overview_group_signal_cards(df: pd.DataFrame, cmp_mode: str, b1, b2) -> None:
    if df is None or df.empty:
        return
    masks = _overview_group_signal_masks(df)
    render_ops_cards([
        {
            "title": "비용 누수",
            "value": f"{int(masks['cost_leak'].sum()):,}개",
            "note": _overview_top_operational_note(df, masks["cost_leak"], "광고비"),
            "tone": "danger" if bool(masks["cost_leak"].any()) else "success",
            "icon": "누수",
        },
        {
            "title": "효율 악화",
            "value": f"{int(masks['efficiency_down'].sum()):,}개",
            "note": _overview_top_operational_note(df, masks["efficiency_down"], "광고비"),
            "tone": "danger" if bool(masks["efficiency_down"].any()) else "success",
            "icon": "효율",
        },
        {
            "title": "노출 약화",
            "value": f"{int(masks['exposure_weak'].sum()):,}개",
            "note": _overview_top_operational_note(df, masks["exposure_weak"], "광고비"),
            "tone": "warning" if bool(masks["exposure_weak"].any()) else "success",
            "icon": "노출",
        },
        {
            "title": "확장 후보",
            "value": f"{int(masks['growth_candidate'].sum()):,}개",
            "note": _overview_top_operational_note(df, masks["growth_candidate"], "통합 ROAS(%)"),
            "tone": "success" if bool(masks["growth_candidate"].any()) else "info",
            "icon": "확장",
        },
        {
            "title": "데이터 부족",
            "value": f"{int(masks['data_sparse'].sum()):,}개",
            "note": _overview_top_operational_note(df, masks["data_sparse"], "광고비"),
            "tone": "info" if bool(masks["data_sparse"].any()) else "success",
            "icon": "보류",
        },
    ])
    st.caption(f"비교 기준: {cmp_mode} · {b1} ~ {b2}")


def _overview_group_visible_cols(df: pd.DataFrame, show_deltas: bool, funnel_cols: list[str]) -> list[str]:
    if df is None or df.empty:
        return []
    cols = [
        "광고그룹", "운영 판단", "판단 근거", "확인 대상", "캠페인명", "계정명", "캠페인유형", "지출 비중(%)",
        *[c for c in funnel_cols if c in df.columns],
    ]
    seen = []
    for col in cols:
        if col in df.columns and col not in seen:
            seen.append(col)
    return seen


def _overview_cols_by_preference(view_cols: list[str], preferred_cols: list[str]) -> list[str]:
    preferred = [c for c in preferred_cols if c in view_cols]
    return preferred + [c for c in view_cols if c not in preferred]


def _overview_group_preset_order(view_cols: list[str], mode: str) -> list[str]:
    if not view_cols or mode == "기본":
        return view_cols
    if mode == "운영":
        preferred = [
            "광고그룹", "운영 판단", "판단 근거", "확인 대상", "지출 비중(%)", "광고비", "광고비 증감", "광고비 차이",
            "CPC", "CPC 증감", "CPC 차이", "평균순위", "순위 변화",
            "클릭수", "클릭 증감", "클릭 차이", "노출수", "노출 증감", "노출 차이",
            "캠페인명", "계정명", "캠페인유형",
        ]
    elif mode == "성과":
        preferred = [
            "광고그룹", "운영 판단", "판단 근거", "확인 대상", "구매완료수", "구매완료 증감", "구매완료 차이",
            "구매완료 매출", "구매완료 매출 증감", "구매완료 매출 차이",
            "총 전환수", "총 전환 증감", "총 전환 차이",
            "총 전환매출", "총 매출 증감", "총 매출 차이",
            "광고비", "지출 비중(%)", "캠페인명", "계정명", "캠페인유형",
        ]
    elif mode == "효율":
        preferred = [
            "광고그룹", "운영 판단", "판단 근거", "확인 대상", "클릭률(%)", "클릭률 증감",
            "CPC", "CPC 증감", "CPC 차이",
            "구매 전환율(%)", "구매 전환율 증감",
            "구매완료 ROAS(%)", "구매완료 ROAS 증감",
            "통합 ROAS(%)", "통합 ROAS 증감",
            "광고비", "지출 비중(%)", "캠페인명", "계정명", "캠페인유형",
        ]
    else:
        return view_cols
    return _overview_cols_by_preference(view_cols, preferred)


def _overview_group_custom_order(view_cols: list[str], show_deltas: bool) -> list[str]:
    if not view_cols:
        return view_cols
    fixed_cols = [c for c in ["광고그룹"] if c in view_cols]
    editable_cols = [c for c in view_cols if c not in fixed_cols]
    state_key = "overview_group_custom_column_order"
    saved = st.session_state.get(state_key, [])
    saved = [c for c in saved if c in editable_cols] + [c for c in editable_cols if c not in saved]

    order_df = pd.DataFrame({"컬럼": saved, "순서": list(range(1, len(saved) + 1))})
    edited = st.data_editor(
        order_df,
        width="stretch",
        hide_index=True,
        disabled=["컬럼"],
        key=f"overview_group_column_order_editor_{int(show_deltas)}_{len(editable_cols)}",
        column_config={
            "컬럼": st.column_config.TextColumn("컬럼", width="medium"),
            "순서": st.column_config.NumberColumn("순서", min_value=1, step=1, format="%d"),
        },
    )
    if isinstance(edited, pd.DataFrame) and {"컬럼", "순서"}.issubset(edited.columns):
        edited = edited.copy()
        edited["_default_order"] = range(len(edited.index))
        edited["_sort_order"] = pd.to_numeric(edited["순서"], errors="coerce").fillna(9999)
        ordered = edited.sort_values(["_sort_order", "_default_order"])["컬럼"].astype(str).tolist()
        ordered = [c for c in ordered if c in editable_cols]
        st.session_state[state_key] = ordered
        return fixed_cols + ordered
    st.session_state[state_key] = saved
    return fixed_cols + saved


def _overview_export_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    hidden = {c for c in df.columns if str(c).startswith("b_") or str(c).startswith("_") or str(c) == "avg_rank"}
    return df[[c for c in df.columns if c not in hidden]].copy()


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
    if "META" in s or "메타" in s: return "메타"
    if "GOOGLE" in s or "구글" in s: return "구글"
    return str(val).strip()


def _infer_kpi_mode(type_sel: tuple, cur_camp: pd.DataFrame, is_split_only: bool) -> str:
    if is_split_only and _overview_is_shopping_only_context(type_sel, cur_camp):
        return "shopping_purchase"
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
    raw_cids = tuple(f.get("selected_customer_ids", []))
    media_sel = tuple(f.get("media_sel", []))
    # 매체 필터가 지정된 경우에는 전역 필터에서 이미 해당 매체 ID로 제한되어 있으므로
    # 오버뷰의 고객 ID 확장 로직이 다시 메타/구글/네이버를 섞지 않도록 확장을 건너뜁니다.
    cids = tuple(raw_cids) if media_sel else _expand_overview_customer_ids(meta, engine, raw_cids)
    type_sel = tuple(f.get("type_sel", []))
    initial_detail_panel = st.session_state.get("overview_detail_panel", "업체별 요약")
    opts = get_dynamic_cmp_options(f["start"], f["end"])
    cmp_mode = opts[1] if len(opts) > 1 else "이전 같은 기간 대비"
    b1, b2 = period_compare_range(f["start"], f["end"], cmp_mode)
    _diag_add(
        diag,
        "필터",
        "ok",
        len(cids),
        "filters",
        f"기간={f['start']}~{f['end']} | 비교={cmp_mode} | 매체={', '.join(media_sel) if media_sel else '전체'} | 유형={', '.join(type_sel) if type_sel else '전체'} | 원선택={len(raw_cids)} / 조회={len(cids)}",
    )

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
            
        cur_kw = pd.DataFrame()
        if initial_detail_panel == "키워드 상세 분석":
            try:
                cur_kw = _cached_keyword_bundle(engine, f["start"], f["end"], cids, type_sel)
                _diag_add(diag, "키워드 번들(현재)", "ok" if cur_kw is not None and not cur_kw.empty else "zero_data", 0 if cur_kw is None else len(cur_kw.index), "query_keyword_bundle", "현재 기간 키워드 상세")
            except Exception as e:
                cur_kw = pd.DataFrame()
                _diag_add(diag, "키워드 번들(현재)", "error", 0, "query_keyword_bundle", f"{type(e).__name__}: {e}")
        else:
            _diag_add(diag, "키워드 번들(현재)", "warn", 0, "lazy_skip", "키워드 상세/보고서 영역에서 지연 조회")
            
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

    account_name = _selected_account_title(meta, engine, cids, cur_camp)

    selected_type_label = _selected_type_label(type_sel)

    st.markdown(f"<div class='nv-sec-title'>{account_name} 종합 성과 요약</div>", unsafe_allow_html=True)
    patch_date = date(2026, 3, 11)
    is_legacy_only = f["end"] < patch_date
    is_split_only = f["start"] >= patch_date
    is_mixed_period = (f["start"] < patch_date <= f["end"])
    combined_toggle = not is_split_only
    auto_kpi_mode = _infer_kpi_mode(type_sel, cur_camp, is_split_only)
    can_use_purchase_toggle = (f["end"] >= patch_date)
    shopping_only_context = _overview_is_shopping_only_context(type_sel, cur_camp)
    force_purchase_view = bool(can_use_purchase_toggle and (not is_mixed_period) and shopping_only_context)
    if force_purchase_view:
        # Streamlit keeps toggle state by key. Without resetting it, a previous
        # generic-conversion view can make a shopping-only overview keep showing
        # total conversion KPI instead of explicit purchase-complete KPI.
        st.session_state["overview_purchase_view_toggle"] = True

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
            disabled=(not can_use_purchase_toggle) or force_purchase_view,
            help="쇼핑검색만 조회할 때는 캠페인 일별 split의 구매완료 기준을 자동 적용합니다.",
        )
        if force_purchase_view:
            purchase_view = True

    if is_mixed_period:
        st.info("안내: 3월 11일 이전 및 이후 데이터가 혼재되어 있어, 상단 성과 지표와 추이 그래프는 '총 전환' 기준으로 표시됩니다.")
    elif purchase_view and can_use_purchase_toggle and _overview_type_allows_shopping(type_sel):
        render_inline_notice("구매완료 기준", "쇼핑검색 오버뷰는 캠페인 일별 수집값의 명시적 구매완료 split(purchase_conv/primary_conv)만 사용합니다. split이 없으면 구매완료를 0으로 두고, 총 전환수로 대체하지 않습니다.")
    elif is_legacy_only:
        st.info("안내: 3월 11일 이전 데이터 조회 시, 상단 성과 지표와 추이 그래프는 '총 전환' 기준으로 표시됩니다.")

    cur = cur_summary or {}
    base = base_summary or {}

    if purchase_view and can_use_purchase_toggle and _overview_type_allows_shopping(type_sel):
        _diag_add(diag, "요약 구매완료 기준", "ok", 1, "fact_campaign_daily", "쇼핑검색 구매완료 KPI는 검색어 상세가 아닌 캠페인 일별 split 기준")

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
    keyword_note = f"키워드 후보 {keyword_count:,}건" if keyword_count else "키워드는 상세/보고서에서 지연 조회"
    roas_now = float((perf_items[0].get("cur") or 0))
    roas_base = float((perf_items[0].get("base") or 0))
    cost_now = float(cur.get("cost", 0) or 0)
    cost_base = float(base.get("cost", 0) or 0)
    cost_diff_txt = "비교비 없음"
    if cost_base > 0:
        cost_diff_txt = f"{((cost_now - cost_base) / cost_base) * 100:+.1f}%"
    click_now = float(cur.get("clk", 0) or 0)
    click_base = float(base.get("clk", 0) or 0)
    click_diff_txt = _format_report_pct_delta(click_now, click_base) if base else "비교비 없음"
    conv_key = "conv" if kpi_mode == "shopping_purchase" else "tot_conv"
    conv_now = float(cur.get(conv_key, 0) or 0)
    conv_base = float(base.get(conv_key, 0) or 0)
    conv_diff_txt = _format_report_pct_delta(conv_now, conv_base) if base else "비교비 없음"
    roas_gap_txt = "비교비 없음"
    if roas_base > 0:
        roas_gap_txt = f"{roas_now - roas_base:+.1f}p"
    render_ops_cards([
        {"title": "분석 대상", "value": f"{campaign_count:,}개 캠페인", "note": keyword_note, "tone": "info", "icon": "01"},
        {"title": "클릭 변화", "value": click_diff_txt, "note": f"현재 {click_now:,.0f}회 / 비교 {click_base:,.0f}회", "tone": "success" if click_now >= click_base else "warning", "icon": "CLK"},
        {"title": "비용 변화", "value": cost_diff_txt, "note": f"비교 기간 {b1} ~ {b2}", "tone": "warning" if cost_now > cost_base and cost_base > 0 else "success", "icon": "↕"},
        {"title": "전환 변화", "value": conv_diff_txt, "note": f"현재 {conv_now:,.0f}건 / 비교 {conv_base:,.0f}건", "tone": "success" if conv_now >= conv_base else "danger", "icon": "CV"},
        {"title": "ROAS 변화", "value": roas_gap_txt, "note": "성과 지표 기준 전기 대비", "tone": "success" if roas_now >= roas_base else "danger", "icon": "%"},
    ])

    type_snapshot_camp = cur_camp
    type_snapshot_base = pd.DataFrame()
    if type_sel:
        try:
            type_snapshot_camp = _cached_campaign_bundle(engine, f["start"], f["end"], cids, tuple())
            _diag_add(diag, "유형별 스냅샷", "ok" if type_snapshot_camp is not None and not type_snapshot_camp.empty else "zero_data", 0 if type_snapshot_camp is None else len(type_snapshot_camp.index), "query_campaign_bundle", "현재 유형 필터와 별개로 전체 유형 조회")
        except Exception as e:
            type_snapshot_camp = pd.DataFrame()
            _diag_add(diag, "유형별 스냅샷", "error", 0, "query_campaign_bundle", f"{type(e).__name__}: {e}")
        try:
            type_snapshot_base = _cached_campaign_bundle(engine, b1, b2, cids, tuple())
        except Exception:
            type_snapshot_base = pd.DataFrame()
    else:
        try:
            type_snapshot_base = _cached_campaign_bundle(engine, b1, b2, cids, type_sel)
        except Exception:
            type_snapshot_base = pd.DataFrame()
    type_perf_summary = _build_type_performance_summary(type_snapshot_camp, type_snapshot_base)

    if not type_perf_summary.empty:
        with st.container(border=True):
            scope_label = "전체 유형 기준" if type_sel else "현재 조회 기준"
            render_toolbar(
                "유형별 성과 한눈에 보기",
                "선택한 계정과 기간에서 캠페인 유형별 핵심 성과를 함께 비교합니다.",
                [{"label": scope_label, "tone": "primary", "icon": "유형 "}, {"label": f"{len(type_perf_summary.index)}개 유형", "tone": "info", "icon": "행 "}],
            )
            _render_type_performance_snapshot(type_perf_summary)

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
                render_echarts_dual_axis("노출 및 클릭 추이", daily_ts_chart, "dt", "imp", "노출수", "clk", "클릭수", height=320, show_weekday=True)
            else:
                if combined_toggle:
                    render_echarts_dual_axis("비용 및 총 전환 매출 추이", daily_ts_chart, "dt", "cost", "광고비", "tot_sales", "매출", height=320, show_weekday=True)
                else:
                    render_echarts_dual_axis("비용 및 구매 완료 매출 추이", daily_ts_chart, "dt", "cost", "광고비", "sales", "매출", height=320, show_weekday=True)
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
                    disp_target_view = disp_target[disp_cols].copy()
                    st.dataframe(
                        disp_target_view,
                        width="stretch", hide_index=True,
                        column_config=numeric_column_config(disp_target_view, base={
                            "달성 상태": st.column_config.TextColumn("상태", width="small"),
                            "달성률(%)": st.column_config.ProgressColumn("달성률", format="%,.1f%%", min_value=0, max_value=100),
                            "구매완료수": st.column_config.NumberColumn("구매완료수", format="%,.0f"),
                            "구매완료 ROAS(%)": st.column_config.NumberColumn("구매완료 ROAS(%)", format="%,.1f%%"),
                            "최소 ROAS(%)": st.column_config.NumberColumn("최소 ROAS(%)", format="%,.0f%%"),
                            "목표 ROAS(%)": st.column_config.NumberColumn("목표 ROAS(%)", format="%,.0f%%"),
                            "광고비": st.column_config.NumberColumn("광고비", format="%,.0f 원")
                        })
                    )
                else: st.info("조건에 맞는 캠페인이 없습니다.")
            else: st.info("안내: 최소/목표 ROAS가 설정된 캠페인이 없습니다.")
        else: st.info("안내: 최소/목표 ROAS가 설정된 캠페인이 없습니다.")

    # ----------------------------------------------------
    # 상세 데이터 전처리 (지연 조회 전 기본값만 준비)
    # ----------------------------------------------------
    df_display, df_type_display, camp_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    daily_disp, dow_disp, weekly_disp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    group_disp = pd.DataFrame()
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
        "지출 비중(%)": "{:,.1f}%",
        "순위 변화": lambda x: f"{x:+.0f}" if pd.notna(x) else "-"
    }

    # ====================================================
    # 퍼널 뷰 배치 및 증감/절대값 토글 로직
    # ====================================================
    st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)
    
    render_toolbar(
        "세부 성과 표",
        "업체, 유형, 기간, 캠페인, 광고그룹, 키워드 단위로 같은 KPI 묶음을 비교합니다.",
        [{"label": "절대값/증감 전환", "tone": "primary", "icon": "표시 "}, {"label": "표시 행 전체 정렬", "tone": "info", "icon": "정렬 "}],
    )
    show_deltas = st.toggle("증감율 보기", value=True, key="ov_abs_toggle_v2")

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
        ["업체별 요약", "유형별 요약", "기간별 상세", "캠페인 상세 분석", "그룹 상세 분석", "키워드 상세 분석"],
        default="업체별 요약",
        key="overview_detail_panel",
        label_visibility="collapsed",
    )

    # 탭별 화면 렌더링에 필요한 데이터만 지연 로드 (UI 응답성 최적화)
    if detail_panel in {"업체별 요약", "유형별 요약", "캠페인 상세 분석"}:
        if base_camp is None or base_camp.empty:
            try: base_camp = _cached_campaign_bundle(engine, b1, b2, cids, type_sel)
            except Exception: base_camp = pd.DataFrame()
        df_display, df_type_display, camp_disp = _build_overview_campaign_frames(cur_camp, base_camp, meta, engine)

    if detail_panel == "키워드 상세 분석":
        # 키워드 상세는 광고비 상위 N개 제한을 쓰지 않습니다.
        # 제한된 번들을 쓰면 전환수/매출/CPC 등으로 정렬해도 이미 제외된 행은 복구되지 않습니다.
        try:
            cur_kw = _cached_keyword_full_bundle(engine, f["start"], f["end"], cids, type_sel)
            _diag_add(diag, "키워드 번들(현재/전체)", "ok" if cur_kw is not None and not cur_kw.empty else "zero_data", 0 if cur_kw is None else len(cur_kw.index), "query_keyword_bundle", "키워드 상세 전체 행")
        except Exception as e:
            cur_kw = pd.DataFrame()
            _diag_add(diag, "키워드 번들(현재/전체)", "error", 0, "query_keyword_bundle", f"{type(e).__name__}: {e}")
        try:
            base_kw = _cached_keyword_full_bundle(engine, b1, b2, cids, type_sel)
            _diag_add(diag, "키워드 번들(비교/전체)", "ok" if base_kw is not None and not base_kw.empty else "zero_data", 0 if base_kw is None else len(base_kw.index), "query_keyword_bundle", "키워드 상세 비교 전체 행")
        except Exception as e:
            base_kw = pd.DataFrame()
            _diag_add(diag, "키워드 번들(비교/전체)", "error", 0, "query_keyword_bundle", f"{type(e).__name__}: {e}")
        kw_disp = _build_overview_keyword_frames(cur_kw, base_kw, cur_camp, base_camp)

    if detail_panel == "그룹 상세 분석":
        try:
            cur_group_kw = _cached_keyword_full_bundle(engine, f["start"], f["end"], cids, type_sel)
            cur_group_ad = _cached_ad_full_bundle(engine, f["start"], f["end"], cids, type_sel)
            cur_group_detail = _overview_build_detail_source(cur_group_kw, cur_group_ad)
            _diag_add(diag, "그룹 번들(현재/전체)", "ok" if not cur_group_detail.empty else "zero_data", len(cur_group_detail.index), "keyword/ad bundle", "광고그룹 상세 전체 행")
        except Exception as e:
            cur_group_detail = pd.DataFrame()
            _diag_add(diag, "그룹 번들(현재/전체)", "error", 0, "keyword/ad bundle", f"{type(e).__name__}: {e}")
        try:
            base_group_kw = _cached_keyword_full_bundle(engine, b1, b2, cids, type_sel)
            base_group_ad = _cached_ad_full_bundle(engine, b1, b2, cids, type_sel)
            base_group_detail = _overview_build_detail_source(base_group_kw, base_group_ad)
            _diag_add(diag, "그룹 번들(비교/전체)", "ok" if not base_group_detail.empty else "zero_data", len(base_group_detail.index), "keyword/ad bundle", "광고그룹 상세 비교 전체 행")
        except Exception as e:
            base_group_detail = pd.DataFrame()
            _diag_add(diag, "그룹 번들(비교/전체)", "error", 0, "keyword/ad bundle", f"{type(e).__name__}: {e}")
        group_disp = _build_overview_group_frames(cur_group_detail, base_group_detail, meta, engine)

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

    elif detail_panel == "유형별 요약":
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

    elif detail_panel == "그룹 상세 분석":
        if not group_disp.empty:
            _render_overview_group_signal_cards(group_disp, cmp_mode, b1, b2)
            group_tool_a, group_tool_b = st.columns([1.45, 1], gap="small")
            with group_tool_a:
                group_preset = st.segmented_control(
                    "그룹 업무 보기",
                    ["전체", "비용 누수", "효율 악화", "노출 약화", "확장 후보", "데이터 부족", "정상"],
                    default="전체",
                    key="overview_group_preset",
                )
            with group_tool_b:
                group_col_order = st.segmented_control(
                    "컬럼 순서",
                    ["기본", "운영", "성과", "효율", "직접"],
                    default="기본",
                    key="overview_group_col_order",
                )
            group_work = _sort_overview_group_workbench(_filter_overview_group_workbench(group_disp, group_preset), group_preset)
            if group_work.empty:
                st.info("선택한 그룹 업무 보기 조건에 맞는 광고그룹이 없습니다.")
            else:
                view_cols = _overview_group_visible_cols(group_work, show_deltas, get_funnel_cols(show_deltas))
                view_cols = _overview_group_preset_order(view_cols, group_col_order)
                if group_col_order == "직접":
                    view_cols = _overview_group_custom_order(view_cols, show_deltas)
                disp_group = group_work[view_cols].head(500).copy()
                styled_group_df = disp_group.style.format(fmt_dict_standard)
                styled_group_df = _apply_overview_delta_styles(styled_group_df, disp_group)
                _render_overview_sticky_table(styled_group_df, "광고그룹", height=500, hide_index=True)
                st.caption(f"총 {len(group_disp):,}개 광고그룹 중 {len(disp_group):,}개를 표시했습니다.")
        else:
            st.info("조건에 맞는 데이터가 없습니다.")

    elif detail_panel == "키워드 상세 분석":
        if not kw_disp.empty:
            kw_tool_a, kw_tool_b = st.columns([2, 1], gap="small")
            with kw_tool_a:
                keyword_preset = st.segmented_control(
                    "키워드 업무 보기",
                    ["전체", "구매 발생", "비용 발생·구매 0", "클릭 많고 구매 0", "ROAS 우수", "총전환 있음"],
                    default="전체",
                    key="overview_keyword_preset",
                )
            with kw_tool_b:
                keyword_col_mode = st.segmented_control(
                    "컬럼 보기",
                    ["핵심", "전환 상세", "전체 지표"],
                    default="핵심",
                    key="overview_keyword_col_mode",
                )
            kw_work = _filter_overview_keyword_workbench(kw_disp, keyword_preset)
            if kw_work.empty:
                st.info("선택한 키워드 업무 보기 조건에 맞는 데이터가 없습니다.")
            else:
                kw_work = _add_overview_keyword_status(kw_work)
                _render_overview_keyword_workbench_cards(kw_work)
                funnel_cols = get_funnel_cols(show_deltas)
                view_cols = _overview_keyword_visible_cols(kw_work, show_deltas, keyword_col_mode, funnel_cols)
                disp_kw = _render_overview_keyword_sort_controls(kw_work[view_cols].copy(), view_cols)
                styled_kw_df = disp_kw.style.format(fmt_dict_standard)
                styled_kw_df = _apply_overview_delta_styles(styled_kw_df, disp_kw)
                _render_overview_sticky_table(styled_kw_df, "키워드", height=460, hide_index=True)
                st.caption(f"총 {len(kw_disp):,}개 키워드 중 {len(disp_kw):,}개를 표시했습니다.")
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
        df_display, df_type_display, camp_disp = _build_overview_campaign_frames(cur_camp, base_camp, meta, engine)

    if group_disp.empty:
        try:
            cur_group_detail_export = _overview_build_detail_source(
                _cached_keyword_full_bundle(engine, f["start"], f["end"], cids, type_sel),
                _cached_ad_full_bundle(engine, f["start"], f["end"], cids, type_sel),
            )
            base_group_detail_export = _overview_build_detail_source(
                _cached_keyword_full_bundle(engine, b1, b2, cids, type_sel),
                _cached_ad_full_bundle(engine, b1, b2, cids, type_sel),
            )
            group_disp = _build_overview_group_frames(cur_group_detail_export, base_group_detail_export, meta, engine)
        except Exception:
            group_disp = pd.DataFrame()

    if base_kw is None or base_kw.empty:
        try: base_kw = _cached_keyword_full_bundle(engine, b1, b2, cids, type_sel)
        except Exception: base_kw = pd.DataFrame()
    try:
        cur_kw_export = _cached_keyword_full_bundle(engine, f["start"], f["end"], cids, type_sel)
    except Exception:
        cur_kw_export = cur_kw
    if kw_disp.empty or len(kw_disp.index) <= 300:
        kw_disp = _build_overview_keyword_frames(cur_kw_export, base_kw)

    if base_daily_ts is None or base_daily_ts.empty:
        try: base_daily_ts = _cached_campaign_timeseries(engine, b1, b2, cids, type_sel)
        except Exception: base_daily_ts = pd.DataFrame()
    if daily_disp.empty:
        daily_disp, dow_disp, weekly_disp = _build_overview_timeseries_frames(daily_ts, base_daily_ts)

    has_data_to_export = any([not df_display.empty, not df_type_display.empty, not camp_disp.empty, not group_disp.empty, not daily_disp.empty, not kw_disp.empty])
    if has_data_to_export:
        with st.container(border=True):
            st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:8px;'>엑셀 데이터 일괄 다운로드</div>", unsafe_allow_html=True)
            st.markdown("<div style='font-size:12px; color:var(--nv-muted); margin-bottom:10px;'>계정/유형/캠페인/그룹/키워드/일자/요일별 상세 데이터를 하나의 엑셀 파일로 내려받습니다.</div>", unsafe_allow_html=True)
            excel_bytes = _build_overview_excel_bytes(df_display, df_type_display, camp_disp, group_disp, kw_disp, daily_disp, dow_disp, weekly_disp)
            st.download_button("통합 엑셀 다운로드", data=excel_bytes, file_name=f"통합_상세_성과보고서_{f['start']}_{f['end']}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width="stretch")

    with st.expander("텍스트 보고서 생성", expanded=False):
        include_type_breakdown = st.checkbox(
            "유형별 성과도 함께 생성",
            key="overview_include_type_text_report",
            value=True,
            help="현재 광고 유형 필터를 바꾸지 않아도 파워링크/쇼핑검색 등 유형별 요약을 한 번에 추가합니다.",
        )
        include_campaign_breakdown = st.checkbox(
            "캠페인별 성과도 함께 생성",
            key="overview_include_campaign_text_report",
            value=False,
            help="기존 요약 아래에 캠페인별 성과 요약을 동일한 형식으로 추가합니다.",
        )
        report_metric_options = ["보고서 전환"]
        if can_use_purchase_toggle:
            report_metric_options.append("구매완료 데이터")
        report_metric_default = "구매완료 데이터" if "구매완료 데이터" in report_metric_options and kpi_mode == "shopping_purchase" else "보고서 전환"
        if st.session_state.get("overview_text_report_metric_mode") not in report_metric_options:
            st.session_state["overview_text_report_metric_mode"] = report_metric_default
        report_metric_mode = st.segmented_control(
            "보고서 데이터 기준",
            report_metric_options,
            default=report_metric_default,
            key="overview_text_report_metric_mode",
        )
        report_uses_purchase = report_metric_mode == "구매완료 데이터"
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
        shop_terms_df = pd.DataFrame()
        campaign_top_shopping_query_map: dict[str, str] = {}
        if cids:
            try:
                shop_kw_str = _cached_shopping_top_terms_text(engine, f["start"], f["end"], tuple(cids), report_uses_purchase, 3)
                if shop_kw_str == "없음" and generate_text_report:
                    shop_terms_df = query_shopping_search_terms(engine, f["start"], f["end"], tuple(cids))
                    if shop_terms_df is not None and not shop_terms_df.empty:
                        metric_col = _pick_shopping_query_metric_col(shop_terms_df, prefer_purchase=report_uses_purchase)
                        if metric_col is not None:
                            top_shop_terms = (
                                shop_terms_df.groupby("query_text", dropna=False)[metric_col]
                                .sum()
                                .reset_index()
                            )
                            top_shop_terms = top_shop_terms[top_shop_terms[metric_col] > 0].sort_values(metric_col, ascending=False).head(3)
                            if not top_shop_terms.empty:
                                shop_kw_str = ", ".join([f"{r['query_text']}({int(r[metric_col]):,}회)" for _, r in top_shop_terms.iterrows()])
                if include_campaign_breakdown:
                    if shop_terms_df.empty:
                        shop_terms_df = query_shopping_search_terms(engine, f["start"], f["end"], tuple(cids))
                    campaign_top_shopping_query_map = _get_campaign_top_shopping_query_map(shop_terms_df, top_n=3, prefer_purchase=report_uses_purchase)
            except Exception:
                pass

        type_top_keyword_map: dict[str, str] = {}
        if include_type_breakdown:
            try:
                if not generate_text_report:
                    type_top_keyword_map = _cached_type_top_source_map(engine, "powerlink_keyword", f["start"], f["end"], tuple(cids), tuple(), 5)
                if not type_top_keyword_map:
                    type_kw_key = f"overview_text_kw_all_types::{f['start']}::{f['end']}::{','.join(map(str, cids))}"
                    type_kw_bundle = _load_report_keyword_bundle(engine, f["start"], f["end"], tuple(cids), tuple(), type_kw_key, force_refresh=generate_text_report)
                    type_top_keyword_map = _get_type_top_keyword_map(type_kw_bundle, top_n=5)
            except Exception:
                type_top_keyword_map = {}

        campaign_top_keyword_map = {}
        if include_campaign_breakdown:
            if cur_kw is None or cur_kw.empty:
                try:
                    cur_kw = _cached_keyword_bundle(engine, f["start"], f["end"], cids, type_sel)
                except Exception:
                    cur_kw = pd.DataFrame()
            campaign_top_keyword_map = _get_campaign_top_keyword_map(cur_kw, top_n=5)
        is_shopping_only = ("쇼핑" in selected_type_label and "파워링크" not in selected_type_label and selected_type_label != "전체 유형")

        if report_uses_purchase:
            report_text = "\n".join([
                f"[ {selected_type_label} 성과 요약 ]",
                _format_report_line("노출수", f"{int(float(cur.get('imp', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(cur.get('clk', 0))):,}"),
                _format_report_line("클릭률", f"{float(cur.get('ctr', 0)):.1f}%"),
                _format_report_line("광고 소진비용", f"{int(float(cur.get('cost', 0))):,}원"),
                _format_report_line("구매완료수", _format_report_count(cur.get('conv', 0.0))),
                _format_report_line("구매완료 매출", f"{int(float(cur.get('sales', 0))):,}원"),
                _format_report_line("구매 ROAS", f"{float(cur.get('roas', 0)):.1f}%"),
                _format_report_line("주요 전환 키워드", shop_kw_str if is_shopping_only else top_kw_str)
            ])
        else:
            c_conv_val = cur.get('tot_conv', 0)
            c_sales_val = cur.get('tot_sales', 0)
            c_roas_val = cur.get('tot_roas', 0)

            report_text = "\n".join([
                f"[ {selected_type_label} 성과 요약 ]",
                _format_report_line("노출수", f"{int(float(cur.get('imp', 0))):,}"),
                _format_report_line("클릭수", f"{int(float(cur.get('clk', 0))):,}"),
                _format_report_line("클릭률", f"{float(cur.get('ctr', 0)):.1f}%"),
                _format_report_line("광고 소진비용", f"{int(float(cur.get('cost', 0))):,}원"),
                _format_report_line("전환수", _format_report_count(c_conv_val)),
                _format_report_line("총전환매출", f"{int(float(c_sales_val)):,}원"),
                _format_report_line("ROAS", f"{float(c_roas_val):.1f}%"),
                _format_report_line("주요 유입 키워드", shop_kw_str if is_shopping_only else top_kw_str)
            ])
            
        delta_lines = _build_report_delta_lines(cur, base, report_uses_purchase)
        if delta_lines:
            report_text = f"{report_text}\n\n[ 전기 대비 증감 ]\n" + "\n".join(delta_lines)

        if include_type_breakdown:
            type_report_text = _build_type_report_text(
                type_perf_summary,
                report_uses_purchase=report_uses_purchase,
                type_top_keyword_map=type_top_keyword_map,
                shopping_keyword_text=shop_kw_str,
            )
            if type_report_text:
                report_text = f"{report_text}\n\n{type_report_text}"

        if include_campaign_breakdown:
            campaign_report_text = _build_campaign_report_text(
                cur_camp=cur_camp,
                selected_type_label=selected_type_label,
                is_shopping_only=is_shopping_only,
                combined_toggle=combined_toggle,
                kpi_mode=kpi_mode,
                report_uses_purchase=report_uses_purchase,
                campaign_top_keyword_map=campaign_top_keyword_map,
                campaign_top_shopping_query_map=campaign_top_shopping_query_map,
            )
            if campaign_report_text:
                report_text = f"{report_text}\n\n{campaign_report_text}"

        st.code(report_text, language="text")

    _render_diag_panel(diag, enabled=bool(f.get("show_diagnostics", False)))
