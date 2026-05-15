# -*- coding: utf-8 -*-
"""view_keyword.py - Keyword & Adgroup performance page view (Comma Formatting Fixed)."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import streamlit_compat  # noqa: F401
from typing import Dict
from datetime import date

from data import query_keyword_bundle, query_ad_bundle, query_shopping_search_terms, format_currency
from page_helpers import get_dynamic_cmp_options, period_compare_range, _perf_common_merge_meta, render_item_comparison_search
from ui import render_kpi_strip, render_toolbar, safe_numeric_col

FMT_DICT = {
    "노출": "{:,.0f}", "노출 증감": "{:+.1f}%", "노출 차이": "{:+,.0f}",
    "클릭": "{:,.0f}", "클릭 증감": "{:+.1f}%", "클릭 차이": "{:+,.0f}",
    "CTR(%)": "{:,.2f}%", 
    "광고비": "{:,.0f}원", "광고비 증감": "{:+.1f}%", "광고비 차이": "{:+,.0f}원",
    "CPC(원)": "{:,.0f}원", "CPC 증감": "{:+.1f}%", "CPC 차이": "{:+,.0f}원",
    "전환": "{:,.0f}", "전환 증감": "{:+.1f}%", "전환 차이": "{:+,.0f}",
    "CPA(원)": "{:,.0f}원",
    "전환매출": "{:,.0f}원", "전환매출 증감": "{:+.1f}%", "전환매출 차이": "{:+,.0f}원",
    "ROAS(%)": "{:,.1f}%", "ROAS 증감": "{:+.1f}%",
    "순위 변화": lambda x: f"{x:+.0f}" if pd.notna(x) else "-"
}


def _keyword_fetch_limit(top_n: int, daily_breakdown: bool = False) -> int:
    # Keyword tables must receive every matching row; Streamlit column sorting
    # only works within rows already sent to the browser.
    return -1

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
    pos_cols = [c for c in ['노출 증감', '노출 차이', '클릭 증감', '클릭 차이', '전환 증감', '전환 차이', '전환매출 증감', '전환매출 차이', 'ROAS 증감'] if c in df.columns]
    neg_cols = [c for c in ['광고비 증감', '광고비 차이', 'CPC 증감', 'CPC 차이', '순위 변화'] if c in df.columns]
    try:
        if pos_cols: styler = styler.map(_style_delta_numeric, subset=pos_cols)
        if neg_cols: styler = styler.map(_style_delta_numeric_neg, subset=neg_cols)
    except AttributeError:
        if pos_cols: styler = styler.applymap(_style_delta_numeric, subset=pos_cols)
        if neg_cols: styler = styler.applymap(_style_delta_numeric_neg, subset=neg_cols)
    return styler


def _build_table_styler(df: pd.DataFrame):
    fmt_map = {c: FMT_DICT[c] for c in df.columns if c in FMT_DICT}
    styler = df.style.format(fmt_map, na_rep='-')
    styler = _apply_delta_styles(styler, df)
    return styler

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
    tmp[imp_col] = pd.to_numeric(tmp[imp_col], errors="coerce").fillna(0.0) if imp_col in tmp.columns else 0.0
    tmp[rank_col] = pd.to_numeric(tmp[rank_col], errors="coerce")
    tmp["_rank_imp"] = tmp[rank_col].fillna(0.0) * tmp[imp_col]
    grp = tmp.groupby(usable_keys, as_index=False, dropna=False)[["_rank_imp", imp_col]].sum()
    grp[rank_col] = np.where(grp[imp_col] > 0, grp["_rank_imp"] / grp[imp_col], np.nan)
    return grp[usable_keys + [rank_col]]


def _filter_shopping_general_ads(df: pd.DataFrame, allow_unknown_type: bool = False) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame() if df is None else df
    work = df.copy()
    campaign_type = work.get("캠페인유형", pd.Series("", index=work.index)).astype(str).str.strip()
    def _is_general_ad(row):
        ctype = str(row.get("캠페인유형", "")).strip()
        if "쇼핑" in ctype or "SHOPPING" in ctype:
            ad_name = str(row.get("키워드", "")).strip()
            if ad_name.startswith("http") or ad_name.endswith(".jpg") or ad_name.endswith(".png"): return False
            if len(ad_name) > 30 and ("할인" in ad_name or "혜택" in ad_name or "리뷰" in ad_name): return False
            return True
        return allow_unknown_type
    if "키워드" in work.columns:
        mask = work.apply(_is_general_ad, axis=1)
        return work[mask].copy()
    return work

def _is_shopping_campaign_type(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype=bool)
    return series.astype(str).str.contains(r"쇼핑|SHOPPING", case=False, na=False)


def _prefer_keyword_source_by_campaign(view_kw: pd.DataFrame, view_ad: pd.DataFrame) -> pd.DataFrame:
    view_kw = view_kw.copy() if view_kw is not None and not view_kw.empty else pd.DataFrame()
    view_ad = view_ad.copy() if view_ad is not None and not view_ad.empty else pd.DataFrame()

    if view_kw.empty and view_ad.empty:
        return pd.DataFrame()
    if view_kw.empty:
        return view_ad.reset_index(drop=True)
    if view_ad.empty:
        return view_kw.reset_index(drop=True)

    for df in (view_kw, view_ad):
        if "캠페인" in df.columns:
            df["캠페인"] = df["캠페인"].astype(str)

    pref = {}
    campaign_ids = set(view_kw.get("캠페인", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
    campaign_ids.update(view_ad.get("캠페인", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())

    for camp in campaign_ids:
        kw_rows = view_kw[view_kw["캠페인"] == camp] if "캠페인" in view_kw.columns else pd.DataFrame()
        ad_rows = view_ad[view_ad["캠페인"] == camp] if "캠페인" in view_ad.columns else pd.DataFrame()
        kw_shop = (not kw_rows.empty and "캠페인유형" in kw_rows.columns and _is_shopping_campaign_type(kw_rows["캠페인유형"]).any())
        ad_shop = (not ad_rows.empty and "캠페인유형" in ad_rows.columns and _is_shopping_campaign_type(ad_rows["캠페인유형"]).any())

        if kw_shop or ad_shop:
            pref[camp] = "ad"
        elif not kw_rows.empty:
            pref[camp] = "kw"
        elif not ad_rows.empty:
            pref[camp] = "ad"

    kept = []
    kw_keep = view_kw[view_kw["캠페인"].map(lambda x: pref.get(str(x), "kw") == "kw")].copy() if "캠페인" in view_kw.columns else pd.DataFrame()
    ad_keep = view_ad[view_ad["캠페인"].map(lambda x: pref.get(str(x), "ad") == "ad")].copy() if "캠페인" in view_ad.columns else pd.DataFrame()
    if not kw_keep.empty:
        kept.append(kw_keep)
    if not ad_keep.empty:
        kept.append(ad_keep)

    if kept:
        return pd.concat(kept, ignore_index=True)
    return view_kw.reset_index(drop=True) if not view_kw.empty else view_ad.reset_index(drop=True)


def _add_perf_metrics(view: pd.DataFrame) -> pd.DataFrame:
    for c in ["광고비", "전환매출", "노출", "클릭", "전환"]:
        if c in view.columns: view[c] = pd.to_numeric(view[c], errors="coerce").fillna(0)
    view["CTR(%)"] = np.where(view["노출"] > 0, (view["클릭"] / view["노출"]) * 100, 0.0)
    view["CPC(원)"] = np.where(view["클릭"] > 0, view["광고비"] / view["클릭"], 0.0)
    view["CPA(원)"] = np.where(view["전환"] > 0, view["광고비"] / view["전환"], 0.0)
    view["ROAS(%)"] = np.where(view["광고비"] > 0, (view["전환매출"] / view["광고비"]) * 100, 0.0)
    return view


def _prefer_total_conversion_for_keyword(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    out = df.copy()
    if "tot_conv" in out.columns:
        total_conv = pd.to_numeric(out["tot_conv"], errors="coerce")
        base_conv = pd.to_numeric(out["conv"], errors="coerce") if "conv" in out.columns else pd.Series(0, index=out.index)
        out["conv"] = total_conv.where(total_conv.notna() & (total_conv > 0), base_conv).fillna(0)
    if "tot_sales" in out.columns:
        total_sales = pd.to_numeric(out["tot_sales"], errors="coerce")
        base_sales = pd.to_numeric(out["sales"], errors="coerce") if "sales" in out.columns else pd.Series(0, index=out.index)
        out["sales"] = total_sales.where(total_sales.notna() & (total_sales > 0), base_sales).fillna(0)
    return out


_KEYWORD_SUM_METRICS = ["노출", "클릭", "광고비", "전환", "전환매출"]
_KEYWORD_DERIVED_METRICS = ["CTR(%)", "CPC(원)", "CPA(원)", "ROAS(%)"]
_KEYWORD_PERIOD_GROUP_COLS = [
    "customer_id", "업체명", "담당자", "캠페인유형", "구분",
    "campaign_id", "캠페인", "adgroup_id", "광고그룹",
    "keyword_id", "ad_id", "키워드",
]


def _present_unique_cols(df: pd.DataFrame, cols: list[str]) -> list[str]:
    out: list[str] = []
    for col in cols:
        if col in df.columns and col not in out:
            out.append(col)
    return out


def _aggregate_keyword_rows(df: pd.DataFrame, group_cols: list[str], include_rank: bool = True) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    work = df.copy()
    group_cols = _present_unique_cols(work, group_cols)
    if not group_cols:
        group_cols = _present_unique_cols(work, ["customer_id", "업체명", "키워드"])
    if work.empty:
        return work

    for metric_col in _KEYWORD_SUM_METRICS:
        if metric_col not in work.columns:
            work[metric_col] = 0
        work[metric_col] = pd.to_numeric(work[metric_col], errors="coerce").fillna(0)

    agg_dict = {metric_col: "sum" for metric_col in _KEYWORD_SUM_METRICS}

    grouped = work.groupby(group_cols, as_index=False, dropna=False).agg(agg_dict)
    if include_rank and "avg_rank" in work.columns:
        rank_grp = _weighted_avg_rank_by_keys(work, group_cols, imp_col="노출")
        if not rank_grp.empty:
            grouped = grouped.merge(rank_grp, on=group_cols, how="left")
    grouped = _add_perf_metrics(grouped)
    if "avg_rank" in grouped.columns:
        grouped["평균순위"] = grouped["avg_rank"].apply(_format_avg_rank)
    return grouped


def _includes_shopping_type(type_sel: tuple) -> bool:
    if not type_sel:
        return True
    return any("쇼핑" in str(value) or "SHOPPING" in str(value).upper() for value in type_sel)


@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def _cached_keyword_shopping_terms(_engine, d1: date, d2: date, cids: tuple) -> pd.DataFrame:
    try:
        return query_shopping_search_terms(_engine, d1, d2, cids)
    except Exception:
        return pd.DataFrame()


def _build_shopping_terms_keyword_view(shop_terms: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if shop_terms is None or shop_terms.empty:
        return pd.DataFrame()
    work = _perf_common_merge_meta(shop_terms, meta) if meta is not None and not meta.empty else shop_terms.copy()
    view = work.rename(columns={
        "account_name": "업체명",
        "manager": "담당자",
        "campaign_name": "캠페인",
        "adgroup_name": "광고그룹",
        "query_text": "키워드",
    }).copy()
    view["캠페인유형"] = "쇼핑검색"
    view["구분"] = "쇼핑 검색어"
    for col in ["노출", "클릭", "광고비"]:
        view[col] = 0
    total_conv = safe_numeric_col(view, "total_conv")
    purchase_conv = safe_numeric_col(view, "purchase_conv")
    total_sales = safe_numeric_col(view, "total_sales")
    purchase_sales = safe_numeric_col(view, "purchase_sales")
    view["전환"] = total_conv.where(total_conv > 0, purchase_conv)
    view["전환매출"] = total_sales.where(total_sales > 0, purchase_sales)
    for col in ["업체명", "담당자", "캠페인", "광고그룹", "키워드"]:
        if col not in view.columns:
            view[col] = ""
    view = view[view["키워드"].astype(str).str.strip() != ""].copy()
    return _add_perf_metrics(view)


def _build_shopping_terms_base_bundle(shop_terms: pd.DataFrame) -> pd.DataFrame:
    if shop_terms is None or shop_terms.empty:
        return pd.DataFrame()
    out = shop_terms.rename(columns={"query_text": "키워드"}).copy()
    total_conv = safe_numeric_col(out, "total_conv")
    purchase_conv = safe_numeric_col(out, "purchase_conv")
    total_sales = safe_numeric_col(out, "total_sales")
    purchase_sales = safe_numeric_col(out, "purchase_sales")
    out["conv"] = total_conv.where(total_conv > 0, purchase_conv)
    out["sales"] = total_sales.where(total_sales > 0, purchase_sales)
    out["imp"] = 0
    out["clk"] = 0
    out["cost"] = 0
    out["구분"] = "쇼핑 검색어"
    return out


def _limit_display_rows_preserving_conversions(df: pd.DataFrame, sort_cols: list[str], top_n: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    work = df.copy()
    if sort_cols:
        work = work.sort_values(sort_cols, ascending=[False] * len(sort_cols))
    return work.reset_index(drop=True)


def _conversion_metric_cols(df: pd.DataFrame) -> tuple[str | None, str | None]:
    if df is None or df.empty:
        return None, None
    conv_col = "전환" if "전환" in df.columns else ("conv" if "conv" in df.columns else None)
    sales_col = "전환매출" if "전환매출" in df.columns else ("sales" if "sales" in df.columns else None)
    return conv_col, sales_col


def _shopping_residual_group_cols(base: pd.DataFrame, detail: pd.DataFrame) -> list[str]:
    candidates = [
        "customer_id",
        "업체명",
        "account_name",
        "캠페인",
        "campaign_name",
        "광고그룹",
        "adgroup_name",
    ]
    return [col for col in candidates if col in base.columns and col in detail.columns]


def _build_shopping_unmapped_conversion_rows(base: pd.DataFrame, detail: pd.DataFrame) -> pd.DataFrame:
    if base is None or base.empty or detail is None or detail.empty:
        return pd.DataFrame()
    type_col = "캠페인유형" if "캠페인유형" in base.columns else ("campaign_type_label" if "campaign_type_label" in base.columns else None)
    if not type_col:
        return pd.DataFrame()

    base_conv_col, base_sales_col = _conversion_metric_cols(base)
    detail_conv_col, detail_sales_col = _conversion_metric_cols(detail)
    if not base_conv_col and not base_sales_col:
        return pd.DataFrame()

    shopping_base = base.loc[_is_shopping_campaign_type(base[type_col])].copy()
    if shopping_base.empty:
        return pd.DataFrame()

    group_cols = _shopping_residual_group_cols(shopping_base, detail)
    temp_group = False
    if not group_cols:
        group_cols = ["__shopping_all__"]
        temp_group = True
        shopping_base[group_cols[0]] = "all"
        detail = detail.copy()
        detail[group_cols[0]] = "all"

    def _group_sum(frame: pd.DataFrame, col: str | None, name: str) -> pd.DataFrame:
        if not col or col not in frame.columns:
            out = frame[group_cols].drop_duplicates().copy()
            out[name] = 0.0
            return out
        work = frame[group_cols + [col]].copy()
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0)
        return work.groupby(group_cols, as_index=False, dropna=False)[col].sum().rename(columns={col: name})

    base_conv = _group_sum(shopping_base, base_conv_col, "__base_conv__")
    base_sales = _group_sum(shopping_base, base_sales_col, "__base_sales__")
    detail_conv = _group_sum(detail, detail_conv_col, "__detail_conv__")
    detail_sales = _group_sum(detail, detail_sales_col, "__detail_sales__")

    first_cols = _present_unique_cols(shopping_base, group_cols + ["customer_id", "업체명", "담당자", "캠페인유형", "캠페인", "광고그룹"])
    rows = shopping_base[first_cols].groupby(group_cols, as_index=False, dropna=False).first()
    rows = rows.merge(base_conv, on=group_cols, how="left")
    rows = rows.merge(base_sales, on=group_cols, how="left")
    rows = rows.merge(detail_conv, on=group_cols, how="left")
    rows = rows.merge(detail_sales, on=group_cols, how="left")

    for col in ["__base_conv__", "__base_sales__", "__detail_conv__", "__detail_sales__"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce").fillna(0)

    rows["전환"] = (rows["__base_conv__"] - rows["__detail_conv__"]).clip(lower=0)
    rows["전환매출"] = (rows["__base_sales__"] - rows["__detail_sales__"]).clip(lower=0)
    rows = rows[(rows["전환"] > 0) | (rows["전환매출"] > 0)].copy()
    if rows.empty:
        return pd.DataFrame()

    rows["키워드"] = "(검색어 미매핑 전환)"
    rows["구분"] = "미매핑 전환"
    if "캠페인유형" not in rows.columns:
        rows["캠페인유형"] = "쇼핑검색"
    for col in ["노출", "클릭", "광고비"]:
        rows[col] = 0
    rows = rows.drop(columns=[c for c in ["__base_conv__", "__base_sales__", "__detail_conv__", "__detail_sales__"] if c in rows.columns])
    if temp_group and "__shopping_all__" in rows.columns:
        rows = rows.drop(columns=["__shopping_all__"])
    return _add_perf_metrics(rows)


def _residualize_shopping_fact_conversions(base: pd.DataFrame, detail: pd.DataFrame) -> pd.DataFrame:
    """Keep fact cost/clicks, but only leave conversion residuals not covered by search terms."""
    if base is None or base.empty or detail is None or detail.empty:
        return pd.DataFrame() if base is None else base
    out = base.copy()
    type_col = "캠페인유형" if "캠페인유형" in out.columns else ("campaign_type_label" if "campaign_type_label" in out.columns else None)
    if not type_col:
        return out

    base_conv_col, base_sales_col = _conversion_metric_cols(out)
    detail_conv_col, detail_sales_col = _conversion_metric_cols(detail)
    if not base_conv_col and not base_sales_col:
        return out

    shopping_mask = _is_shopping_campaign_type(out[type_col])
    if not shopping_mask.any():
        return out

    group_cols = _shopping_residual_group_cols(out, detail)
    if not group_cols:
        group_cols = ["__shopping_all__"]
        out[group_cols[0]] = "all"
        detail = detail.copy()
        detail[group_cols[0]] = "all"

    def _apply_metric_residual(metric_col: str | None, detail_metric_col: str | None) -> None:
        if not metric_col:
            return
        out[metric_col] = pd.to_numeric(out[metric_col], errors="coerce").fillna(0)
        base_part = out.loc[shopping_mask, group_cols + [metric_col]].copy()
        if base_part.empty:
            return

        base_sum = (
            base_part.groupby(group_cols, dropna=False)[metric_col]
            .sum()
            .rename("__base_sum__")
            .reset_index()
        )
        if detail_metric_col and detail_metric_col in detail.columns:
            detail_work = detail[group_cols + [detail_metric_col]].copy()
            detail_work[detail_metric_col] = pd.to_numeric(detail_work[detail_metric_col], errors="coerce").fillna(0)
            detail_sum = (
                detail_work.groupby(group_cols, dropna=False)[detail_metric_col]
                .sum()
                .rename("__detail_sum__")
                .reset_index()
            )
        else:
            detail_sum = base_sum[group_cols].copy()
            detail_sum["__detail_sum__"] = 0.0

        ratios = base_sum.merge(detail_sum, on=group_cols, how="left")
        ratios["__detail_sum__"] = pd.to_numeric(ratios["__detail_sum__"], errors="coerce").fillna(0)
        ratios["__residual__"] = (ratios["__base_sum__"] - ratios["__detail_sum__"]).clip(lower=0)
        ratios["__ratio__"] = np.where(ratios["__base_sum__"] > 0, ratios["__residual__"] / ratios["__base_sum__"], 0.0)
        ratios = ratios[group_cols + ["__ratio__"]]

        indexed = out.loc[shopping_mask, group_cols + [metric_col]].merge(ratios, on=group_cols, how="left")
        out.loc[shopping_mask, metric_col] = (
            pd.to_numeric(indexed[metric_col], errors="coerce").fillna(0)
            * pd.to_numeric(indexed["__ratio__"], errors="coerce").fillna(1.0)
        ).values

    _apply_metric_residual(base_conv_col, detail_conv_col)
    _apply_metric_residual(base_sales_col, detail_sales_col)

    if group_cols == ["__shopping_all__"] and "__shopping_all__" in out.columns:
        out = out.drop(columns=["__shopping_all__"])
    return out


def _zero_shopping_fact_conversions(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    out = df.copy()
    type_col = "캠페인유형" if "캠페인유형" in out.columns else ("campaign_type_label" if "campaign_type_label" in out.columns else None)
    if not type_col:
        return out
    shopping_mask = _is_shopping_campaign_type(out[type_col])
    for col in ["전환", "전환매출", "conv", "sales", "tot_conv", "tot_sales"]:
        if col in out.columns:
            out.loc[shopping_mask, col] = 0
    return out


def _merge_keyword_view_with_shopping_terms(view: pd.DataFrame, shop_view: pd.DataFrame) -> pd.DataFrame:
    if shop_view is None or shop_view.empty:
        return view
    if view is None or view.empty:
        return shop_view
    base = view.copy()
    if "구분" not in base.columns:
        base["구분"] = "키워드/소재"
    residual_rows = _build_shopping_unmapped_conversion_rows(base, shop_view)
    base = _zero_shopping_fact_conversions(base)
    parts = [base, shop_view]
    if residual_rows is not None and not residual_rows.empty:
        parts.append(residual_rows)
    return pd.concat(parts, ignore_index=True, sort=False)


def _apply_comparison_metrics(view_df: pd.DataFrame, base_df: pd.DataFrame, merge_keys: list) -> pd.DataFrame:
    if view_df.empty: return view_df
    for k in merge_keys:
        if k in view_df.columns: view_df[k] = view_df[k].astype(str)
        if k in base_df.columns: base_df[k] = base_df[k].astype(str)
            
    agg_dict = {'imp': 'sum', 'clk': 'sum', 'cost': 'sum', 'conv': 'sum', 'sales': 'sum'}
        
    if not base_df.empty and merge_keys:
        base_agg = base_df.groupby(merge_keys, dropna=False).agg(agg_dict).reset_index()
        if 'avg_rank' in base_df.columns:
            rank_agg = _weighted_avg_rank_by_keys(base_df, merge_keys)
            if not rank_agg.empty:
                base_agg = base_agg.merge(rank_agg, on=merge_keys, how="left")
        base_agg = base_agg.rename(columns={'imp': 'b_imp', 'clk': 'b_clk', 'cost': 'b_cost', 'conv': 'b_conv', 'sales': 'b_sales', 'avg_rank': 'b_avg_rank'})
        merged = pd.merge(view_df, base_agg, on=merge_keys, how='left')
    else: merged = view_df.copy()
        
    for c in ['b_imp', 'b_clk', 'b_cost', 'b_conv', 'b_sales']:
        if c not in merged.columns: merged[c] = 0
        merged[c] = merged[c].fillna(0)
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
    c_conv, b_conv = merged.get('전환', 0), merged.get('b_conv', 0)
    c_sales, b_sales = merged.get('전환매출', 0), merged.get('b_sales', 0)

    merged['노출 증감'], merged['노출 차이'] = _vec_pct_diff(c_imp, b_imp)
    merged['클릭 증감'], merged['클릭 차이'] = _vec_pct_diff(c_clk, b_clk)
    merged['광고비 증감'], merged['광고비 차이'] = _vec_pct_diff(c_cost, b_cost)
    merged['CPC 증감'], merged['CPC 차이'] = _vec_pct_diff(c_cpc, b_cpc)
    merged['전환 증감'], merged['전환 차이'] = _vec_pct_diff(c_conv, b_conv)
    merged['전환매출 증감'], merged['전환매출 차이'] = _vec_pct_diff(c_sales, b_sales)

    c_roas = np.where(c_cost > 0, (c_sales / c_cost) * 100, 0)
    b_roas = np.where(b_cost > 0, (b_sales / b_cost) * 100, 0)
    merged['ROAS 증감'] = c_roas - b_roas

    if "avg_rank" in merged.columns:
        if "평균순위" not in merged.columns: merged['평균순위'] = merged['avg_rank'].apply(_format_avg_rank)
        merged['순위 변화'] = np.where((merged['b_avg_rank'] > 0) & (merged['avg_rank'] > 0), merged['avg_rank'] - merged['b_avg_rank'], np.nan)
        
    return merged


@st.cache_data(show_spinner=False, max_entries=20, ttl=300)
def compute_keyword_view(kw_bundle, ad_bundle, meta):
    if (kw_bundle is None or kw_bundle.empty) and (ad_bundle is None or ad_bundle.empty): return pd.DataFrame()
    df_kw = _perf_common_merge_meta(kw_bundle, meta) if not kw_bundle.empty else pd.DataFrame()
    df_ad = _perf_common_merge_meta(ad_bundle, meta) if not ad_bundle.empty else pd.DataFrame()
    df_kw = _prefer_total_conversion_for_keyword(df_kw)
    df_ad = _prefer_total_conversion_for_keyword(df_ad)
    view_kw, view_ad = pd.DataFrame(), pd.DataFrame()
    
    if not df_kw.empty:
        rename_dict = {"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "keyword": "키워드", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"}
        if "dt" in df_kw.columns: rename_dict["dt"] = "일자"
        view_kw = df_kw.rename(columns=rename_dict)
        view_kw["구분"] = "키워드"
        if "일자" in view_kw.columns: view_kw["일자"] = pd.to_datetime(view_kw["일자"]).dt.strftime('%Y-%m-%d')
        
    if not df_ad.empty:
        rename_dict = {"account_name": "업체명", "manager": "담당자", "campaign_type_label": "캠페인유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "ad_name": "키워드", "imp": "노출", "clk": "클릭", "cost": "광고비", "conv": "전환", "sales": "전환매출"}
        if "dt" in df_ad.columns: rename_dict["dt"] = "일자"
        view_ad = df_ad.rename(columns=rename_dict)
        view_ad = _filter_shopping_general_ads(view_ad, allow_unknown_type=True)
        if "캠페인유형" in view_ad.columns:
            view_ad["구분"] = np.where(_is_shopping_campaign_type(view_ad["캠페인유형"]), "쇼핑 소재", "소재")
        else:
            view_ad["구분"] = "소재"
        if "일자" in view_ad.columns: view_ad["일자"] = pd.to_datetime(view_ad["일자"]).dt.strftime('%Y-%m-%d')
        
    if view_kw.empty and view_ad.empty: return pd.DataFrame()
    view = _prefer_keyword_source_by_campaign(view_kw, view_ad)
    
    view = _add_perf_metrics(view)
    if "avg_rank" in view.columns: view["평균순위"] = view["avg_rank"].apply(_format_avg_rank)

    # 파워링크의 빈칸(캐시 찌꺼기) 키워드 핀셋 삭제
    if "키워드" in view.columns and "캠페인유형" in view.columns:
        is_powerlink = view["캠페인유형"] == "파워링크"
        is_empty_kw = view["키워드"].isna() | view["키워드"].astype(str).str.strip().isin(["", "nan", "None", "NaN"])
        view = view[~(is_powerlink & is_empty_kw)]

    return view


def _render_sticky_table(df, first_col: str, height: int = 550, col_config: dict = None):
    try:
        rows = len(df.index)
        if rows <= 0:
            calc_height = 100
        elif rows == 1:
            calc_height = 80
        else:
            calc_height = min(height, max(100, 40 + (rows * 36)))
    except Exception:
        calc_height = height
    cfg = col_config.copy() if col_config else {}
    cfg[first_col] = st.column_config.TextColumn(first_col, pinned=True, width="medium")
    st.dataframe(_build_table_styler(df), use_container_width=True, height=calc_height, hide_index=True, column_config=cfg)


def _keyword_fast_col_config(df: pd.DataFrame, first_col: str = "키워드") -> dict:
    cfg: dict = {}
    if first_col in df.columns:
        cfg[first_col] = st.column_config.TextColumn(first_col, pinned=True, width="medium")
    pct_cols = {"CTR(%)", "CVR(%)", "ROAS(%)"} | {c for c in df.columns if "증감" in c and "차이" not in c}
    currency_cols = {"CPC(원)", "CPA(원)", "광고비", "전환매출"} | {c for c in df.columns if c.endswith("차이") and ("광고비" in c or "매출" in c or "CPC" in c)}
    count_cols = {"노출", "클릭", "전환"} | {c for c in df.columns if c.endswith("차이") and c not in currency_cols}
    for c in df.columns:
        if c in cfg:
            continue
        if c in pct_cols:
            decimals = 2 if c in {"CTR(%)", "CVR(%)"} else 1
            cfg[c] = st.column_config.NumberColumn(c, format=f"%,.{decimals}f %%")
        elif c in currency_cols:
            cfg[c] = st.column_config.NumberColumn(c, format="%,.0f 원")
        elif c in count_cols or c == "순위 변화":
            cfg[c] = st.column_config.NumberColumn(c, format="%,.0f")
        elif c == "평균순위":
            cfg[c] = st.column_config.TextColumn(c)
    return cfg


@st.fragment
def render_keyword_main(view, top_n):
    if view.empty:
        st.info("해당 기간의 키워드/소재 성과 데이터가 없습니다.")
        return

    col1, col2 = st.columns(2)
    camps = ["전체"] + sorted([str(x) for x in view["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view.columns else ["전체"]
    sel_camp = col1.selectbox("캠페인 필터", camps, key="kw_camp_filter_main")
    
    filtered_for_grp = view.copy()
    if sel_camp != "전체": filtered_for_grp = filtered_for_grp[filtered_for_grp["캠페인"] == sel_camp]
    
    grps = ["전체"] + sorted([str(x) for x in filtered_for_grp["광고그룹"].dropna().unique() if str(x).strip()]) if "광고그룹" in filtered_for_grp.columns else ["전체"]
    sel_grp = col2.selectbox("광고그룹 필터", grps, key="kw_grp_filter_main")

    col3, col4 = st.columns([3, 1])
    search_kw = col3.text_input("키워드 검색", key="kw_search_main")
    exact_match_main = col3.checkbox("완전 일치", key="kw_exact_main")
    
    col4.markdown("<div style='margin-top: 32px;'></div>", unsafe_allow_html=True)
    agg_period = col4.checkbox("선택 기간 합산 (일자 통합)", key="kw_agg_period_main", help="조회된 여러 날짜의 데이터를 기간 전체 성과로 합산하여 보여줍니다.", value=False)
    agg_kw = col4.checkbox("동일 키워드 합산 (PC/MO 통합)", key="kw_agg_main", help="캠페인·광고그룹이 달라도 이름이 같은 키워드의 성과를 하나로 합산합니다.")
    
    disp = filtered_for_grp.copy()
    if sel_grp != "전체": disp = disp[disp["광고그룹"] == sel_grp]
    if search_kw:
        if exact_match_main:
            disp = disp[disp["키워드"].astype(str).str.lower() == search_kw.strip().lower()]
        else:
            disp = disp[disp["키워드"].astype(str).str.contains(search_kw, case=False, na=False)]

    base_cols = ["일자", "키워드", "구분", "캠페인", "광고그룹", "업체명", "담당자", "캠페인유형"]
    if "일자" not in disp.columns:
        base_cols.remove("일자")
    if "평균순위" in disp.columns: base_cols.append("평균순위")
    
    metrics_cols = ["노출", "클릭", "CTR(%)", "CPC(원)", "광고비", "전환", "CPA(원)", "전환매출", "ROAS(%)"]

    # 기간 합산(Agg) 처리
    if agg_period and "일자" in disp.columns:
        disp = _aggregate_keyword_rows(disp, _KEYWORD_PERIOD_GROUP_COLS, include_rank=True)
        if "일자" in base_cols: base_cols.remove("일자")

    # 키워드 합산(Agg) 처리
    if agg_kw:
        grp_cols = ["업체명", "키워드"]
        if "customer_id" in disp.columns: grp_cols.insert(0, "customer_id")
        if "일자" in disp.columns: grp_cols.insert(1, "일자")
        
        disp = _aggregate_keyword_rows(disp, grp_cols, include_rank=True)
        base_cols = ["일자", "키워드", "구분", "업체명"] if "일자" in disp.columns else ["키워드", "구분", "업체명"]
        if "평균순위" in disp.columns:
            base_cols.append("평균순위")

    final_cols = [c for c in base_cols + metrics_cols if c in disp.columns]
    sort_cols = [c for c in ["광고비", "전환매출", "전환"] if c in disp.columns]
    disp = _limit_display_rows_preserving_conversions(disp[final_cols], sort_cols, top_n)
    
    st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>키워드/소재 종합 성과 데이터</div>", unsafe_allow_html=True)
    
    _render_sticky_table(disp, "키워드", height=550, col_config=_keyword_fast_col_config(disp, "키워드"))


@st.fragment
def render_keyword_cmp(view_orig, engine, cids, type_sel, top_n, start_dt, end_dt):
    # 기간 비교 시, 일자가 포함되어 있으면 키워드/그룹 단위 비교가 흩어지므로 다시 묶어줍니다.
    if "일자" in view_orig.columns:
        view = _aggregate_keyword_rows(view_orig, _KEYWORD_PERIOD_GROUP_COLS, include_rank=True)
    else:
        view = view_orig.copy()

    st.markdown("<div style='display:flex; justify-content:flex-start; margin-bottom:8px;'>", unsafe_allow_html=True)
    show_deltas = st.toggle("증감률 보기", value=False, key="kw_abs_toggle")
    st.markdown("</div>", unsafe_allow_html=True)

    opts = get_dynamic_cmp_options(start_dt, end_dt)
    cmp_opts = [o for o in opts if o != "비교 안함"]
    cmp_mode = st.radio("비교 기준", cmp_opts if cmp_opts else ["이전 같은 기간 대비"], horizontal=True, key="kw_cmp_mode")

    if view.empty:
        st.info("현재 기간의 키워드/소재 데이터가 없습니다.")
        return

    col_camp_cmp, col_grp_cmp = st.columns(2)
    camps_cmp = ["전체"] + sorted([str(x) for x in view["캠페인"].dropna().unique() if str(x).strip()]) if "캠페인" in view.columns else ["전체"]
    sel_camp_cmp = col_camp_cmp.selectbox("캠페인 필터", camps_cmp, key="kw_camp_filter_cmp")

    filtered_cmp = view.copy()
    if sel_camp_cmp != "전체": filtered_cmp = filtered_cmp[filtered_cmp["캠페인"] == sel_camp_cmp]

    grps_cmp = ["전체"] + sorted([str(x) for x in filtered_cmp["광고그룹"].dropna().unique() if str(x).strip()]) if "광고그룹" in filtered_cmp.columns else ["전체"]
    sel_grp_cmp = col_grp_cmp.selectbox("광고그룹 필터", grps_cmp, key="kw_grp_filter_cmp")

    col_search_cmp, col_agg_cmp = st.columns([3, 1])
    search_kw_cmp = col_search_cmp.text_input("키워드 검색", key="kw_search_cmp")
    exact_match_cmp = col_search_cmp.checkbox("완전 일치", key="kw_exact_cmp")
    
    col_agg_cmp.markdown("<div style='margin-top: 32px;'></div>", unsafe_allow_html=True)
    agg_kw_cmp = col_agg_cmp.checkbox("동일 키워드 합산", key="kw_agg_cmp", help="PC·MO 등 그룹으로 나뉜 동일 키워드 성과를 하나로 합산합니다.")

    disp = filtered_cmp.copy()
    if sel_grp_cmp != "전체": disp = disp[disp["광고그룹"] == sel_grp_cmp]
    if search_kw_cmp:
        if exact_match_cmp:
            disp = disp[disp["키워드"].astype(str).str.lower() == search_kw_cmp.strip().lower()]
        else:
            disp = disp[disp["키워드"].astype(str).str.contains(search_kw_cmp, case=False, na=False)]

    b1, b2 = period_compare_range(start_dt, end_dt, cmp_mode)
    base_kw_bundle = query_keyword_bundle(engine, b1, b2, list(cids), type_sel, topn_cost=_keyword_fetch_limit(top_n, daily_breakdown=False))
    base_ad_bundle = query_ad_bundle(engine, b1, b2, cids, type_sel, topn_cost=_keyword_fetch_limit(top_n, daily_breakdown=False), top_k=50)
    base_kw_bundle = _prefer_total_conversion_for_keyword(base_kw_bundle)
    base_ad_bundle = _prefer_total_conversion_for_keyword(base_ad_bundle)
    base_shop_bundle = pd.DataFrame()
    if _includes_shopping_type(type_sel):
        base_shop_terms = _cached_keyword_shopping_terms(engine, b1, b2, tuple(cids))
        base_shop_bundle = _build_shopping_terms_base_bundle(base_shop_terms)
        base_kw_bundle = _residualize_shopping_fact_conversions(base_kw_bundle, base_shop_bundle)
        base_ad_bundle = _residualize_shopping_fact_conversions(base_ad_bundle, base_shop_bundle)

    base_kw = base_kw_bundle.rename(columns={"keyword": "키워드"}) if not base_kw_bundle.empty else pd.DataFrame()
    base_ad = base_ad_bundle.rename(columns={"ad_name": "키워드"}) if not base_ad_bundle.empty else pd.DataFrame()
    base_bundle = pd.concat([base_kw, base_ad, base_shop_bundle], ignore_index=True, sort=False)

    if agg_kw_cmp:
        # 1. 대상 기간 합산
        grp_cols = ["업체명", "키워드"]
        if "customer_id" in disp.columns: grp_cols.insert(0, "customer_id")
        disp = _aggregate_keyword_rows(disp, grp_cols, include_rank=True)

        # 2. 비교 기간(base) 합산
        if not base_bundle.empty:
            base_grp_cols = ["키워드"]
            if "customer_id" in base_bundle.columns: base_grp_cols.insert(0, "customer_id")
            base_agg_dict = {'imp': 'sum', 'clk': 'sum', 'cost': 'sum', 'conv': 'sum', 'sales': 'sum'}
            base_bundle = base_bundle.groupby(base_grp_cols, as_index=False, dropna=False).agg(base_agg_dict)
            base_rank_grp = _weighted_avg_rank_by_keys(pd.concat([base_kw, base_ad], ignore_index=True, sort=False), base_grp_cols)
            if not base_rank_grp.empty:
                base_bundle = base_bundle.merge(base_rank_grp, on=base_grp_cols, how="left")
        
        valid_keys = [k for k in ["customer_id", "키워드"] if k in disp.columns and k in base_bundle.columns]
        base_cols_cmp = ["키워드", "구분", "업체명"]
        if "평균순위" in disp.columns:
            base_cols_cmp.append("평균순위")
    else:
        valid_keys = [k for k in ["customer_id", "adgroup_id", "키워드"] if k in disp.columns and k in base_bundle.columns]
        base_cols_cmp = ["키워드", "구분", "캠페인", "광고그룹", "업체명", "담당자", "캠페인유형"]
        if "평균순위" in disp.columns: base_cols_cmp.append("평균순위")

    if not base_bundle.empty:
        disp_cmp = _apply_comparison_metrics(disp, base_bundle, valid_keys)
    else: 
        disp_cmp = _apply_comparison_metrics(disp, pd.DataFrame(), [])

    metrics_cols_cmp = []
    metrics_cols_cmp.extend(["노출", "노출 증감", "노출 차이"] if show_deltas else ["노출"])
    metrics_cols_cmp.extend(["클릭", "클릭 증감", "클릭 차이"] if show_deltas else ["클릭"])
    metrics_cols_cmp.extend(["광고비", "광고비 증감", "광고비 차이"] if show_deltas else ["광고비"])
    metrics_cols_cmp.extend(["CPC(원)", "CPC 증감", "CPC 차이"] if show_deltas else ["CPC(원)"])
    metrics_cols_cmp.extend(["전환", "전환 증감", "전환 차이"] if show_deltas else ["전환"])
    metrics_cols_cmp.extend(["CPA(원)"])
    metrics_cols_cmp.extend(["전환매출", "전환매출 증감", "전환매출 차이"] if show_deltas else ["전환매출"])
    metrics_cols_cmp.extend(["ROAS(%)", "ROAS 증감"] if show_deltas else ["ROAS(%)"])

    if show_deltas and ("avg_rank" in disp_cmp.columns or "평균순위" in disp_cmp.columns):
        metrics_cols_cmp.append("순위 변화")

    render_item_comparison_search("키워드/소재", disp_cmp, base_bundle, "키워드", start_dt, end_dt, b1, b2)

    final_cols_cmp = [c for c in base_cols_cmp + metrics_cols_cmp if c in disp_cmp.columns]
    sort_cols_cmp = [c for c in ["광고비", "전환매출", "전환"] if c in disp_cmp.columns]
    disp_final = _limit_display_rows_preserving_conversions(disp_cmp[final_cols_cmp], sort_cols_cmp, top_n).copy()

    st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:8px;'>키워드 기간 비교 표</div>", unsafe_allow_html=True)
    st.dataframe(_build_table_styler(disp_final), use_container_width=True, height=550, hide_index=True, column_config=_keyword_fast_col_config(disp_final, "키워드"))

def page_perf_keyword(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    render_toolbar(
        "키워드/소재 성과",
        "파워링크는 키워드 단위, 쇼핑검색은 일반 상품소재 단위 성과를 보여줍니다.",
        [{"label": f"{f['start']} ~ {f['end']}", "tone": "primary"}, {"label": "전체 행 정렬", "tone": "info"}],
    )
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    top_n = int(f.get("top_n_keyword", 150))

    selected_tab = st.pills("분석 탭 선택", ["종합 성과", "기간 비교"], default="종합 성과")

    if selected_tab == "기간 비교":
        kw_bundle = query_keyword_bundle(
            engine,
            f["start"],
            f["end"],
            list(cids),
            type_sel,
            topn_cost=_keyword_fetch_limit(top_n, daily_breakdown=False),
            include_dt=False,
        )
        ad_bundle = query_ad_bundle(
            engine,
            f["start"],
            f["end"],
            cids,
            type_sel,
            topn_cost=_keyword_fetch_limit(top_n, daily_breakdown=False),
            top_k=50,
            include_dt=False,
        )
    else:
        kw_bundle = query_keyword_bundle(
            engine,
            f["start"],
            f["end"],
            list(cids),
            type_sel,
            topn_cost=_keyword_fetch_limit(top_n, daily_breakdown=True),
            include_dt=True,
        )
        ad_bundle = query_ad_bundle(
            engine,
            f["start"],
            f["end"],
            cids,
            type_sel,
            topn_cost=_keyword_fetch_limit(top_n, daily_breakdown=True),
            top_k=50,
            include_dt=True,
        )

    view = compute_keyword_view(kw_bundle, ad_bundle, meta)
    if _includes_shopping_type(type_sel):
        shop_terms = _cached_keyword_shopping_terms(engine, f["start"], f["end"], cids)
        shop_view = _build_shopping_terms_keyword_view(shop_terms, meta)
        view = _merge_keyword_view_with_shopping_terms(view, shop_view)

    if view is not None and not view.empty:
        item_col = "키워드" if "키워드" in view.columns else ("항목명" if "항목명" in view.columns else view.columns[0])
        total_cost = float(safe_numeric_col(view, "광고비").sum())
        total_clk = float(safe_numeric_col(view, "클릭").sum())
        total_conv_col = "전환" if "전환" in view.columns else "구매완료수"
        total_sales_col = "전환매출" if "전환매출" in view.columns else "구매완료 매출"
        total_conv = float(safe_numeric_col(view, total_conv_col).sum())
        total_sales = float(safe_numeric_col(view, total_sales_col).sum())
        total_roas = (total_sales / total_cost * 100.0) if total_cost > 0 else 0.0
        total_cpc = (total_cost / total_clk) if total_clk > 0 else 0.0
        render_kpi_strip([
            {"label": "분석 항목", "value": f"{view[item_col].nunique():,}개", "sub": "현재 필터", "tone": "neu"},
            {"label": "광고비", "value": format_currency(total_cost), "sub": "집행 합계", "tone": "neu"},
            {"label": "클릭", "value": f"{total_clk:,.0f}", "sub": "유입 합계", "tone": "neu"},
            {"label": "CPC", "value": format_currency(total_cpc), "sub": "평균 비용", "tone": "neu"},
            {"label": "전환", "value": f"{total_conv:,.0f}", "sub": "전환 합계", "tone": "neu"},
            {"label": "ROAS", "value": f"{total_roas:,.1f}%", "sub": "수익성", "tone": "neu"},
        ])

    if selected_tab == "종합 성과":
        render_keyword_main(view, top_n)
    elif selected_tab == "기간 비교":
        render_keyword_cmp(view, engine, cids, type_sel, top_n, f["start"], f["end"])
