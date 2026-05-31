# -*- coding: utf-8 -*-
"""view_time_age.py - Hourly and age-range performance dashboard."""
from __future__ import annotations

from html import escape
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd
import streamlit as st

from data import get_table_columns, sql_read, table_exists
from ui import THEME, render_empty_state
from targeting_collector_helpers import AGE_BUCKETS


TYPE_LABEL_MAP = {
    "WEB_SITE": "파워링크",
    "SHOPPING": "쇼핑검색",
    "POWER_CONTENTS": "파워컨텐츠",
    "BRAND_SEARCH": "브랜드검색",
    "PLACE": "플레이스",
}


AGE_SORT_ORDER = {label: idx for idx, label in enumerate(AGE_BUCKETS)}
AGE_SORT_ORDER.update({
    "연령 알 수 없음": AGE_SORT_ORDER.get("연령 알 수 없음", 98),
    "알 수 없음": AGE_SORT_ORDER.get("연령 알 수 없음", 98),
    "미분류": 99,
})

CHART_METRIC_OPTIONS = {
    "광고비": "cost",
    "클릭": "clk",
    "전환수": "conv",
    "ROAS": "ROAS(%)",
}


def _sql_in_str_list(values: Iterable[str]) -> str:
    vals = []
    for v in values or []:
        s = str(v or "").replace("'", "''").strip()
        if s:
            vals.append(f"'{s}'")
    return ",".join(vals) if vals else "''"


def _expand_campaign_type_values(type_sel: Iterable[str]) -> List[str]:
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
    out: List[str] = []
    for v in type_sel or []:
        s = str(v or "").strip()
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


def _campaign_type_column(engine) -> str:
    cols = get_table_columns(engine, "dim_campaign") or []
    for c in ["campaign_tp", "campaign_type", "type", "campaignType"]:
        if c in cols:
            return c
    return "campaign_tp"


def _type_filter_sql(engine, alias: str, type_sel: Iterable[str]) -> str:
    type_vals = _expand_campaign_type_values(type_sel)
    if not type_vals:
        return ""
    cp_col = _campaign_type_column(engine)
    type_list = _sql_in_str_list(type_vals)
    return f"""
        AND (
            COALESCE(CAST({alias}.{cp_col} AS TEXT), '') IN ({type_list})
            OR (
                CASE
                    WHEN COALESCE(CAST({alias}.{cp_col} AS TEXT), '') = 'WEB_SITE' THEN '파워링크'
                    WHEN COALESCE(CAST({alias}.{cp_col} AS TEXT), '') = 'SHOPPING' THEN '쇼핑검색'
                    WHEN COALESCE(CAST({alias}.{cp_col} AS TEXT), '') = 'POWER_CONTENTS' THEN '파워컨텐츠'
                    WHEN COALESCE(CAST({alias}.{cp_col} AS TEXT), '') = 'BRAND_SEARCH' THEN '브랜드검색'
                    WHEN COALESCE(CAST({alias}.{cp_col} AS TEXT), '') = 'PLACE' THEN '플레이스'
                    ELSE COALESCE(CAST({alias}.{cp_col} AS TEXT), '')
                END
            ) IN ({type_list})
        )
    """


def _metric_expr(prefix: str = "f") -> str:
    return f"""
        SUM(CAST(COALESCE({prefix}.imp,0) AS NUMERIC)) AS imp,
        SUM(CAST(COALESCE({prefix}.clk,0) AS NUMERIC)) AS clk,
        SUM(CAST(COALESCE({prefix}.cost,0) AS NUMERIC)) AS cost,
        SUM(CAST(COALESCE({prefix}.conv,0) AS NUMERIC)) AS conv,
        SUM(CAST(COALESCE({prefix}.sales,0) AS NUMERIC)) AS sales
    """


def _metric_columns() -> list[str]:
    return ["imp", "clk", "cost", "conv", "sales"]


def _add_calc_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    for c in _metric_columns():
        out[c] = pd.to_numeric(out.get(c, 0), errors="coerce").fillna(0)
    out["CTR(%)"] = np.where(out["imp"] > 0, out["clk"] / out["imp"] * 100.0, 0.0)
    out["CPC"] = np.where(out["clk"] > 0, out["cost"] / out["clk"], 0.0)
    out["ROAS(%)"] = np.where(out["cost"] > 0, out["sales"] / out["cost"] * 100.0, 0.0)
    return out


def _aggregate_metrics(df: pd.DataFrame, by_cols: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    for c in _metric_columns():
        work[c] = pd.to_numeric(work.get(c, 0), errors="coerce").fillna(0)
    return work.groupby(by_cols, dropna=False, as_index=False)[_metric_columns()].sum()


def _sort_options_by_cost(df: pd.DataFrame, col: str) -> list[str]:
    if df is None or df.empty or col not in df.columns:
        return []
    work = df.copy()
    work[col] = work[col].astype(str).replace({"": "미분류"}).fillna("미분류")
    work["cost"] = pd.to_numeric(work.get("cost", 0), errors="coerce").fillna(0)
    ranked = work.groupby(col, dropna=False)["cost"].sum().sort_values(ascending=False)
    return [str(x) for x in ranked.index.tolist() if str(x).strip()]


def _format_hour_range(value) -> str:
    """Return a user-friendly hour bucket label such as 00시~01시."""
    try:
        hour = int(float(value)) % 24
    except Exception:
        return "미분류"
    next_hour = (hour + 1) % 24
    return f"{hour:02d}시~{next_hour:02d}시"


def _normalize_age_label(value) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "미분류"
    upper = raw.upper()
    mapping = {
        "UNKNOWN": "연령 알 수 없음",
        "알 수 없음": "연령 알 수 없음",
        "ETC": "기타",
        "NONE": "미분류",
        "-": "미분류",
    }
    return mapping.get(upper, raw)


def _normalize_type_label(value) -> str:
    raw = str(value or "").strip()
    return TYPE_LABEL_MAP.get(raw, raw or "미분류")


def _render_section_title(title: str, desc: str = "") -> None:
    safe_title = escape(str(title or ""))
    safe_desc = escape(str(desc or ""))
    desc_html = f"<div class='ta-section-desc'>{safe_desc}</div>" if safe_desc else ""
    st.markdown(
        f"""
        <div class='ta-section-head'>
            <div class='ta-section-title'>{safe_title}</div>
            {desc_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _format_display(df: pd.DataFrame, first_cols: List[str]) -> pd.DataFrame:
    """Return a table layout aligned with the overview period/detail tables."""
    if df is None or df.empty:
        return pd.DataFrame()
    out = _add_calc_cols(df)
    out["전환율(%)"] = np.where(out["clk"] > 0, out["conv"] / out["clk"] * 100.0, 0.0)
    rename = {
        "imp": "노출수",
        "clk": "클릭수",
        "CTR(%)": "클릭률(%)",
        "cost": "광고비",
        "conv": "전환수",
        "sales": "전환매출",
    }
    out = out.rename(columns=rename)
    metric_cols = [
        "노출수", "클릭수", "클릭률(%)", "광고비", "CPC",
        "전환수", "전환율(%)", "전환매출", "ROAS(%)",
    ]
    cols = [c for c in first_cols + metric_cols if c in out.columns]
    return out[cols]


_TABLE_FORMATS = {
    "노출수": "{:,.0f}",
    "클릭수": "{:,.0f}",
    "클릭률(%)": "{:,.2f}%",
    "광고비": "{:,.0f}원",
    "CPC": "{:,.0f}원",
    "전환수": "{:,.1f}",
    "전환율(%)": "{:,.2f}%",
    "전환매출": "{:,.0f}원",
    "ROAS(%)": "{:,.1f}%",
}


def _auto_table_height(df: pd.DataFrame, default_height: int = 420, min_height: int = 108, max_height: int = 520) -> int:
    try:
        rows = len(df.index)
        if rows <= 0:
            return min_height
        calc = 38 + (rows * 35)
        return max(min_height, min(calc, max_height))
    except Exception:
        return default_height


def _table_column_config(first_col: str) -> dict:
    return {first_col: st.column_config.TextColumn(first_col, pinned=True, width="medium")}


def _render_table(df: pd.DataFrame) -> None:
    """Render tables in the same compact, sortable style as the overview detail tables."""
    if df is None or df.empty:
        st.info("표시할 데이터가 없습니다.")
        return
    view = df.copy()
    first_col = str(view.columns[0]) if len(view.columns) else ""
    height = _auto_table_height(view)
    try:
        styled = view.style.format(_TABLE_FORMATS)
        st.dataframe(
            styled,
            width="stretch",
            height=height,
            hide_index=True,
            column_config=_table_column_config(first_col) if first_col else None,
        )
    except Exception:
        st.dataframe(
            view,
            width="stretch",
            height=height,
            hide_index=True,
            column_config=_table_column_config(first_col) if first_col else None,
        )


def _kpi_row(summary: pd.DataFrame) -> None:
    if summary is None or summary.empty:
        return
    row = _add_calc_cols(summary).iloc[0]
    cols = st.columns(5, gap="medium")
    values = [
        ("노출", f"{int(row.get('imp', 0)):,}"),
        ("클릭", f"{int(row.get('clk', 0)):,}"),
        ("광고비", f"{int(row.get('cost', 0)):,}원"),
        ("전환수", f"{float(row.get('conv', 0)):.1f}"),
        ("ROAS", f"{float(row.get('ROAS(%)', 0)):.1f}%"),
    ]
    for col, (label, value) in zip(cols, values):
        with col:
            st.markdown(
                f"""
                <div class='ta-kpi-card'>
                    <div class='ta-kpi-label'>{escape(label)}</div>
                    <div class='ta-kpi-value'>{escape(value)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _chart_values(df: pd.DataFrame, col: str) -> list[float]:
    return pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0).astype(float).round(2).tolist()


def _render_time_age_dual_axis(
    title: str,
    desc: str,
    df: pd.DataFrame,
    x_col: str,
    y1_col: str,
    y1_name: str,
    y2_col: str,
    y2_name: str,
    *,
    key: str,
    height: int = 370,
) -> None:
    """Render a roomy overview-style bar+line chart for the time/age/device tabs."""
    if df is None or df.empty or x_col not in df.columns:
        render_empty_state("차트를 그릴 데이터가 부족합니다.", height, "기간이나 필터 조건을 변경해보세요.")
        return

    chart_df = df.copy()
    x_data = chart_df[x_col].astype(str).tolist()
    y1_data = _chart_values(chart_df, y1_col)
    y2_data = _chart_values(chart_df, y2_col)
    dense_x = len(x_data) >= 18

    options = {
        "backgroundColor": "#FFFFFF",
        "animation": True,
        "color": [THEME["primary_soft"], THEME["primary"]],
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "cross", "crossStyle": {"color": THEME["line"]}},
            "backgroundColor": "#FFFFFF",
            "borderColor": THEME["line"],
            "borderWidth": 1,
            "textStyle": {"color": THEME["text"], "fontSize": 12},
            "padding": [10, 12],
        },
        "legend": {
            "data": [y1_name, y2_name],
            "top": 8,
            "right": 18,
            "itemWidth": 10,
            "itemHeight": 10,
            "itemGap": 16,
            "textStyle": {"color": THEME["muted"], "fontSize": 12, "fontWeight": 600},
        },
        "grid": {
            "left": 64,
            "right": 72,
            "bottom": 54,
            "top": 58,
            "containLabel": False,
        },
        "xAxis": [{
            "type": "category",
            "data": x_data,
            "axisPointer": {"type": "shadow"},
            "axisLine": {"lineStyle": {"color": THEME["line"]}},
            "axisTick": {"show": False},
            "axisLabel": {
                "color": THEME["muted"],
                "fontSize": 11,
                "fontWeight": 600,
                "margin": 15,
                "hideOverlap": True,
                **({"interval": 1} if dense_x else {"interval": 0}),
            },
        }],
        "yAxis": [
            {
                "type": "value",
                "name": y1_name,
                "nameGap": 16,
                "nameTextStyle": {"color": THEME["muted"], "fontSize": 11, "fontWeight": 700, "padding": [0, 0, 4, 0]},
                "axisLabel": {"color": THEME["muted"], "fontSize": 11, "margin": 12},
                "splitLine": {"lineStyle": {"type": "solid", "color": "#EEF2F7"}},
            },
            {
                "type": "value",
                "name": y2_name,
                "nameGap": 16,
                "nameTextStyle": {"color": THEME["muted"], "fontSize": 11, "fontWeight": 700, "padding": [0, 0, 4, 0]},
                "axisLabel": {"color": THEME["muted"], "fontSize": 11, "margin": 12},
                "splitLine": {"show": False},
            },
        ],
        "series": [
            {
                "name": y1_name,
                "type": "bar",
                "data": y1_data,
                "barMaxWidth": 28,
                "barCategoryGap": "46%",
                "itemStyle": {"color": THEME["primary_soft"], "borderRadius": [7, 7, 0, 0]},
                "emphasis": {"focus": "series"},
            },
            {
                "name": y2_name,
                "type": "line",
                "yAxisIndex": 1,
                "data": y2_data,
                "smooth": True,
                "showSymbol": True,
                "symbol": "circle",
                "symbolSize": 6,
                "itemStyle": {"color": THEME["primary"]},
                "lineStyle": {"width": 2.7},
                "emphasis": {"focus": "series"},
            },
        ],
    }

    with st.container(border=True):
        echarts_renderer = None
        try:
            from streamlit_echarts import st_echarts as echarts_renderer
        except Exception:
            echarts_renderer = None
        if echarts_renderer:
            echarts_renderer(options=options, height=f"{height}px", key=f"{key}_chart")
        else:
            fallback = pd.DataFrame({x_col: x_data, y1_name: y1_data, y2_name: y2_data}).set_index(x_col)
            st.line_chart(fallback, height=height)


def _render_overview_like_chart(df: pd.DataFrame, label_col: str, *, key: str, title_prefix: str) -> None:
    """Render the time/age/device chart in the same bar+line style as overview trend charts."""
    if df is None or df.empty or label_col not in df.columns:
        st.info("차트로 표시할 데이터가 없습니다.")
        return

    chart_df = _add_calc_cols(df).copy()
    for col in ["imp", "clk", "cost", "sales"]:
        chart_df[col] = pd.to_numeric(chart_df.get(col, 0), errors="coerce").fillna(0)
    chart_df[label_col] = chart_df[label_col].astype(str)

    st.markdown("<div class='ta-chart-toolbar-gap'></div>", unsafe_allow_html=True)
    trend_view = st.segmented_control(
        "추이 보기",
        ["비용 및 매출 추이", "유입 지표 추이"],
        default="비용 및 매출 추이",
        key=key,
        label_visibility="collapsed",
    )
    st.markdown("<div class='ta-chart-after-toolbar'></div>", unsafe_allow_html=True)

    if trend_view == "유입 지표 추이":
        _render_time_age_dual_axis(
            "",
            "",
            chart_df,
            label_col,
            "imp",
            "노출수",
            "clk",
            "클릭수",
            key=f"{key}_traffic",
            height=370,
        )
    else:
        _render_time_age_dual_axis(
            "",
            "",
            chart_df,
            label_col,
            "cost",
            "광고비",
            "sales",
            "매출",
            key=f"{key}_cost_sales",
            height=370,
        )

def _filter_campaign_and_group(
    df: pd.DataFrame,
    *,
    campaign_key: str,
    group_key: str,
    desc: str = "",
) -> tuple[pd.DataFrame, list[str], list[str]]:
    if df is None or df.empty:
        return pd.DataFrame(), [], []
    work = df.copy()
    selected_campaigns: list[str] = []
    selected_groups: list[str] = []

    _render_section_title("필터", desc or "캠페인/그룹을 선택하면 KPI, 차트, 표가 모두 같은 조건으로 변경됩니다.")
    col_campaign, col_group = st.columns(2, gap="medium")
    with col_campaign:
        campaign_options = _sort_options_by_cost(work, "campaign_name") if "campaign_name" in work.columns else []
        selected_campaigns = st.multiselect(
            "캠페인",
            campaign_options,
            default=[],
            key=campaign_key,
            help="미선택 시 전체 캠페인을 표시합니다.",
            placeholder="캠페인 선택 또는 전체",
        ) if campaign_options else []
    if selected_campaigns and "campaign_name" in work.columns:
        work = work[work["campaign_name"].astype(str).isin(selected_campaigns)].copy()

    with col_group:
        if "adgroup_name" in work.columns:
            group_options = _sort_options_by_cost(work, "adgroup_name")
            selected_groups = st.multiselect(
                "광고그룹",
                group_options,
                default=[],
                key=group_key,
                help="미선택 시 선택 캠페인의 전체 그룹을 표시합니다.",
                placeholder="광고그룹 선택 또는 전체",
            ) if group_options else []
        else:
            st.multiselect(
                "광고그룹",
                [],
                default=[],
                key=group_key,
                disabled=True,
                help="그룹별 시간·연령 수집 테이블이 있을 때 활성화됩니다.",
                placeholder="그룹 데이터 없음",
            )
    if selected_groups and "adgroup_name" in work.columns:
        work = work[work["adgroup_name"].astype(str).isin(selected_groups)].copy()
    return work, selected_campaigns, selected_groups


def _query_hourly(engine, d1, d2, cids: tuple, type_sel: tuple, by_campaign: bool = False) -> pd.DataFrame:
    if not table_exists(engine, "fact_campaign_hourly_daily"):
        return pd.DataFrame()
    where_cid = f"AND CAST(f.customer_id AS TEXT) IN ({_sql_in_str_list(cids)})" if cids else ""
    type_filter = _type_filter_sql(engine, "c", type_sel)
    cp_col = _campaign_type_column(engine)
    group_select = ""
    group_by = "f.hour_of_day"
    order_by = "f.hour_of_day"
    if by_campaign:
        group_select = f"""
            COALESCE(NULLIF(TRIM(c.campaign_name), ''), f.campaign_id) AS campaign_name,
            COALESCE(NULLIF(TRIM(CAST(c.{cp_col} AS TEXT)), ''), '미분류') AS campaign_type,
        """
        group_by = f"f.hour_of_day, COALESCE(NULLIF(TRIM(c.campaign_name), ''), f.campaign_id), COALESCE(NULLIF(TRIM(CAST(c.{cp_col} AS TEXT)), ''), '미분류')"
        order_by = "cost DESC"
    sql = f"""
        SELECT
            f.hour_of_day,
            {group_select}
            {_metric_expr('f')}
        FROM fact_campaign_hourly_daily f
        LEFT JOIN dim_campaign c
          ON CAST(f.customer_id AS TEXT)=CAST(c.customer_id AS TEXT)
         AND CAST(f.campaign_id AS TEXT)=CAST(c.campaign_id AS TEXT)
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
          {type_filter}
        GROUP BY {group_by}
        ORDER BY {order_by}
    """
    return sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})


def _query_adgroup_hourly(engine, d1, d2, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    """Adgroup-level hourly view if the group collector/table exists."""
    if not table_exists(engine, "fact_adgroup_hourly_daily"):
        return pd.DataFrame()
    where_cid = f"AND CAST(f.customer_id AS TEXT) IN ({_sql_in_str_list(cids)})" if cids else ""
    type_filter = _type_filter_sql(engine, "c", type_sel)
    cp_col = _campaign_type_column(engine)
    sql = f"""
        SELECT
            f.hour_of_day,
            COALESCE(NULLIF(TRIM(CAST(c.{cp_col} AS TEXT)), ''), '미분류') AS campaign_type,
            COALESCE(NULLIF(TRIM(c.campaign_name), ''), g.campaign_id, f.campaign_id, '미분류') AS campaign_name,
            COALESCE(NULLIF(TRIM(g.adgroup_name), ''), f.adgroup_id, '미분류') AS adgroup_name,
            {_metric_expr('f')}
        FROM fact_adgroup_hourly_daily f
        LEFT JOIN dim_adgroup g
          ON CAST(f.customer_id AS TEXT)=CAST(g.customer_id AS TEXT)
         AND CAST(f.adgroup_id AS TEXT)=CAST(g.adgroup_id AS TEXT)
        LEFT JOIN dim_campaign c
          ON CAST(f.customer_id AS TEXT)=CAST(c.customer_id AS TEXT)
         AND CAST(COALESCE(f.campaign_id, g.campaign_id) AS TEXT)=CAST(c.campaign_id AS TEXT)
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
          {type_filter}
        GROUP BY f.hour_of_day, COALESCE(NULLIF(TRIM(CAST(c.{cp_col} AS TEXT)), ''), '미분류'), COALESCE(NULLIF(TRIM(c.campaign_name), ''), g.campaign_id, f.campaign_id, '미분류'), COALESCE(NULLIF(TRIM(g.adgroup_name), ''), f.adgroup_id, '미분류')
        HAVING SUM(CAST(COALESCE(f.imp,0) AS NUMERIC)) + SUM(CAST(COALESCE(f.clk,0) AS NUMERIC)) + SUM(CAST(COALESCE(f.cost,0) AS NUMERIC)) > 0
        ORDER BY cost DESC
    """
    return sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})


def _query_age(engine, d1, d2, cids: tuple, type_sel: tuple, by_campaign: bool = False) -> pd.DataFrame:
    if not table_exists(engine, "fact_campaign_age_daily"):
        return pd.DataFrame()
    where_cid = f"AND CAST(f.customer_id AS TEXT) IN ({_sql_in_str_list(cids)})" if cids else ""
    type_filter = _type_filter_sql(engine, "c", type_sel)
    cp_col = _campaign_type_column(engine)
    group_select = ""
    group_by = "COALESCE(NULLIF(TRIM(f.age_range), ''), '미분류')"
    order_by = "cost DESC"
    if by_campaign:
        group_select = f"""
            COALESCE(NULLIF(TRIM(c.campaign_name), ''), f.campaign_id) AS campaign_name,
            COALESCE(NULLIF(TRIM(CAST(c.{cp_col} AS TEXT)), ''), '미분류') AS campaign_type,
        """
        group_by = f"COALESCE(NULLIF(TRIM(f.age_range), ''), '미분류'), COALESCE(NULLIF(TRIM(c.campaign_name), ''), f.campaign_id), COALESCE(NULLIF(TRIM(CAST(c.{cp_col} AS TEXT)), ''), '미분류')"
    sql = f"""
        SELECT
            COALESCE(NULLIF(TRIM(f.age_range), ''), '미분류') AS age_range,
            {group_select}
            {_metric_expr('f')}
        FROM fact_campaign_age_daily f
        LEFT JOIN dim_campaign c
          ON CAST(f.customer_id AS TEXT)=CAST(c.customer_id AS TEXT)
         AND CAST(f.campaign_id AS TEXT)=CAST(c.campaign_id AS TEXT)
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
          {type_filter}
        GROUP BY {group_by}
        HAVING SUM(CAST(COALESCE(f.imp,0) AS NUMERIC)) + SUM(CAST(COALESCE(f.clk,0) AS NUMERIC)) + SUM(CAST(COALESCE(f.cost,0) AS NUMERIC)) > 0
        ORDER BY {order_by}
    """
    return sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})


def _query_adgroup_age(engine, d1, d2, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    """Adgroup-level age view if the group collector/table exists."""
    if not table_exists(engine, "fact_adgroup_age_daily"):
        return pd.DataFrame()
    where_cid = f"AND CAST(f.customer_id AS TEXT) IN ({_sql_in_str_list(cids)})" if cids else ""
    type_filter = _type_filter_sql(engine, "c", type_sel)
    cp_col = _campaign_type_column(engine)
    sql = f"""
        SELECT
            COALESCE(NULLIF(TRIM(f.age_range), ''), '미분류') AS age_range,
            COALESCE(NULLIF(TRIM(CAST(c.{cp_col} AS TEXT)), ''), '미분류') AS campaign_type,
            COALESCE(NULLIF(TRIM(c.campaign_name), ''), g.campaign_id, f.campaign_id, '미분류') AS campaign_name,
            COALESCE(NULLIF(TRIM(g.adgroup_name), ''), f.adgroup_id, '미분류') AS adgroup_name,
            {_metric_expr('f')}
        FROM fact_adgroup_age_daily f
        LEFT JOIN dim_adgroup g
          ON CAST(f.customer_id AS TEXT)=CAST(g.customer_id AS TEXT)
         AND CAST(f.adgroup_id AS TEXT)=CAST(g.adgroup_id AS TEXT)
        LEFT JOIN dim_campaign c
          ON CAST(f.customer_id AS TEXT)=CAST(c.customer_id AS TEXT)
         AND CAST(COALESCE(f.campaign_id, g.campaign_id) AS TEXT)=CAST(c.campaign_id AS TEXT)
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
          {type_filter}
        GROUP BY COALESCE(NULLIF(TRIM(f.age_range), ''), '미분류'), COALESCE(NULLIF(TRIM(CAST(c.{cp_col} AS TEXT)), ''), '미분류'), COALESCE(NULLIF(TRIM(c.campaign_name), ''), g.campaign_id, f.campaign_id, '미분류'), COALESCE(NULLIF(TRIM(g.adgroup_name), ''), f.adgroup_id, '미분류')
        ORDER BY cost DESC
    """
    return sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})


def _query_device(engine, d1, d2, cids: tuple, type_sel: tuple, by_campaign: bool = False) -> pd.DataFrame:
    if not table_exists(engine, "fact_campaign_device_daily"):
        return pd.DataFrame()
    where_cid = f"AND CAST(f.customer_id AS TEXT) IN ({_sql_in_str_list(cids)})" if cids else ""
    type_filter = _type_filter_sql(engine, "c", type_sel)
    cp_col = _campaign_type_column(engine)
    group_select = ""
    group_by = "COALESCE(NULLIF(TRIM(f.device_name), ''), 'UNSEGMENTED')"
    order_by = "cost DESC"
    if by_campaign:
        group_select = f"""
            COALESCE(NULLIF(TRIM(c.campaign_name), ''), f.campaign_id) AS campaign_name,
            COALESCE(NULLIF(TRIM(CAST(c.{cp_col} AS TEXT)), ''), '미분류') AS campaign_type,
        """
        group_by = f"COALESCE(NULLIF(TRIM(f.device_name), ''), 'UNSEGMENTED'), COALESCE(NULLIF(TRIM(c.campaign_name), ''), f.campaign_id), COALESCE(NULLIF(TRIM(CAST(c.{cp_col} AS TEXT)), ''), '미분류')"
    sql = f"""
        SELECT
            COALESCE(NULLIF(TRIM(f.device_name), ''), 'UNSEGMENTED') AS device_name,
            {group_select}
            {_metric_expr('f')}
        FROM fact_campaign_device_daily f
        LEFT JOIN dim_campaign c
          ON CAST(f.customer_id AS TEXT)=CAST(c.customer_id AS TEXT)
         AND CAST(f.campaign_id AS TEXT)=CAST(c.campaign_id AS TEXT)
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
          {type_filter}
        GROUP BY {group_by}
        HAVING SUM(CAST(COALESCE(f.imp,0) AS NUMERIC)) + SUM(CAST(COALESCE(f.clk,0) AS NUMERIC)) + SUM(CAST(COALESCE(f.cost,0) AS NUMERIC)) > 0
        ORDER BY {order_by}
    """
    return sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})


def _query_ad_device(engine, d1, d2, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    if not table_exists(engine, "fact_ad_device_daily"):
        return pd.DataFrame()
    where_cid = f"AND CAST(f.customer_id AS TEXT) IN ({_sql_in_str_list(cids)})" if cids else ""
    type_filter = _type_filter_sql(engine, "c", type_sel)
    cp_col = _campaign_type_column(engine)
    sql = f"""
        SELECT
            COALESCE(NULLIF(TRIM(f.device_name), ''), 'UNSEGMENTED') AS device_name,
            COALESCE(NULLIF(TRIM(CAST(c.{cp_col} AS TEXT)), ''), '미분류') AS campaign_type,
            COALESCE(NULLIF(TRIM(c.campaign_name), ''), g.campaign_id, '미분류') AS campaign_name,
            COALESCE(NULLIF(TRIM(g.adgroup_name), ''), a.adgroup_id, '미분류') AS adgroup_name,
            COALESCE(NULLIF(TRIM(a.ad_name), ''), f.ad_id, '미분류') AS ad_name,
            {_metric_expr('f')}
        FROM fact_ad_device_daily f
        LEFT JOIN dim_ad a
          ON CAST(f.customer_id AS TEXT)=CAST(a.customer_id AS TEXT)
         AND CAST(f.ad_id AS TEXT)=CAST(a.ad_id AS TEXT)
        LEFT JOIN dim_adgroup g
          ON CAST(a.customer_id AS TEXT)=CAST(g.customer_id AS TEXT)
         AND CAST(a.adgroup_id AS TEXT)=CAST(g.adgroup_id AS TEXT)
        LEFT JOIN dim_campaign c
          ON CAST(f.customer_id AS TEXT)=CAST(c.customer_id AS TEXT)
         AND CAST(g.campaign_id AS TEXT)=CAST(c.campaign_id AS TEXT)
        WHERE f.dt BETWEEN :d1 AND :d2
          {where_cid}
          {type_filter}
        GROUP BY COALESCE(NULLIF(TRIM(f.device_name), ''), 'UNSEGMENTED'), COALESCE(NULLIF(TRIM(CAST(c.{cp_col} AS TEXT)), ''), '미분류'), COALESCE(NULLIF(TRIM(c.campaign_name), ''), g.campaign_id, '미분류'), COALESCE(NULLIF(TRIM(g.adgroup_name), ''), a.adgroup_id, '미분류'), COALESCE(NULLIF(TRIM(a.ad_name), ''), f.ad_id, '미분류')
        HAVING SUM(CAST(COALESCE(f.imp,0) AS NUMERIC)) + SUM(CAST(COALESCE(f.clk,0) AS NUMERIC)) + SUM(CAST(COALESCE(f.cost,0) AS NUMERIC)) > 0
        ORDER BY cost DESC
    """
    return sql_read(engine, sql, {"d1": str(d1), "d2": str(d2)})


def _prepare_hour_frame(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["hour_of_day"] = pd.to_numeric(work["hour_of_day"], errors="coerce").fillna(0).astype(int)
    work["시간대"] = work["hour_of_day"].map(_format_hour_range)
    return work.sort_values("hour_of_day")


def _prepare_age_frame(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["age_range"] = work["age_range"].map(_normalize_age_label)
    work["_age_sort"] = work["age_range"].map(lambda x: AGE_SORT_ORDER.get(str(x), 50))
    return work.sort_values(["_age_sort", "age_range"]).drop(columns=["_age_sort"], errors="ignore")


def _normalize_device_label(value) -> str:
    raw = str(value or "").strip()
    upper = raw.upper()
    mapping = {
        "PC": "PC",
        "P": "PC",
        "MOBILE": "모바일",
        "MO": "모바일",
        "M": "모바일",
        "UNSEGMENTED": "미분리 합계",
        "UNKNOWN": "알 수 없음",
    }
    return mapping.get(upper, raw or "미분류")


def _prepare_device_frame(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["device_name"] = work["device_name"].map(_normalize_device_label)
    order = {"PC": 0, "모바일": 1, "미분리 합계": 2, "알 수 없음": 3, "미분류": 4}
    work["_device_sort"] = work["device_name"].map(lambda x: order.get(str(x), 50))
    return work.sort_values(["_device_sort", "cost"], ascending=[True, False]).drop(columns=["_device_sort"], errors="ignore")


def _render_hour_tab(engine, f: Dict) -> None:
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))

    by_camp_all = _query_hourly(engine, f["start"], f["end"], cids, type_sel, by_campaign=True)
    by_group_all = _query_adgroup_hourly(engine, f["start"], f["end"], cids, type_sel)
    if by_camp_all.empty and by_group_all.empty:
        st.info("시간대별 데이터가 아직 없습니다. 시간·연령 수집을 실행한 날짜부터 표시됩니다.")
        return

    filter_base = by_group_all if not by_group_all.empty else by_camp_all
    filtered, selected_campaigns, selected_groups = _filter_campaign_and_group(
        filter_base,
        campaign_key="ta_hour_campaign_filter_v10",
        group_key="ta_hour_adgroup_filter_v10",
        desc="캠페인/광고그룹을 선택하면 KPI, 차트, 표가 같은 조건으로 바뀝니다.",
    )
    if filtered.empty:
        st.info("선택한 캠페인/그룹 조건에 해당하는 시간대별 데이터가 없습니다.")
        return

    hourly = _aggregate_metrics(filtered, ["hour_of_day"])
    if hourly.empty:
        st.info("선택 조건에 해당하는 시간대별 데이터가 없습니다.")
        return

    summary = hourly[_metric_columns()].sum().to_frame().T
    _kpi_row(summary)

    chart = _prepare_hour_frame(_add_calc_cols(hourly))
    _render_overview_like_chart(chart, "시간대", key="ta_hour_trend_view_v12", title_prefix="시간대별")

    tab_summary, tab_campaign, tab_group = st.tabs(["시간대 요약", "캠페인별", "그룹별"])
    with tab_summary:
        _render_section_title("시간대별 상세", "시간 표시는 00시~01시 형식입니다.")
        disp = _format_display(chart.rename(columns={"시간대": "시간"}), ["시간"])
        _render_table(disp)

    with tab_campaign:
        if not by_group_all.empty:
            camp_src = _aggregate_metrics(filtered, ["campaign_type", "campaign_name", "hour_of_day"])
        else:
            camp_src = filtered.copy()
        camp_src = _prepare_hour_frame(camp_src)
        camp_src["campaign_type"] = camp_src["campaign_type"].map(_normalize_type_label)
        camp_src = camp_src.rename(columns={"campaign_type": "유형", "campaign_name": "캠페인", "시간대": "시간"})
        camp_src = camp_src.sort_values("cost", ascending=False)
        disp2 = _format_display(camp_src, ["유형", "캠페인", "시간"])
        desc = "상단 캠페인/그룹 필터가 적용된 캠페인별 시간대 데이터입니다."
        if selected_campaigns:
            desc = f"선택 캠페인 {len(selected_campaigns):,}개 기준입니다."
        if selected_groups:
            desc += f" 선택 그룹 {len(selected_groups):,}개만 반영했습니다."
        _render_section_title("캠페인별 시간대 상세", desc)
        _render_table(disp2)

    with tab_group:
        if by_group_all.empty:
            st.info("그룹별 시간대 데이터가 아직 없습니다. 그룹 단위 수집 데이터가 쌓이면 이 표가 활성화됩니다.")
        else:
            group_src = _aggregate_metrics(filtered, ["campaign_type", "campaign_name", "adgroup_name", "hour_of_day"])
            group_src = _prepare_hour_frame(group_src)
            group_src["campaign_type"] = group_src["campaign_type"].map(_normalize_type_label)
            group_src = group_src.rename(columns={"campaign_type": "유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "시간대": "시간"})
            group_src = group_src.sort_values("cost", ascending=False)
            disp3 = _format_display(group_src, ["유형", "캠페인", "광고그룹", "시간"])
            desc = "상단 광고그룹 필터로 원하는 그룹만 좁혀 볼 수 있습니다."
            if selected_groups:
                desc = f"선택 그룹 {len(selected_groups):,}개 기준입니다."
            _render_section_title("그룹별 시간대 상세", desc)
            _render_table(disp3)


def _render_device_tab(engine, f: Dict) -> None:
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))

    by_camp_all = _query_device(engine, f["start"], f["end"], cids, type_sel, by_campaign=True)
    by_ad_all = _query_ad_device(engine, f["start"], f["end"], cids, type_sel)
    if by_camp_all.empty and by_ad_all.empty:
        st.info("기기별 성과 데이터가 아직 없습니다. PC/모바일 수집 데이터가 쌓이면 표시됩니다.")
        return

    filter_base = by_ad_all if not by_ad_all.empty else by_camp_all
    filtered, selected_campaigns, selected_groups = _filter_campaign_and_group(
        filter_base,
        campaign_key="ta_device_campaign_filter_v10",
        group_key="ta_device_adgroup_filter_v10",
        desc="캠페인과 광고그룹을 선택하면 기기별 요약, 캠페인별, 소재별 표가 같은 조건으로 바뀝니다.",
    )
    if filtered.empty:
        st.info("선택한 캠페인/광고그룹 조건에 해당하는 기기별 데이터가 없습니다.")
        return

    device = _aggregate_metrics(filtered, ["device_name"])
    if device.empty:
        st.info("선택 조건에 해당하는 기기별 데이터가 없습니다.")
        return

    summary = device[_metric_columns()].sum().to_frame().T
    _kpi_row(summary)

    chart = _prepare_device_frame(_add_calc_cols(device))
    chart = chart.rename(columns={"device_name": "기기"})
    _render_overview_like_chart(chart, "기기", key="ta_device_trend_view_v12", title_prefix="기기별")

    tab_summary, tab_campaign, tab_ad = st.tabs(["기기별 요약", "캠페인별", "소재별"])
    with tab_summary:
        _render_section_title("기기별 상세", "PC, 모바일, 미분리 합계를 같은 표에서 확인합니다.")
        disp = _format_display(chart, ["기기"])
        _render_table(disp)

    with tab_campaign:
        if not by_ad_all.empty:
            camp_src = _aggregate_metrics(filtered, ["campaign_type", "campaign_name", "device_name"])
        else:
            camp_src = filtered.copy()
        camp_src = _prepare_device_frame(camp_src)
        camp_src["campaign_type"] = camp_src["campaign_type"].map(_normalize_type_label)
        camp_src = camp_src.rename(columns={"device_name": "기기", "campaign_type": "유형", "campaign_name": "캠페인"})
        camp_src = camp_src.sort_values("cost", ascending=False)
        disp2 = _format_display(camp_src, ["유형", "캠페인", "기기"])
        desc = "상단 캠페인/광고그룹 필터가 적용된 캠페인별 기기 성과입니다."
        if selected_campaigns:
            desc = f"선택 캠페인 {len(selected_campaigns):,}개 기준입니다."
        if selected_groups:
            desc += f" 선택 그룹 {len(selected_groups):,}개만 반영했습니다."
        _render_section_title("캠페인별 기기 상세", desc)
        _render_table(disp2)

    with tab_ad:
        if by_ad_all.empty:
            st.info("소재별 기기 데이터가 아직 없습니다. 소재 기준 PC/모바일 데이터가 쌓이면 표시됩니다.")
        else:
            ad_src = _aggregate_metrics(filtered, ["campaign_type", "campaign_name", "adgroup_name", "ad_name", "device_name"])
            ad_src = _prepare_device_frame(ad_src)
            ad_src["campaign_type"] = ad_src["campaign_type"].map(_normalize_type_label)
            ad_src = ad_src.rename(columns={"device_name": "기기", "campaign_type": "유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹", "ad_name": "소재"})
            ad_src = ad_src.sort_values("cost", ascending=False)
            disp3 = _format_display(ad_src, ["유형", "캠페인", "광고그룹", "소재", "기기"])
            desc = "소재 기준 PC/모바일 성과입니다."
            if selected_groups:
                desc = f"선택 그룹 {len(selected_groups):,}개 기준입니다."
            _render_section_title("소재별 기기 상세", desc)
            _render_table(disp3)


def _render_age_tab(engine, f: Dict) -> None:
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))

    by_camp_all = _query_age(engine, f["start"], f["end"], cids, type_sel, by_campaign=True)
    by_group_all = _query_adgroup_age(engine, f["start"], f["end"], cids, type_sel)
    if by_camp_all.empty and by_group_all.empty:
        st.info("연령대별 데이터가 아직 없습니다. 연령대 수집 데이터가 제공되는 캠페인부터 표시됩니다.")
        return

    filter_base = by_group_all if not by_group_all.empty else by_camp_all
    filtered, selected_campaigns, selected_groups = _filter_campaign_and_group(
        filter_base,
        campaign_key="ta_age_campaign_filter_v10",
        group_key="ta_age_adgroup_filter_v10",
        desc="캠페인/광고그룹을 선택하면 KPI, 차트, 표가 같은 조건으로 바뀝니다.",
    )
    if filtered.empty:
        st.info("선택한 캠페인/그룹 조건에 해당하는 연령대별 데이터가 없습니다.")
        return

    age = _aggregate_metrics(filtered, ["age_range"])
    if age.empty:
        st.info("선택 조건에 해당하는 연령대별 데이터가 없습니다.")
        return

    summary = age[_metric_columns()].sum().to_frame().T
    _kpi_row(summary)

    chart = _prepare_age_frame(_add_calc_cols(age))
    chart = chart.rename(columns={"age_range": "연령대"})
    _render_overview_like_chart(chart, "연령대", key="ta_age_trend_view_v12", title_prefix="연령대별")

    tab_summary, tab_campaign, tab_group = st.tabs(["연령대 요약", "캠페인별", "그룹별"])
    with tab_summary:
        _render_section_title("연령대별 상세", "쇼핑 캠페인에서 제공되는 연령대 breakdown 기준입니다.")
        disp = _format_display(chart, ["연령대"])
        _render_table(disp)

    with tab_campaign:
        if not by_group_all.empty:
            camp_src = _aggregate_metrics(filtered, ["campaign_type", "campaign_name", "age_range"])
        else:
            camp_src = filtered.copy()
        camp_src = _prepare_age_frame(camp_src)
        camp_src["campaign_type"] = camp_src["campaign_type"].map(_normalize_type_label)
        camp_src = camp_src.rename(columns={"age_range": "연령대", "campaign_type": "유형", "campaign_name": "캠페인"})
        camp_src = camp_src.sort_values("cost", ascending=False)
        disp2 = _format_display(camp_src, ["유형", "캠페인", "연령대"])
        desc = "상단 캠페인/그룹 필터가 적용된 캠페인별 연령대 데이터입니다."
        if selected_campaigns:
            desc = f"선택 캠페인 {len(selected_campaigns):,}개 기준입니다."
        if selected_groups:
            desc += f" 선택 그룹 {len(selected_groups):,}개만 반영했습니다."
        _render_section_title("캠페인별 연령대 상세", desc)
        _render_table(disp2)

    with tab_group:
        if by_group_all.empty:
            st.info("그룹별 연령대 데이터가 아직 없습니다. 그룹 단위 수집 데이터가 쌓이면 이 표가 활성화됩니다.")
        else:
            group_src = _aggregate_metrics(filtered, ["campaign_type", "campaign_name", "adgroup_name", "age_range"])
            group_src = _prepare_age_frame(group_src)
            group_src["campaign_type"] = group_src["campaign_type"].map(_normalize_type_label)
            group_src = group_src.rename(columns={"age_range": "연령대", "campaign_type": "유형", "campaign_name": "캠페인", "adgroup_name": "광고그룹"})
            group_src = group_src.sort_values("cost", ascending=False)
            disp3 = _format_display(group_src, ["유형", "캠페인", "광고그룹", "연령대"])
            desc = "상단 광고그룹 필터로 원하는 그룹만 좁혀 볼 수 있습니다."
            if selected_groups:
                desc = f"선택 그룹 {len(selected_groups):,}개 기준입니다."
            _render_section_title("그룹별 연령대 상세", desc)
            _render_table(disp3)


def page_time_age(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown(
        """
        <style>
        .ta-kpi-card{border:1px solid #E5E7EB;border-radius:10px;padding:14px 16px;background:#fff;box-shadow:0 2px 8px rgba(15,23,42,.045);transform:none!important;transition:none!important;}
        .ta-kpi-card:hover{transform:none!important;box-shadow:0 2px 8px rgba(15,23,42,.045)!important;}
        .ta-kpi-label{font-size:12px;font-weight:750;color:#64748B;margin-bottom:7px;letter-spacing:-.01em;}
        .ta-kpi-value{font-size:22px;font-weight:850;color:#0F172A;line-height:1.1;letter-spacing:-.02em;}
        .ta-section-head{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;margin:22px 0 10px;padding:0 2px;}
        .ta-section-title{font-size:15px;font-weight:850;color:#0F172A;letter-spacing:-.02em;}
        .ta-section-desc{font-size:12px;font-weight:650;color:#64748B;text-align:right;line-height:1.35;}
        [data-baseweb="tab-list"]{margin-top:14px!important;}
        div[data-baseweb="select"]{transform:none!important;transition:none!important;}
        div[data-testid="stIFrame"]{overflow:hidden!important;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.caption("선택한 기간/계정 기준으로 시간대·연령대·기기별 성과를 비교합니다. 캠페인과 광고그룹 필터를 쓰면 KPI, 차트, 표가 함께 좁혀집니다.")

    if not any(table_exists(engine, table) for table in ["fact_campaign_hourly_daily", "fact_campaign_age_daily", "fact_campaign_device_daily", "fact_ad_device_daily"]):
        st.info("시간대/연령대/기기별 수집 테이블이 아직 없습니다. 관련 데이터 수집 후 자동으로 표시됩니다.")
        return

    tab_hour, tab_age, tab_device = st.tabs(["시간대별", "연령대별", "기기별"])
    with tab_hour:
        _render_hour_tab(engine, f)
    with tab_age:
        _render_age_tab(engine, f)
    with tab_device:
        _render_device_tab(engine, f)
