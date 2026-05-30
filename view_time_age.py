# -*- coding: utf-8 -*-
"""view_time_age.py - Hourly and age-range performance dashboard."""
from __future__ import annotations

from html import escape
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy import text

from data import get_table_columns, sql_read, table_exists


TYPE_LABEL_MAP = {
    "WEB_SITE": "파워링크",
    "SHOPPING": "쇼핑검색",
    "POWER_CONTENTS": "파워컨텐츠",
    "BRAND_SEARCH": "브랜드검색",
    "PLACE": "플레이스",
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


def _add_calc_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    for c in ["imp", "clk", "cost", "conv", "sales"]:
        out[c] = pd.to_numeric(out.get(c, 0), errors="coerce").fillna(0)
    out["CTR(%)"] = np.where(out["imp"] > 0, out["clk"] / out["imp"] * 100.0, 0.0)
    out["CPC"] = np.where(out["clk"] > 0, out["cost"] / out["clk"], 0.0)
    out["ROAS(%)"] = np.where(out["cost"] > 0, out["sales"] / out["cost"] * 100.0, 0.0)
    return out


def _format_display(df: pd.DataFrame, first_cols: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = _add_calc_cols(df)
    rename = {
        "imp": "노출",
        "clk": "클릭",
        "cost": "광고비",
        "conv": "전환수",
        "sales": "전환매출",
    }
    out = out.rename(columns=rename)
    metric_cols = ["노출", "클릭", "CTR(%)", "CPC", "광고비", "전환수", "전환매출", "ROAS(%)"]
    cols = [c for c in first_cols + metric_cols if c in out.columns]
    return out[cols]


def _column_config(df: pd.DataFrame) -> dict:
    cfg = {}
    for c in df.columns:
        if c in ["노출", "클릭", "광고비", "전환매출"]:
            cfg[c] = st.column_config.NumberColumn(c, format="%,.0f")
        elif c in ["전환수"]:
            cfg[c] = st.column_config.NumberColumn(c, format="%,.1f")
        elif c in ["CTR(%)", "ROAS(%)"]:
            cfg[c] = st.column_config.NumberColumn(c, format="%.1f%%")
        elif c == "CPC":
            cfg[c] = st.column_config.NumberColumn("CPC", format="%,.0f원")
    return cfg


def _kpi_row(summary: pd.DataFrame) -> None:
    if summary is None or summary.empty:
        return
    row = _add_calc_cols(summary).iloc[0]
    cols = st.columns(5)
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
        HAVING SUM(CAST(COALESCE(f.imp,0) AS NUMERIC)) + SUM(CAST(COALESCE(f.clk,0) AS NUMERIC)) + SUM(CAST(COALESCE(f.cost,0) AS NUMERIC)) > 0
        ORDER BY {order_by}
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


def _render_hour_tab(engine, f: Dict) -> None:
    hourly = _query_hourly(engine, f["start"], f["end"], tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])))
    if hourly.empty:
        st.info("시간대별 데이터가 아직 없습니다. 패치 적용 후 해당 날짜를 다시 수집하면 표시됩니다.")
        return

    summary = hourly[["imp", "clk", "cost", "conv", "sales"]].sum().to_frame().T
    _kpi_row(summary)

    chart = _add_calc_cols(hourly).copy()
    chart["시간대"] = chart["hour_of_day"].astype(int).map(lambda x: f"{x:02d}시")
    chart = chart.sort_values("hour_of_day")
    st.markdown("#### 시간대별 광고비")
    st.bar_chart(chart.set_index("시간대")[["cost"]], height=260)

    st.markdown("#### 시간대별 상세")
    disp = _format_display(chart.rename(columns={"hour_of_day": "시간"}), ["시간"])
    disp["시간"] = disp["시간"].astype(int).map(lambda x: f"{x:02d}시")
    st.dataframe(disp, width="stretch", hide_index=True, column_config=_column_config(disp))

    with st.expander("캠페인별 시간대 상세", expanded=False):
        by_camp = _query_hourly(engine, f["start"], f["end"], tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), by_campaign=True)
        if by_camp.empty:
            st.info("캠페인별 상세 데이터가 없습니다.")
        else:
            by_camp["hour_of_day"] = pd.to_numeric(by_camp["hour_of_day"], errors="coerce").fillna(0).astype(int)
            by_camp = by_camp.rename(columns={"hour_of_day": "시간", "campaign_name": "캠페인", "campaign_type": "유형"})
            by_camp["시간"] = by_camp["시간"].map(lambda x: f"{x:02d}시")
            disp2 = _format_display(by_camp, ["유형", "캠페인", "시간"])
            st.dataframe(disp2, width="stretch", hide_index=True, column_config=_column_config(disp2))


def _render_age_tab(engine, f: Dict) -> None:
    age = _query_age(engine, f["start"], f["end"], tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])))
    if age.empty:
        st.info("연령대별 데이터가 아직 없습니다. 패치 적용 후 해당 날짜를 다시 수집하면 표시됩니다. 단, 계정/캠페인별 API 응답 가능 여부에 따라 빈 값일 수 있습니다.")
        return

    summary = age[["imp", "clk", "cost", "conv", "sales"]].sum().to_frame().T
    _kpi_row(summary)

    chart = _add_calc_cols(age).copy()
    chart = chart.rename(columns={"age_range": "연령대"})
    st.markdown("#### 연령대별 광고비")
    st.bar_chart(chart.set_index("연령대")[["cost"]], height=260)

    st.markdown("#### 연령대별 상세")
    disp = _format_display(chart, ["연령대"])
    st.dataframe(disp, width="stretch", hide_index=True, column_config=_column_config(disp))

    with st.expander("캠페인별 연령대 상세", expanded=False):
        by_camp = _query_age(engine, f["start"], f["end"], tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), by_campaign=True)
        if by_camp.empty:
            st.info("캠페인별 상세 데이터가 없습니다.")
        else:
            by_camp = by_camp.rename(columns={"age_range": "연령대", "campaign_name": "캠페인", "campaign_type": "유형"})
            disp2 = _format_display(by_camp, ["유형", "캠페인", "연령대"])
            st.dataframe(disp2, width="stretch", hide_index=True, column_config=_column_config(disp2))


def page_time_age(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown(
        """
        <style>
        .ta-kpi-card{border:1px solid #E5E7EB;border-radius:18px;padding:14px 16px;background:#fff;box-shadow:0 8px 22px rgba(15,23,42,.04);}
        .ta-kpi-label{font-size:12px;font-weight:700;color:#6B7280;margin-bottom:6px;}
        .ta-kpi-value{font-size:22px;font-weight:800;color:#111827;line-height:1.1;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.caption("시간대는 /stats hh24 breakdown, 연령대는 쇼핑 캠페인 /stats ageRangeNm breakdown 기반으로 표시됩니다.")

    if not table_exists(engine, "fact_campaign_hourly_daily") and not table_exists(engine, "fact_campaign_age_daily"):
        st.info("시간대/연령대 수집 테이블이 아직 없습니다. 패치 적용 후 수집기를 한 번 실행하면 자동 생성됩니다.")
        return

    tab_hour, tab_age = st.tabs(["시간대별", "연령대별"])
    with tab_hour:
        _render_hour_tab(engine, f)
    with tab_age:
        _render_age_tab(engine, f)
