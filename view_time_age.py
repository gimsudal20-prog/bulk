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
        "UNKNOWN": "알 수 없음",
        "ETC": "기타",
        "NONE": "미분류",
        "-": "미분류",
    }
    return mapping.get(upper, raw)


def _table_height(df: pd.DataFrame, *, min_height: int = 260, max_height: int = 560) -> int:
    if df is None or df.empty:
        return min_height
    rows = len(df)
    return int(max(min_height, min(max_height, 54 + rows * 36)))


def _style_display_table(df: pd.DataFrame, key_cols: Iterable[str] | None = None):
    """Apply a light, readable table style while keeping Streamlit sorting usable."""
    if df is None or df.empty:
        return df
    key_cols = set(key_cols or [])

    def _style_col(col: pd.Series):
        if col.name in key_cols:
            return ["font-weight:700;color:#0F172A;background-color:#F8FAFC;" for _ in col]
        if col.name in {"광고비", "전환수", "전환매출", "ROAS(%)"}:
            return ["font-weight:650;color:#111827;" for _ in col]
        return ["" for _ in col]

    return (
        df.style
        .apply(_style_col, axis=0)
        .set_table_styles([
            {"selector": "thead th", "props": [("background-color", "#F8FAFC"), ("color", "#334155"), ("font-weight", "800")]},
            {"selector": "tbody td", "props": [("border-bottom", "1px solid #EEF2F7")]},
        ])
    )


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


def _render_table(df: pd.DataFrame, *, key_cols: Iterable[str] | None = None, height: int | None = None) -> None:
    if df is None or df.empty:
        st.info("표시할 데이터가 없습니다.")
        return
    st.dataframe(
        _style_display_table(df, key_cols=key_cols),
        width="stretch",
        height=height or _table_height(df),
        hide_index=True,
        column_config=_column_config(df),
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


def _query_adgroup_hourly(engine, d1, d2, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    """Optional adgroup-level hourly view if a future collector/table exists."""
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
    """Optional adgroup-level age view if a future collector/table exists."""
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
        HAVING SUM(CAST(COALESCE(f.imp,0) AS NUMERIC)) + SUM(CAST(COALESCE(f.clk,0) AS NUMERIC)) + SUM(CAST(COALESCE(f.cost,0) AS NUMERIC)) > 0
        ORDER BY cost DESC
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
    chart["시간대"] = chart["hour_of_day"].map(_format_hour_range)
    chart = chart.sort_values("hour_of_day")
    _render_section_title("시간대별 광고비", "차트는 고정형으로 표시되며 스크롤/호버 중 확대되지 않도록 처리했습니다.")
    st.bar_chart(chart.set_index("시간대")[["cost"]], height=260)

    tab_summary, tab_campaign, tab_group = st.tabs(["시간대 요약", "캠페인별", "그룹별"])
    with tab_summary:
        _render_section_title("시간대별 상세", "시간 표시는 00시~01시 형식으로 통일했습니다.")
        disp = _format_display(chart.rename(columns={"hour_of_day": "시간"}), ["시간"])
        disp["시간"] = chart["시간대"].values
        _render_table(disp, key_cols=["시간"])

    with tab_campaign:
        by_camp = _query_hourly(engine, f["start"], f["end"], tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), by_campaign=True)
        if by_camp.empty:
            st.info("캠페인별 상세 데이터가 없습니다.")
        else:
            by_camp["hour_of_day"] = pd.to_numeric(by_camp["hour_of_day"], errors="coerce").fillna(0).astype(int)
            by_camp = by_camp.rename(columns={"hour_of_day": "시간", "campaign_name": "캠페인", "campaign_type": "유형"})
            by_camp["시간"] = by_camp["시간"].map(_format_hour_range)
            disp2 = _format_display(by_camp, ["유형", "캠페인", "시간"])
            _render_section_title("캠페인별 시간대 상세", "비용이 큰 캠페인부터 정렬됩니다.")
            _render_table(disp2, key_cols=["유형", "캠페인", "시간"])

    with tab_group:
        by_group = _query_adgroup_hourly(engine, f["start"], f["end"], tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])))
        if by_group.empty:
            st.info("그룹별 시간대 데이터가 없습니다. 현재 수집 테이블은 캠페인 기준으로 저장되어 있어, 그룹별 breakdown 수집 테이블이 있을 때 자동으로 표시됩니다.")
        else:
            by_group["hour_of_day"] = pd.to_numeric(by_group["hour_of_day"], errors="coerce").fillna(0).astype(int)
            by_group = by_group.rename(columns={"hour_of_day": "시간", "campaign_name": "캠페인", "campaign_type": "유형", "adgroup_name": "광고그룹"})
            by_group["시간"] = by_group["시간"].map(_format_hour_range)
            disp3 = _format_display(by_group, ["유형", "캠페인", "광고그룹", "시간"])
            _render_section_title("그룹별 시간대 상세", "그룹 단위 수집 테이블이 있는 경우 실제 그룹별 성과를 표시합니다.")
            _render_table(disp3, key_cols=["유형", "캠페인", "광고그룹", "시간"])


def _render_age_tab(engine, f: Dict) -> None:
    age = _query_age(engine, f["start"], f["end"], tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])))
    if age.empty:
        st.info("연령대별 데이터가 아직 없습니다. 패치 적용 후 해당 날짜를 다시 수집하면 표시됩니다. 단, 계정/캠페인별 API 응답 가능 여부에 따라 빈 값일 수 있습니다.")
        return

    summary = age[["imp", "clk", "cost", "conv", "sales"]].sum().to_frame().T
    _kpi_row(summary)

    chart = _add_calc_cols(age).copy()
    chart = chart.rename(columns={"age_range": "연령대"})
    chart["연령대"] = chart["연령대"].map(_normalize_age_label)
    _render_section_title("연령대별 광고비", "연령 구간별 비용 분포를 고정형 차트로 표시합니다.")
    st.bar_chart(chart.set_index("연령대")[["cost"]], height=260)

    tab_summary, tab_campaign, tab_group = st.tabs(["연령대 요약", "캠페인별", "그룹별"])
    with tab_summary:
        _render_section_title("연령대별 상세", "쇼핑 캠페인에서 제공되는 연령대 breakdown 기준입니다.")
        disp = _format_display(chart, ["연령대"])
        _render_table(disp, key_cols=["연령대"])

    with tab_campaign:
        by_camp = _query_age(engine, f["start"], f["end"], tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])), by_campaign=True)
        if by_camp.empty:
            st.info("캠페인별 상세 데이터가 없습니다.")
        else:
            by_camp = by_camp.rename(columns={"age_range": "연령대", "campaign_name": "캠페인", "campaign_type": "유형"})
            by_camp["연령대"] = by_camp["연령대"].map(_normalize_age_label)
            disp2 = _format_display(by_camp, ["유형", "캠페인", "연령대"])
            _render_section_title("캠페인별 연령대 상세", "비용이 큰 캠페인부터 정렬됩니다.")
            _render_table(disp2, key_cols=["유형", "캠페인", "연령대"])

    with tab_group:
        by_group = _query_adgroup_age(engine, f["start"], f["end"], tuple(f.get("selected_customer_ids", [])), tuple(f.get("type_sel", [])))
        if by_group.empty:
            st.info("그룹별 연령대 데이터가 없습니다. 현재 수집 테이블은 캠페인 기준으로 저장되어 있어, 그룹별 breakdown 수집 테이블이 있을 때 자동으로 표시됩니다.")
        else:
            by_group = by_group.rename(columns={"age_range": "연령대", "campaign_name": "캠페인", "campaign_type": "유형", "adgroup_name": "광고그룹"})
            by_group["연령대"] = by_group["연령대"].map(_normalize_age_label)
            disp3 = _format_display(by_group, ["유형", "캠페인", "광고그룹", "연령대"])
            _render_section_title("그룹별 연령대 상세", "그룹 단위 수집 테이블이 있는 경우 실제 그룹별 성과를 표시합니다.")
            _render_table(disp3, key_cols=["유형", "캠페인", "광고그룹", "연령대"])


def page_time_age(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown(
        """
        <style>
        .ta-kpi-card{border:1px solid #E5E7EB;border-radius:16px;padding:15px 17px;background:#fff;box-shadow:0 6px 18px rgba(15,23,42,.045);transform:none!important;transition:none!important;}
        .ta-kpi-card:hover{transform:none!important;box-shadow:0 6px 18px rgba(15,23,42,.045)!important;}
        .ta-kpi-label{font-size:12px;font-weight:750;color:#64748B;margin-bottom:7px;letter-spacing:-.01em;}
        .ta-kpi-value{font-size:22px;font-weight:850;color:#0F172A;line-height:1.1;letter-spacing:-.02em;}
        .ta-section-head{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;margin:22px 0 10px;padding:0 2px;}
        .ta-section-title{font-size:15px;font-weight:850;color:#0F172A;letter-spacing:-.02em;}
        .ta-section-desc{font-size:12px;font-weight:650;color:#64748B;text-align:right;line-height:1.35;}
        [data-testid="stVegaLiteChart"], [data-testid="stDataFrame"]{transform:none!important;transition:none!important;}
        [data-testid="stVegaLiteChart"] canvas,
        [data-testid="stVegaLiteChart"] svg{overscroll-behavior:contain;touch-action:pan-y;}
        [data-testid="stDataFrame"]{border-radius:14px!important;border:1px solid #E2E8F0!important;box-shadow:0 4px 14px rgba(15,23,42,.035)!important;}
        [data-testid="stDataFrame"] *{transform:none!important;}
        [data-baseweb="tab-list"]{margin-top:14px!important;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.caption("시간대는 /stats hh24 breakdown, 연령대는 쇼핑 캠페인 /stats ageRangeNm breakdown 기반으로 표시됩니다. 시간 표시는 00시~01시 형식입니다.")

    if not table_exists(engine, "fact_campaign_hourly_daily") and not table_exists(engine, "fact_campaign_age_daily"):
        st.info("시간대/연령대 수집 테이블이 아직 없습니다. 패치 적용 후 수집기를 한 번 실행하면 자동 생성됩니다.")
        return

    tab_hour, tab_age = st.tabs(["시간대별", "연령대별"])
    with tab_hour:
        _render_hour_tab(engine, f)
    with tab_age:
        _render_age_tab(engine, f)
