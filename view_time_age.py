# -*- coding: utf-8 -*-
"""view_time_age.py - Hourly and age-range performance dashboard."""
from __future__ import annotations

from html import escape
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd
import streamlit as st

from data import get_table_columns, sql_read, table_exists
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


def _render_table(df: pd.DataFrame) -> None:
    """Use Streamlit's native dataframe so header click sorting stays available."""
    if df is None or df.empty:
        st.info("표시할 데이터가 없습니다.")
        return
    st.dataframe(
        df.copy(),
        width="stretch",
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


def _render_static_bar_chart(df: pd.DataFrame, label_col: str, value_col: str, *, value_label: str = "광고비") -> None:
    """Render a non-interactive horizontal bar chart to avoid wheel/hover zoom side effects."""
    if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
        st.info("차트로 표시할 데이터가 없습니다.")
        return
    work = df[[label_col, value_col]].copy()
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce").fillna(0)
    max_val = float(work[value_col].max() or 0)
    rows_html = []
    for _, row in work.iterrows():
        label = escape(str(row.get(label_col, "")))
        val = float(row.get(value_col, 0) or 0)
        pct = 0 if max_val <= 0 else max(1.5, min(100, val / max_val * 100))
        val_text = f"{val:,.0f}원" if value_label == "광고비" else f"{val:,.0f}"
        rows_html.append(
            f"""
            <div class='ta-chart-row'>
                <div class='ta-chart-label'>{label}</div>
                <div class='ta-chart-track'><div class='ta-chart-bar' style='width:{pct:.2f}%'></div></div>
                <div class='ta-chart-value'>{escape(val_text)}</div>
            </div>
            """
        )
    st.markdown(
        f"<div class='ta-chart-card'>{''.join(rows_html)}</div>",
        unsafe_allow_html=True,
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


def _render_hour_tab(engine, f: Dict) -> None:
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))

    by_camp_all = _query_hourly(engine, f["start"], f["end"], cids, type_sel, by_campaign=True)
    by_group_all = _query_adgroup_hourly(engine, f["start"], f["end"], cids, type_sel)
    if by_camp_all.empty and by_group_all.empty:
        st.info("시간대별 데이터가 아직 없습니다. 패치 적용 후 해당 날짜를 다시 수집하면 표시됩니다.")
        return

    filter_base = by_group_all if not by_group_all.empty else by_camp_all
    filtered, selected_campaigns, selected_groups = _filter_campaign_and_group(
        filter_base,
        campaign_key="ta_hour_campaign_filter_v10",
        group_key="ta_hour_adgroup_filter_v10",
        desc="캠페인/광고그룹을 선택하면 시간대 요약, 캠페인별, 그룹별 표가 동일 조건으로 변경됩니다.",
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
    _render_section_title("시간대별 광고비", "표는 기존 dataframe 방식이라 헤더 클릭 정렬이 가능합니다.")
    _render_static_bar_chart(chart, "시간대", "cost")

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
            st.info("그룹별 시간대 데이터가 아직 없습니다. 이번 패치의 수집기 보강 후 시간·연령 수집을 다시 실행하면 그룹 필터/표가 활성화됩니다.")
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


def _render_age_tab(engine, f: Dict) -> None:
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))

    by_camp_all = _query_age(engine, f["start"], f["end"], cids, type_sel, by_campaign=True)
    by_group_all = _query_adgroup_age(engine, f["start"], f["end"], cids, type_sel)
    if by_camp_all.empty and by_group_all.empty:
        st.info("연령대별 데이터가 아직 없습니다. 패치 적용 후 해당 날짜를 다시 수집하면 표시됩니다. 단, 계정/캠페인별 API 응답 가능 여부에 따라 빈 값일 수 있습니다.")
        return

    filter_base = by_group_all if not by_group_all.empty else by_camp_all
    filtered, selected_campaigns, selected_groups = _filter_campaign_and_group(
        filter_base,
        campaign_key="ta_age_campaign_filter_v10",
        group_key="ta_age_adgroup_filter_v10",
        desc="캠페인/광고그룹을 선택하면 연령대 요약, 캠페인별, 그룹별 표가 동일 조건으로 변경됩니다.",
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
    _render_section_title("연령대별 광고비", "표는 기존 dataframe 방식이라 헤더 클릭 정렬이 가능합니다.")
    _render_static_bar_chart(chart, "연령대", "cost")

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
            st.info("그룹별 연령대 데이터가 아직 없습니다. 이번 패치의 수집기 보강 후 시간·연령 수집을 다시 실행하면 그룹 필터/표가 활성화됩니다.")
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
        .ta-kpi-card{border:1px solid #E5E7EB;border-radius:16px;padding:15px 17px;background:#fff;box-shadow:0 6px 18px rgba(15,23,42,.045);transform:none!important;transition:none!important;}
        .ta-kpi-card:hover{transform:none!important;box-shadow:0 6px 18px rgba(15,23,42,.045)!important;}
        .ta-kpi-label{font-size:12px;font-weight:750;color:#64748B;margin-bottom:7px;letter-spacing:-.01em;}
        .ta-kpi-value{font-size:22px;font-weight:850;color:#0F172A;line-height:1.1;letter-spacing:-.02em;}
        .ta-section-head{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;margin:22px 0 10px;padding:0 2px;}
        .ta-section-title{font-size:15px;font-weight:850;color:#0F172A;letter-spacing:-.02em;}
        .ta-section-desc{font-size:12px;font-weight:650;color:#64748B;text-align:right;line-height:1.35;}
        .ta-chart-card{border:1px solid #E2E8F0;border-radius:16px;background:#fff;box-shadow:0 6px 18px rgba(15,23,42,.035);padding:14px 16px;margin:8px 0 16px;user-select:none;}
        .ta-chart-row{display:grid;grid-template-columns:minmax(84px,140px) 1fr minmax(84px,104px);gap:10px;align-items:center;margin:8px 0;}
        .ta-chart-label{font-size:12px;font-weight:750;color:#334155;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
        .ta-chart-track{height:12px;border-radius:999px;background:#F1F5F9;overflow:hidden;}
        .ta-chart-bar{height:100%;border-radius:999px;background:#CBD5E1;}
        .ta-chart-value{font-size:12px;font-weight:750;color:#475569;text-align:right;font-variant-numeric:tabular-nums;}
        [data-baseweb="tab-list"]{margin-top:14px!important;}
        div[data-baseweb="select"]{transform:none!important;transition:none!important;}
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
