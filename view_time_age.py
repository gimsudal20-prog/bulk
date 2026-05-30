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


def _normalize_type_label(value) -> str:
    raw = str(value or "").strip()
    return TYPE_LABEL_MAP.get(raw, raw or "미분류")


def _metric_columns() -> list[str]:
    return ["imp", "clk", "cost", "conv", "sales"]


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


def _campaign_filter(df: pd.DataFrame, *, key: str, label: str = "캠페인 필터") -> tuple[pd.DataFrame, list[str]]:
    if df is None or df.empty or "campaign_name" not in df.columns:
        return df if df is not None else pd.DataFrame(), []
    options = _sort_options_by_cost(df, "campaign_name")
    if not options:
        return df, []
    selected = st.multiselect(
        label,
        options,
        default=[],
        key=key,
        help="미선택 시 전체 캠페인을 표시합니다.",
        placeholder="캠페인 선택 또는 전체",
    )
    if not selected:
        return df, []
    return df[df["campaign_name"].astype(str).isin(selected)].copy(), selected


def _adgroup_filter(df: pd.DataFrame, *, key: str, campaign_selected: list[str] | None = None) -> tuple[pd.DataFrame, list[str]]:
    if df is None or df.empty or "adgroup_name" not in df.columns:
        return df if df is not None else pd.DataFrame(), []
    work = df.copy()
    if campaign_selected and "campaign_name" in work.columns:
        work = work[work["campaign_name"].astype(str).isin(campaign_selected)].copy()
    options = _sort_options_by_cost(work, "adgroup_name")
    if not options:
        return work, []
    selected = st.multiselect(
        "그룹 필터",
        options,
        default=[],
        key=key,
        help="미선택 시 선택된 캠페인의 전체 그룹을 표시합니다.",
        placeholder="그룹 선택 또는 전체",
    )
    if not selected:
        return work, []
    return work[work["adgroup_name"].astype(str).isin(selected)].copy(), selected


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


def _format_table_cell(column: str, value) -> str:
    if pd.isna(value):
        return ""
    if column in {"노출", "클릭", "광고비", "전환매출"}:
        try:
            suffix = "원" if column in {"광고비", "전환매출"} else ""
            return f"{float(value):,.0f}{suffix}"
        except Exception:
            return escape(str(value))
    if column in {"전환수"}:
        try:
            return f"{float(value):,.1f}"
        except Exception:
            return escape(str(value))
    if column in {"CTR(%)", "ROAS(%)"}:
        try:
            return f"{float(value):,.1f}%"
        except Exception:
            return escape(str(value))
    if column == "CPC":
        try:
            return f"{float(value):,.0f}원"
        except Exception:
            return escape(str(value))
    return escape(str(value))


def _limit_rows_for_static_table(df: pd.DataFrame, *, key: str) -> pd.DataFrame:
    if df is None or df.empty or len(df) <= 30:
        return df
    options = ["30개", "50개", "100개", "전체"]
    label = st.selectbox("표시 행 수", options, index=1 if len(df) > 50 else 0, key=f"{key}_limit")
    if label == "전체":
        st.caption(f"전체 {len(df):,}개 행을 표시합니다. 내부 표 스크롤 없이 페이지 스크롤로만 이동합니다.")
        return df
    limit = int(label.replace("개", ""))
    st.caption(f"전체 {len(df):,}개 중 상위 {min(limit, len(df)):,}개 표시 · 내부 표 스크롤 없음")
    return df.head(limit).copy()


def _render_table(df: pd.DataFrame, *, key_cols: Iterable[str] | None = None, table_key: str = "table") -> None:
    """Render a static, non-scroll table so the dataframe does not zoom/scroll internally."""
    if df is None or df.empty:
        st.info("표시할 데이터가 없습니다.")
        return
    view = _limit_rows_for_static_table(df.copy(), key=table_key)
    key_cols = set(key_cols or [])
    num_cols = {"노출", "클릭", "CTR(%)", "CPC", "광고비", "전환수", "전환매출", "ROAS(%)"}
    header_html = "".join(f"<th>{escape(str(c))}</th>" for c in view.columns)
    rows_html = []
    for _, row in view.iterrows():
        cells = []
        for c in view.columns:
            classes = []
            if c in key_cols:
                classes.append("key")
            if c in num_cols:
                classes.append("num")
            class_attr = f" class='{' '.join(classes)}'" if classes else ""
            cells.append(f"<td{class_attr}>{_format_table_cell(c, row.get(c))}</td>")
        rows_html.append("<tr>" + "".join(cells) + "</tr>")
    st.markdown(
        f"""
        <div class='ta-static-table-wrap'>
            <table class='ta-static-table'>
                <thead><tr>{header_html}</tr></thead>
                <tbody>{''.join(rows_html)}</tbody>
            </table>
        </div>
        """,
        unsafe_allow_html=True,
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
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    by_camp_all = _query_hourly(engine, f["start"], f["end"], cids, type_sel, by_campaign=True)
    if by_camp_all.empty:
        st.info("시간대별 데이터가 아직 없습니다. 패치 적용 후 해당 날짜를 다시 수집하면 표시됩니다.")
        return

    _render_section_title("캠페인 필터", "미선택 시 전체 캠페인 기준으로 요약/차트/표가 표시됩니다.")
    by_camp_filtered, selected_campaigns = _campaign_filter(by_camp_all, key="ta_hour_campaign_filter")

    hourly = _aggregate_metrics(by_camp_filtered, ["hour_of_day"])
    if hourly.empty:
        st.info("선택한 캠페인 조건에 해당하는 시간대별 데이터가 없습니다.")
        return

    summary = hourly[["imp", "clk", "cost", "conv", "sales"]].sum().to_frame().T
    _kpi_row(summary)

    chart = _add_calc_cols(hourly).copy()
    chart["hour_of_day"] = pd.to_numeric(chart["hour_of_day"], errors="coerce").fillna(0).astype(int)
    chart["시간대"] = chart["hour_of_day"].map(_format_hour_range)
    chart = chart.sort_values("hour_of_day")
    _render_section_title("시간대별 광고비", "선택한 캠페인 기준으로 차트와 표가 함께 바뀝니다.")
    st.bar_chart(chart.set_index("시간대")[["cost"]], height=260)

    tab_summary, tab_campaign, tab_group = st.tabs(["시간대 요약", "캠페인별", "그룹별"])
    with tab_summary:
        _render_section_title("시간대별 상세", "시간 표시는 00시~01시 형식으로 통일했습니다.")
        disp = _format_display(chart.rename(columns={"hour_of_day": "시간"}), ["시간"])
        disp["시간"] = chart["시간대"].values
        _render_table(disp, key_cols=["시간"], table_key="hour_summary")

    with tab_campaign:
        work = by_camp_filtered.copy()
        work["hour_of_day"] = pd.to_numeric(work["hour_of_day"], errors="coerce").fillna(0).astype(int)
        work["campaign_type"] = work["campaign_type"].map(_normalize_type_label)
        work = work.rename(columns={"hour_of_day": "시간", "campaign_name": "캠페인", "campaign_type": "유형"})
        work["시간"] = work["시간"].map(_format_hour_range)
        # 캠페인별 탭은 캠페인/시간 조합을 그대로 보여준다. 비용 큰 순서로 정렬.
        work = work.sort_values("cost", ascending=False)
        disp2 = _format_display(work, ["유형", "캠페인", "시간"])
        _render_section_title("캠페인별 시간대 상세", "상단 캠페인 필터로 원하는 캠페인만 좁혀 볼 수 있습니다.")
        _render_table(disp2, key_cols=["유형", "캠페인", "시간"], table_key="hour_campaign")

    with tab_group:
        by_group = _query_adgroup_hourly(engine, f["start"], f["end"], cids, type_sel)
        if by_group.empty:
            st.info("현재 시간·연령 수집 데이터는 캠페인 단위까지만 저장되어 있어 그룹별 필터/표시는 제공할 수 없습니다. 그룹별 시간·연령 테이블이 추가되면 이 탭에서 바로 필터링됩니다.")
        else:
            by_group, selected_groups = _adgroup_filter(by_group, key="ta_hour_adgroup_filter", campaign_selected=selected_campaigns)
            by_group["hour_of_day"] = pd.to_numeric(by_group["hour_of_day"], errors="coerce").fillna(0).astype(int)
            by_group["campaign_type"] = by_group["campaign_type"].map(_normalize_type_label)
            by_group = by_group.rename(columns={"hour_of_day": "시간", "campaign_name": "캠페인", "campaign_type": "유형", "adgroup_name": "광고그룹"})
            by_group["시간"] = by_group["시간"].map(_format_hour_range)
            by_group = by_group.sort_values("cost", ascending=False)
            disp3 = _format_display(by_group, ["유형", "캠페인", "광고그룹", "시간"])
            desc = "그룹 필터 미선택 시 선택 캠페인의 전체 그룹이 표시됩니다."
            if selected_groups:
                desc = f"선택 그룹 {len(selected_groups):,}개 기준입니다."
            _render_section_title("그룹별 시간대 상세", desc)
            _render_table(disp3, key_cols=["유형", "캠페인", "광고그룹", "시간"], table_key="hour_group")


def _render_age_tab(engine, f: Dict) -> None:
    cids = tuple(f.get("selected_customer_ids", []))
    type_sel = tuple(f.get("type_sel", []))
    by_camp_all = _query_age(engine, f["start"], f["end"], cids, type_sel, by_campaign=True)
    if by_camp_all.empty:
        st.info("연령대별 데이터가 아직 없습니다. 패치 적용 후 해당 날짜를 다시 수집하면 표시됩니다. 단, 계정/캠페인별 API 응답 가능 여부에 따라 빈 값일 수 있습니다.")
        return

    _render_section_title("캠페인 필터", "미선택 시 전체 캠페인 기준으로 요약/차트/표가 표시됩니다.")
    by_camp_filtered, selected_campaigns = _campaign_filter(by_camp_all, key="ta_age_campaign_filter")

    age = _aggregate_metrics(by_camp_filtered, ["age_range"])
    if age.empty:
        st.info("선택한 캠페인 조건에 해당하는 연령대별 데이터가 없습니다.")
        return

    summary = age[["imp", "clk", "cost", "conv", "sales"]].sum().to_frame().T
    _kpi_row(summary)

    chart = _add_calc_cols(age).copy()
    chart = chart.rename(columns={"age_range": "연령대"})
    chart["연령대"] = chart["연령대"].map(_normalize_age_label)
    _render_section_title("연령대별 광고비", "선택한 캠페인 기준으로 연령대 분포를 표시합니다.")
    st.bar_chart(chart.set_index("연령대")[["cost"]], height=260)

    tab_summary, tab_campaign, tab_group = st.tabs(["연령대 요약", "캠페인별", "그룹별"])
    with tab_summary:
        _render_section_title("연령대별 상세", "쇼핑 캠페인에서 제공되는 연령대 breakdown 기준입니다.")
        disp = _format_display(chart, ["연령대"])
        _render_table(disp, key_cols=["연령대"], table_key="age_summary")

    with tab_campaign:
        work = by_camp_filtered.copy()
        work["campaign_type"] = work["campaign_type"].map(_normalize_type_label)
        work = work.rename(columns={"age_range": "연령대", "campaign_name": "캠페인", "campaign_type": "유형"})
        work["연령대"] = work["연령대"].map(_normalize_age_label)
        work = work.sort_values("cost", ascending=False)
        disp2 = _format_display(work, ["유형", "캠페인", "연령대"])
        _render_section_title("캠페인별 연령대 상세", "상단 캠페인 필터로 원하는 캠페인만 좁혀 볼 수 있습니다.")
        _render_table(disp2, key_cols=["유형", "캠페인", "연령대"], table_key="age_campaign")

    with tab_group:
        by_group = _query_adgroup_age(engine, f["start"], f["end"], cids, type_sel)
        if by_group.empty:
            st.info("현재 시간·연령 수집 데이터는 캠페인 단위까지만 저장되어 있어 그룹별 필터/표시는 제공할 수 없습니다. 그룹별 시간·연령 테이블이 추가되면 이 탭에서 바로 필터링됩니다.")
        else:
            by_group, selected_groups = _adgroup_filter(by_group, key="ta_age_adgroup_filter", campaign_selected=selected_campaigns)
            by_group["campaign_type"] = by_group["campaign_type"].map(_normalize_type_label)
            by_group = by_group.rename(columns={"age_range": "연령대", "campaign_name": "캠페인", "campaign_type": "유형", "adgroup_name": "광고그룹"})
            by_group["연령대"] = by_group["연령대"].map(_normalize_age_label)
            by_group = by_group.sort_values("cost", ascending=False)
            disp3 = _format_display(by_group, ["유형", "캠페인", "광고그룹", "연령대"])
            desc = "그룹 필터 미선택 시 선택 캠페인의 전체 그룹이 표시됩니다."
            if selected_groups:
                desc = f"선택 그룹 {len(selected_groups):,}개 기준입니다."
            _render_section_title("그룹별 연령대 상세", desc)
            _render_table(disp3, key_cols=["유형", "캠페인", "광고그룹", "연령대"], table_key="age_group")


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
        .ta-static-table-wrap{width:100%;border:1px solid #E2E8F0;border-radius:16px;background:#fff;box-shadow:0 6px 18px rgba(15,23,42,.035);margin:8px 0 18px;overflow:visible!important;}
        .ta-static-table{width:100%;border-collapse:separate;border-spacing:0;table-layout:auto;font-size:13px;color:#0F172A;}
        .ta-static-table th{background:#F8FAFC;color:#475569;font-size:12px;font-weight:850;text-align:left;padding:11px 12px;border-bottom:1px solid #E2E8F0;white-space:nowrap;}
        .ta-static-table td{padding:10px 12px;border-bottom:1px solid #EEF2F7;vertical-align:middle;line-height:1.35;word-break:keep-all;overflow-wrap:anywhere;}
        .ta-static-table tbody tr:last-child td{border-bottom:0;}
        .ta-static-table tbody tr:nth-child(even) td{background:#FCFCFD;}
        .ta-static-table td.key{font-weight:750;color:#0F172A;background:#F8FAFC;}
        .ta-static-table td.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap;font-weight:650;}
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
