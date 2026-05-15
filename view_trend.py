# -*- coding: utf-8 -*-
"""view_trend.py - 시장 트렌드(DataLab) vs 자사 노출수 비교 분석 탭"""

from __future__ import annotations
import os
import json
import urllib.request
import pandas as pd
import numpy as np
import streamlit as st
from datetime import date, timedelta
from typing import Dict
import altair as alt

from data import sql_read, get_table_columns, table_exists, _sql_in_str_list



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


def _render_diag_panel(diag: list | None) -> None:
    if not diag:
        return
    df = pd.DataFrame(diag)
    if df.empty:
        return
    status_order = {"error": 0, "zero_data": 1, "warn": 2, "ok": 3}
    if "status" in df.columns:
        df["_ord"] = df["status"].map(status_order).fillna(9)
        df = df.sort_values(["_ord", "step"], ascending=[True, True]).drop(columns=["_ord"])
    df = df.rename(columns={"step": "단계", "status": "상태", "rows": "건수", "source": "원천", "note": "메모"})
    with st.expander("조회 진단", expanded=False):
        st.caption("트렌드 화면에서 내부 노출 데이터와 데이터랩 조회 상태를 함께 확인하는 용도입니다.")
        st.dataframe(df, width="stretch", hide_index=True)

try:
    from streamlit_echarts import st_echarts
    HAS_ECHARTS = True
except Exception:
    st_echarts = None
    HAS_ECHARTS = False

@st.cache_data(show_spinner=False, ttl=900)
def _cached_datalab_trend(client_id: str, client_secret: str, keyword: str, start_date: date, end_date: date) -> pd.DataFrame:
    return get_datalab_trend(client_id, client_secret, keyword, start_date, end_date, diag=None)

def get_datalab_trend(client_id: str, client_secret: str, keyword: str, start_date: date, end_date: date, diag: list | None = None) -> pd.DataFrame:
    if not client_id or not client_secret:
        _diag_add(diag, "데이터랩 인증", "error", 0, "NAVER_DATALAB_CLIENT_ID/SECRET", "API 키가 비어 있습니다.")
        return pd.DataFrame()
    client_id = client_id.replace('"', '').replace("'", "").strip()
    client_secret = client_secret.replace('"', '').replace("'", "").strip()

    url = "https://openapi.naver.com/v1/datalab/search"
    s_date = start_date
    e_date = end_date
    if s_date == e_date: s_date = s_date - timedelta(days=1)

    body = {
        "startDate": s_date.strftime("%Y-%m-%d"),
        "endDate": e_date.strftime("%Y-%m-%d"),
        "timeUnit": "date",
        "keywordGroups": [{"groupName": keyword, "keywords": [keyword]}],
        "device": "", "ages": [], "gender": ""
    }

    req = urllib.request.Request(url)
    req.add_header("X-Naver-Client-Id", client_id)
    req.add_header("X-Naver-Client-Secret", client_secret)
    req.add_header("Content-Type", "application/json")

    try:
        response = urllib.request.urlopen(req, data=json.dumps(body).encode("utf-8"))
        if response.getcode() == 200:
            data = json.loads(response.read().decode('utf-8'))
            if "results" in data and len(data["results"]) > 0:
                df = pd.DataFrame(data["results"][0]["data"])
                df = df.rename(columns={"period": "dt", "ratio": "트렌드지수(%)"})
                df["dt"] = pd.to_datetime(df["dt"])
                _diag_add(diag, '데이터랩 조회', 'ok' if not df.empty else 'zero_data', len(df.index), 'Naver DataLab', f'검색어={keyword}')
                return df
    except Exception as e:
        _diag_add(diag, '데이터랩 조회', 'error', 0, 'Naver DataLab', f'{type(e).__name__}: {e}')
        return pd.DataFrame()

    _diag_add(diag, '데이터랩 조회', 'zero_data', 0, 'Naver DataLab', f'검색어={keyword} 결과 없음')
    return pd.DataFrame()

@st.cache_data(show_spinner=False, ttl=300)
def _cached_internal_daily_detail(_engine, d1: date, d2: date, cids: tuple) -> pd.DataFrame:
    return get_internal_daily_detail(_engine, d1, d2, cids, diag=None)

def get_internal_daily_detail(_engine, d1: date, d2: date, cids: tuple, diag: list | None = None) -> pd.DataFrame:
    df_list = []
    cid_str = _sql_in_str_list(list(cids))
    where_cid = f"AND f.customer_id::text IN ({cid_str})" if cids else ""
    
    has_camp = table_exists(_engine, "dim_campaign")
    has_grp = table_exists(_engine, "dim_adgroup")
    
    if table_exists(_engine, "fact_keyword_daily") and table_exists(_engine, "dim_keyword"):
        _diag_add(diag, "내부 원천", "ok", None, "fact_keyword_daily + dim_keyword", "키워드 일별 상세 기준")
        f_cols = get_table_columns(_engine, "fact_keyword_daily")
        sales_col = "f.sales" if "sales" in f_cols else "0"
        
        g_join = "LEFT JOIN dim_adgroup g ON dk.customer_id::text = g.customer_id::text AND dk.adgroup_id::text = g.adgroup_id::text" if has_grp else ""
        c_join = "LEFT JOIN dim_campaign c ON g.customer_id::text = c.customer_id::text AND g.campaign_id::text = c.campaign_id::text" if (has_grp and has_camp) else ""
        
        c_name = "COALESCE(c.campaign_name, g.campaign_id::text, '알 수 없음')" if (has_grp and has_camp) else "'알 수 없음'"
        g_name = "COALESCE(g.adgroup_name, dk.adgroup_id::text, '알 수 없음')" if has_grp else "dk.adgroup_id::text"

        sql = f"""
        SELECT f.dt::date AS dt, {c_name} AS campaign_name, {g_name} AS adgroup_name,
            SUM(f.imp) AS imp, SUM(f.clk) AS clk, SUM(f.cost) AS cost, SUM(COALESCE({sales_col}, 0)) AS sales
        FROM fact_keyword_daily f JOIN dim_keyword dk ON f.customer_id::text = dk.customer_id::text AND f.keyword_id::text = dk.keyword_id::text
        {g_join} {c_join} WHERE f.dt BETWEEN :d1 AND :d2 {where_cid} GROUP BY 1, 2, 3
        """
        try:
            df1 = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2)})
            if df1 is not None and not df1.empty:
                df_list.append(df1)
                _diag_add(diag, "내부 원천 조회", "ok", len(df1.index), "fact_keyword_daily", "내부 키워드 일별 로우 조회 성공")
            else:
                _diag_add(diag, "내부 원천 조회", "zero_data", 0, "fact_keyword_daily", "기간/필터 기준 데이터 없음")
        except Exception as e:
            _diag_add(diag, "내부 원천 조회", "error", 0, "fact_keyword_daily", f"{type(e).__name__}: {e}")

    else:
        _diag_add(diag, "내부 원천", "error", 0, "fact_keyword_daily + dim_keyword", "필수 테이블이 없어 내부 일별 상세를 만들 수 없습니다.")

    if df_list:
        res = pd.concat(df_list, ignore_index=True)
        out = res.groupby(["dt", "campaign_name", "adgroup_name"], as_index=False)[["imp", "clk", "cost", "sales"]].sum()
        _diag_add(diag, "내부 일별 집계", "ok" if not out.empty else "zero_data", len(out.index), "fact_keyword_daily", "캠페인/광고그룹 기준 일별 집계")
        return out
    _diag_add(diag, "내부 일별 집계", "zero_data", 0, "fact_keyword_daily", "집계 가능한 내부 데이터가 없습니다.")
    return pd.DataFrame()

def render_trend_chart(df: pd.DataFrame, datalab_name: str, ad_type_label: str):
    if df.empty: return
    legend_name = f"내부 선택 그룹 노출수"

    if HAS_ECHARTS:
        x_data = df["dt"].dt.strftime('%m-%d').tolist()
        imp_data = df["노출"].fillna(0).tolist()
        trend_data = df["트렌드지수(%)"].fillna(0).round(1).tolist()
        
        options = {
            "backgroundColor": "#FFFFFF",
            "tooltip": {
                "trigger": "axis",
                "axisPointer": {"type": "cross", "crossStyle": {"color": "#D7DCE5"}},
                "backgroundColor": "#FFFFFF",
                "borderColor": "#D7DCE5",
                "borderWidth": 1,
                "textStyle": {"color": "#111827", "fontSize": 12},
                "padding": [8, 10],
            },
            "legend": {"data": [legend_name, f"네이버 트렌드 ('{datalab_name}')"], "top": 6, "right": 0, "itemWidth": 10, "itemHeight": 10, "textStyle": {"color": "#6B7280", "fontSize": 11}},
            "grid": {"left": "1%", "right": "1%", "bottom": "10%", "top": 56, "containLabel": True},
            "xAxis": [{"type": "category", "data": x_data, "axisPointer": {"type": "shadow"}, "axisLine": {"lineStyle": {"color": "#D7DCE5"}}, "axisTick": {"show": False}, "axisLabel": {"color": "#6B7280", "fontSize": 11}}],
            "yAxis": [
                {"type": "value", "name": "내부 노출수", "nameTextStyle": {"color": "#6B7280", "fontSize": 11}, "axisLabel": {"color": "#6B7280", "fontSize": 11}, "splitLine": {"lineStyle": {"type": "solid", "color": "#EEF2F7"}}},
                {"type": "value", "name": "트렌드 지수", "min": 0, "max": 100, "nameTextStyle": {"color": "#6B7280", "fontSize": 11}, "axisLabel": {"color": "#6B7280", "fontSize": 11}, "splitLine": {"show": False}}
            ],
            "series": [
                {"name": legend_name, "type": "bar", "data": imp_data, "barMaxWidth": 24, "itemStyle": {"color": "#DDE6FF", "borderRadius": [6,6,0,0]}},
                {"name": f"네이버 트렌드 ('{datalab_name}')", "type": "line", "yAxisIndex": 1, "data": trend_data, "smooth": True, "itemStyle": {"color": "#375FFF"}, "lineStyle": {"width": 2.5}, "symbol": "circle", "symbolSize": 6}
            ]
        }
        st_echarts(options=options, height="400px")

def page_trend(meta: pd.DataFrame, engine, f: Dict) -> None:
    if not f.get("ready", False): return
    diag: list[dict] = []
    cids = tuple(f.get("selected_customer_ids", []))
    d1, d2 = f["start"], f["end"]
    type_sel = tuple(f.get("type_sel", []) or ())
    _diag_add(diag, '필터', 'ok', len(cids), 'filters', f"기간={d1}~{d2} | 유형={', '.join(type_sel) if type_sel else '전체'}")

    st.markdown("<div class='nv-sec-title'>시장 트렌드 비교</div>", unsafe_allow_html=True)
    st.divider()

    client_id = os.getenv("NAVER_DATALAB_CLIENT_ID", "")
    client_secret = os.getenv("NAVER_DATALAB_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        st.warning("네이버 데이터랩 API 키가 설정되지 않았습니다.")
        _diag_add(diag, '데이터랩 인증', 'error', 0, 'NAVER_DATALAB_CLIENT_ID/SECRET', 'API 키가 설정되지 않았습니다.')
        _render_diag_panel(diag)
        return

    df_raw = _cached_internal_daily_detail(engine, d1, d2, cids)
    _diag_add(diag, "내부 원천 조회", "ok" if not df_raw.empty else "zero_data", len(df_raw.index) if df_raw is not None else 0, "fact_keyword_daily", "캐시/원천 조회")
    if df_raw.empty:
        st.info("선택한 기간의 내부 노출 데이터가 없어 트렌드 비교를 진행할 수 없습니다.")
        _render_diag_panel(diag)
        return

    c1, c2, c3 = st.columns([1.5, 1.5, 1.5])
    with c1: datalab_kw = st.text_input("데이터랩 검색어", placeholder="예: 나이키운동화").strip()
    with c2:
        camps = ["전체"] + sorted([str(x) for x in df_raw["campaign_name"].dropna().unique() if str(x).strip() and x != "알 수 없음"])
        sel_camp = st.selectbox("비교할 캠페인", camps)
    with c3:
        if sel_camp == "전체": grps = ["전체"] + sorted([str(x) for x in df_raw["adgroup_name"].dropna().unique() if str(x).strip() and x != "알 수 없음"])
        else: grps = ["전체"] + sorted([str(x) for x in df_raw[df_raw["campaign_name"] == sel_camp]["adgroup_name"].dropna().unique() if str(x).strip() and x != "알 수 없음"])
        sel_grp = st.selectbox("비교할 광고그룹", grps)

    run_trend = st.button('트렌드 조회', key='trend_run_btn', use_container_width=False)
    if not datalab_kw:
        _diag_add(diag, "검색어 입력", "warn", 0, "st.text_input", "데이터랩 검색어를 입력하면 비교가 시작됩니다.")
        _render_diag_panel(diag)
        return
    if not run_trend:
        st.info('검색어와 조건을 선택한 뒤 `트렌드 조회`를 눌러 비교를 시작하세요.')
        _render_diag_panel(diag)
        return

    df_filtered = df_raw.copy()
    if sel_camp != "전체": df_filtered = df_filtered[df_filtered["campaign_name"] == sel_camp]
    if sel_grp != "전체": df_filtered = df_filtered[df_filtered["adgroup_name"] == sel_grp]
    _diag_add(diag, "내부 필터 적용", "ok" if not df_filtered.empty else "zero_data", len(df_filtered.index), "fact_keyword_daily", f"캠페인={sel_camp} | 광고그룹={sel_grp}")
    if df_filtered.empty:
        st.info("선택한 캠페인/광고그룹 조건에서 내부 데이터가 없습니다.")
        _render_diag_panel(diag)
        return

    df_daily = df_filtered.groupby("dt", as_index=False)[["imp", "clk", "cost", "sales"]].sum()
    df_daily["dt"] = pd.to_datetime(df_daily["dt"])
    df_daily = df_daily.rename(columns={"imp": "노출", "clk": "클릭", "cost": "광고비"})

    trend_df = _cached_datalab_trend(client_id, client_secret, datalab_kw, d1, d2)
    _diag_add(diag, "데이터랩 조회", "ok" if not trend_df.empty else "zero_data", len(trend_df.index) if trend_df is not None else 0, "Naver DataLab", f"검색어={datalab_kw}")
    if trend_df.empty:
        st.info("데이터랩 결과가 없어 비교 그래프를 만들 수 없습니다. 조회 진단을 확인해 주세요.")
        _render_diag_panel(diag)
        return

    merged_df = pd.merge(trend_df, df_daily, on="dt", how="left").fillna(0)

    render_trend_chart(merged_df, datalab_kw, "전체")
    
    st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:24px;'>일자별 상세 데이터</div>", unsafe_allow_html=True)
    detail = merged_df[["dt", "트렌드지수(%)", "노출", "클릭", "광고비"]].sort_values("dt", ascending=False).copy()
    detail['dt'] = pd.to_datetime(detail['dt']).dt.date
    st.dataframe(
        detail,
        use_container_width=True, hide_index=True,
        column_config={
            "트렌드지수(%)": st.column_config.NumberColumn("트렌드지수(%)", format="%.1f"),
            "노출": st.column_config.NumberColumn("노출", format="%d"),
            "클릭": st.column_config.NumberColumn("클릭", format="%d"),
            "광고비": st.column_config.NumberColumn("광고비", format="%d"),
        }
    )

    _render_diag_panel(diag)
