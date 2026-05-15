# -*- coding: utf-8 -*-
"""view_budget.py - Budget and Balance page view (Aligned with other standard tables)."""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import streamlit_compat  # noqa: F401
import streamlit.components.v1 as components
from typing import Dict
from datetime import date, timedelta

from data import *
from ui import *
from page_helpers import *

# ⚡ 고속 렌더링을 위한 DB 데이터 캐싱 래퍼 함수
@st.cache_data(ttl=300, show_spinner=False, max_entries=20)
def _cached_budget_bundle(_engine, cids: tuple, yesterday: date, avg_d1: date, avg_d2: date, month_d1: date, month_d2: date, prev_month_d1: date, prev_month_d2: date, topup_avg_days: int) -> pd.DataFrame:
    try:
        return query_budget_bundle(_engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, topup_avg_days)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=180, show_spinner=False, max_entries=20)
def _prepare_biz_view(bundle: pd.DataFrame) -> pd.DataFrame:
    if bundle is None or bundle.empty:
        return pd.DataFrame()
    biz_view = bundle.copy()
    balance = safe_numeric_col(biz_view, "bizmoney_balance")
    avg_cost = safe_numeric_col(biz_view, "avg_cost")
    biz_view["days_cover"] = np.where(avg_cost > 0, balance / avg_cost, np.nan)
    biz_view["threshold"] = (avg_cost * float(TOPUP_DAYS_COVER)).fillna(0.0)
    biz_view["threshold"] = biz_view["threshold"].map(lambda x: max(float(x), float(TOPUP_STATIC_THRESHOLD)))
    return biz_view


@st.cache_data(ttl=180, show_spinner=False, max_entries=20)
def _prepare_alert_view(bundle: pd.DataFrame) -> pd.DataFrame:
    if bundle is None or bundle.empty:
        return pd.DataFrame()
    alert_view = bundle.copy()
    balance = safe_numeric_col(alert_view, "bizmoney_balance")
    avg_cost = safe_numeric_col(alert_view, "avg_cost")
    alert_view["days_cover"] = np.where(avg_cost > 0, balance / avg_cost, np.nan)
    return alert_view


def _numeric_series(values, default: float = 0.0) -> pd.Series:
    if values is None:
        return pd.Series(dtype="float64")
    s = pd.Series(values)
    cleaned = (
        s.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("원", "", regex=False)
        .str.replace(r"[^0-9.-]", "", regex=True)
        .replace({"": np.nan, "nan": np.nan, "None": np.nan})
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(default)


@st.cache_data(ttl=180, show_spinner=False, max_entries=20)
def _build_alert_display(alert_view: pd.DataFrame) -> pd.DataFrame:
    if alert_view is None or alert_view.empty:
        return pd.DataFrame()

    df = alert_view.copy()
    today = pd.Timestamp.now(tz="Asia/Seoul").date()
    balance = _numeric_series(df.get("bizmoney_balance"), default=0).round(0).astype(int)
    avg_cost = _numeric_series(df.get("avg_cost"), default=0).round(0).astype(int)
    days_raw = pd.Series(np.where(avg_cost > 0, balance / avg_cost, np.nan), index=df.index)
    df["잔여일수"] = days_raw.where(days_raw < 9999)
    df["_sort_days"] = days_raw.fillna(9999)

    def coerce_base_date(value):
        try:
            parsed = pd.to_datetime(value).date()
        except Exception:
            return today
        if parsed > today:
            return today
        return parsed

    if "bizmoney_dt" in df.columns:
        df["_base_date"] = df["bizmoney_dt"].apply(coerce_base_date)
    else:
        df["_base_date"] = today
    df["계산 기준일"] = df["_base_date"].apply(lambda x: x.strftime("%m월 %d일") if pd.notna(x) else "-")

    def get_depletion_date(row):
        days_left = row.get("잔여일수")
        if pd.isna(days_left) or float(days_left) >= 99:
            return "여유"
        days = float(days_left)
        if days <= 0:
            return "오늘"
        deplete_date = row.get("_base_date", today) + timedelta(days=max(1, int(np.ceil(days))))
        return deplete_date.strftime("%m월 %d일")

    def get_risk_label(days_left):
        if pd.isna(days_left) or float(days_left) >= 99:
            return "여유"
        days = float(days_left)
        if days <= 0:
            return "즉시 충전"
        if days <= TOPUP_DAYS_COVER:
            return "소진 임박"
        if days <= max(TOPUP_DAYS_COVER + 2, 5):
            return "주의"
        return "여유"

    df["소진 위험"] = days_raw.apply(get_risk_label)
    df["_risk_rank"] = df["소진 위험"].map({"즉시 충전": 0, "소진 임박": 1, "주의": 2, "여유": 3}).fillna(4)
    df["예상 소진일"] = df.apply(get_depletion_date, axis=1)
    df["비즈머니 잔액"] = balance
    avg_days_label = f"최근 {TOPUP_AVG_DAYS}일 평균소진"
    df[avg_days_label] = avg_cost
    df["담당자"] = df.get("manager", "미배정").fillna("미배정").replace("", "미배정")
    df["업체명"] = df.get("account_name", df.get("customer_id", "-")).fillna("-").replace("", "-")
    df = df.sort_values(["_risk_rank", "_sort_days", "업체명"], ascending=[True, True, True]).reset_index(drop=True)
    return df[["업체명", "소진 위험", "담당자", "비즈머니 잔액", avg_days_label, "잔여일수", "예상 소진일", "계산 기준일"]]


WEEKDAY_LABELS = [("월", 0), ("화", 1), ("수", 2), ("목", 3), ("금", 4), ("토", 5), ("일", 6)]
WEEKDAY_INDEX_TO_LABEL = {idx: label for label, idx in WEEKDAY_LABELS}


def _count_operating_days(start_dt: date, end_dt: date, weekdays: list[int]) -> int:
    if start_dt is None or end_dt is None or end_dt < start_dt:
        return 0
    selected = set(weekdays or range(7))
    count = 0
    cur = start_dt
    while cur <= end_dt:
        if cur.weekday() in selected:
            count += 1
        cur += timedelta(days=1)
    return count


def _normalize_weekday_csv(value) -> str:
    fallback = globals().get("DEFAULT_OPERATING_WEEKDAYS", "0,1,2,3,4,5,6")
    try:
        return normalize_operating_weekdays(value)
    except Exception:
        return fallback


def _parse_operating_weekdays(value) -> list[int]:
    normalized = _normalize_weekday_csv(value)
    selected: list[int] = []
    for part in str(normalized).split(","):
        try:
            day = int(str(part).strip())
        except Exception:
            continue
        if 0 <= day <= 6:
            selected.append(day)
    selected = sorted(set(selected))
    return selected or list(range(7))


def _format_operating_weekdays(value) -> str:
    selected = _parse_operating_weekdays(value)
    if selected == list(range(7)):
        return "매일"
    if selected == [0, 1, 2, 3, 4]:
        return "평일"
    if selected == [5, 6]:
        return "주말"
    return ", ".join(WEEKDAY_INDEX_TO_LABEL.get(day, str(day)) for day in selected)


def _recompute_operating_day_fields(
    budget_view: pd.DataFrame,
    month_d1: date,
    month_d2: date,
    end_dt: date,
) -> pd.DataFrame:
    if budget_view is None or budget_view.empty:
        return pd.DataFrame()
    if "operating_weekdays" not in budget_view.columns:
        budget_view["operating_weekdays"] = globals().get("DEFAULT_OPERATING_WEEKDAYS", "0,1,2,3,4,5,6")

    elapsed_end = min(end_dt, month_d2)
    budget_view["operating_weekdays"] = budget_view["operating_weekdays"].apply(_normalize_weekday_csv)
    budget_view["operating_label"] = budget_view["operating_weekdays"].apply(_format_operating_weekdays)
    budget_view["_elapsed_operating_days"] = budget_view["operating_weekdays"].apply(
        lambda value: _count_operating_days(month_d1, elapsed_end, _parse_operating_weekdays(value))
    )
    budget_view["_total_operating_days"] = budget_view["operating_weekdays"].apply(
        lambda value: _count_operating_days(month_d1, month_d2, _parse_operating_weekdays(value))
    )
    total_days = safe_numeric_col(budget_view, "_total_operating_days")
    elapsed_days = safe_numeric_col(budget_view, "_elapsed_operating_days")
    budget_view["_target_pacing_rate"] = np.where(total_days > 0, elapsed_days / total_days, 0.0)
    budget_view["operating_days_label"] = (
        safe_numeric_col(budget_view, "_elapsed_operating_days").round(0).astype(int).astype(str)
        + " / "
        + safe_numeric_col(budget_view, "_total_operating_days").round(0).astype(int).astype(str)
        + "일"
    )
    return budget_view


def _recalculate_budget_metrics(budget_view: pd.DataFrame) -> pd.DataFrame:
    if budget_view is None or budget_view.empty:
        return pd.DataFrame()

    budget_view["monthly_budget_val"] = safe_numeric_col(budget_view, "monthly_budget_val").round(0).astype(int)
    budget_view["prev_month_cost_val"] = safe_numeric_col(budget_view, "prev_month_cost_val").round(0).astype(int)
    budget_view["current_month_cost_val"] = safe_numeric_col(budget_view, "current_month_cost_val").round(0).astype(int)

    elapsed_days = safe_numeric_col(budget_view, "_elapsed_operating_days").where(lambda s: s > 0, 1.0)
    total_days = safe_numeric_col(budget_view, "_total_operating_days").where(lambda s: s > 0, 1.0)
    current_cost = safe_numeric_col(budget_view, "current_month_cost_val")
    monthly_budget = safe_numeric_col(budget_view, "monthly_budget_val")

    budget_view["current_daily_avg_val"] = (current_cost / elapsed_days).fillna(0).round(0).astype(int)
    budget_view["recommended_daily_avg_val"] = np.where(
        monthly_budget > 0,
        monthly_budget / total_days,
        0.0,
    ).round(0).astype(int)
    budget_view["usage_rate"] = 0.0
    m2 = budget_view["monthly_budget_val"] > 0
    budget_view.loc[m2, "usage_rate"] = (
        safe_numeric_col(budget_view.loc[m2], "current_month_cost_val")
        / safe_numeric_col(budget_view.loc[m2], "monthly_budget_val")
    )
    budget_view["usage_pct"] = (safe_numeric_col(budget_view, "usage_rate") * 100.0).fillna(0.0)

    target_rate = safe_numeric_col(budget_view, "_target_pacing_rate")
    cond_zero = budget_view["monthly_budget_val"] == 0
    cond_over = budget_view["usage_rate"] >= 1.0
    cond_fast = budget_view["usage_rate"] > target_rate + 0.1
    cond_slow = budget_view["usage_rate"] < target_rate - 0.1
    budget_view["상태"] = np.select(
        [cond_zero, cond_over, cond_fast, cond_slow],
        ["미설정", "예산 초과", "과속 소진", "과소 소진"],
        default="적정 페이스",
    )
    budget_view["_rank"] = np.select(
        [cond_zero, cond_over, cond_fast, cond_slow],
        [4, 0, 1, 3],
        default=2,
    )
    return budget_view


def _sort_budget_view(budget_view: pd.DataFrame) -> pd.DataFrame:
    if budget_view is None or budget_view.empty:
        return pd.DataFrame()
    return budget_view.sort_values(["_rank", "usage_rate", "account_name"], ascending=[True, False, True]).reset_index(drop=True)


@st.cache_data(ttl=180, show_spinner=False, max_entries=20)
def _build_budget_editor_view(
    biz_view: pd.DataFrame,
    month_d1: date,
    month_d2: date,
    end_dt: date,
) -> pd.DataFrame:
    if biz_view is None or biz_view.empty:
        return pd.DataFrame()
    for col in ["customer_id", "account_name", "manager", "monthly_budget", "prev_month_cost", "current_month_cost", "operating_weekdays"]:
        if col not in biz_view.columns:
            biz_view[col] = "" if col in {"customer_id", "account_name", "manager"} else 0
    budget_view = biz_view[[
        "customer_id", "account_name", "manager", "monthly_budget", "prev_month_cost", "current_month_cost", "operating_weekdays"
    ]].copy()
    budget_view["monthly_budget_val"] = safe_numeric_col(budget_view, "monthly_budget").astype(int)
    budget_view["prev_month_cost_val"] = safe_numeric_col(budget_view, "prev_month_cost").astype(int)
    budget_view["current_month_cost_val"] = safe_numeric_col(budget_view, "current_month_cost").astype(int)
    budget_view = _recompute_operating_day_fields(budget_view, month_d1, month_d2, end_dt)
    budget_view = _recalculate_budget_metrics(budget_view)
    return _sort_budget_view(budget_view)


def _ensure_budget_input_js_once():
    if st.session_state.get("_budget_input_js_once"):
        return
    st.session_state["_budget_input_js_once"] = True


def _resolve_budget_reference_date(engine, fallback_end_dt: date) -> date:
    latest_dates = get_latest_dates(engine) or {}
    candidates = []
    for key in ["fact_campaign_daily", "fact_adgroup_daily", "fact_keyword_daily", "fact_ad_daily"]:
        dt_val = latest_dates.get(key)
        if pd.notna(dt_val):
            try:
                candidates.append(pd.to_datetime(dt_val).date())
            except Exception:
                pass
    if candidates:
        return max(candidates)
    return fallback_end_dt


def _build_budget_table_styler(df: pd.DataFrame, avg_days_label: str):
    """다른 테이블들(overview, campaign 등)과 동일하게 df.style.format을 사용"""
    fmt_map = {
        "비즈머니 잔액": "{:,.0f}원",
        avg_days_label: "{:,.0f}원",
        "잔여일수": "{:,.1f}일",
    }
    # 포맷 맵에 있는 컬럼만 적용
    fmt_map = {k: v for k, v in fmt_map.items() if k in df.columns}
    styler = df.style.format(fmt_map, na_rep='-')
    if "소진 위험" in df.columns:
        def _risk_style(value):
            value = str(value or "")
            if value == "즉시 충전":
                return "background-color:#FEE2E2;color:#B91C1C;font-weight:800;"
            if value == "소진 임박":
                return "background-color:#FEF3C7;color:#92400E;font-weight:800;"
            if value == "주의":
                return "background-color:#E0F2FE;color:#075985;font-weight:800;"
            if value == "여유":
                return "background-color:#DCFCE7;color:#166534;font-weight:800;"
            return ""
        try:
            styler = styler.map(_risk_style, subset=["소진 위험"])
        except AttributeError:
            styler = styler.applymap(_risk_style, subset=["소진 위험"])
    return styler


def _trigger_rerun():
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()


@st.fragment
def render_operating_weekday_editor(budget_view: pd.DataFrame, engine):
    if budget_view is None or budget_view.empty:
        return

    with st.expander("업체별 운영 요일 설정", expanded=False):
        st.caption("각 업체의 실제 광고 운영 요일을 체크해두면 현재 일평균, 권장 일평균, 권장 소진 페이스가 업체별로 다시 계산됩니다.")
        weekday_df = budget_view[["customer_id", "account_name", "manager", "operating_weekdays"]].drop_duplicates("customer_id").copy()
        weekday_df["operating_weekdays"] = weekday_df["operating_weekdays"].apply(_normalize_weekday_csv)
        weekday_df["업체명"] = weekday_df["account_name"].fillna("").replace("", "-")
        weekday_df["담당자"] = weekday_df["manager"].fillna("미배정").replace("", "미배정")

        for label, weekday_idx in WEEKDAY_LABELS:
            weekday_df[label] = weekday_df["operating_weekdays"].apply(lambda value, idx=weekday_idx: idx in _parse_operating_weekdays(value))

        original_map = dict(zip(weekday_df["customer_id"].astype(str), weekday_df["operating_weekdays"]))
        editor_input = weekday_df[["customer_id", "업체명", "담당자"] + [label for label, _ in WEEKDAY_LABELS]]
        editor_height = min(420, max(180, 72 + len(editor_input.index) * 36))

        edited_weekdays = st.data_editor(
            editor_input,
            key="budget_operating_weekday_editor",
            hide_index=True,
            use_container_width=True,
            height=editor_height,
            column_config={
                "customer_id": None,
                "업체명": st.column_config.TextColumn("업체명", disabled=True, pinned=True),
                "담당자": st.column_config.TextColumn("담당자", disabled=True, width="small"),
                **{
                    label: st.column_config.CheckboxColumn(label, width="small")
                    for label, _ in WEEKDAY_LABELS
                },
            },
        )

        if st.button("운영 요일 저장", type="primary", key="save_budget_operating_weekdays"):
            updated_count = 0
            invalid_accounts: list[str] = []
            if "local_operating_weekday_overrides" not in st.session_state:
                st.session_state["local_operating_weekday_overrides"] = {}

            for _, row in edited_weekdays.iterrows():
                cid = str(row.get("customer_id", "")).strip()
                selected = [weekday_idx for label, weekday_idx in WEEKDAY_LABELS if bool(row.get(label, False))]
                if not selected:
                    invalid_accounts.append(str(row.get("업체명", cid)))
                    continue
                weekdays_csv = ",".join(str(day) for day in selected)
                if cid and weekdays_csv != original_map.get(cid):
                    update_customer_operating_weekdays(engine, cid, weekdays_csv)
                    st.session_state["local_operating_weekday_overrides"][cid] = weekdays_csv
                    updated_count += 1

            if invalid_accounts:
                st.warning(f"운영 요일은 업체당 1개 이상 필요합니다: {', '.join(invalid_accounts[:5])}")
            if updated_count:
                _cached_budget_bundle.clear()
                _build_budget_editor_view.clear()
                st.toast(f"운영 요일 {updated_count:,}건이 저장되었습니다.")
                _trigger_rerun()


@st.fragment
def render_budget_editor(
    budget_view: pd.DataFrame,
    engine,
    end_dt: date,
):
    prev_month_dt = (end_dt.replace(day=1) - timedelta(days=1))
    prev_m_num = prev_month_dt.month
    
    for col in ["current_daily_avg_val", "recommended_daily_avg_val", "operating_label", "operating_days_label"]:
        if col not in budget_view.columns:
            budget_view[col] = 0 if col.endswith("_val") else "-"
    editor_df = budget_view[[
        "customer_id", "account_name", "manager", "operating_label", "operating_days_label",
        "monthly_budget_val", "prev_month_cost_val", "current_month_cost_val",
        "current_daily_avg_val", "recommended_daily_avg_val", "usage_pct", "상태"
    ]].copy()
    
    editor_df["월 예산"] = editor_df["monthly_budget_val"].apply(lambda x: f"{int(x):,}".rjust(15, ' ') if pd.notna(x) else "0".rjust(15, ' '))
    editor_df[f"{end_dt.month}월 사용액"] = editor_df["current_month_cost_val"].apply(lambda x: f"{int(x):,}".rjust(15, ' ') if pd.notna(x) else "0".rjust(15, ' '))
    editor_df["현재 일평균 소진액"] = safe_numeric_col(editor_df, "current_daily_avg_val").round(0).astype(int)
    editor_df["일 평균 권장 소진액"] = safe_numeric_col(editor_df, "recommended_daily_avg_val").round(0).astype(int)
    editor_df[f"{prev_m_num}월 사용액"] = editor_df["prev_month_cost_val"].apply(lambda x: f"{int(x):,}".rjust(15, ' ') if pd.notna(x) else "0".rjust(15, ' '))
    
    editor_df = editor_df.rename(columns={
        "account_name": "업체명", 
        "manager": "담당자", 
        "operating_label": "운영 요일",
        "operating_days_label": "운영일 기준",
        "usage_pct": "집행률(%)"
    })

    ordered_cols = [
        "customer_id", "monthly_budget_val", "prev_month_cost_val", "current_month_cost_val",
        "current_daily_avg_val", "recommended_daily_avg_val",
        "업체명", "담당자", "운영 요일", "운영일 기준", "월 예산", f"{end_dt.month}월 사용액",
        "현재 일평균 소진액", "일 평균 권장 소진액", f"{prev_m_num}월 사용액", "집행률(%)", "상태"
    ]
    editor_df = editor_df[ordered_cols]

    def update_budget_from_table():
        if "budget_table_editor" in st.session_state:
            edits = st.session_state["budget_table_editor"].get("edited_rows", {})
            updated_count = 0
            
            if "local_budget_overrides" not in st.session_state:
                st.session_state["local_budget_overrides"] = {}
                
            for row_idx, col_data in edits.items():
                if "월 예산" in col_data:
                    raw_input = str(col_data["월 예산"]).replace(",", "").replace("원", "").strip()
                    if raw_input.isdigit():
                        new_budget = int(raw_input)
                        cid = str(editor_df.iloc[row_idx]["customer_id"])
                        
                        update_monthly_budget(engine, cid, new_budget)
                        st.session_state["local_budget_overrides"][cid] = new_budget
                        updated_count += 1
            
            if updated_count > 0:
                _cached_budget_bundle.clear()
                _build_budget_editor_view.clear()
                st.toast("예산이 저장되었습니다.")

    st.markdown(f"<div style='font-size:14px; font-weight:700; margin-bottom:4px;'>{end_dt.strftime('%Y년 %m월')} 예산 집행률</div>", unsafe_allow_html=True)
    st.caption(
        "표의 '월 예산(원)' 칸을 더블클릭하여 수정하세요. 현재 일평균은 이번 달 사용액 ÷ 업체별 운영 경과일, "
        "권장 일평균은 월 예산 ÷ 업체별 월 운영일로 계산합니다."
    )
    _ensure_budget_input_js_once()

    st.data_editor(
        editor_df,
        key="budget_table_editor",
        on_change=update_budget_from_table,
        hide_index=True,
        use_container_width=True,
        height=550,
        column_config={
            "customer_id": None, 
            "monthly_budget_val": None, 
            "prev_month_cost_val": None,
            "current_month_cost_val": None,
            "current_daily_avg_val": None,
            "recommended_daily_avg_val": None,
            "업체명": st.column_config.TextColumn("업체명", disabled=True),
            "담당자": st.column_config.TextColumn("담당자", disabled=True),
            "운영 요일": st.column_config.TextColumn("운영 요일", disabled=True, width="small"),
            "운영일 기준": st.column_config.TextColumn(
                "운영일 기준",
                disabled=True,
                width="small",
                help="현재 운영 경과일 / 해당 월 전체 운영일"
            ),
            "월 예산": st.column_config.TextColumn(
                "월 예산(원)", 
                help="더블클릭하여 예산을 바로 수정하세요.",
                required=True
            ),
            f"{end_dt.month}월 사용액": st.column_config.TextColumn(
                f"{end_dt.month}월 사용액", 
                disabled=True
            ),
            "현재 일평균 소진액": st.column_config.NumberColumn(
                "현재 일평균 소진액",
                disabled=True,
                format="%,.0f 원",
                help="이번 달 누적 사용액을 업체별 운영 경과일로 나눈 값입니다."
            ),
            "일 평균 권장 소진액": st.column_config.NumberColumn(
                "일 평균 권장 소진액",
                disabled=True,
                format="%,.0f 원",
                help="월 예산을 업체별 월 운영일로 나눈 권장 일평균입니다."
            ),
            f"{prev_m_num}월 사용액": st.column_config.TextColumn(
                f"{prev_m_num}월 사용액", 
                disabled=True
            ),
            "집행률(%)": st.column_config.ProgressColumn(
                "집행률(%)",
                help="월 예산 대비 현재 사용액 비율",
                format="%.1f%%",
                min_value=0,
                max_value=100
            ),
            "상태": st.column_config.TextColumn("상태", disabled=True)
        }
    )


@st.fragment
def render_alert_table(alert_view: pd.DataFrame):
    display_df = _build_alert_display(alert_view)
    st.markdown("<div style='font-size:14px; font-weight:700; margin-bottom:12px; margin-top:20px;'>비즈머니 잔액 관리 계정</div>", unsafe_allow_html=True)
    
    if display_df.empty:
        st.info("비즈머니 관리 데이터가 없습니다.")
        return
        
    avg_days_label = f"최근 {TOPUP_AVG_DAYS}일 평균소진"
    st.caption("컬럼명을 클릭하면 오름차순/내림차순 정렬할 수 있습니다. 금액과 잔여일수는 숫자 기준으로 정렬됩니다.")

    table_df = display_df.copy()
    # 숫자 정렬을 유지하기 위해 float/int로 변환 (표시는 Styler에서 처리)
    for col in ["비즈머니 잔액", avg_days_label]:
        if col in table_df.columns:
            table_df[col] = _numeric_series(table_df[col], default=0).round(0).astype("float64")
    if "잔여일수" in table_df.columns:
        table_df["잔여일수"] = pd.to_numeric(table_df["잔여일수"], errors="coerce")

    # 다른 뷰와 동일하게 Styler 객체를 전달하고, 첫 번째 주요 컬럼("업체명")을 pinned 처리
    cfg = {
        "업체명": st.column_config.TextColumn("업체명", pinned=True, width="medium"),
        "소진 위험": st.column_config.TextColumn("소진 위험", width="small"),
        "담당자": st.column_config.TextColumn("담당자"),
        "비즈머니 잔액": st.column_config.NumberColumn("비즈머니 잔액", format="%,.0f 원"),
        avg_days_label: st.column_config.NumberColumn(avg_days_label, format="%,.0f 원"),
        "잔여일수": st.column_config.NumberColumn("잔여일수", format="%,.1f 일"),
        "예상 소진일": st.column_config.TextColumn("예상 소진일"),
        "계산 기준일": st.column_config.TextColumn("계산 기준일", width="small"),
    }

    styled_df = _build_budget_table_styler(table_df, avg_days_label)

    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        height=550,
        column_config=cfg,
    )


@st.fragment
def render_budget_kpis(biz_view: pd.DataFrame, end_dt: date):
    total_balance = int(safe_numeric_col(biz_view, "bizmoney_balance").sum())
    total_month_cost = int(safe_numeric_col(biz_view, "current_month_cost").sum())
    monthly_budget_src = biz_view["monthly_budget"] if "monthly_budget" in biz_view.columns else pd.Series([0] * len(biz_view.index))
    avg_cost_src = biz_view["avg_cost"] if "avg_cost" in biz_view.columns else pd.Series([0] * len(biz_view.index))
    total_budget = int(_numeric_series(monthly_budget_src, default=0).sum())
    usage_pct = (total_month_cost / total_budget * 100.0) if total_budget > 0 else 0.0
    avg_cost = int(_numeric_series(avg_cost_src, default=0).sum())

    render_kpi_strip([
        {"label": "총 비즈머니 잔액", "value": format_currency(total_balance), "sub": "현재 잔액", "tone": "neu"},
        {"label": f"{end_dt.month}월 총 사용액", "value": format_currency(total_month_cost), "sub": "월 누적", "tone": "neu"},
        {"label": "월 예산 합계", "value": format_currency(total_budget), "sub": "등록 기준", "tone": "neu"},
        {"label": "예산 집행률", "value": f"{usage_pct:.1f}%", "sub": "전체 페이스", "tone": "neu"},
        {"label": f"최근 {TOPUP_AVG_DAYS}일 평균소진", "value": format_currency(avg_cost), "sub": "일 평균", "tone": "neu"},
        {"label": "관리 계정", "value": f"{len(biz_view.index):,}개", "sub": "현재 필터", "tone": "neu"},
    ])


def page_budget(meta: pd.DataFrame, engine, f: Dict) -> None:
    st.markdown("<div class='nv-sec-title'>예산 관리</div>", unsafe_allow_html=True)
    
    selected_view = st.radio("보기", ["월 예산 현황", "비즈머니 관리"], horizontal=True, label_visibility="collapsed", key="budget_view_mode")

    cids = tuple(f.get("selected_customer_ids", []) or [])
    yesterday = date.today() - timedelta(days=1)
    fallback_end_dt = f.get("end") or yesterday
    end_dt = _resolve_budget_reference_date(engine, fallback_end_dt)
    end_dt = min(end_dt, yesterday)
    avg_d2 = end_dt
    avg_d1 = avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)

    month_d1 = end_dt.replace(day=1)
    month_d2 = date(end_dt.year + 1, 1, 1) - timedelta(days=1) if end_dt.month == 12 else date(end_dt.year, end_dt.month + 1, 1) - timedelta(days=1)

    prev_month_last_day = month_d1 - timedelta(days=1)
    prev_month_d1 = prev_month_last_day.replace(day=1)
    prev_month_d2 = prev_month_last_day

    if selected_view == "월 예산 현황":
        bundle = _cached_budget_bundle(engine, cids, yesterday, avg_d1, avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, TOPUP_AVG_DAYS)
        biz_view = _prepare_biz_view(bundle)
        if biz_view.empty:
            st.info("예산 현황 데이터가 없습니다.")
        else:
            render_budget_kpis(biz_view.copy(), end_dt)

            if "local_operating_weekday_overrides" in st.session_state and not biz_view.empty:
                if "operating_weekdays" not in biz_view.columns:
                    biz_view["operating_weekdays"] = globals().get("DEFAULT_OPERATING_WEEKDAYS", "0,1,2,3,4,5,6")
                for cid, weekdays in st.session_state["local_operating_weekday_overrides"].items():
                    m_cid = biz_view["customer_id"].astype(str) == str(cid)
                    biz_view.loc[m_cid, "operating_weekdays"] = _normalize_weekday_csv(weekdays)

            budget_view = _build_budget_editor_view(biz_view, month_d1, month_d2, end_dt)

            if "local_budget_overrides" in st.session_state and not budget_view.empty:
                for cid, new_val in st.session_state["local_budget_overrides"].items():
                    m_cid = budget_view["customer_id"].astype(str) == str(cid)
                    budget_view.loc[m_cid, "monthly_budget"] = new_val
                    budget_view.loc[m_cid, "monthly_budget_val"] = int(new_val)
                budget_view = _recalculate_budget_metrics(budget_view)
                budget_view = _sort_budget_view(budget_view)

            status_counts = budget_view["상태"].value_counts().to_dict() if not budget_view.empty and "상태" in budget_view.columns else {}
            render_ops_cards([
                {"title": "즉시 점검", "value": f"{int(status_counts.get('예산 초과', 0)):,}개", "note": "월 예산을 초과한 계정", "tone": "danger"},
                {"title": "과속 소진", "value": f"{int(status_counts.get('과속 소진', 0)):,}개", "note": "권장 페이스보다 빠른 계정", "tone": "warning"},
                {"title": "정상 페이스", "value": f"{int(status_counts.get('적정 페이스', 0)):,}개", "note": "현재 기준 안정 범위", "tone": "success"},
            ])
            render_operating_weekday_editor(budget_view, engine)
            render_budget_editor(budget_view, engine, end_dt)
    else:
        alert_avg_d2 = end_dt
        alert_avg_d1 = alert_avg_d2 - timedelta(days=max(TOPUP_AVG_DAYS, 1) - 1)
        alert_bundle = _cached_budget_bundle(engine, cids, yesterday, alert_avg_d1, alert_avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, TOPUP_AVG_DAYS)
        alert_view = _prepare_alert_view(alert_bundle)
        if alert_view.empty:
            st.info("비즈머니 관리 데이터가 없습니다.")
        else:
            alert_display = _build_alert_display(alert_view)
            urgent_count = int(alert_display["소진 위험"].astype(str).isin(["즉시 충전", "소진 임박"]).sum()) if not alert_display.empty else 0
            safe_count = max(len(alert_display.index) - urgent_count, 0) if not alert_display.empty else 0
            render_ops_cards([
                {"title": "충전 우선순위", "value": f"{urgent_count:,}개", "note": "즉시 또는 3일 내 확인", "tone": "danger" if urgent_count else "success"},
                {"title": "안정 계정", "value": f"{safe_count:,}개", "note": "잔여일수 여유", "tone": "success"},
                {"title": "관리 기준", "value": f"{TOPUP_DAYS_COVER}일", "note": f"최근 {TOPUP_AVG_DAYS}일 평균소진 기준", "tone": "info"},
            ])
            render_alert_table(alert_view)
