# -*- coding: utf-8 -*-
"""view_ops.py - Daily operations center for action items, sync status, and audit trail."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from data import (
    format_currency,
    query_action_items,
    query_budget_bundle,
    query_campaign_bundle,
    query_campaign_off_log,
    query_collection_status,
    query_dashboard_audit_log,
    query_keyword_bundle,
    update_action_item,
    upsert_action_items,
)
from ui import render_empty_state, render_kpi_strip, render_toolbar, safe_numeric_col


def _today_kst() -> date:
    try:
        return datetime.now(ZoneInfo("Asia/Seoul")).date()
    except Exception:
        return date.today()


def _cid_text(value) -> str:
    raw = str(value or "").strip()
    if raw.endswith(".0") and raw.replace(".", "", 1).isdigit():
        raw = raw[:-2]
    return raw


def _meta_context(meta: pd.DataFrame) -> dict[str, dict]:
    if meta is None or meta.empty or "customer_id" not in meta.columns:
        return {}
    work = meta.copy()
    work["_cid"] = work["customer_id"].map(_cid_text)
    out = {}
    for _, row in work.drop_duplicates("_cid").iterrows():
        cid = str(row.get("_cid", "") or "").strip()
        if not cid:
            continue
        out[cid] = {
            "account_name": str(row.get("account_name", "") or row.get("업체명", "") or cid),
            "manager": str(row.get("manager", "") or row.get("담당자", "") or "미배정"),
        }
    return out


def _row_context(row, meta_map: dict[str, dict]) -> dict:
    cid = _cid_text(row.get("customer_id", ""))
    ctx = dict(meta_map.get(cid, {}))
    ctx.setdefault("account_name", str(row.get("account_name", "") or cid))
    ctx.setdefault("manager", str(row.get("manager", "") or "미배정"))
    ctx["customer_id"] = cid
    return ctx


def _fmt_num(value) -> str:
    try:
        return f"{int(round(float(value or 0))):,}"
    except Exception:
        return "0"


def _fmt_pct(value) -> str:
    try:
        return f"{float(value or 0):,.1f}%"
    except Exception:
        return "0.0%"


def _add_action(actions: list[dict], *, item_key: str, category: str, severity: str, title: str, body: str, context: dict, source_page: str, source_ref: str = "") -> None:
    actions.append(
        {
            "item_key": item_key,
            "category": category,
            "severity": severity,
            "title": title,
            "body": body,
            "manager": context.get("manager", ""),
            "account_name": context.get("account_name", ""),
            "customer_id": context.get("customer_id", ""),
            "source_page": source_page,
            "source_ref": source_ref,
        }
    )


def _budget_ranges(f: dict) -> tuple[date, date, date, date, date, date, date, int]:
    today = _today_kst()
    yesterday = min(f.get("end") or today - timedelta(days=1), today - timedelta(days=1))
    avg_days = 7
    avg_d2 = yesterday
    avg_d1 = avg_d2 - timedelta(days=avg_days - 1)
    month_d1 = date(yesterday.year, yesterday.month, 1)
    month_d2 = yesterday
    prev_month_d2 = month_d1 - timedelta(days=1)
    prev_month_d1 = date(prev_month_d2.year, prev_month_d2.month, 1)
    return yesterday, avg_d1, avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, avg_days


def _build_budget_actions(meta: pd.DataFrame, engine, f: dict) -> list[dict]:
    cids = tuple(f.get("customer_ids") or [])
    if not cids:
        return []
    try:
        df = query_budget_bundle(engine, cids, *_budget_ranges(f))
    except Exception:
        return []
    if df is None or df.empty:
        return []

    meta_map = _meta_context(meta)
    work = df.copy()
    work["monthly_budget_num"] = safe_numeric_col(work, "monthly_budget")
    work["month_cost_num"] = safe_numeric_col(work, "current_month_cost")
    work["avg_cost_num"] = safe_numeric_col(work, "avg_cost")
    work["bizmoney_balance_num"] = safe_numeric_col(work, "bizmoney_balance")

    today = _today_kst()
    month_end = date(today.year + int(today.month == 12), 1 if today.month == 12 else today.month + 1, 1) - timedelta(days=1)
    elapsed_days = max(1, min(today.day, month_end.day))
    month_days = month_end.day
    work["projected_cost"] = work["month_cost_num"] / elapsed_days * month_days
    budget_base = work["monthly_budget_num"].where(work["monthly_budget_num"] != 0)
    avg_cost_base = work["avg_cost_num"].where(work["avg_cost_num"] != 0)
    work["budget_use_rate"] = work["projected_cost"] / budget_base
    work["balance_cover_days"] = work["bizmoney_balance_num"] / avg_cost_base

    actions = []
    budget_hits = work[(work["monthly_budget_num"] > 0) & (work["budget_use_rate"] >= 1.05)].copy()
    for _, row in budget_hits.sort_values("budget_use_rate", ascending=False).head(20).iterrows():
        ctx = _row_context(row, meta_map)
        severity = "danger" if float(row.get("budget_use_rate") or 0) >= 1.2 else "warning"
        _add_action(
            actions,
            item_key=f"budget_projection:{ctx['customer_id']}",
            category="예산",
            severity=severity,
            title=f"월 예산 초과 예상: {ctx['account_name']}",
            body=(
                f"예상 광고비 {format_currency(row.get('projected_cost'))} / "
                f"월 예산 {format_currency(row.get('monthly_budget_num'))} "
                f"({_fmt_pct((row.get('budget_use_rate') or 0) * 100)})"
            ),
            context=ctx,
            source_page="예산 및 잔액",
            source_ref=ctx["customer_id"],
        )

    balance_hits = work[(work["avg_cost_num"] > 0) & (work["balance_cover_days"].fillna(999) <= 2.0)].copy()
    for _, row in balance_hits.sort_values("balance_cover_days", ascending=True).head(20).iterrows():
        ctx = _row_context(row, meta_map)
        cover_days = float(row.get("balance_cover_days") or 0)
        severity = "danger" if cover_days <= 1.0 else "warning"
        _add_action(
            actions,
            item_key=f"bizmoney_low:{ctx['customer_id']}",
            category="잔액",
            severity=severity,
            title=f"비즈머니 충전 필요: {ctx['account_name']}",
            body=f"잔액 {format_currency(row.get('bizmoney_balance_num'))}, 최근 평균 지출 기준 약 {cover_days:.1f}일치 남았습니다.",
            context=ctx,
            source_page="예산 및 잔액",
            source_ref=ctx["customer_id"],
        )
    return actions


def _build_campaign_actions(meta: pd.DataFrame, engine, f: dict) -> list[dict]:
    try:
        df = query_campaign_bundle(
            engine,
            f.get("start"),
            f.get("end"),
            tuple(f.get("customer_ids") or []),
            tuple(f.get("type_sel") or []),
            int(f.get("top_n_campaign", 200) or 200),
        )
    except Exception:
        return []
    if df is None or df.empty:
        return []

    meta_map = _meta_context(meta)
    work = df.copy()
    work["cost_num"] = safe_numeric_col(work, "cost")
    work["sales_num"] = safe_numeric_col(work, "sales")
    work["target_roas_num"] = safe_numeric_col(work, "target_roas")
    work["min_roas_num"] = safe_numeric_col(work, "min_roas")
    cost_base = work["cost_num"].where(work["cost_num"] != 0)
    work["roas_num"] = work["sales_num"] / cost_base * 100
    cost_floor = max(50000, float(work["cost_num"].quantile(0.75) or 0))

    target_hits = work[
        (work["cost_num"] >= cost_floor)
        & (
            ((work["min_roas_num"] > 0) & (work["roas_num"].fillna(0) < work["min_roas_num"]))
            | ((work["target_roas_num"] > 0) & (work["roas_num"].fillna(0) < work["target_roas_num"] * 0.8))
        )
    ].copy()

    actions = []
    for _, row in target_hits.sort_values("cost_num", ascending=False).head(30).iterrows():
        ctx = _row_context(row, meta_map)
        campaign_name = str(row.get("campaign_name", "") or row.get("campaign_id", "캠페인"))
        severity = "danger" if float(row.get("min_roas_num") or 0) > 0 and float(row.get("roas_num") or 0) < float(row.get("min_roas_num") or 0) else "warning"
        _add_action(
            actions,
            item_key=f"campaign_roas:{ctx['customer_id']}:{row.get('campaign_id')}",
            category="캠페인",
            severity=severity,
            title=f"목표 ROAS 미달: {campaign_name}",
            body=(
                f"광고비 {format_currency(row.get('cost_num'))}, 현재 ROAS {_fmt_pct(row.get('roas_num'))}, "
                f"목표 {_fmt_pct(row.get('target_roas_num'))}, 최소 {_fmt_pct(row.get('min_roas_num'))}"
            ),
            context=ctx,
            source_page="성과 분석 · 캠페인",
            source_ref=str(row.get("campaign_id", "")),
        )

    zero_sales = work[(work["cost_num"] >= cost_floor) & (work["sales_num"] <= 0)].copy()
    for _, row in zero_sales.sort_values("cost_num", ascending=False).head(20).iterrows():
        ctx = _row_context(row, meta_map)
        campaign_name = str(row.get("campaign_name", "") or row.get("campaign_id", "캠페인"))
        _add_action(
            actions,
            item_key=f"campaign_no_sales:{ctx['customer_id']}:{row.get('campaign_id')}",
            category="캠페인",
            severity="warning",
            title=f"매출 없는 고비용 캠페인: {campaign_name}",
            body=f"조회 기간 광고비 {format_currency(row.get('cost_num'))}이지만 구매완료 매출이 없습니다.",
            context=ctx,
            source_page="성과 분석 · 캠페인",
            source_ref=str(row.get("campaign_id", "")),
        )
    return actions


def _build_keyword_actions(meta: pd.DataFrame, engine, f: dict) -> list[dict]:
    try:
        df = query_keyword_bundle(
            engine,
            f.get("start"),
            f.get("end"),
            tuple(f.get("customer_ids") or []),
            tuple(f.get("type_sel") or []),
            int(f.get("top_n_keyword", 300) or 300),
        )
    except Exception:
        return []
    if df is None or df.empty:
        return []

    meta_map = _meta_context(meta)
    work = df.copy()
    work["cost_num"] = safe_numeric_col(work, "cost")
    work["clk_num"] = safe_numeric_col(work, "clk")
    conv_col = "tot_conv" if "tot_conv" in work.columns else "conv"
    work["conv_num"] = safe_numeric_col(work, conv_col)
    cost_floor = max(30000, float(work["cost_num"].quantile(0.8) or 0))
    hits = work[(work["cost_num"] >= cost_floor) & (work["clk_num"] >= 20) & (work["conv_num"] <= 0)].copy()

    actions = []
    for _, row in hits.sort_values("cost_num", ascending=False).head(30).iterrows():
        ctx = _row_context(row, meta_map)
        keyword = str(row.get("keyword", "") or row.get("keyword_id", "키워드"))
        _add_action(
            actions,
            item_key=f"keyword_waste:{ctx['customer_id']}:{row.get('keyword_id')}",
            category="키워드",
            severity="warning",
            title=f"전환 없는 고비용 키워드: {keyword}",
            body=f"클릭 {_fmt_num(row.get('clk_num'))}회, 광고비 {format_currency(row.get('cost_num'))}, 전환 0건입니다.",
            context=ctx,
            source_page="성과 분석 · 키워드",
            source_ref=str(row.get("keyword_id", "")),
        )
    return actions


def _build_off_log_actions(meta: pd.DataFrame, engine, f: dict) -> list[dict]:
    try:
        df = query_campaign_off_log(engine, f.get("start"), f.get("end"), tuple(f.get("customer_ids") or []))
    except Exception:
        return []
    if df is None or df.empty:
        return []
    meta_map = _meta_context(meta)
    grouped = (
        df.assign(customer_id=df["customer_id"].map(_cid_text))
        .groupby(["customer_id", "campaign_id"], dropna=False)
        .agg(off_count=("off_time", "count"), last_off_time=("off_time", "max"))
        .reset_index()
    )
    actions = []
    for _, row in grouped.sort_values("off_count", ascending=False).head(30).iterrows():
        ctx = _row_context(row, meta_map)
        _add_action(
            actions,
            item_key=f"campaign_off:{ctx['customer_id']}:{row.get('campaign_id')}",
            category="수집/운영",
            severity="info",
            title=f"캠페인 OFF 이력 확인: {row.get('campaign_id')}",
            body=f"조회 기간 OFF 기록 {_fmt_num(row.get('off_count'))}건, 최근 시간 {row.get('last_off_time')}",
            context=ctx,
            source_page="성과 분석 · 캠페인",
            source_ref=str(row.get("campaign_id", "")),
        )
    return actions


def _generate_action_items(meta: pd.DataFrame, engine, f: dict) -> int:
    builders = [
        _build_budget_actions,
        _build_campaign_actions,
        _build_keyword_actions,
        _build_off_log_actions,
    ]
    items = []
    for builder in builders:
        items.extend(builder(meta, engine, f))
    return upsert_action_items(engine, items)


def _action_status_label(value: str) -> str:
    return {
        "open": "대기",
        "in_progress": "진행 중",
        "resolved": "완료",
        "skipped": "보류",
    }.get(str(value or ""), str(value or ""))


def _render_action_queue(meta: pd.DataFrame, engine, f: dict) -> None:
    c_refresh, c_status = st.columns([1.5, 2.5])
    with c_refresh:
        if st.button("현재 조건으로 조치 항목 갱신", key="ops_refresh_actions", type="primary", use_container_width=True):
            with st.spinner("분석 결과에서 조치 항목을 만들고 있습니다."):
                written = _generate_action_items(meta, engine, f)
            st.success(f"{written:,}개 항목을 갱신했습니다.")
    with c_status:
        status_label = st.selectbox(
            "상태",
            ["대기", "진행 중", "완료", "보류", "전체"],
            key="ops_action_status_filter",
        )
        status_filter = {"대기": "open", "진행 중": "in_progress", "완료": "resolved", "보류": "skipped", "전체": ""}.get(status_label, "open")

    auto_key = f"ops_auto_generated_{f.get('start')}_{f.get('end')}_{hash(tuple(f.get('customer_ids') or []))}_{hash(tuple(f.get('type_sel') or []))}"
    if not st.session_state.get(auto_key):
        try:
            _generate_action_items(meta, engine, f)
            st.session_state[auto_key] = True
        except Exception:
            pass

    try:
        action_df = query_action_items(engine, status_filter, limit=500)
    except Exception as e:
        st.warning(f"조치 큐 저장소를 준비하지 못했습니다: {e}")
        return
    if action_df is None or action_df.empty:
        render_empty_state("현재 조치 항목이 없습니다.", detail="필터를 넓히거나 조치 항목 갱신 버튼을 눌러보세요.")
        return

    open_count = int((action_df["status"] == "open").sum()) if "status" in action_df.columns else len(action_df)
    urgent_count = int(action_df["severity"].isin(["critical", "danger"]).sum()) if "severity" in action_df.columns else 0
    render_kpi_strip(
        [
            {"label": "표시 항목", "value": f"{len(action_df):,}개", "sub": _action_status_label(status_filter) if status_filter else "전체", "accent": "blue"},
            {"label": "대기", "value": f"{open_count:,}개", "sub": "바로 처리 대상", "accent": "amber" if open_count else "green"},
            {"label": "긴급", "value": f"{urgent_count:,}개", "sub": "위험도 danger 이상", "accent": "red" if urgent_count else "green"},
        ]
    )

    display = action_df.copy()
    display["상태"] = display["status"].map(_action_status_label)
    display = display.rename(
        columns={
            "id": "ID",
            "severity": "위험도",
            "category": "분류",
            "title": "제목",
            "body": "근거",
            "manager": "담당자",
            "account_name": "계정",
            "owner": "처리자",
            "note": "메모",
            "source_page": "출처",
            "last_seen_at": "최근 감지",
        }
    )
    view_cols = ["ID", "위험도", "분류", "제목", "근거", "담당자", "계정", "상태", "처리자", "메모", "출처", "최근 감지"]
    display = display[[c for c in view_cols if c in display.columns]].copy()
    edited = st.data_editor(
        display,
        key="ops_action_editor",
        hide_index=True,
        use_container_width=True,
        height=430,
        disabled=[c for c in display.columns if c not in {"상태", "처리자", "메모"}],
        column_config={
            "상태": st.column_config.SelectboxColumn("상태", options=["대기", "진행 중", "완료", "보류"]),
            "메모": st.column_config.TextColumn("메모", width="medium"),
        },
    )
    if st.button("상태/메모 저장", key="ops_save_action_changes", use_container_width=True):
        reverse_status = {"대기": "open", "진행 중": "in_progress", "완료": "resolved", "보류": "skipped"}
        original = action_df.set_index("id")
        saved = 0
        for _, row in edited.iterrows():
            item_id = int(row.get("ID"))
            before = original.loc[item_id]
            status = reverse_status.get(str(row.get("상태", "")), str(before.get("status", "open")))
            owner = str(row.get("처리자", "") or "")
            note = str(row.get("메모", "") or "")
            if status != before.get("status") or owner != str(before.get("owner", "") or "") or note != str(before.get("note", "") or ""):
                update_action_item(engine, item_id, status, owner, note)
                saved += 1
        st.success(f"{saved:,}개 변경 사항을 저장했습니다.")
        st.rerun()


def _render_collection_status(meta: pd.DataFrame, engine, f: dict) -> None:
    status_df = query_collection_status(engine, tuple(f.get("customer_ids") or []))
    if status_df is None or status_df.empty:
        render_empty_state("수집 상태를 확인할 데이터가 없습니다.", detail="수집 테이블 또는 계정 필터를 확인해주세요.")
        return

    meta_map = _meta_context(meta)
    work = status_df.copy()
    work["_cid"] = work["customer_id"].map(_cid_text)
    work["계정"] = work["_cid"].map(lambda x: meta_map.get(x, {}).get("account_name", x))
    work["담당자"] = work["_cid"].map(lambda x: meta_map.get(x, {}).get("manager", "미배정"))
    work["상태"] = work["stale_days"].map(lambda x: "지연" if int(x) >= 3 else ("확인 필요" if int(x) >= 1 else "정상"))

    stale_accounts = work[work["stale_days"] >= 1]["_cid"].nunique()
    delayed_accounts = work[work["stale_days"] >= 3]["_cid"].nunique()
    render_kpi_strip(
        [
            {"label": "수집 소스", "value": f"{work['source_label'].nunique():,}개", "sub": "fact 테이블 기준", "accent": "blue"},
            {"label": "확인 필요 계정", "value": f"{stale_accounts:,}개", "sub": "1일 이상 지연", "accent": "amber" if stale_accounts else "green"},
            {"label": "지연 계정", "value": f"{delayed_accounts:,}개", "sub": "3일 이상 지연", "accent": "red" if delayed_accounts else "green"},
        ]
    )

    summary = (
        work.groupby("source_label", dropna=False)
        .agg(최신일=("latest_dt", "max"), 지연_최대일=("stale_days", "max"), 계정수=("_cid", "nunique"), 행수=("row_count", "sum"))
        .reset_index()
        .rename(columns={"source_label": "수집 소스"})
        .sort_values(["지연_최대일", "수집 소스"], ascending=[False, True])
    )
    st.dataframe(summary, use_container_width=True, hide_index=True, height=220)

    detail = work.rename(
        columns={
            "source_label": "수집 소스",
            "latest_dt": "최신 수집일",
            "stale_days": "지연일",
            "row_count": "행수",
        }
    )
    detail_cols = ["상태", "담당자", "계정", "customer_id", "수집 소스", "최신 수집일", "지연일", "행수"]
    st.dataframe(detail[[c for c in detail_cols if c in detail.columns]], use_container_width=True, hide_index=True, height=430)


def _render_audit_log(engine) -> None:
    try:
        audit = query_dashboard_audit_log(engine, 300)
    except Exception as e:
        st.warning(f"변경 이력 저장소를 준비하지 못했습니다: {e}")
        return
    if audit is None or audit.empty:
        render_empty_state("아직 변경 이력이 없습니다.", detail="예산, 목표 ROAS, 연결 정보, 조치 상태를 바꾸면 이곳에 남습니다.")
        return
    work = audit.copy()
    work = work.rename(
        columns={
            "event_time": "시간",
            "actor": "사용자",
            "action_type": "작업",
            "target_type": "대상",
            "target_id": "대상 ID",
            "summary": "내용",
        }
    )
    cols = ["시간", "사용자", "작업", "대상", "대상 ID", "내용"]
    st.dataframe(work[[c for c in cols if c in work.columns]], use_container_width=True, hide_index=True, height=520)


def page_ops_center(meta: pd.DataFrame, engine, f: dict) -> None:
    render_toolbar(
        "오늘 처리할 운영 항목",
        "분석 화면에서 발견되는 위험 신호를 조치 큐로 모으고, 수집 상태와 변경 이력을 한 곳에서 확인합니다.",
        chips=[
            {"label": f"{f.get('start')} ~ {f.get('end')}", "tone": "primary"},
            {"label": f.get("scope_label", "전체 계정"), "tone": "info"},
        ],
    )

    tab_queue, tab_status, tab_audit = st.tabs(["조치 큐", "수집 상태", "변경 이력"])
    with tab_queue:
        _render_action_queue(meta, engine, f)
    with tab_status:
        _render_collection_status(meta, engine, f)
    with tab_audit:
        _render_audit_log(engine)
