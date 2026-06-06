# -*- coding: utf-8 -*-
"""pages.py - Main Router connecting all views."""

from __future__ import annotations

import os
from html import escape
import streamlit as st
import streamlit_compat  # noqa: F401

from data import *
from ui import render_hero, media_badge_html, render_connection_error
from page_helpers import BUILD_TAG, build_filters
from perf_utils import render_perf_panel, reset_perf_events


PAGE_DESCRIPTIONS = {
    "운영 센터": "오늘 처리할 조치 항목, 수집 상태, 변경 이력을 한 곳에서 관리합니다.",
    "요약": "광고비, 전환, ROAS, 위험 신호를 한 번에 보는 운영 홈입니다.",
    "예산 및 잔액": "월 예산 페이스와 비즈머니 소진 위험을 우선순위로 정리합니다.",
    "성과 분석 · 캠페인": "캠페인과 광고그룹을 선택해 하위 키워드와 소재까지 내려갑니다.",
    "성과 분석 · 키워드": "키워드와 쇼핑 상품소재의 유입, 비용, 전환 효율을 분석합니다.",
    "성과 분석 · 소재": "광고 소재와 랜딩페이지 단위의 성과를 점검합니다.",
    "쇼핑 검색어 분석": "쇼핑 검색어 기준으로 구매와 전환 기회를 찾습니다.",
    "시간·연령 분석": "시간대와 연령대별로 지출, 클릭, 전환 효율을 확인합니다.",
    "Meta 도구": "Meta 소재 다운로드와 PAUSED 세팅 생성을 관리합니다.",
    "설정 및 연결": "계정 연결, 담당자 연동, 동기화 도구를 관리합니다.",
}

NAV_CONFIG = [
    ("요약", "대시보드 요약", ":material/dashboard:"),
    ("예산 및 잔액", "예산·잔액", ":material/account_balance_wallet:"),
    ("성과 분석 · 캠페인", "캠페인 분석", ":material/campaign:"),
    ("성과 분석 · 키워드", "키워드 분석", ":material/key:"),
    ("성과 분석 · 소재", "소재 분석", ":material/ads_click:"),
    ("쇼핑 검색어 분석", "쇼핑 검색어", ":material/manage_search:"),
    ("시간·연령 분석", "시간·연령", ":material/schedule:"),
    ("Meta 도구", "Meta 도구", ":material/linked_services:"),
    ("운영 센터", "운영 센터", ":material/task_alt:"),
    ("설정 및 연결", "설정·연결", ":material/settings:"),
]

NAV_GROUPS = [
    ("summary", "요약", "핵심 화면", ["요약", "운영 센터"]),
    (
        "analysis",
        "성과 분석",
        "성과 분석",
        ["성과 분석 · 캠페인", "성과 분석 · 키워드", "성과 분석 · 소재", "쇼핑 검색어 분석", "시간·연령 분석"],
    ),
    ("admin", "관리 도구", "관리 도구", ["예산 및 잔액", "Meta 도구", "설정 및 연결"]),
]

NAV_LABELS = {page_key: short_label for page_key, short_label, _icon in NAV_CONFIG}
NAV_META = {page_key: (short_label, icon) for page_key, short_label, icon in NAV_CONFIG}


def _nav_group_for_page(page_key: str) -> str:
    for group_key, _label, _title, pages in NAV_GROUPS:
        if page_key in pages:
            return group_key
    return NAV_GROUPS[0][0]


def _visible_nav_groups(nav_items: list[str]) -> list[tuple[str, str, str, list[str]]]:
    allowed = set(nav_items)
    groups = []
    for group_key, label, title, pages in NAV_GROUPS:
        visible_pages = [page for page in pages if page in allowed]
        if visible_pages:
            groups.append((group_key, label, title, visible_pages))
    return groups


def _render_sidebar_nav(nav_items: list[str]) -> str:
    if st.session_state.get("nav_page") not in nav_items:
        st.session_state["nav_page"] = nav_items[0]

    current_nav = st.session_state.get("nav_page", nav_items[0])
    groups = _visible_nav_groups(nav_items)
    if not groups:
        return current_nav

    group_keys = [group[0] for group in groups]
    current_group = _nav_group_for_page(current_nav)
    if current_group not in group_keys:
        current_group = group_keys[0]
    if st.session_state.get("nav_group") not in group_keys:
        st.session_state["nav_group"] = current_group

    selected_group = st.selectbox(
        "메뉴 그룹",
        group_keys,
        index=group_keys.index(st.session_state.get("nav_group", current_group)),
        format_func=lambda key: next(label for group_key, label, _title, _pages in groups if group_key == key),
        key="nav_group",
        label_visibility="collapsed",
    )

    if selected_group != current_group:
        target_pages = next(pages for group_key, _label, _title, pages in groups if group_key == selected_group)
        st.session_state["nav_page"] = target_pages[0]
        st.rerun()

    group_title, group_pages = next((title, pages) for group_key, _label, title, pages in groups if group_key == selected_group)
    st.markdown(f"<div class='nav-group-heading'>{escape(group_title)}</div>", unsafe_allow_html=True)
    for page_key in group_pages:
        short_label, icon = NAV_META[page_key]
        is_active = page_key == current_nav
        clicked = st.button(
            short_label,
            key=f"nav_btn_{page_key}",
            icon=icon,
            type="primary" if is_active else "secondary",
            use_container_width=True,
        )
        if clicked and not is_active:
            st.session_state["nav_page"] = page_key
            st.session_state["nav_group"] = selected_group
            st.rerun()
    return st.session_state.get("nav_page", nav_items[0])


def _latest_status(latest: dict | None) -> tuple[str, str]:
    if not latest:
        return "동기화 대기", "수집된 데이터 날짜가 없습니다"
    values = [v for v in latest.values() if v is not None]
    if not values:
        return "동기화 대기", "수집된 데이터 날짜가 없습니다"
    newest = max(values, key=lambda x: str(x))
    return str(newest), f"{len(values):,}개 테이블 기준"


def _page_action(nav: str) -> tuple[str, str]:
    actions = {
        "운영 센터": ("조치 큐 갱신", "예산, 잔액, ROAS, 키워드 낭비 신호를 처리 항목으로 모으세요."),
        "요약": ("성과 이상 신호부터 확인", "광고비, 전환, ROAS의 변화가 큰 계정부터 훑어보세요."),
        "예산 및 잔액": ("소진 위험 우선 점검", "잔액과 월 예산 페이스가 어긋난 계정을 먼저 조정하세요."),
        "성과 분석 · 캠페인": ("캠페인 단위 병목 탐색", "비용은 크지만 전환 효율이 낮은 캠페인을 드릴다운하세요."),
        "성과 분석 · 키워드": ("키워드 낭비 구간 정리", "클릭은 많고 전환이 약한 키워드를 빠르게 찾으세요."),
        "성과 분석 · 소재": ("소재별 승자/패자 비교", "CTR과 전환 효율이 갈리는 소재를 같은 기준으로 비교하세요."),
        "쇼핑 검색어 분석": ("검색어 기회 발굴", "구매완료와 장바구니 신호가 있는 검색어를 분리해서 보세요."),
        "시간·연령 분석": ("타깃 시간대 재배분", "요일, 시간, 연령대별 효율 차이를 예산 조정에 연결하세요."),
        "Meta 도구": ("Meta 세팅 안전 생성", "헤이즈코리아와 핵이득마켓 Meta 소재를 내려받고 PAUSED 상태로 세팅하세요."),
        "설정 및 연결": ("데이터 연결 상태 확인", "계정 연결, 매체 매핑, 수집 상태를 먼저 정리하세요."),
    }
    return actions.get(nav, ("현재 화면 점검", "필터를 좁힌 뒤 표와 차트를 함께 확인하세요."))


def _render_command_center(nav: str, latest: dict | None, f: dict | None) -> None:
    latest_date, latest_note = _latest_status(latest)
    action_title, action_body = _page_action(nav)
    date_range = "-"
    scope = "전체 계정"
    media_count = "전체 매체"
    if f:
        date_range = f"{f.get('start')} ~ {f.get('end')}"
        scope = str(f.get("scope_label") or "전체 계정")
        media_sel = list(f.get("media_sel") or [])
        media_count = f"{len(media_sel):,}개 매체" if media_sel else "전체 매체"
    st.markdown(
        f"""
        <div class='nv-command-center'>
            <div class='nv-command-card primary'>
                <div class='nv-command-label'>조회 컨텍스트</div>
                <div class='nv-command-value'>{escape(scope)}</div>
                <div class='nv-command-note'>{escape(date_range)} · {escape(media_count)}</div>
            </div>
            <div class='nv-command-card'>
                <div class='nv-command-label'>최근 수집일</div>
                <div class='nv-command-value'>{escape(latest_date)}</div>
                <div class='nv-command-note'>{escape(latest_note)}</div>
            </div>
            <div class='nv-command-card action'>
                <div class='nv-command-label'>추천 액션</div>
                <div class='nv-command-value'>{escape(action_title)}</div>
                <div class='nv-command-note'>{escape(action_body)}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_page_header(nav: str, latest: dict | None, f: dict | None = None) -> None:
    subtitle = PAGE_DESCRIPTIONS.get(nav, "")
    chips = []
    media_html = media_badge_html("전체 매체")
    scope_label = "전체 계정"
    if f:
        media_sel = list(f.get("media_sel") or [])
        media_html = "".join(media_badge_html(x) for x in media_sel) if media_sel else media_badge_html("전체 매체")
        scope_label = str(f.get("scope_label") or "전체 계정")
        chips.append(("primary", f"{f.get('start')} ~ {f.get('end')}"))
        if f.get("type_sel"):
            chips.append(("info", ", ".join(map(str, f.get("type_sel", [])))))

    chip_html = "".join(
        f"<span class='nv-meta-chip {escape(tone)}'>{escape(label)}</span>"
        for tone, label in chips
    )
    st.markdown(
        f"""
        <div class='nv-console-head nv-console-head-compact'>
            <div class='nv-console-top'>
                <div class='nv-page-head-left'>
                    <div class='nv-h1'>{escape(nav)}</div>
                    <p class='nv-page-sub'>{escape(subtitle)}</p>
                </div>
                <div class='nv-page-meta'>{chip_html}</div>
            </div>
            <div class='nv-filter-bar'>
                <div class='nv-scope-bar'>
                    <div class='nv-scope-left'>
                        <span class='nv-scope-label'>조회 범위</span>{media_html}
                        <span class='nv-meta-chip'>{escape(scope_label)}</span>
                    </div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def main():
    try:
        engine = get_engine()
        latest = get_latest_dates(engine)
    except Exception as e:
        render_hero(None, BUILD_TAG)
        render_connection_error(str(e))
        return

    try:
        for ext in ['png', 'jpg', 'jpeg', 'webp']:
            if os.path.exists(f"logo.{ext}"):
                st.logo(f"logo.{ext}")
                break
    except Exception:
        pass

    render_hero(latest, BUILD_TAG)
    meta = get_meta(engine)
    meta_ready = (meta is not None) and (not meta.empty)

    with st.sidebar:
        st.markdown("<div class='nav-sidebar-title'>메뉴</div>", unsafe_allow_html=True)

        if not meta_ready:
            st.warning("동기화가 필요합니다.")

        nav_items = [item[0] for item in NAV_CONFIG] if meta_ready else ["설정 및 연결"]
        nav = _render_sidebar_nav(nav_items)

    f = None
    if nav in {"설정 및 연결", "Meta 도구"}:
        st.session_state["_show_perf_diag"] = False
    else:
        if not meta_ready:
            st.error("설정 메뉴에서 동기화를 진행해주세요.")
            return
        f = build_filters(meta, get_campaign_type_options_cached(engine), engine)
        st.session_state["_show_perf_diag"] = bool(f.get("show_diagnostics", False))
        reset_perf_events()

    _render_page_header(nav, latest, f)
    _render_command_center(nav, latest, f)

    requires_selection_pages = {
        "요약",
        "성과 분석 · 캠페인",
        "성과 분석 · 키워드",
        "성과 분석 · 소재",
        "쇼핑 검색어 분석",
        "시간·연령 분석",
    }

    if f is not None and nav in requires_selection_pages and not (f.get("manager") or f.get("account")):
        st.info("담당자 또는 광고주(계정) 필터를 먼저 1개 이상 선택하면 데이터가 표시됩니다.")
        st.stop()

    if nav == "운영 센터":
        from view_ops import page_ops_center
        page_ops_center(meta, engine, f)
    elif nav == "요약":
        from view_overview import page_overview
        page_overview(meta, engine, f)
    elif nav == "예산 및 잔액":
        from view_budget import page_budget
        page_budget(meta, engine, f)
    elif nav == "성과 분석 · 캠페인":
        from view_campaign import page_perf_campaign
        page_perf_campaign(meta, engine, f)
    elif nav == "성과 분석 · 키워드":
        from view_keyword import page_perf_keyword
        page_perf_keyword(meta, engine, f)
    elif nav == "쇼핑 검색어 분석":
        from view_shopping_query import page_perf_shopping_query
        page_perf_shopping_query(meta, engine, f)
    elif nav == "성과 분석 · 소재":
        from view_ad import page_perf_ad
        page_perf_ad(meta, engine, f)
    elif nav == "시간·연령 분석":
        from view_time_age import page_time_age
        page_time_age(meta, engine, f)
    elif nav == "Meta 도구":
        from view_meta_tools import page_meta_tools
        page_meta_tools(engine)
    else:
        from view_settings import page_settings
        page_settings(engine)

    render_perf_panel()

if __name__ == "__main__":
    main()
