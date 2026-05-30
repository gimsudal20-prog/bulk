# -*- coding: utf-8 -*-
"""pages.py - Main Router connecting all views."""

from __future__ import annotations

import os
from html import escape
import streamlit as st
import streamlit_compat  # noqa: F401

from data import *
from ui import render_hero, media_badge_html
from page_helpers import BUILD_TAG, build_filters
from perf_utils import render_perf_panel, reset_perf_events


PAGE_DESCRIPTIONS = {
    "요약": "광고비, 전환, ROAS, 위험 신호를 한 번에 보는 운영 홈입니다.",
    "예산 및 잔액": "월 예산 페이스와 비즈머니 소진 위험을 우선순위로 정리합니다.",
    "성과 분석 · 캠페인": "캠페인과 광고그룹을 선택해 하위 키워드와 소재까지 내려갑니다.",
    "성과 분석 · 키워드": "키워드와 쇼핑 상품소재의 유입, 비용, 전환 효율을 분석합니다.",
    "성과 분석 · 소재": "광고 소재와 랜딩페이지 단위의 성과를 점검합니다.",
    "쇼핑 검색어 분석": "쇼핑 검색어 기준으로 구매와 전환 기회를 찾습니다.",
    "시간·연령 분석": "시간대와 연령대별로 지출, 클릭, 전환 효율을 확인합니다.",
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
    ("설정 및 연결", "설정·연결", ":material/settings:"),
]

NAV_LABELS = {page_key: short_label for page_key, short_label, _icon in NAV_CONFIG}


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
        else:
            chips.append(("info", "전체 유형"))
        selected_count = len(f.get("selected_customer_ids", []) or [])
        chips.append(("success" if selected_count else "warning", f"조회 ID {selected_count:,}개"))

    chip_html = "".join(
        f"<span class='nv-meta-chip {escape(tone)}'>{escape(label)}</span>"
        for tone, label in chips
    )
    st.markdown(
        f"""
        <div class='nv-console-head'>
            <div class='nv-console-top'>
                <div class='nv-page-head-left'>
                    <div class='nv-h1'>{escape(nav)}</div>
                    <p class='nv-page-sub'>{escape(subtitle)}</p>
                </div>
            </div>
            <div class='nv-filter-bar'>
                <div class='nv-scope-bar'>
                    <div class='nv-scope-left'>
                        <span class='nv-scope-label'>현재 조회</span>{media_html}
                        <span class='nv-meta-chip'>{escape(scope_label)}</span>
                    </div>
                    <div class='nv-page-meta'>{chip_html}</div>
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
        st.error(str(e))
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
        st.markdown("<div class='nav-sidebar-title'>Navigation</div>", unsafe_allow_html=True)

        if not meta_ready:
            st.warning("동기화가 필요합니다.")

        nav_items = [item[0] for item in NAV_CONFIG] if meta_ready else ["설정 및 연결"]
        if st.session_state.get("nav_page") not in nav_items:
            st.session_state["nav_page"] = nav_items[0]

        current_nav = st.session_state.get("nav_page", nav_items[0])
        for page_key, short_label, icon in NAV_CONFIG:
            if page_key not in nav_items:
                continue
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
                st.rerun()
        nav = st.session_state.get("nav_page", nav_items[0])

    f = None
    if nav == "설정 및 연결":
        st.session_state["_show_perf_diag"] = False
    else:
        if not meta_ready:
            st.error("설정 메뉴에서 동기화를 진행해주세요.")
            return
        f = build_filters(meta, get_campaign_type_options_cached(engine), engine)
        st.session_state["_show_perf_diag"] = bool(f.get("show_diagnostics", False))
        reset_perf_events()

    _render_page_header(nav, latest, f)

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

    if nav == "요약":
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
    else:
        from view_settings import page_settings
        page_settings(engine)

    render_perf_panel()

if __name__ == "__main__":
    main()
