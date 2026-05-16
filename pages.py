# -*- coding: utf-8 -*-
"""pages.py - Main Router connecting all views."""

from __future__ import annotations

import os
from html import escape
import streamlit as st
import streamlit_compat  # noqa: F401

from data import *
from ui import render_hero
from page_helpers import BUILD_TAG, build_filters


PAGE_DESCRIPTIONS = {
    "요약": "광고비, 전환, ROAS, 위험 신호를 한 번에 보는 운영 홈입니다.",
    "예산 및 잔액": "월 예산 페이스와 비즈머니 소진 위험을 우선순위로 정리합니다.",
    "매체(지면) 분석": "지면, 기기, 비용 누수 항목을 나눠 효율을 비교합니다.",
    "성과 분석 · 캠페인": "캠페인과 광고그룹을 선택해 하위 키워드와 소재까지 내려갑니다.",
    "성과 분석 · 키워드": "키워드와 쇼핑 상품소재의 유입, 비용, 전환 효율을 분석합니다.",
    "성과 분석 · 소재": "광고 소재와 랜딩페이지 단위의 성과를 점검합니다.",
    "쇼핑 검색어 분석": "쇼핑 검색어 기준으로 구매와 전환 기회를 찾습니다.",
    "설정 및 연결": "계정 연결, 동기화, 목표 ROAS 기준을 관리합니다.",
}

NAV_CONFIG = [
    ("요약", "대시보드", ":material/dashboard:"),
    ("예산 및 잔액", "비즈머니", ":material/account_balance_wallet:"),
    ("매체(지면) 분석", "매체", ":material/hub:"),
    ("성과 분석 · 캠페인", "캠페인", ":material/campaign:"),
    ("성과 분석 · 키워드", "키워드", ":material/key:"),
    ("성과 분석 · 소재", "소재", ":material/ads_click:"),
    ("쇼핑 검색어 분석", "검색어", ":material/manage_search:"),
    ("설정 및 연결", "설정", ":material/settings:"),
]


def _render_page_header(nav: str, latest: dict | None, f: dict | None = None) -> None:
    subtitle = PAGE_DESCRIPTIONS.get(nav, "")
    chips = []
    if f:
        chips.append(("primary", f"{f.get('start')} ~ {f.get('end')}"))
        if f.get("type_sel"):
            chips.append(("info", ", ".join(map(str, f.get("type_sel", [])))))
        else:
            chips.append(("info", "전체 유형"))
        selected_count = len(f.get("selected_customer_ids", []) or [])
        chips.append(("success" if selected_count else "warning", f"{selected_count:,}개 계정"))
    elif latest:
        cd = latest.get("campaign")
        chips.append(("primary", f"최근 수집 {str(cd)[:10] if cd else '대기 중'}"))
    if BUILD_TAG:
        chips.append(("", f"Build {BUILD_TAG}"))

    chip_html = "".join(
        f"<span class='nv-meta-chip {escape(tone)}'>{escape(label)}</span>"
        for tone, label in chips
    )
    action_html = "".join([
        "<span class='nv-action-chip'>스냅샷</span>",
        "<span class='nv-icon-chip'>↻</span>",
        "<span class='nv-icon-chip'>↗</span>",
        "<span class='nv-action-chip primary'>새 광고 만들기</span>",
    ])
    st.markdown(
        f"""
        <div class='nv-console-head'>
            <div class='nv-console-top'>
                <div class='nv-page-head-left'>
                    <div class='nv-page-eyebrow'>Ad Ops Console</div>
                    <div class='nv-h1'>{escape(nav)}</div>
                    <p class='nv-page-sub'>{escape(subtitle)}</p>
                </div>
                <div class='nv-console-actions'>{action_html}</div>
            </div>
            <div class='nv-filter-bar'>
                <div class='nv-filter-search'>제목, 채널, 상태로 필터하기</div>
                <div class='nv-page-meta'>{chip_html}</div>
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

        for page_key, short_label, icon in NAV_CONFIG:
            if page_key not in nav_items:
                continue
            is_active = st.session_state.get("nav_page") == page_key
            if st.button(
                short_label,
                key=f"nav_btn_{page_key}",
                icon=icon,
                use_container_width=True,
                type="primary" if is_active else "secondary",
            ):
                st.session_state["nav_page"] = page_key

        nav = st.session_state.get("nav_page", nav_items[0])

    f = None
    if nav != "설정 및 연결":
        if not meta_ready:
            st.error("설정 메뉴에서 동기화를 진행해주세요.")
            return
        f = build_filters(meta, get_campaign_type_options_cached(engine), engine)

    _render_page_header(nav, latest, f)

    requires_selection_pages = {
        "요약",
        "예산 및 잔액",
        "매체(지면) 분석",
        "성과 분석 · 캠페인",
        "성과 분석 · 키워드",
        "성과 분석 · 소재",
        "쇼핑 검색어 분석",
    }

    if nav in requires_selection_pages and not (f.get("manager") or f.get("account")):
        st.info("담당자 또는 광고주(계정) 필터를 먼저 1개 이상 선택하면 데이터가 표시됩니다.")
        st.stop()

    if nav == "요약":
        from view_overview import page_overview
        page_overview(meta, engine, f)
    elif nav == "예산 및 잔액":
        from view_budget import page_budget
        page_budget(meta, engine, f)
    elif nav == "매체(지면) 분석":
        from view_media import page_media
        page_media(engine, f)
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
    else:
        from view_settings import page_settings
        page_settings(engine)

if __name__ == "__main__":
    main()
