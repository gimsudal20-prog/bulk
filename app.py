# -*- coding: utf-8 -*-
"""app.py - Entry point (kept intentionally small)."""

from __future__ import annotations

import streamlit as st
from datetime import date

# Streamlit page config MUST be the first Streamlit command
st.set_page_config(
    page_title="네이버 검색광고 통합 대시보드",
    layout="wide",
    initial_sidebar_state="expanded",
)

from streamlit_compat import apply_streamlit_compat  # noqa: E402
apply_streamlit_compat()

try:  # noqa: E402
    from dotenv import load_dotenv
    load_dotenv(override=False)
except Exception:
    pass

def clear_cache_daily():
    """날짜 변경 시 필요한 캐시만 정리해 아침 첫 진입 재로딩을 줄입니다."""
    today = date.today()
    prev_date = st.session_state.get("current_date")
    if prev_date is None:
        st.session_state["current_date"] = today
        return

    if prev_date == today:
        return

    st.cache_data.clear()
    st.cache_resource.clear()

    # session_state 전체 삭제는 첫 화면 렌더링 연쇄를 키워서 피한다.
    keep_keys = {"nav_page"}
    preserved = {k: v for k, v in st.session_state.items() if k in keep_keys}
    for key in list(st.session_state.keys()):
        if key not in keep_keys:
            del st.session_state[key]
    st.session_state.update(preserved)
    st.session_state["current_date"] = today

clear_cache_daily()

from styles import apply_global_css  # noqa: E402
apply_global_css()

import pages  # noqa: E402

pages.main()
