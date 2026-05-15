# -*- coding: utf-8 -*-
"""Lightweight profiling helpers for dashboard runtime diagnostics."""
from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Dict

import streamlit as st

_SESSION_KEY = "_perf_events"
_ENABLE_KEY = "_show_perf_diag"


def perf_enabled() -> bool:
    try:
        return bool(st.session_state.get(_ENABLE_KEY, False))
    except Exception:
        return False


def reset_perf_events() -> None:
    try:
        st.session_state[_SESSION_KEY] = []
    except Exception:
        pass


def _append_event(kind: str, label: str, elapsed_ms: float, extra: Dict[str, Any] | None = None) -> None:
    if not perf_enabled():
        return
    try:
        events = st.session_state.setdefault(_SESSION_KEY, [])
        events.append({
            "kind": str(kind),
            "label": str(label),
            "elapsed_ms": round(float(elapsed_ms), 1),
            "extra": extra or {},
        })
    except Exception:
        pass


def record_db_timing(kind: str, label: str, elapsed_ms: float, **extra: Any) -> None:
    _append_event(kind, label, elapsed_ms, extra)


@contextmanager
def timed_block(label: str, kind: str = "block", **extra: Any):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        _append_event(kind, label, elapsed_ms, extra)


def render_perf_panel() -> None:
    if not perf_enabled():
        return
    events = st.session_state.get(_SESSION_KEY, []) or []
    if not events:
        return
    total_ms = sum(float(e.get("elapsed_ms") or 0.0) for e in events)
    with st.expander("속도 진단 결과", expanded=False):
        st.caption(f"누적 측정 {len(events)}건 · 총 {total_ms:.1f} ms")
        rows = []
        for e in events:
            extra = e.get("extra") or {}
            extra_txt = " | ".join(f"{k}={v}" for k, v in extra.items() if v not in (None, "", [], {}))
            rows.append({
                "구분": e.get("kind", ""),
                "항목": e.get("label", ""),
                "시간(ms)": e.get("elapsed_ms", 0.0),
                "상세": extra_txt,
            })
        st.dataframe(rows, use_container_width=True, hide_index=True)
