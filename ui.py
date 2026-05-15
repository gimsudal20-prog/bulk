# -*- coding: utf-8 -*-
"""ui.py - UI components (tables/charts/downloads) for the Streamlit dashboard."""

from __future__ import annotations

import html
import pandas as pd
import streamlit as st

from data import format_currency

# ==========================================
# 🎨 Theme Constants (Sync with styles.py)
# ==========================================
THEME = {
    "primary": "#0528F2",
    "primary_soft": "#EEF2FF",
    "bg": "#FFFFFF",
    "surface": "#FFFFFF",
    "line": "#E5E7EB",
    "text": "#0F172A",
    "muted": "#64748B",
    "success": "#16A34A",
    "success_bg": "#DCFCE7",
    "warning": "#F59E0B",
    "warning_bg": "#FEF3C7",
    "danger": "#DC2626",
    "danger_bg": "#FEE2E2",
    "info": "#0891B2",
    "info_bg": "#E0F2FE",
}


def _ensure_echarts():
    try:
        from streamlit_echarts import st_echarts as echarts_renderer
        return echarts_renderer
    except Exception:
        return None


def _ensure_aggrid():
    try:
        from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode, GridUpdateMode
        return AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode, GridUpdateMode
    except Exception:
        return None


def safe_numeric_series(values, *, length: int | None = None, default: float = 0.0) -> pd.Series:
    """Return a numeric Series even when callers pass a scalar fallback."""
    if isinstance(values, pd.Series):
        source = values
    elif values is None:
        source = pd.Series([default] * int(length or 0), dtype="float64")
    elif isinstance(values, (list, tuple, pd.Index)):
        source = pd.Series(values)
    else:
        source = pd.Series([values] * int(length or 1))
    cleaned = (
        source.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("원", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace(r"[^0-9.-]", "", regex=True)
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
    )
    out = pd.to_numeric(cleaned, errors="coerce").fillna(default)
    if length is not None and len(out.index) != int(length):
        out = pd.Series([default] * int(length), dtype="float64")
    return out


def safe_numeric_col(df: pd.DataFrame, col: str, *, default: float = 0.0) -> pd.Series:
    if df is None:
        return pd.Series(dtype="float64")
    if col in df.columns:
        return safe_numeric_series(df[col], length=len(df.index), default=default)
    return pd.Series([default] * len(df.index), dtype="float64")


# ==========================================
# 🧩 UI Components
# ==========================================

def render_empty_state(message: str = "조회된 데이터가 없습니다.", height: int = 300) -> None:
    safe_msg = html.escape(message)
    empty_html = f"""
    <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; height: {height}px; width: 100%; background-color: {THEME['bg']}; border: 1px dashed {THEME['line']}; border-radius: 12px; color: {THEME['muted']}; text-align: center;">
        <div style="font-size: 14px; font-weight: 600;">{safe_msg}</div>
        <div style="font-size: 12px; margin-top: 4px; opacity: 0.7;">조건을 변경하거나 동기화를 확인해주세요.</div>
    </div>
    """
    st.markdown(empty_html, unsafe_allow_html=True)


def render_hero(latest_dates: dict | None, build_tag: str, dashboard_title: str = "마케팅 통합 대시보드") -> None:
    dt_str = "수집 대기 중"
    if latest_dates:
        cd = latest_dates.get("campaign")
        dt_str = str(cd)[:10] if cd else "수집 대기 중"

    safe_title = html.escape(dashboard_title)
    safe_dt = html.escape(dt_str)
    safe_tag = html.escape(str(build_tag or "local"))

    st.sidebar.markdown(f"""
    <div class='sidebar-brand-card'>
        <div class='sidebar-brand-mark'>A</div>
        <div class='sidebar-brand-copy'>
            <div class='sidebar-info-label'>{safe_title}</div>
            <div class='sidebar-info-value'>최근 수집 <span>{safe_dt}</span></div>
            <div class='sidebar-build-tag'>Build {safe_tag}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _delta_class_and_text(cur_val, base_val, improve_when_up: bool = True) -> tuple[str, str]:
    try:
        cur = float(cur_val or 0)
        base = float(base_val or 0)
    except Exception:
        return "neu", "비교 없음"
    if base == 0:
        diff = 100.0 if cur > 0 else 0.0
    else:
        diff = ((cur - base) / abs(base)) * 100.0
    if abs(diff) < 3:
        return "neu", f"{diff:+.1f}%"
    improved = diff > 0 if improve_when_up else diff < 0
    return ("pos" if improved else "neg"), f"{diff:+.1f}%"


def render_kpi_strip(items: list[dict]) -> None:
    """Render a dense top-line KPI strip for ad operation pages."""
    cells = []
    for item in items:
        label = html.escape(str(item.get("label", "")))
        value = html.escape(str(item.get("value", "-")))
        cls = str(item.get("tone", "") or "")
        sub = item.get("sub")
        if sub is None and "cur" in item and "base" in item:
            cls, sub = _delta_class_and_text(item.get("cur"), item.get("base"), bool(item.get("improve_when_up", True)))
        sub = html.escape(str(sub or ""))
        cls = html.escape(cls if cls in {"pos", "neg", "neu"} else "neu")
        tone = str(item.get("accent", "") or "")
        tone_cls = tone if tone in {"blue", "green", "amber", "red", "cyan"} else ""
        cells.append(
            f"<div class='nv-kpi-item {tone_cls}'>"
            f"<div class='nv-kpi-item-label'>{label}</div>"
            f"<div class='nv-kpi-item-value' title='{value}'>{value}</div>"
            f"<div class='nv-kpi-item-sub {cls}'>{sub}</div>"
            "</div>"
        )
    st.markdown(f"<div class='nv-kpi-strip'>{''.join(cells)}</div>", unsafe_allow_html=True)


def render_ops_cards(items: list[dict]) -> None:
    """Render compact status/action cards for budget and campaign triage."""
    cards = []
    for item in items:
        tone = str(item.get("tone", "") or "")
        tone_cls = tone if tone in {"primary", "success", "warning", "danger", "info"} else ""
        title = html.escape(str(item.get("title", "")))
        value = html.escape(str(item.get("value", "-")))
        note = html.escape(str(item.get("note", "")))
        icon = html.escape(str(item.get("icon", "") or ""))
        cards.append(
            f"<div class='nv-op-card {tone_cls}'>"
            f"<div class='nv-op-card-icon'>{icon}</div>"
            f"<div class='nv-op-card-title'>{title}</div>"
            f"<div class='nv-op-card-value'>{value}</div>"
            f"<div class='nv-op-card-note'>{note}</div>"
            "</div>"
        )
    st.markdown(f"<div class='nv-op-grid'>{''.join(cards)}</div>", unsafe_allow_html=True)


def render_toolbar(title: str, subtitle: str = "", chips: list[dict] | None = None) -> None:
    chips = chips or []
    chip_html = []
    for chip in chips:
        label = html.escape(str(chip.get("label", "")))
        tone = str(chip.get("tone", "") or "")
        tone_cls = tone if tone in {"primary", "success", "warning", "danger", "info"} else ""
        icon = html.escape(str(chip.get("icon", "") or ""))
        chip_html.append(f"<span class='nv-chip {tone_cls}'>{icon}{label}</span>")
    safe_title = html.escape(str(title))
    safe_sub = html.escape(str(subtitle))
    st.markdown(
        "<div class='nv-toolbar'>"
        "<div>"
        f"<div class='nv-toolbar-title'>{safe_title}</div>"
        f"<div class='nv-toolbar-sub'>{safe_sub}</div>"
        "</div>"
        f"<div class='nv-toolbar-actions'>{''.join(chip_html)}</div>"
        "</div>",
        unsafe_allow_html=True,
    )


def ui_metric_or_stmetric(title: str, value: str, desc: str = "", key: str = ""):
    safe_title = html.escape(str(title))
    safe_value = html.escape(str(value))
    safe_desc = html.escape(str(desc))

    html_str = f"""
    <div class="nv-metric-card">
        <div class="nv-metric-card-title">{safe_title}</div>
        <div class="nv-metric-card-value">{safe_value}</div>
        <div class="nv-metric-card-desc">{safe_desc}</div>
    </div>
    """
    st.markdown(html_str, unsafe_allow_html=True)


def render_big_table(df, key: str, height: int = 400) -> None:
    if df is None:
        render_empty_state("데이터 로드 실패", height)
        return

    is_styler = hasattr(df, "data")
    check_df = df.data if is_styler else df

    if check_df.empty:
        render_empty_state("조회된 데이터가 없습니다.", height)
        return

    if len(check_df) > 1000:
        try:
            st.dataframe(check_df, use_container_width=True, height=height, hide_index=True)
        except Exception:
            st.dataframe(check_df, use_container_width=True, height=height)
        return

    aggrid_parts = _ensure_aggrid()
    if aggrid_parts:
        AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode, GridUpdateMode = aggrid_parts
        gb = GridOptionsBuilder.from_dataframe(check_df)
        gb.configure_default_column(resizable=True, filterable=True, sortable=True)
        gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=15)
        gb.configure_selection('single')
        grid_options = gb.build()

        AgGrid(
            check_df,
            gridOptions=grid_options,
            height=height,
            width='100%',
            theme='alpine',
            update_mode=GridUpdateMode.NO_UPDATE,
            columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
            allow_unsafe_jscode=False,
            key=f"aggrid_{key}",
        )
        return

    try:
        st.dataframe(df, use_container_width=True, height=height, hide_index=True)
    except Exception:
        st.dataframe(check_df, use_container_width=True, height=height, hide_index=True)


def render_budget_month_table_with_bars(df: pd.DataFrame, key: str, height: int = 400) -> None:
    if df is None or df.empty:
        render_empty_state("예산 데이터가 없습니다.", height)
        return

    df_disp = df.copy()

    def _pbar(val):
        try:
            v = float(val) if pd.notna(val) else 0.0
        except Exception:
            v = 0.0
        w = min(v, 100)
        c = THEME['primary']
        if v >= 100:
            c = THEME['danger']
        elif v >= 90:
            c = THEME['warning']
        return f"<div class='nv-pbar'><div class='nv-pbar-bg'><div class='nv-pbar-fill' style='width:{w}%; background:{c};'></div></div><div class='nv-pbar-txt'>{v:.1f}%</div></div>"

    if "집행률(%)" in df_disp.columns:
        df_disp["집행률 바"] = df_disp["집행률(%)"].apply(_pbar)

    html_rows = []
    cols = [c for c in df_disp.columns if c != "집행률(%)"]
    th_html = "".join(f"<th>{html.escape(str(c))}</th>" for c in cols)

    for _, row in df_disp.iterrows():
        tds = []
        for c in cols:
            val = row[c]
            if c == "상태":
                v_str = html.escape(str(val))
                bg, text = THEME['surface'], THEME['muted']
                if "적정" in v_str:
                    bg, text = THEME['primary_soft'], THEME['primary']
                elif "주의" in v_str:
                    bg, text = THEME['warning_bg'], THEME['warning']
                elif "초과" in v_str or "악화" in v_str:
                    bg, text = THEME['danger_bg'], THEME['danger']
                tds.append(f"<td><span style='background:{bg}; color:{text}; padding:4px 8px; border-radius:6px; font-weight:600; font-size:12px;'>{v_str}</span></td>")
            elif c == "집행률 바":
                tds.append(f"<td>{val}</td>")
            else:
                tds.append(f"<td>{html.escape(str(val))}</td>")
        html_rows.append(f"<tr>{''.join(tds)}</tr>")

    table_html = f"<div style='height:{height}px; overflow-y:auto;'><table class='nv-table'><thead><tr>{th_html}</tr></thead><tbody>{''.join(html_rows)}</tbody></table></div>"
    st.markdown(table_html, unsafe_allow_html=True)


def render_echarts_dual_axis(title: str, df: pd.DataFrame, x_col: str, y1_col: str, y1_name: str, y2_col: str, y2_name: str, height: int = 300):
    if df.empty:
        render_empty_state("차트를 그릴 데이터가 부족합니다.", height)
        return

    x_data = df[x_col].dt.strftime('%m-%d').tolist() if pd.api.types.is_datetime64_any_dtype(df[x_col]) else df[x_col].astype(str).tolist()
    y1_data = df[y1_col].fillna(0).tolist()
    y2_data = df[y2_col].fillna(0).tolist()

    options = {
        "backgroundColor": "#FFFFFF",
        "title": {"text": title, "textStyle": {"fontSize": 13, "color": THEME['text'], "fontWeight": 600}, "left": "left", "top": 4},
        "color": [THEME['primary_soft'], THEME['primary']],
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "cross", "crossStyle": {"color": THEME['line']}},
            "backgroundColor": "#FFFFFF",
            "borderColor": THEME['line'],
            "borderWidth": 1,
            "textStyle": {"color": THEME['text'], "fontSize": 12},
            "padding": [8, 10],
        },
        "legend": {"data": [y1_name, y2_name], "top": 6, "right": 0, "itemWidth": 10, "itemHeight": 10, "textStyle": {"color": THEME['muted'], "fontSize": 11}},
        "grid": {"left": "1%", "right": "1%", "bottom": "10%", "top": 56, "containLabel": True},
        "xAxis": [{
            "type": "category", "data": x_data, "axisPointer": {"type": "shadow"},
            "axisLine": {"lineStyle": {"color": THEME['line']}},
            "axisTick": {"show": False},
            "axisLabel": {"color": THEME['muted'], "fontSize": 11}
        }],
        "yAxis": [
            {
                "type": "value", "name": y1_name, "nameTextStyle": {"color": THEME['muted'], "fontSize": 11, "padding": [0, 0, 0, 4]},
                "axisLabel": {"color": THEME['muted'], "fontSize": 11},
                "splitLine": {"lineStyle": {"type": "solid", "color": "#EEF2F7"}},
            },
            {
                "type": "value", "name": y2_name, "nameTextStyle": {"color": THEME['muted'], "fontSize": 11, "padding": [0, 0, 0, 4]},
                "axisLabel": {"color": THEME['muted'], "fontSize": 11},
                "splitLine": {"show": False},
            },
        ],
        "series": [
            {"name": y1_name, "type": "bar", "data": y1_data, "barMaxWidth": 24, "itemStyle": {"color": THEME['primary_soft'], "borderRadius": [6, 6, 0, 0]}},
            {"name": y2_name, "type": "line", "yAxisIndex": 1, "data": y2_data, "smooth": True, "itemStyle": {"color": THEME['primary']}, "lineStyle": {"width": 2.5}, "symbol": "circle", "symbolSize": 6},
        ],
    }

    echarts_renderer = _ensure_echarts()
    if echarts_renderer:
        echarts_renderer(options=options, height=f"{height}px")
        return

    fallback = pd.DataFrame({x_col: x_data, y1_name: y1_data, y2_name: y2_data}).set_index(x_col)
    st.line_chart(fallback, height=height)


def render_echarts_single_axis(title: str, df: pd.DataFrame, x_col: str, y_col: str, y_name: str, height: int = 300):
    if df.empty:
        render_empty_state("차트를 그릴 데이터가 부족합니다.", height)
        return

    x_data = df[x_col].dt.strftime('%m-%d').tolist() if pd.api.types.is_datetime64_any_dtype(df[x_col]) else df[x_col].astype(str).tolist()
    y_data = df[y_col].fillna(0).tolist()

    options = {
        "backgroundColor": "#FFFFFF",
        "title": {"text": title, "textStyle": {"fontSize": 13, "color": THEME['text'], "fontWeight": 600}, "left": "left", "top": 4},
        "color": [THEME['primary']],
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "line", "lineStyle": {"color": THEME['line']}},
            "backgroundColor": "#FFFFFF",
            "borderColor": THEME['line'],
            "borderWidth": 1,
            "textStyle": {"color": THEME['text'], "fontSize": 12},
            "padding": [8, 10],
        },
        "legend": {"data": [y_name], "top": 6, "right": 0, "itemWidth": 10, "itemHeight": 10, "textStyle": {"color": THEME['muted'], "fontSize": 11}},
        "grid": {"left": "1%", "right": "1%", "bottom": "10%", "top": 56, "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisLine": {"lineStyle": {"color": THEME['line']}}, "axisTick": {"show": False}, "axisLabel": {"color": THEME['muted'], "fontSize": 11}}],
        "yAxis": [{"type": "value", "name": y_name, "nameTextStyle": {"color": THEME['muted'], "fontSize": 11, "padding": [0, 0, 0, 4]}, "axisLabel": {"color": THEME['muted'], "fontSize": 11}, "splitLine": {"lineStyle": {"type": "solid", "color": "#EEF2F7"}}}],
        "series": [
            {"name": y_name, "type": "line", "data": y_data, "smooth": True, "itemStyle": {"color": THEME['primary']}, "lineStyle": {"width": 2.5}, "symbol": "circle", "symbolSize": 6}
        ],
    }

    echarts_renderer = _ensure_echarts()
    if echarts_renderer:
        echarts_renderer(options=options, height=f"{height}px")
        return

    fallback = pd.DataFrame({x_col: x_data, y_name: y_data}).set_index(x_col)
    st.line_chart(fallback, height=height)
