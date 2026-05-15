# -*- coding: utf-8 -*-
"""styles.py - Global CSS for the Streamlit dashboard."""

from __future__ import annotations
import streamlit as st
import streamlit.components.v1 as components

GLOBAL_UI_CSS = """
<style>
@import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css");

:root {
  /* Brand Colors */
  --nv-primary: #0747D9;
  --nv-primary-hover: #063AB2;
  --nv-primary-soft: #EAF2FF;
  --nv-primary-line: #B8D0FF;

  /* Ad ops neutrals */
  --nv-app-bg: #FFFFFF;
  --nv-bg: #FFFFFF;
  --nv-surface: #FFFFFF;
  --nv-surface-2: #F8FAFC;
  --nv-surface-3: #F1F5F9;
  --nv-line: #DDE6F0;
  --nv-line-strong: #B8C7D9;
  --nv-muted-light: #94A3B8;
  --nv-muted: #64748B;
  --nv-text: #0F172A;
  --nv-text-soft: #334155;

  /* Status Colors */
  --nv-success: #16A34A;
  --nv-success-soft: #DCFCE7;
  --nv-warning: #F59E0B;
  --nv-warning-soft: #FEF3C7;
  --nv-danger: #DC2626;
  --nv-danger-soft: #FEE2E2;
  --nv-info: #0891B2;
  --nv-info-soft: #E0F2FE;

  /* Geometry */
  --nv-radius: 6px;
  --nv-radius-lg: 8px;
  --nv-shadow-soft: 0 1px 2px rgba(15, 23, 42, 0.05);
  --nv-shadow-hover: 0 8px 18px rgba(15, 23, 42, 0.08);
}

/* =========================================
   Base Layout & Typography
   ========================================= */
footer { display: none !important; }
header[data-testid="stHeader"] { background-color: transparent !important; }
[data-testid="stHeaderActionElements"] { display: none !important; }
[data-testid="stSidebarNav"] { display: none !important; }

div.block-container {
  padding-top: 1.15rem !important;
  padding-bottom: 4rem !important;
  max-width: 1510px;
  padding-left: 1.5rem;
  padding-right: 1.5rem;
}

html, body, [class*="css"] {
  font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, sans-serif !important;
  color: var(--nv-text);
  background: var(--nv-app-bg);
}

.stApp,
[data-testid="stAppViewContainer"] {
  background: var(--nv-app-bg) !important;
}

[data-testid="stMain"] {
  background: transparent !important;
}

[data-testid="stVerticalBlockBorderWrapper"],
[data-testid="stDataFrame"],
[data-testid="stDataEditor"] {
  background: #FFFFFF !important;
}

[data-testid="stVerticalBlockBorderWrapper"] > div,
[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlock"] {
  background: #FFFFFF !important;
}

[data-testid="stDataFrame"],
[data-testid="stDataEditor"] {
  --gdg-bg-cell: #FFFFFF !important;
  --gdg-bg-cell-medium: #FFFFFF !important;
  --gdg-bg-header: #FFFFFF !important;
  --gdg-bg-header-has-focus: #FFFFFF !important;
  --gdg-bg-bubble: #FFFFFF !important;
  --gdg-bg-search-result: #EFF6FF !important;
  --gdg-bg-search-result-current: #DBEAFE !important;
  --gdg-border-color: var(--nv-line) !important;
  --gdg-horizontal-border-color: var(--nv-line) !important;
  --gdg-text-dark: #0F172A !important;
  --gdg-text-medium: #334155 !important;
  --gdg-text-light: #64748B !important;
  --gdg-text-header: #0F172A !important;
  --gdg-accent-color: var(--nv-primary) !important;
  --gdg-accent-fg: #FFFFFF !important;
  color: var(--nv-text) !important;
}

h1, h2, h3, h4, h5, h6 {
  font-weight: 700 !important;
  letter-spacing: 0 !important;
  color: var(--nv-text);
}

/* =========================================
   Page Components
   ========================================= */
.nv-page-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 18px;
  padding: 4px 0 18px 0;
  background: transparent;
  border-bottom: 1px solid var(--nv-line);
  border-radius: 0;
  box-shadow: none;
}

.nv-console-head {
  background: rgba(255, 255, 255, 0.96);
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius-lg);
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
  margin: 0 0 16px;
  overflow: hidden;
}
.nv-console-top {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 18px;
  padding: 16px 18px 14px;
}
.nv-console-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 8px;
}
.nv-action-chip,
.nv-icon-chip {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 34px;
  border: 1px solid var(--nv-line);
  background: var(--nv-bg);
  color: var(--nv-text-soft);
  font-size: 12px;
  font-weight: 800;
  border-radius: 5px;
  box-shadow: var(--nv-shadow-soft);
}
.nv-action-chip {
  padding: 0 12px;
}
.nv-action-chip.primary {
  background: var(--nv-primary);
  color: #fff;
  border-color: var(--nv-primary);
}
.nv-icon-chip {
  width: 34px;
  font-size: 15px;
}
.nv-filter-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 14px;
  border-top: 1px solid var(--nv-line);
  background: var(--nv-surface);
}
.nv-filter-search {
  flex: 1;
  min-width: 220px;
  height: 34px;
  display: flex;
  align-items: center;
  padding: 0 12px;
  border: 1px solid var(--nv-line);
  border-radius: 4px;
  background: var(--nv-bg);
  color: var(--nv-muted);
  font-size: 12px;
  font-weight: 600;
}
.nv-filter-search::before {
  content: "";
  width: 8px;
  height: 8px;
  border: 2px solid var(--nv-muted-light);
  border-radius: 999px;
  margin-right: 9px;
  box-sizing: border-box;
}
.nv-page-head-left { min-width: 0; }
.nv-page-eyebrow {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  color: var(--nv-muted);
  font-size: 12px;
  font-weight: 750;
  margin-bottom: 7px;
  text-transform: uppercase;
}
.nv-h1 {
  font-size: 24px;
  line-height: 1.18;
  font-weight: 800;
  letter-spacing: 0;
  color: var(--nv-text);
  margin: 0 0 6px 0;
}
.nv-page-sub {
  color: var(--nv-muted);
  font-size: 14px;
  line-height: 1.55;
  margin: 0;
}
.nv-page-meta {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: flex-end;
  gap: 8px;
  min-width: 240px;
}
.nv-meta-chip,
.nv-chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 26px;
  padding: 4px 9px;
  border-radius: 5px;
  border: 1px solid var(--nv-line);
  background: rgba(255, 255, 255, 0.74);
  color: var(--nv-text-soft);
  font-size: 12px;
  font-weight: 700;
  line-height: 1;
}
.nv-meta-chip.primary,
.nv-chip.primary {
  background: var(--nv-primary-soft);
  color: var(--nv-primary);
  border-color: var(--nv-primary-line);
}
.nv-meta-chip.success,
.nv-chip.success { background: var(--nv-success-soft); color: var(--nv-success); border-color: #BBF7D0; }
.nv-meta-chip.warning,
.nv-chip.warning { background: var(--nv-warning-soft); color: #B45309; border-color: #FDE68A; }
.nv-meta-chip.danger,
.nv-chip.danger { background: var(--nv-danger-soft); color: var(--nv-danger); border-color: #FECACA; }
.nv-meta-chip.info,
.nv-chip.info { background: var(--nv-info-soft); color: var(--nv-info); border-color: #BAE6FD; }

.nv-section {
  background: var(--nv-bg);
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius-lg);
  padding: 16px 18px;
  margin-top: 14px;
  box-shadow: var(--nv-shadow-soft);
}
.nv-section-muted {
  background: var(--nv-surface);
  border: none;
}
.nv-section-head {
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:16px;
  margin-bottom:16px;
}
.nv-sec-eyebrow {
  display:inline-flex;
  align-items:center;
  gap:6px;
  margin:0 0 6px 0;
  color: var(--nv-muted-light);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}
.nv-sec-title {
  font-size: 17px;
  line-height: 1.3;
  font-weight: 750;
  margin: 0;
  color: var(--nv-text);
  display: flex;
  align-items: center;
  gap: 8px;
  letter-spacing: 0;
}
.nv-sec-sub {
  margin: 6px 0 0 0;
  color: var(--nv-muted);
  font-size: 13px;
  line-height: 1.55;
  font-weight: 500;
}
.nv-card-title {
  margin: 0 0 10px 0;
  color: var(--nv-text);
  font-size: 14px;
  line-height: 1.4;
  font-weight: 700;
  letter-spacing: 0;
}
.nv-card-sub {
  margin: 4px 0 0 0;
  color: var(--nv-muted);
  font-size: 12px;
  line-height: 1.5;
  font-weight: 500;
}

/* =========================================
   Metric Cards
   ========================================= */
.nv-metric-card {
  background: var(--nv-bg);
  padding: 15px;
  border-radius: var(--nv-radius-lg);
  border: 1px solid var(--nv-line);
  margin-bottom: 12px;
  transition: all 0.2s ease;
  box-shadow: var(--nv-shadow-soft);
}
.nv-metric-card:hover {
  border-color: var(--nv-line-strong);
  box-shadow: var(--nv-shadow-hover);
  transform: translateY(-1px);
}
.nv-metric-card-title {
  color: var(--nv-muted);
  font-size: 14px;
  font-weight: 600;
  margin-bottom: 12px;
}
.nv-metric-card-value {
  color: var(--nv-text);
  font-size: 27px;
  font-weight: 800;
  letter-spacing: 0;
}
.nv-metric-card-desc {
  color: var(--nv-primary);
  font-size: 13px;
  font-weight: 600;
  margin-top: 12px;
  background: var(--nv-primary-soft);
  display: inline-block;
  padding: 6px 12px;
  border-radius: 999px;
}

.nv-kpi-strip {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 1px;
  overflow: hidden;
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius-lg);
  background: var(--nv-line);
  box-shadow: 0 14px 38px rgba(15, 23, 42, 0.06);
  margin: 12px 0 16px;
}
.nv-kpi-item {
  background: var(--nv-bg);
  padding: 14px 14px;
  min-width: 0;
  position: relative;
}
.nv-kpi-item::before {
  content: "";
  position: absolute;
  top: 12px;
  right: 12px;
  width: 7px;
  height: 7px;
  border-radius: 999px;
  background: var(--nv-primary-line);
}
.nv-kpi-item.green::before { background: var(--nv-success); }
.nv-kpi-item.amber::before { background: var(--nv-warning); }
.nv-kpi-item.red::before { background: var(--nv-danger); }
.nv-kpi-item.cyan::before { background: var(--nv-info); }
.nv-kpi-item.blue::before { background: var(--nv-primary); }
.nv-kpi-item-label {
  color: var(--nv-muted);
  font-size: 12px;
  font-weight: 750;
  margin-bottom: 8px;
}
.nv-kpi-item-value {
  color: var(--nv-text);
  font-size: 22px;
  font-weight: 850;
  line-height: 1.12;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.nv-kpi-item-sub {
  margin-top: 9px;
  display: inline-flex;
  align-items: center;
  border-radius: 5px;
  padding: 4px 8px;
  font-size: 11px;
  font-weight: 800;
}
.nv-kpi-item-sub.pos { background: var(--nv-success-soft); color: var(--nv-success); }
.nv-kpi-item-sub.neg { background: var(--nv-danger-soft); color: var(--nv-danger); }
.nv-kpi-item-sub.neu { background: var(--nv-surface-2); color: var(--nv-muted); }

.nv-op-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
  margin: 14px 0 16px;
}
.nv-op-card {
  background: var(--nv-bg);
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius-lg);
  padding: 14px 14px 14px 15px;
  box-shadow: var(--nv-shadow-soft);
  min-width: 0;
  position: relative;
}
.nv-op-card::before {
  content: "";
  position: absolute;
  left: 0;
  top: 10px;
  bottom: 10px;
  width: 3px;
  border-radius: 0 999px 999px 0;
  background: var(--nv-line-strong);
}
.nv-op-card.primary::before { background: var(--nv-primary); }
.nv-op-card.success::before { background: var(--nv-success); }
.nv-op-card.warning::before { background: var(--nv-warning); }
.nv-op-card.danger::before { background: var(--nv-danger); }
.nv-op-card.info::before { background: var(--nv-info); }
.nv-op-card-icon {
  position: absolute;
  right: 12px;
  top: 12px;
  color: var(--nv-muted-light);
  font-size: 14px;
  font-weight: 900;
}
.nv-op-card-title {
  font-size: 12px;
  color: var(--nv-muted);
  font-weight: 800;
  margin-bottom: 8px;
}
.nv-op-card-value {
  font-size: 22px;
  line-height: 1.15;
  font-weight: 850;
  color: var(--nv-text);
}
.nv-op-card-note {
  color: var(--nv-muted);
  font-size: 12px;
  margin-top: 8px;
}

.nv-toolbar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 11px 13px;
  margin: 12px 0;
  background: var(--nv-bg);
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius-lg);
  box-shadow: var(--nv-shadow-soft);
}
.nv-toolbar-title {
  color: var(--nv-text);
  font-size: 13px;
  font-weight: 800;
}
.nv-toolbar-sub {
  color: var(--nv-muted);
  font-size: 12px;
  margin-top: 2px;
}
.nv-toolbar-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}


/* Typography alignment for Streamlit markdown headings inside sections */
.element-container h2, .element-container h3, .element-container h4 {
  color: var(--nv-text);
  letter-spacing: 0;
}
.element-container h2 {
  font-size: 22px !important;
  line-height: 1.28 !important;
  margin: 0 0 12px 0 !important;
}
.element-container h3 {
  font-size: 18px !important;
  line-height: 1.35 !important;
  margin: 0 0 10px 0 !important;
}
.element-container h4 {
  font-size: 15px !important;
  line-height: 1.4 !important;
  margin: 0 0 8px 0 !important;
}

/* =========================================
   Streamlit Native Overrides
   ========================================= */
[data-baseweb="tab-list"] {
  gap: 24px;
  padding-bottom: 0px;
  border-bottom: 1px solid var(--nv-line);
}
[data-baseweb="tab"] {
  background: transparent !important;
  border: none !important;
  font-weight: 600;
  padding: 16px 4px !important;
  margin: 0 !important;
  color: var(--nv-muted-light) !important;
  font-size: 15px;
  border-radius: 0 !important;
  transition: color 0.2s ease;
}
[data-baseweb="tab"]:hover { color: var(--nv-text) !important; }
[aria-selected="true"] {
  color: var(--nv-text) !important;
  font-weight: 800 !important;
  border-bottom: 2px solid var(--nv-text) !important;
  box-shadow: none !important;
  background: transparent !important;
}

/* =========================================
   Sidebar & Navigation (Minimalist Radio)
   ========================================= */
[data-testid="stSidebar"] {
  background: #FFFFFF !important;
  border-right: 1px solid var(--nv-line) !important;
}
[data-testid="stSidebar"] .block-container {
  padding-top: 1.05rem !important;
  padding-left: 0.85rem !important;
  padding-right: 0.85rem !important;
}
.nav-sidebar-title {
  font-size: 12px;
  font-weight: 700;
  color: var(--nv-muted-light);
  margin: 12px 0 10px;
  text-transform: uppercase;
  letter-spacing: 0;
  padding-left: 4px;
}

.sidebar-brand-card {
  display: grid;
  grid-template-columns: 42px minmax(0, 1fr);
  gap: 11px;
  align-items: center;
  padding: 11px;
  margin-bottom: 12px;
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius-lg);
  background: var(--nv-bg);
  box-shadow: var(--nv-shadow-soft);
}
.sidebar-brand-mark {
  width: 42px;
  height: 42px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 6px;
  background: var(--nv-primary);
  color: #fff;
  font-size: 22px;
  line-height: 1;
  font-weight: 500;
}
.sidebar-brand-copy { min-width: 0; }
.sidebar-build-tag {
  margin-top: 4px;
  color: var(--nv-muted-light);
  font-size: 10px;
  font-weight: 800;
}

[data-testid="stSidebar"] [data-testid="stButton"] button {
  justify-content: flex-start !important;
  min-height: 38px !important;
  border-radius: 6px !important;
  font-weight: 800 !important;
  box-shadow: none !important;
}
[data-testid="stSidebar"] [data-testid="baseButton-secondary"] {
  color: var(--nv-text-soft) !important;
  background: transparent !important;
  border: 1px solid transparent !important;
}
[data-testid="stSidebar"] [data-testid="baseButton-secondary"]:hover {
  background: var(--nv-surface-2) !important;
  border-color: var(--nv-line) !important;
}
[data-testid="stSidebar"] [data-testid="baseButton-primary"] {
  background: var(--nv-primary) !important;
  color: #fff !important;
  border: 1px solid var(--nv-primary) !important;
}

[data-testid="stSidebar"] [role="radiogroup"] {
  background: transparent;
  padding: 0;
  gap: 6px;
  display: flex;
  flex-direction: column;
}
[data-testid="stSidebar"] [role="radiogroup"] label {
  padding: 12px 16px !important;
  margin-bottom: 2px !important;
  border-radius: 10px !important;
  background: transparent !important;
  border: 1px solid transparent !important;
  transition: all 0.2s ease;
}
[data-testid="stSidebar"] [role="radiogroup"] label:hover { 
  background: var(--nv-surface-2) !important; 
}
[data-testid="stSidebar"] [role="radiogroup"] label p {
  color: var(--nv-muted) !important;
  font-weight: 600 !important;
  font-size: 14px !important;
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
  background: var(--nv-bg) !important;
  box-shadow: var(--nv-shadow-soft) !important;
  border: 1px solid var(--nv-line) !important;
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p {
  color: var(--nv-primary) !important;
  font-weight: 800 !important;
}

/* =========================================
   Inputs & Controls
   ========================================= */
div[data-baseweb="select"] > div,
[data-testid="stDateInput"] > div,
[data-testid="stNumberInput"] > div,
[data-testid="stTextInput"] > div,
[data-testid="stSelectbox"] > div {
  border-radius: 10px !important;
  border-color: var(--nv-line) !important;
  background: var(--nv-bg) !important;
  box-shadow: none !important;
}
div[data-baseweb="select"] > div:focus-within {
  box-shadow: 0 0 0 1px var(--nv-primary) inset !important;
  border-color: var(--nv-primary) !important;
}

[data-testid="stExpander"] {
  border: 1px solid var(--nv-line) !important;
  border-radius: var(--nv-radius-lg) !important;
  box-shadow: var(--nv-shadow-soft) !important;
  background: var(--nv-bg) !important;
  overflow: hidden;
}
[data-testid="stExpander"] summary {
  padding: 18px 20px !important;
  background-color: transparent !important;
  border-radius: 0 !important;
}
[data-testid="stExpander"] summary p {
  font-weight: 700 !important;
  font-size: 15px !important;
  color: var(--nv-text) !important;
}

[data-testid="baseButton-primary"] {
  background-color: var(--nv-primary) !important;
  color: white !important;
  border: none !important;
  border-radius: 6px !important;
  font-weight: 700 !important;
  padding: 8px 16px !important;
  box-shadow: var(--nv-shadow-soft) !important;
}
[data-testid="baseButton-primary"]:hover { 
  background-color: var(--nv-primary-hover) !important; 
  box-shadow: var(--nv-shadow-hover) !important;
}

.sidebar-info-box {
  background: var(--nv-bg);
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius-lg);
  padding: 16px;
  margin-bottom: 24px;
  box-shadow: var(--nv-shadow-soft);
}
.sidebar-info-label {
  font-size: 11px;
  color: var(--nv-muted-light);
  font-weight: 700;
  margin-bottom: 6px;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}
.sidebar-info-value {
  font-size: 14px;
  font-weight: 700;
  color: var(--nv-text);
}
.sidebar-info-value span { color: var(--nv-primary); font-weight: 800; }

[data-testid="stDataFrame"],
[data-testid="stDataEditor"] {
  border-radius: var(--nv-radius-lg);
  overflow: hidden;
  border: 1px solid var(--nv-line);
}

[data-testid="stMetric"] {
  background: var(--nv-bg);
  border: 1px solid var(--nv-line);
  border-radius: var(--nv-radius-lg);
  padding: 12px 14px;
  box-shadow: var(--nv-shadow-soft);
}

div[data-testid="stHorizontalBlock"] {
  gap: 1rem;
}

@media (max-width: 1100px) {
  div.block-container {
    padding-left: 1.5rem !important;
    padding-right: 1.5rem !important;
  }
  .nv-page-head {
    padding: 4px 0 16px 0;
    margin-bottom: 24px;
    flex-direction: column;
  }
  .nv-h1 {
    font-size: 24px;
  }
  .nv-sec-title {
    font-size: 17px;
  }
  .nv-kpi-strip,
  .nv-op-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  .nv-console-top,
  .nv-filter-bar {
    align-items: flex-start;
    flex-direction: column;
  }
  .nv-console-actions,
  .nv-page-meta {
    justify-content: flex-start;
  }
  .nv-filter-search {
    width: 100%;
  }
}

@media (max-width: 720px) {
  div.block-container {
    padding-left: 1rem !important;
    padding-right: 1rem !important;
  }
  .nv-kpi-strip,
  .nv-op-grid {
    grid-template-columns: 1fr;
  }
  .nv-page-meta {
    justify-content: flex-start;
  }
}
</style>
"""

JS_CUSTOM_ENHANCEMENTS = """
<script>
(function() {
    const parentDoc = window.parent.document;
    if (parentDoc.getElementById('custom-ux-enhancements')) return;

    const marker = parentDoc.createElement('div');
    marker.id = 'custom-ux-enhancements';
    marker.style.display = 'none';
    parentDoc.body.appendChild(marker);

    parentDoc.addEventListener('click', function(e) {
        let target = e.target;
        let isOptionClicked = false;
        while (target && target !== parentDoc) {
            if (target.getAttribute && target.getAttribute('role') === 'option') { isOptionClicked = true; break; }
            target = target.parentNode;
        }
        if (isOptionClicked) {
            setTimeout(function() {
                const escEvent = new KeyboardEvent('keydown', { key: 'Escape', code: 'Escape', keyCode: 27, which: 27, bubbles: true });
                if (parentDoc.activeElement) { parentDoc.activeElement.dispatchEvent(escEvent); parentDoc.activeElement.blur(); }
                else { parentDoc.dispatchEvent(escEvent); }
                const popover = parentDoc.querySelector('[data-baseweb="popover"]');
                if(popover && popover.parentNode) parentDoc.body.click();
            }, 50);
        }
    }, true);
})();
</script>
"""

def apply_global_css():
    st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
    enable_parent_js = str(st.session_state.get("_enable_parent_js_enhancements", "1")) not in {"0", "false", "False"}
    if not enable_parent_js:
        return
    if st.session_state.get("_parent_js_enhancements_injected"):
        return
    components.html(JS_CUSTOM_ENHANCEMENTS, height=0, width=0)
    st.session_state["_parent_js_enhancements_injected"] = True
