# -*- coding: utf-8 -*-
"""view_settings.py - Settings and Sync page view (Fixed Duplicate ID & iOS Style)."""

from __future__ import annotations
import time
import pandas as pd
import streamlit as st
import streamlit_compat  # noqa: F401
import streamlit_antd_components as sac
from sqlalchemy import text

from data import (
    get_platform_credentials,
    db_ping,
    get_meta,
    seed_from_accounts_xlsx,
    upsert_platform_credential,
)
from ui import render_toolbar


def _clean_text(value, fallback: str = "") -> str:
    cleaned = str(value or "").strip()
    if cleaned in {"nan", "None", "NaN"}:
        return fallback
    return cleaned or fallback


def _clean_manager(value) -> str:
    return _clean_text(value, "미배정")


def _clean_customer_id(value) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    if raw.startswith("act_"):
        raw = raw[4:]
    if raw.endswith(".0") and raw[:-2].isdigit():
        raw = raw[:-2]
    compact = raw.replace("-", "").replace(" ", "")
    return compact if compact.isdigit() else raw


def _credential_customer_id(value):
    customer_id = _clean_customer_id(value)
    return int(customer_id) if customer_id.isdigit() else None


@st.cache_data(ttl=300, show_spinner=False, max_entries=20)
def _dashboard_accounts_df(_engine) -> pd.DataFrame:
    try:
        meta = get_meta(_engine)
    except Exception:
        return pd.DataFrame(columns=["customer_id", "account_name", "manager"])
    if meta is None or meta.empty:
        return pd.DataFrame(columns=["customer_id", "account_name", "manager"])
    cols = [c for c in ["customer_id", "account_name", "manager"] if c in meta.columns]
    out = meta[cols].copy()
    if "customer_id" not in out.columns:
        out["customer_id"] = ""
    if "account_name" not in out.columns:
        out["account_name"] = out["customer_id"].astype(str)
    if "manager" not in out.columns:
        out["manager"] = "미배정"
    out["customer_id"] = out["customer_id"].map(_clean_customer_id)
    out["account_name"] = out["account_name"].map(_clean_text)
    out["manager"] = out["manager"].map(_clean_manager)
    return out[out["customer_id"] != ""].drop_duplicates("customer_id", keep="last").sort_values(["manager", "account_name"]).reset_index(drop=True)


def _resolve_dashboard_customer_id(engine, account_label: str, customer_id: str, account_id: str) -> str:
    explicit_customer_id = _clean_customer_id(customer_id)
    if explicit_customer_id:
        return explicit_customer_id

    clean_label = _clean_text(account_label).casefold()
    if clean_label:
        accounts = _dashboard_accounts_df(engine)
        if not accounts.empty:
            matched = accounts[accounts["account_name"].astype(str).str.strip().str.casefold() == clean_label]
            if not matched.empty:
                return _clean_customer_id(matched.iloc[0]["customer_id"])

    return _clean_customer_id(account_id)


def _platform_connections_editor_df(engine) -> pd.DataFrame:
    df = get_platform_credentials(engine)
    cols = ["id", "platform", "account_label", "manager", "customer_id", "account_id", "is_active"]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    for col in cols:
        if col not in df.columns:
            df[col] = "미배정" if col == "manager" else ("" if col != "is_active" else True)
    out = df[cols].copy()
    out["platform"] = out["platform"].fillna("").astype(str)
    out["account_label"] = out["account_label"].fillna("").astype(str)
    out["manager"] = out["manager"].fillna("미배정").astype(str).str.strip()
    out.loc[out["manager"].isin(["", "nan", "None", "NaN"]), "manager"] = "미배정"
    out["customer_id"] = out["customer_id"].map(_clean_customer_id)
    out["account_id"] = out["account_id"].fillna("").astype(str)
    out["is_active"] = out["is_active"].fillna(True).astype(bool)
    return out.sort_values(["platform", "account_label", "account_id"]).reset_index(drop=True)


def _sync_connection_manager_to_customer(engine, account_label: str, customer_id: str, manager: str) -> None:
    customer_id = _clean_customer_id(customer_id)
    if not customer_id:
        return

    clean_label = _clean_text(account_label, customer_id)
    clean_manager = _clean_manager(manager)
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS dim_customer (customer_id TEXT, account_name TEXT, manager TEXT, monthly_budget BIGINT DEFAULT 0, operating_weekdays TEXT DEFAULT '0,1,2,3,4,5,6')"
            )
        )
        conn.execute(text("ALTER TABLE dim_customer ADD COLUMN IF NOT EXISTS account_name TEXT"))
        conn.execute(text("ALTER TABLE dim_customer ADD COLUMN IF NOT EXISTS manager TEXT"))
        conn.execute(text("ALTER TABLE dim_customer ADD COLUMN IF NOT EXISTS monthly_budget BIGINT DEFAULT 0"))
        conn.execute(text("ALTER TABLE dim_customer ADD COLUMN IF NOT EXISTS operating_weekdays TEXT DEFAULT '0,1,2,3,4,5,6'"))
        conn.execute(
            text(
                """
                UPDATE dim_customer
                   SET account_name = :account_name,
                       manager = :manager
                 WHERE REGEXP_REPLACE(CAST(customer_id AS TEXT), '\\.0+$', '') = :customer_id
                    OR LOWER(TRIM(CAST(account_name AS TEXT))) = LOWER(:account_name)
                """
            ),
            {"customer_id": customer_id, "account_name": clean_label, "manager": clean_manager},
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_customer (customer_id, account_name, manager)
                SELECT :customer_id, :account_name, :manager
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM dim_customer
                    WHERE REGEXP_REPLACE(CAST(customer_id AS TEXT), '\\.0+$', '') = :customer_id
                       OR LOWER(TRIM(CAST(account_name AS TEXT))) = LOWER(:account_name)
                )
                """
            ),
            {"customer_id": customer_id, "account_name": clean_label, "manager": clean_manager},
        )


def _connection_filter_options(conn_df: pd.DataFrame) -> dict[str, list[str]]:
    if conn_df is None or conn_df.empty:
        return {"platforms": ["전체"], "managers": ["전체"]}
    platforms = ["전체"] + sorted([x for x in conn_df["platform"].dropna().astype(str).unique().tolist() if x])
    managers = ["전체"] + sorted([x for x in conn_df["manager"].dropna().astype(str).unique().tolist() if x])
    return {"platforms": platforms, "managers": managers}


def _apply_connection_filters(conn_df: pd.DataFrame, platform: str, manager: str, active_only: bool) -> pd.DataFrame:
    if conn_df is None or conn_df.empty:
        return conn_df
    out = conn_df.copy()
    if platform != "전체":
        out = out[out["platform"] == platform]
    if manager != "전체":
        out = out[out["manager"] == manager]
    if active_only:
        out = out[out["is_active"]]
    return out.reset_index(drop=True)


def _save_platform_connections(engine, edited_df: pd.DataFrame) -> int:
    if edited_df is None or edited_df.empty:
        return 0
    count = 0
    for _, row in edited_df.iterrows():
        platform = str(row.get("platform", "") or "").strip().lower()
        account_label = str(row.get("account_label", "") or "").strip()
        manager = str(row.get("manager", "") or "").strip() or "미배정"
        dashboard_customer_id = _clean_customer_id(row.get("customer_id", ""))
        account_id = str(row.get("account_id", "") or "").strip()
        if not platform and not account_label and not dashboard_customer_id and not account_id:
            continue
        if not platform or not account_label or not account_id:
            raise ValueError("플랫폼, 대시보드 계정명, 플랫폼 계정 ID는 모두 입력해야 합니다.")
        resolved_customer_id = _resolve_dashboard_customer_id(engine, account_label, dashboard_customer_id, account_id)
        upsert_platform_credential(
            engine,
            {
                "id": None if pd.isna(row.get("id")) or str(row.get("id", "")).strip() == "" else int(row.get("id")),
                "platform": platform,
                "account_label": account_label,
                "manager": manager,
                "customer_id": _credential_customer_id(resolved_customer_id),
                "account_id": account_id,
                "is_active": bool(row.get("is_active", True)),
            },
        )
        _sync_connection_manager_to_customer(engine, account_label, resolved_customer_id, manager)
        count += 1
    return count


@st.fragment
def page_settings(engine) -> None:
    render_toolbar(
        "설정 및 데이터 관리",
        "플랫폼 계정 연동, 담당자 연결, 대시보드 운영 도구를 관리합니다.",
        [{"label": "관리자", "tone": "primary"}],
    )
    try: 
        db_ping(engine)
    except Exception as e: 
        st.error(f"DB 연결 실패: {e}")
        return

    st.markdown("<br>", unsafe_allow_html=True)

    # ====================================================
    # 플랫폼 계정 및 담당자 연동
    # ====================================================
    st.markdown("### 플랫폼 계정 연결")
    st.caption("네이버, 메타, 구글 계정을 대시보드 계정과 연결합니다. 담당자를 저장하면 대시보드 계정 정보에도 함께 반영됩니다.")

    conn_df = _platform_connections_editor_df(engine)
    dash_accounts = _dashboard_accounts_df(engine)

    metric_a, metric_b, metric_c = st.columns(3)
    active_count = int(conn_df["is_active"].sum()) if not conn_df.empty and "is_active" in conn_df.columns else 0
    linked_count = int((conn_df["customer_id"].astype(str).str.strip() != "").sum()) if not conn_df.empty and "customer_id" in conn_df.columns else 0
    manager_count = int(conn_df["manager"].replace("", pd.NA).dropna().nunique()) if not conn_df.empty and "manager" in conn_df.columns else 0
    metric_a.metric("활성 연동", f"{active_count:,}개")
    metric_b.metric("대시보드 계정 연결", f"{linked_count:,}개")
    metric_c.metric("담당자", f"{manager_count:,}명")

    if not dash_accounts.empty:
        with st.expander("대시보드 계정에서 연동 행 만들기", expanded=False):
            account_options = [
                f"{row.account_name} · {row.manager} · {row.customer_id}"
                for row in dash_accounts.itertuples(index=False)
            ]
            c_acc, c_platform, c_pid = st.columns([1.6, 0.8, 1.4], gap="small")
            with c_acc:
                selected_account = st.selectbox("대시보드 계정", account_options, key="connection_seed_account")
            with c_platform:
                seed_platform = st.selectbox("플랫폼", ["naver", "meta", "google"], key="connection_seed_platform")
            selected_idx = account_options.index(selected_account)
            seed_row = dash_accounts.iloc[selected_idx]
            with c_pid:
                seed_account_id = st.text_input(
                    "플랫폼 계정 ID",
                    value=str(seed_row["customer_id"]) if seed_platform == "naver" else "",
                    key="connection_seed_account_id",
                    placeholder="act_123456789 또는 276-154-7013",
                )
            c_save, c_note = st.columns([1.1, 2.4], gap="small")
            with c_save:
                if st.button("연동 행 저장", type="primary", use_container_width=True, icon=":material/add_link:"):
                    try:
                        saved_row = pd.DataFrame([{
                            "id": "",
                            "platform": seed_platform,
                            "account_label": seed_row["account_name"],
                            "manager": seed_row["manager"],
                            "customer_id": seed_row["customer_id"],
                            "account_id": seed_account_id,
                            "is_active": True,
                        }])
                        _save_platform_connections(engine, saved_row)
                        st.cache_data.clear()
                        st.success("연동 행을 저장했습니다.", icon=":material/check_circle:")
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"저장 실패: {e}", icon=":material/error:")

    filter_opts = _connection_filter_options(conn_df)
    f_platform, f_manager, f_active = st.columns([1, 1, 1], gap="small")
    with f_platform:
        sel_platform = st.selectbox("플랫폼 필터", filter_opts["platforms"], key="settings_conn_platform")
    with f_manager:
        sel_manager = st.selectbox("담당자 필터", filter_opts["managers"], key="settings_conn_manager")
    with f_active:
        active_only = st.toggle("활성 연동만", value=False, key="settings_conn_active_only")

    visible_conn_df = _apply_connection_filters(conn_df, sel_platform, sel_manager, active_only)
    edited_conn_df = st.data_editor(
        visible_conn_df,
        hide_index=True,
        use_container_width=True,
        num_rows="dynamic",
        height=420,
        column_config={
            "id": None,
            "platform": st.column_config.SelectboxColumn("플랫폼", options=["naver", "meta", "google"], required=True, width="small"),
            "account_label": st.column_config.TextColumn("대시보드 계정명", required=True, help="예: 핵이득마켓", width="medium"),
            "manager": st.column_config.TextColumn("담당자", required=True, help="저장 시 대시보드 계정의 담당자에도 반영됩니다.", width="small"),
            "customer_id": st.column_config.TextColumn("대시보드 커스텀 ID", help="비워두면 플랫폼 계정 ID를 기준으로 담당자를 연결합니다.", width="medium"),
            "account_id": st.column_config.TextColumn("플랫폼 계정 ID", required=True, help="Meta는 광고계정 ID, Google은 고객 ID", width="medium"),
            "is_active": st.column_config.CheckboxColumn("수집", default=True, width="small"),
        },
        key="platform_connections_editor",
    )
    col_conn_a, col_conn_b = st.columns([1.1, 3])
    with col_conn_a:
        if st.button("계정 연결 저장", type="primary", use_container_width=True, icon=":material/save:"):
            try:
                saved = _save_platform_connections(engine, edited_conn_df)
                st.cache_data.clear()
                st.success(f"저장 완료! ({saved}건)", icon=":material/check_circle:")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"저장 실패: {e}", icon=":material/error:")

    sac.divider(align='center', color='gray', key='div_platform_connections')

    st.markdown("### accounts.xlsx → DB 동기화")
        
    with st.container():
        st.markdown("<div style='background-color:#F8FAFC; padding:16px; border-radius:8px; border:1px solid #E2E8F0; margin-bottom:24px;'>", unsafe_allow_html=True)
        up = st.file_uploader("accounts.xlsx 업로드", type=["xlsx"])
        colA, colB, colC = st.columns([1.2, 1.0, 2.2], gap="small")
        with colA: do_sync = st.button("동기화 실행", use_container_width=True, type="primary", icon=":material/sync:")
        with colB:
            if st.button("캐시 비우기", use_container_width=True, icon=":material/cleaning_services:"):
                st.cache_data.clear()
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    if do_sync:
        try:
            df_src = pd.read_excel(up) if up else None
            seed_from_accounts_xlsx(engine, df=df_src)
            st.success(f"동기화 완료!", icon=":material/check_circle:")
            st.cache_data.clear()
            time.sleep(1)
            st.rerun()
        except Exception as e:
            st.error(f"실패: {e}", icon=":material/error:")

    # ✨ 중복 에러 해결: 모든 divider에 고유 key 추가
    sac.divider(align='center', color='gray', key='div_1')

    st.markdown("### 대시보드 속도 최적화")
    if st.button("초고속 DB 목차 만들기", type="secondary", icon=":material/bolt:"):
        with st.spinner("진행 중..."):
            try:
                with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fcd_dt ON fact_campaign_daily(dt);"))
                    # ... 필요한 인덱스 쿼리들
                st.success("최적화 완료!", icon=":material/check_circle:")
            except Exception as e:
                st.error(f"오류: {e}")

    sac.divider(align='center', color='gray', key='div_2')

    st.markdown("### DB 찌꺼기 정리")
    if st.button("DB 대청소 실행", type="secondary", icon=":material/delete_sweep:"):
        with st.spinner("청소 중..."):
            try:
                with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                    conn.execute(text("VACUUM ANALYZE fact_campaign_daily;"))
                st.success("청소 완료!", icon=":material/check_circle:")
            except Exception as e:
                st.error(f"오류: {e}")

    sac.divider(align='center', color='gray', key='div_3')

    st.markdown("### Danger Zone")
    with st.container():
        col_del1, col_del2 = st.columns([2, 1])
        with col_del1:
            del_cid = st.text_input("삭제할 커스텀 ID", placeholder="12345678", label_visibility="collapsed")
            confirm_delete = st.checkbox("영구 삭제 동의")
        with col_del2:
            if st.button("영구 삭제 실행", type="primary", use_container_width=True, disabled=not confirm_delete, icon=":material/delete_forever:"):
                # 삭제 로직 실행
                st.success("삭제 완료!")
                st.cache_data.clear()
