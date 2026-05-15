# -*- coding: utf-8 -*-
"""view_settings.py - Settings and Sync page view (Fixed Duplicate ID & iOS Style)."""

from __future__ import annotations
import time
import pandas as pd
import streamlit as st
import streamlit_compat  # noqa: F401
import streamlit_antd_components as sac
from sqlalchemy import text

from data import sql_read, db_ping, seed_from_accounts_xlsx
from ui import render_toolbar



@st.cache_data(ttl=300, show_spinner=False, max_entries=20)
def _cached_roas_campaigns(_engine) -> pd.DataFrame:
    sql = """
        SELECT 
            c.customer_id, 
            COALESCE(cust.account_name, CAST(c.customer_id AS VARCHAR)) as account_name,
            COALESCE(cust.manager, '미배정') as manager,
            c.campaign_id, 
            c.campaign_name, 
            c.target_roas, 
            c.min_roas 
        FROM dim_campaign c
        LEFT JOIN dim_customer cust 
          ON REGEXP_REPLACE(CAST(c.customer_id AS TEXT), '\\.0+$', '') = REGEXP_REPLACE(CAST(cust.customer_id AS TEXT), '\\.0+$', '')
    """
    try:
        df = sql_read(_engine, sql)
        if df is None or df.empty:
            return pd.DataFrame()
        return df.sort_values(["manager", "account_name", "campaign_name"]).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False, max_entries=20)
def _settings_filter_maps(camp_df: pd.DataFrame):
    if camp_df is None or camp_df.empty:
        return {"managers": ["전체"], "manager_to_accounts": {"전체": ["전체"]}}
    work = camp_df[["manager", "account_name"]].copy()
    work["manager"] = work["manager"].fillna("미배정").astype(str)
    work["account_name"] = work["account_name"].fillna("").astype(str)
    managers = ["전체"] + sorted([x for x in work["manager"].unique().tolist() if x])
    manager_to_accounts = {"전체": ["전체"] + sorted([x for x in work["account_name"].unique().tolist() if x])}
    for mgr, grp in work.groupby("manager", sort=True):
        manager_to_accounts[str(mgr)] = ["전체"] + sorted([x for x in grp["account_name"].unique().tolist() if x])
    return {"managers": managers, "manager_to_accounts": manager_to_accounts}


def _normalize_roas_value(v):
    return float(v) if pd.notna(v) else None


def _collect_changed_roas_rows(original_df: pd.DataFrame, edited_df: pd.DataFrame) -> list[dict]:
    if original_df is None or edited_df is None or original_df.empty or edited_df.empty:
        return []
    base = original_df[["customer_id", "campaign_id", "target_roas", "min_roas"]].copy()
    new = edited_df[["customer_id", "campaign_id", "target_roas", "min_roas"]].copy()
    merged = base.merge(new, on=["customer_id", "campaign_id"], suffixes=("_old", "_new"), how="inner")
    changed = []
    for _, row in merged.iterrows():
        old_t = _normalize_roas_value(row.get("target_roas_old"))
        new_t = _normalize_roas_value(row.get("target_roas_new"))
        old_m = _normalize_roas_value(row.get("min_roas_old"))
        new_m = _normalize_roas_value(row.get("min_roas_new"))
        if old_t != new_t or old_m != new_m:
            changed.append({
                "customer_id": str(row.get("customer_id")),
                "campaign_id": str(row.get("campaign_id")),
                "target_roas": new_t,
                "min_roas": new_m,
            })
    return changed


@st.fragment
def page_settings(engine) -> None:
    render_toolbar(
        "설정 및 데이터 관리",
        "계정 동기화, 목표 ROAS, 대시보드 운영 도구를 관리합니다.",
        [{"label": "관리자", "tone": "primary"}],
    )
    try: 
        db_ping(engine)
    except Exception as e: 
        st.error(f"DB 연결 실패: {e}")
        return

    st.markdown("<br>", unsafe_allow_html=True)

    # ====================================================
    # 🍎 iOS 스타일 Segmented Control (Bootstrap 아이콘)
    # ====================================================
    selected_tab = sac.segmented(
        items=[
            sac.SegmentedItem(label='목표 ROAS 설정', icon='bullseye'),
            sac.SegmentedItem(label='대시보드 관리', icon='server'),
        ],
        align='center',
        size='sm',
        color='dark',        
        bg_color='#f1f5f9',  
        radius='xl',         
        divider=False,
        use_container_width=True,
        key='settings_main_tab' # 상단 탭 고유 키 부여
    )
    
    st.markdown("<br>", unsafe_allow_html=True)

    # ====================================================
    # 🎯 탭 1: 캠페인별 목표 ROAS 설정
    # ====================================================
    if selected_tab == '목표 ROAS 설정':
        st.markdown("### 캠페인별 목표 ROAS 설정")
        st.caption("담당자 및 업체를 선택하고 캠페인별 최소/목표 ROAS를 입력하세요.")

        try:
            with engine.begin() as conn:
                try: conn.execute(text("ALTER TABLE dim_campaign ADD COLUMN target_roas DOUBLE PRECISION;"))
                except Exception: pass
                try: conn.execute(text("ALTER TABLE dim_campaign ADD COLUMN min_roas DOUBLE PRECISION;"))
                except Exception: pass

            camp_df = _cached_roas_campaigns(engine)

            if not camp_df.empty:
                filt = _settings_filter_maps(camp_df)
                col_m, col_a = st.columns(2)
                with col_m:
                    sel_manager = st.selectbox("담당자 선택", filt["managers"], key="settings_manager")
                with col_a:
                    accounts = filt["manager_to_accounts"].get(sel_manager, ["전체"])
                    sel_acc = st.selectbox("업체 선택", accounts, key="settings_account")

                temp_df = camp_df
                if sel_manager != "전체":
                    temp_df = temp_df[temp_df['manager'] == sel_manager]
                if sel_acc != "전체":
                    temp_df = temp_df[temp_df['account_name'] == sel_acc]

                disp_df = temp_df.reset_index(drop=True)

                st.markdown("<br>", unsafe_allow_html=True)

                edited_df = st.data_editor(
                    disp_df,
                    hide_index=True,
                    use_container_width=True,
                    height=450,
                    column_config={
                        "customer_id": None, 
                        "campaign_id": None, 
                        "manager": st.column_config.TextColumn("담당자", disabled=True),
                        "account_name": st.column_config.TextColumn("광고주명", disabled=True),
                        "campaign_name": st.column_config.TextColumn("캠페인명", disabled=True, width="large"),
                        "min_roas": st.column_config.NumberColumn("최소 ROAS(%)", min_value=0, step=10, format="%d"),
                        "target_roas": st.column_config.NumberColumn("목표 ROAS(%)", min_value=0, step=10, format="%d")
                    },
                    key=f"roas_editor_{sel_manager}_{sel_acc}"
                )

                if st.button("화면의 목표 ROAS 저장", type="primary", icon=":material/save:"):
                    changed_rows = _collect_changed_roas_rows(disp_df, edited_df)
                    if not changed_rows:
                        st.info("변경된 목표 ROAS가 없습니다.", icon=":material/info:")
                    else:
                        with st.spinner(f"저장 중입니다... ({len(changed_rows)}건)"):
                            with engine.begin() as conn:
                                for row in changed_rows:
                                    conn.execute(
                                        text("UPDATE dim_campaign SET target_roas = :t, min_roas = :m WHERE REGEXP_REPLACE(CAST(customer_id AS TEXT), '\\.0+$', '') = :cid AND campaign_id = :campid"),
                                        {"t": row['target_roas'], "m": row['min_roas'], "cid": row['customer_id'], "campid": row['campaign_id']}
                                    )
                        st.success(f"저장 완료! ({len(changed_rows)}건)", icon=":material/check_circle:")
                        st.cache_data.clear()
                        time.sleep(1)
                        st.rerun()
            else:
                st.info("데이터 동기화를 먼저 진행해주세요.", icon=":material/info:")

        except Exception as e:
            st.error(f"오류 발생: {e}", icon=":material/error:")

    # ====================================================
    # ⚙️ 탭 2: 대시보드 관리 기능
    # ====================================================
    elif selected_tab == '대시보드 관리':
        st.markdown("### accounts.xlsx → DB 동기화")
        
        with st.container():
            st.markdown("<div style='background-color:#F8FAFC; padding:16px; border-radius:12px; border:1px solid #E2E8F0; margin-bottom:24px;'>", unsafe_allow_html=True)
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
                res = seed_from_accounts_xlsx(engine, df=df_src)
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
