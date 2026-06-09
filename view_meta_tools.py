# -*- coding: utf-8 -*-
"""Meta tools page for creative export and PAUSED setup creation."""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import date, timedelta
from io import BytesIO
from typing import Any

import pandas as pd
import streamlit as st
import streamlit_compat  # noqa: F401

from collector_meta import DEFAULT_API_VERSION, MetaApiClient
from data import (
    clear_platform_credentials_cache,
    get_platform_credentials,
    get_table_columns,
    log_dashboard_audit,
    sql_exec,
    sql_read,
    table_exists,
)
from ui import render_toolbar, numeric_column_config


ALLOWED_META_LABELS = ("헤이즈코리아", "핵이득마켓")
META_PLATFORM_VALUES = {"meta", "facebook", "facebook_ads"}


def _clean_text(value: object) -> str:
    text_value = str(value or "").strip()
    return "" if text_value.lower() in {"nan", "none", "nat"} else text_value


def _normalize_label(value: object) -> str:
    return re.sub(r"\s+", "", _clean_text(value)).casefold()


def _is_allowed_label(value: object) -> bool:
    key = _normalize_label(value)
    return any(_normalize_label(label) in key for label in ALLOWED_META_LABELS)


def _normalize_ad_account_id(value: object) -> str:
    account_id = _clean_text(value)
    if account_id.startswith("act_"):
        account_id = account_id[4:]
    if account_id.endswith(".0") and account_id[:-2].isdigit():
        account_id = account_id[:-2]
    return account_id


def _api_account_id(value: object) -> str:
    account_id = _normalize_ad_account_id(value)
    return f"act_{account_id}" if account_id else ""


def _safe_filename(value: object, fallback: str = "meta_asset") -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|\r\n\t]+", "_", _clean_text(value))
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ._")
    return (cleaned or fallback)[:120]


def _json_loads(value: object, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    text_value = _clean_text(value)
    if not text_value:
        return default
    try:
        return json.loads(text_value)
    except Exception:
        return default


def _json_dumps(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False, separators=(",", ":"))


def _extra_json(value: object) -> dict[str, Any]:
    parsed = _json_loads(value, {})
    return parsed if isinstance(parsed, dict) else {}


def _token_for_account(row: pd.Series | dict[str, Any]) -> str:
    account_token = _clean_text(row.get("access_token", ""))
    return account_token or _clean_text(os.getenv("META_ACCESS_TOKEN"))


def _select_meta_accounts(engine) -> pd.DataFrame:
    try:
        df = get_platform_credentials(engine)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    for col in ["id", "platform", "account_label", "manager", "account_id", "access_token", "is_active", "extra_json"]:
        if col not in df.columns:
            df[col] = "" if col != "is_active" else True
    work = df.copy()
    work["platform_norm"] = work["platform"].fillna("").astype(str).str.strip().str.lower()
    work["account_id_norm"] = work["account_id"].map(_normalize_ad_account_id)
    work = work[
        work["platform_norm"].isin(META_PLATFORM_VALUES)
        & work["account_label"].map(_is_allowed_label)
        & work["account_id_norm"].astype(str).str.strip().ne("")
        & work["is_active"].fillna(True).astype(bool)
    ].copy()
    if work.empty:
        return pd.DataFrame()
    work["option_label"] = work.apply(
        lambda row: f"{row['account_label']} · act_{row['account_id_norm']}",
        axis=1,
    )
    return work.drop_duplicates("account_id_norm", keep="last").sort_values("account_label").reset_index(drop=True)


def _account_picker(accounts: pd.DataFrame, key: str) -> pd.Series | None:
    options = accounts["option_label"].tolist()
    selected = st.selectbox("Meta 계정", options, key=key)
    if not selected:
        return None
    return accounts.iloc[options.index(selected)]


def _query_creatives(engine, account_id: str, start_dt: date, end_dt: date) -> pd.DataFrame:
    if not table_exists(engine, "dim_ad"):
        return pd.DataFrame()

    ad_cols = set(get_table_columns(engine, "dim_ad"))
    fact_cols = set(get_table_columns(engine, "fact_ad_daily")) if table_exists(engine, "fact_ad_daily") else set()
    adgroup_cols = set(get_table_columns(engine, "dim_adgroup")) if table_exists(engine, "dim_adgroup") else set()
    campaign_cols = set(get_table_columns(engine, "dim_campaign")) if table_exists(engine, "dim_campaign") else set()

    image_expr = "d.image_url" if "image_url" in ad_cols else "''::text"
    title_expr = "d.ad_title" if "ad_title" in ad_cols else "''::text"
    desc_expr = "d.ad_desc" if "ad_desc" in ad_cols else "''::text"
    text_expr = "d.creative_text" if "creative_text" in ad_cols else "''::text"
    landing_expr = "d.pc_landing_url" if "pc_landing_url" in ad_cols else "''::text"

    adgroup_join = (
        "LEFT JOIN dim_adgroup g ON d.customer_id = g.customer_id AND d.adgroup_id = g.adgroup_id"
        if adgroup_cols
        else ""
    )
    campaign_join = (
        "LEFT JOIN dim_campaign c ON g.customer_id = c.customer_id AND g.campaign_id = c.campaign_id"
        if adgroup_cols and campaign_cols
        else ""
    )
    campaign_select = "MAX(c.campaign_name)" if "campaign_name" in campaign_cols and campaign_join else "''::text"
    adgroup_select = "MAX(g.adgroup_name)" if "adgroup_name" in adgroup_cols and adgroup_join else "''::text"
    campaign_id_select = "MAX(g.campaign_id)" if "campaign_id" in adgroup_cols and adgroup_join else "''::text"

    fact_join = (
        """
        LEFT JOIN fact_ad_daily f
          ON d.customer_id = f.customer_id
         AND d.ad_id = f.ad_id
         AND f.dt BETWEEN :start_dt AND :end_dt
        """
        if fact_cols
        else ""
    )
    def metric_expr(column: str) -> str:
        return f"COALESCE(SUM(f.{column}), 0)" if column in fact_cols else "0"

    metric_select = f"""
        {metric_expr("imp")} AS imp,
        {metric_expr("clk")} AS clk,
        {metric_expr("cost")} AS cost,
        {metric_expr("conv")} AS conv,
        {metric_expr("sales")} AS sales
    """

    sql = f"""
        SELECT
            d.customer_id,
            d.ad_id,
            MAX(d.adgroup_id) AS adgroup_id,
            {campaign_id_select} AS campaign_id,
            {campaign_select} AS campaign_name,
            {adgroup_select} AS adgroup_name,
            MAX(d.ad_name) AS ad_name,
            MAX({title_expr}) AS ad_title,
            MAX({desc_expr}) AS ad_desc,
            MAX({text_expr}) AS creative_text,
            MAX({image_expr}) AS image_url,
            MAX({landing_expr}) AS landing_url,
            {metric_select}
        FROM dim_ad d
        {fact_join}
        {adgroup_join}
        {campaign_join}
        WHERE d.customer_id = :customer_id
        GROUP BY d.customer_id, d.ad_id
        ORDER BY cost DESC, imp DESC, ad_name
    """
    return sql_read(
        engine,
        sql,
        {"customer_id": account_id, "start_dt": str(start_dt), "end_dt": str(end_dt)},
    )


def _filter_creatives(df: pd.DataFrame, campaign: str, query: str, image_only: bool) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    if campaign and campaign != "전체":
        work = work[work["campaign_name"].fillna("").astype(str) == campaign]
    if query:
        haystack = (
            work.get("campaign_name", "").fillna("").astype(str)
            + " "
            + work.get("adgroup_name", "").fillna("").astype(str)
            + " "
            + work.get("ad_name", "").fillna("").astype(str)
            + " "
            + work.get("ad_title", "").fillna("").astype(str)
            + " "
            + work.get("creative_text", "").fillna("").astype(str)
            + " "
            + work.get("ad_desc", "").fillna("").astype(str)
        ).str.casefold()
        work = work[haystack.str.contains(query.casefold(), na=False)]
    if image_only:
        work = work[work["image_url"].fillna("").astype(str).str.strip().ne("")]
    return work.reset_index(drop=True)


def _excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="meta_creatives")
    return output.getvalue()


def _download_bytes(url: str, timeout: int = 20) -> tuple[bytes, str]:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "bulk-dashboard-meta-tools/1.0", "Accept": "image/*,*/*;q=0.8"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read(), response.headers.get("Content-Type", "")


def _extension_for(url: str, content_type: str) -> str:
    ctype = content_type.split(";")[0].strip().lower()
    if ctype == "image/png":
        return ".png"
    if ctype in {"image/webp", "image/x-webp"}:
        return ".webp"
    if ctype == "image/gif":
        return ".gif"
    if ctype in {"image/jpeg", "image/jpg"}:
        return ".jpg"
    ext = os.path.splitext(urllib.parse.urlparse(url).path)[1].lower()
    return ext if ext in {".jpg", ".jpeg", ".png", ".webp", ".gif"} else ".jpg"


def _zip_bytes(df: pd.DataFrame, account_label: str) -> bytes:
    output = BytesIO()
    errors: list[dict[str, str]] = []
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("meta_creatives.csv", df.to_csv(index=False).encode("utf-8-sig"))
        for idx, row in df.reset_index(drop=True).iterrows():
            image_url = _clean_text(row.get("image_url"))
            if not image_url:
                continue
            try:
                data, content_type = _download_bytes(image_url)
                ext = _extension_for(image_url, content_type)
                base_name = "_".join(
                    [
                        f"{idx + 1:03d}",
                        _safe_filename(row.get("campaign_name"), "campaign"),
                        _safe_filename(row.get("adgroup_name"), "adset"),
                        _safe_filename(row.get("ad_name") or row.get("ad_title"), "ad"),
                        _safe_filename(row.get("ad_id"), "id"),
                    ]
                )
                archive.writestr(f"images/{base_name}{ext}", data)
            except Exception as exc:
                errors.append(
                    {
                        "ad_id": _clean_text(row.get("ad_id")),
                        "ad_name": _clean_text(row.get("ad_name")),
                        "image_url": image_url,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
        if errors:
            archive.writestr("download_errors.csv", pd.DataFrame(errors).to_csv(index=False).encode("utf-8-sig"))
        archive.writestr(
            "README.txt",
            f"Meta creative export\naccount={account_label}\ncreated_at={time.strftime('%Y-%m-%d %H:%M:%S')}\n".encode("utf-8"),
        )
    return output.getvalue()


class MetaWriteClient(MetaApiClient):
    def post(self, path: str, params: dict[str, object] | None = None) -> dict[str, Any]:
        endpoint = path if path.startswith("/") else f"/{path}"
        payload = dict(params or {})
        payload["access_token"] = self.access_token
        data = urllib.parse.urlencode(payload).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}{endpoint}",
            data=data,
            headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                message = json.loads(raw).get("error", {}).get("message", raw)
            except Exception:
                message = raw
            raise RuntimeError(f"Meta API error {exc.code}: {message}") from exc


def _client_for_account(account_row: pd.Series) -> MetaWriteClient:
    token = _token_for_account(account_row)
    if not token:
        raise RuntimeError("Meta access_token이 없습니다. 설정 > 플랫폼 계정 연결의 계정별 토큰 또는 META_ACCESS_TOKEN이 필요합니다.")
    api_version = _clean_text(os.getenv("META_GRAPH_API_VERSION")) or DEFAULT_API_VERSION
    return MetaWriteClient(token, api_version=api_version)


def _list_campaigns(client: MetaApiClient, account_id: str) -> pd.DataFrame:
    rows = client.list_all(
        f"/{_api_account_id(account_id)}/campaigns",
        {"fields": "id,name,status,effective_status,objective", "limit": 200},
    )
    return pd.DataFrame(rows)


def _list_adsets(client: MetaApiClient, account_id: str, campaign_id: str = "") -> pd.DataFrame:
    rows = client.list_all(
        f"/{_api_account_id(account_id)}/adsets",
        {"fields": "id,name,status,effective_status,campaign_id,daily_budget", "limit": 200},
    )
    df = pd.DataFrame(rows)
    if campaign_id and not df.empty and "campaign_id" in df.columns:
        df = df[df["campaign_id"].astype(str) == str(campaign_id)].copy()
    return df


def _save_defaults(engine, account_row: pd.Series, defaults: dict[str, str]) -> None:
    row_id = account_row.get("id")
    if not row_id:
        raise RuntimeError("저장할 플랫폼 연결 행 ID가 없습니다.")
    sql_exec(
        engine,
        """
        UPDATE platform_credentials
           SET extra_json = COALESCE(extra_json, '{}'::jsonb) || CAST(:extra_json AS JSONB),
               updated_at = NOW()
         WHERE id = :id
        """,
        {"id": int(row_id), "extra_json": _json_dumps(defaults)},
    )
    clear_platform_credentials_cache()
    log_dashboard_audit(
        engine,
        "meta_account_defaults_update",
        "platform_credential",
        str(row_id),
        f"Meta 기본값 저장: {account_row.get('account_label')}",
        after={"account_id": account_row.get("account_id_norm"), **defaults},
    )


def _create_paused_setup(
    engine,
    account_row: pd.Series,
    *,
    campaign_mode: str,
    existing_campaign_id: str,
    campaign_name: str,
    objective: str,
    adset_mode: str,
    existing_adset_id: str,
    adset_name: str,
    daily_budget: int,
    billing_event: str,
    optimization_goal: str,
    targeting: dict[str, Any],
    promoted_object: dict[str, Any],
    creative_name: str,
    page_id: str,
    instagram_actor_id: str,
    link_url: str,
    image_url: str,
    primary_text: str,
    headline: str,
    description: str,
    call_to_action: str,
    ad_name: str,
) -> dict[str, Any]:
    client = _client_for_account(account_row)
    api_account_id = _api_account_id(account_row.get("account_id_norm"))
    if not api_account_id:
        raise RuntimeError("Meta 광고계정 ID가 없습니다.")

    result = {
        "account_id": api_account_id,
        "campaign_id": _clean_text(existing_campaign_id),
        "adset_id": _clean_text(existing_adset_id),
        "creative_id": "",
        "ad_id": "",
    }
    if campaign_mode == "새 캠페인":
        response = client.post(
            f"/{api_account_id}/campaigns",
            {
                "name": campaign_name,
                "objective": objective,
                "status": "PAUSED",
                "buying_type": "AUCTION",
                "special_ad_categories": "[]",
            },
        )
        result["campaign_id"] = _clean_text(response.get("id"))
    if not result["campaign_id"]:
        raise RuntimeError("campaign_id가 필요합니다.")

    if adset_mode == "새 광고세트":
        adset_payload: dict[str, object] = {
            "name": adset_name,
            "campaign_id": result["campaign_id"],
            "daily_budget": int(daily_budget),
            "billing_event": billing_event,
            "optimization_goal": optimization_goal,
            "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
            "targeting": _json_dumps(targeting),
            "status": "PAUSED",
        }
        if promoted_object:
            adset_payload["promoted_object"] = _json_dumps(promoted_object)
        response = client.post(f"/{api_account_id}/adsets", adset_payload)
        result["adset_id"] = _clean_text(response.get("id"))
    if not result["adset_id"]:
        raise RuntimeError("adset_id가 필요합니다.")

    link_data: dict[str, Any] = {
        "link": link_url,
        "message": primary_text,
        "name": headline,
        "description": description,
        "call_to_action": {"type": call_to_action, "value": {"link": link_url}},
    }
    if image_url:
        link_data["picture"] = image_url
    object_story_spec: dict[str, Any] = {"page_id": page_id, "link_data": link_data}
    if instagram_actor_id:
        object_story_spec["instagram_actor_id"] = instagram_actor_id

    creative = client.post(
        f"/{api_account_id}/adcreatives",
        {"name": creative_name, "object_story_spec": _json_dumps(object_story_spec)},
    )
    result["creative_id"] = _clean_text(creative.get("id"))
    ad = client.post(
        f"/{api_account_id}/ads",
        {
            "name": ad_name,
            "adset_id": result["adset_id"],
            "creative": _json_dumps({"creative_id": result["creative_id"]}),
            "status": "PAUSED",
        },
    )
    result["ad_id"] = _clean_text(ad.get("id"))

    log_dashboard_audit(
        engine,
        "meta_paused_setup_create",
        "meta_ad",
        result["ad_id"],
        f"Meta PAUSED 세팅 생성: {account_row.get('account_label')}",
        after={**result, "campaign_mode": campaign_mode, "adset_mode": adset_mode},
    )
    return result


def _render_metric_cards(filtered: pd.DataFrame, accounts: pd.DataFrame) -> None:
    def numeric_sum(column: str) -> float:
        if column not in filtered.columns:
            return 0.0
        return float(pd.to_numeric(filtered[column], errors="coerce").fillna(0).sum())

    image_count = 0
    if "image_url" in filtered.columns:
        image_count = int(filtered["image_url"].fillna("").astype(str).str.strip().ne("").sum())

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("대상 계정", f"{len(accounts.index):,}개")
    col2.metric("소재", f"{len(filtered.index):,}개")
    col3.metric("이미지", f"{image_count:,}개")
    col4.metric("광고비", f"{int(numeric_sum('cost')):,}")


def _render_download_tab(engine, accounts: pd.DataFrame) -> None:
    account_row = _account_picker(accounts, "meta_tools_download_account")
    if account_row is None:
        return
    today = date.today()
    c1, c2, c3, c4 = st.columns([0.9, 0.9, 1.2, 0.8], gap="small")
    with c1:
        start_dt = st.date_input("시작일", value=today - timedelta(days=30), key="meta_tools_start")
    with c2:
        end_dt = st.date_input("종료일", value=today, key="meta_tools_end")
    with c3:
        query = st.text_input("검색", value="", placeholder="캠페인, 광고세트, 소재명, 문구", key="meta_tools_query")
    with c4:
        image_only = st.toggle("이미지", value=True, key="meta_tools_image_only")
    if end_dt < start_dt:
        st.warning("종료일은 시작일 이후여야 합니다.")
        return

    raw_df = _query_creatives(engine, account_row["account_id_norm"], start_dt, end_dt)
    campaign_options = ["전체"]
    if not raw_df.empty and "campaign_name" in raw_df.columns:
        campaign_options.extend(sorted([x for x in raw_df["campaign_name"].fillna("").astype(str).unique() if x]))
    campaign = st.selectbox("캠페인 필터", campaign_options, key="meta_tools_campaign_filter")
    filtered = _filter_creatives(raw_df, campaign, query, image_only)

    _render_metric_cards(filtered, accounts)

    if filtered.empty:
        st.info("조건에 맞는 Meta 소재가 없습니다.")
        return

    display_cols = [
        "image_url",
        "campaign_name",
        "adgroup_name",
        "ad_name",
        "ad_title",
        "creative_text",
        "imp",
        "clk",
        "cost",
        "conv",
        "sales",
        "landing_url",
    ]
    disp = filtered[[c for c in display_cols if c in filtered.columns]].rename(
        columns={
            "image_url": "이미지",
            "campaign_name": "캠페인",
            "adgroup_name": "광고세트",
            "ad_name": "광고명",
            "ad_title": "제목",
            "creative_text": "본문",
            "imp": "노출",
            "clk": "클릭",
            "cost": "광고비",
            "conv": "전환",
            "sales": "전환매출",
            "landing_url": "랜딩 URL",
        }
    )
    st.dataframe(
        disp,
        use_container_width=True,
        height=520,
        hide_index=True,
        column_config=numeric_column_config(disp, {
            "이미지": st.column_config.ImageColumn("이미지", width="small"),
            "랜딩 URL": st.column_config.LinkColumn("랜딩 URL", display_text="열기"),
        }),
    )

    file_prefix = _safe_filename(f"{account_row['account_label']}_{start_dt}_{end_dt}", "meta_creatives")
    d1, d2, d3 = st.columns([1, 1, 2], gap="small")
    with d1:
        st.download_button(
            "소재 Excel",
            data=_excel_bytes(filtered),
            file_name=f"{file_prefix}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            icon=":material/table:",
        )
    with d2:
        zip_key = f"meta_zip_{account_row['account_id_norm']}_{start_dt}_{end_dt}_{campaign}_{query}_{image_only}"
        if st.button("ZIP 준비", use_container_width=True, icon=":material/archive:"):
            with st.spinner("이미지 ZIP을 준비하는 중입니다..."):
                st.session_state[zip_key] = _zip_bytes(filtered, str(account_row["account_label"]))
        if zip_key in st.session_state:
            st.download_button(
                "이미지 ZIP",
                data=st.session_state[zip_key],
                file_name=f"{file_prefix}_images.zip",
                mime="application/zip",
                use_container_width=True,
                icon=":material/download:",
            )
    with d3:
        st.caption("ZIP은 현재 필터 결과 기준으로 생성합니다. Meta 썸네일 URL만 수집된 소재는 원본이 아니라 썸네일이 포함될 수 있습니다.")


def _render_defaults_tab(engine, accounts: pd.DataFrame) -> None:
    account_row = _account_picker(accounts, "meta_tools_defaults_account")
    if account_row is None:
        return
    extra = _extra_json(account_row.get("extra_json"))
    c1, c2, c3 = st.columns(3, gap="small")
    with c1:
        page_id = st.text_input("Facebook Page ID", value=_clean_text(extra.get("page_id")), key="meta_default_page_id")
    with c2:
        pixel_id = st.text_input("Pixel ID", value=_clean_text(extra.get("pixel_id")), key="meta_default_pixel_id")
    with c3:
        instagram_actor_id = st.text_input("Instagram Actor ID", value=_clean_text(extra.get("instagram_actor_id")), key="meta_default_ig_id")
    if st.button("기본값 저장", type="primary", icon=":material/save:", use_container_width=True):
        try:
            _save_defaults(
                engine,
                account_row,
                {"page_id": page_id, "pixel_id": pixel_id, "instagram_actor_id": instagram_actor_id},
            )
            st.success("Meta 계정 기본값을 저장했습니다.")
            time.sleep(0.5)
            st.rerun()
        except Exception as exc:
            st.error(f"저장 실패: {exc}")


def _render_setup_tab(engine, accounts: pd.DataFrame) -> None:
    account_row = _account_picker(accounts, "meta_tools_setup_account")
    if account_row is None:
        return
    token_available = bool(_token_for_account(account_row))
    extra = _extra_json(account_row.get("extra_json"))
    status_cols = st.columns(3)
    status_cols[0].metric("광고계정", f"act_{account_row['account_id_norm']}")
    status_cols[1].metric("토큰", "있음" if token_available else "없음")
    status_cols[2].metric("생성 상태", "PAUSED 고정")
    if not token_available:
        st.warning("생성/목록 조회에는 Meta access_token이 필요합니다. 설정 > 플랫폼 계정 연결에서 계정별 토큰을 저장하거나 배포 환경에 META_ACCESS_TOKEN을 설정하세요.")

    client = None
    if token_available:
        try:
            client = _client_for_account(account_row)
        except Exception as exc:
            st.warning(f"Meta 클라이언트 준비 실패: {exc}")

    col_check, _ = st.columns([1, 3])
    with col_check:
        if st.button("계정 확인", use_container_width=True, icon=":material/verified:", disabled=client is None):
            try:
                info = client.get(f"/{_api_account_id(account_row['account_id_norm'])}", {"fields": "id,name,account_status,currency,timezone_name"})
                st.success(f"{info.get('name', account_row['account_label'])} · {info.get('currency', '')} · {info.get('timezone_name', '')}")
            except Exception as exc:
                st.error(f"확인 실패: {exc}")

    campaigns = pd.DataFrame()
    if client is not None and st.toggle("기존 캠페인/광고세트 목록 불러오기", value=False, key="meta_setup_load_lists"):
        try:
            campaigns = _list_campaigns(client, account_row["account_id_norm"])
        except Exception as exc:
            st.warning(f"캠페인 목록 조회 실패: {exc}")

    st.markdown("#### 캠페인")
    c1, c2, c3 = st.columns([0.9, 1.7, 1], gap="small")
    with c1:
        campaign_mode = st.segmented_control("방식", ["기존 캠페인", "새 캠페인"], default="기존 캠페인", key="meta_campaign_mode")
    existing_campaign_id = ""
    with c2:
        if campaign_mode == "기존 캠페인" and not campaigns.empty:
            labels = campaigns.apply(lambda r: f"{r.get('name', '')} · {r.get('id', '')}", axis=1).tolist()
            selected = st.selectbox("기존 캠페인", labels, key="meta_existing_campaign")
            existing_campaign_id = selected.rsplit(" · ", 1)[-1] if selected else ""
        else:
            existing_campaign_id = st.text_input("기존 캠페인 ID", value="", key="meta_existing_campaign_id")
    with c3:
        objective = st.selectbox("목표", ["OUTCOME_SALES", "OUTCOME_TRAFFIC", "OUTCOME_LEADS", "OUTCOME_ENGAGEMENT"], key="meta_objective")
    campaign_name = st.text_input("새 캠페인명", value=f"{account_row['account_label']} 신규 캠페인", key="meta_campaign_name")

    adsets = pd.DataFrame()
    if client is not None and existing_campaign_id:
        try:
            adsets = _list_adsets(client, account_row["account_id_norm"], existing_campaign_id)
        except Exception:
            adsets = pd.DataFrame()

    st.markdown("#### 광고세트")
    a1, a2, a3 = st.columns([0.9, 1.7, 1], gap="small")
    with a1:
        adset_mode = st.segmented_control("방식", ["기존 광고세트", "새 광고세트"], default="새 광고세트", key="meta_adset_mode")
    existing_adset_id = ""
    with a2:
        if adset_mode == "기존 광고세트" and not adsets.empty:
            labels = adsets.apply(lambda r: f"{r.get('name', '')} · {r.get('id', '')}", axis=1).tolist()
            selected = st.selectbox("기존 광고세트", labels, key="meta_existing_adset")
            existing_adset_id = selected.rsplit(" · ", 1)[-1] if selected else ""
        else:
            existing_adset_id = st.text_input("기존 광고세트 ID", value="", key="meta_existing_adset_id")
    with a3:
        daily_budget = st.number_input("일 예산", min_value=1000, max_value=10000000, value=10000, step=1000, key="meta_daily_budget")
    adset_name = st.text_input("새 광고세트명", value=f"{account_row['account_label']} 신규 광고세트", key="meta_adset_name")
    b1, b2 = st.columns(2, gap="small")
    with b1:
        optimization_goal = st.selectbox("최적화", ["OFFSITE_CONVERSIONS", "LINK_CLICKS", "LEAD_GENERATION", "REACH", "IMPRESSIONS"], key="meta_optimization_goal")
    with b2:
        billing_event = st.selectbox("과금", ["IMPRESSIONS", "LINK_CLICKS"], key="meta_billing_event")
    targeting = st.text_area("타게팅 JSON", value=_json_dumps({"geo_locations": {"countries": ["KR"]}, "age_min": 20, "age_max": 65}), height=110, key="meta_targeting")
    promoted_default = {"pixel_id": _clean_text(extra.get("pixel_id")), "custom_event_type": "PURCHASE"} if extra.get("pixel_id") else {}
    promoted_object = st.text_area("Promoted Object JSON", value=_json_dumps(promoted_default), height=90, key="meta_promoted_object")

    st.markdown("#### 광고 소재")
    s1, s2, s3 = st.columns(3, gap="small")
    with s1:
        page_id = st.text_input("Facebook Page ID", value=_clean_text(extra.get("page_id")), key="meta_page_id")
    with s2:
        instagram_actor_id = st.text_input("Instagram Actor ID", value=_clean_text(extra.get("instagram_actor_id")), key="meta_ig_id")
    with s3:
        call_to_action = st.selectbox("CTA", ["SHOP_NOW", "LEARN_MORE", "SIGN_UP", "CONTACT_US", "BUY_NOW"], key="meta_cta")
    creative_name = st.text_input("소재명", value=f"{account_row['account_label']} 신규 소재", key="meta_creative_name")
    ad_name = st.text_input("광고명", value=f"{account_row['account_label']} 신규 광고", key="meta_ad_name")
    link_url = st.text_input("랜딩 URL", value="", key="meta_link_url")
    image_url = st.text_input("이미지 URL", value="", key="meta_image_url")
    primary_text = st.text_area("본문", value="", height=90, key="meta_primary_text")
    h1, h2 = st.columns(2, gap="small")
    with h1:
        headline = st.text_input("제목", value="", key="meta_headline")
    with h2:
        description = st.text_input("설명", value="", key="meta_description")

    required_values = [page_id, link_url, primary_text, headline, ad_name]
    if campaign_mode == "기존 캠페인":
        required_values.append(existing_campaign_id)
    if adset_mode == "기존 광고세트":
        required_values.append(existing_adset_id)
    disabled = client is None or not all(_clean_text(v) for v in required_values)

    with st.expander("요청 미리보기", expanded=False):
        st.json(
            {
                "status": "PAUSED",
                "account_id": _api_account_id(account_row["account_id_norm"]),
                "campaign_mode": campaign_mode,
                "campaign_id": existing_campaign_id,
                "campaign_name": campaign_name,
                "adset_mode": adset_mode,
                "adset_id": existing_adset_id,
                "adset_name": adset_name,
                "daily_budget": int(daily_budget),
                "creative_name": creative_name,
                "ad_name": ad_name,
            }
        )

    if st.button("PAUSED 세팅 생성", type="primary", use_container_width=True, disabled=disabled, icon=":material/rocket_launch:"):
        try:
            result = _create_paused_setup(
                engine,
                account_row,
                campaign_mode=campaign_mode,
                existing_campaign_id=existing_campaign_id,
                campaign_name=campaign_name,
                objective=objective,
                adset_mode=adset_mode,
                existing_adset_id=existing_adset_id,
                adset_name=adset_name,
                daily_budget=int(daily_budget),
                billing_event=billing_event,
                optimization_goal=optimization_goal,
                targeting=_json_loads(targeting, {}),
                promoted_object=_json_loads(promoted_object, {}),
                creative_name=creative_name,
                page_id=page_id,
                instagram_actor_id=instagram_actor_id,
                link_url=link_url,
                image_url=image_url,
                primary_text=primary_text,
                headline=headline,
                description=description,
                call_to_action=call_to_action,
                ad_name=ad_name,
            )
            st.success("생성 완료. 모든 객체는 PAUSED 상태입니다.")
            st.json(result)
        except Exception as exc:
            st.error(f"생성 실패: {exc}")


@st.fragment
def page_meta_tools(engine) -> None:
    render_toolbar(
        "Meta 도구",
        "헤이즈코리아와 핵이득마켓 Meta 소재 다운로드 및 PAUSED 세팅을 관리합니다.",
        [{"label": "Meta", "tone": "primary"}, {"label": "PAUSED 생성", "tone": "info"}],
    )
    accounts = _select_meta_accounts(engine)
    if accounts.empty:
        st.info("활성 Meta 연동 중 헤이즈코리아 또는 핵이득마켓 계정을 찾지 못했습니다. 설정 및 연결의 플랫폼 계정 연결을 확인하세요.")
        return

    token_count = int(accounts["access_token"].fillna("").astype(str).str.strip().ne("").sum()) if "access_token" in accounts.columns else 0
    k1, k2, k3 = st.columns(3)
    k1.metric("대상 계정", f"{len(accounts.index):,}개")
    k2.metric("계정별 토큰", f"{token_count:,}개")
    k3.metric("허용 광고주", ", ".join(ALLOWED_META_LABELS))

    tab = st.pills("작업", ["소재 다운로드", "PAUSED 세팅", "계정 기본값"], default="소재 다운로드", key="meta_tools_tab")
    if tab == "소재 다운로드":
        _render_download_tab(engine, accounts)
    elif tab == "PAUSED 세팅":
        _render_setup_tab(engine, accounts)
    else:
        _render_defaults_tab(engine, accounts)
