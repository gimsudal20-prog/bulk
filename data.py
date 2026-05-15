# -*- coding: utf-8 -*-
"""data.py - Database connection, caching, and common queries."""
import os
import re
import time
import json
import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, StatementError, InterfaceError
from sqlalchemy.pool import QueuePool
from datetime import date

# ==========================================
# 1. Database Connection (QueuePool 적용)
# ==========================================
def _env_int(name: str, default: int, min_value: int | None = None) -> int:
    try:
        value = int(os.getenv(name, str(default)) or default)
    except Exception:
        value = default
    if min_value is not None:
        value = max(min_value, value)
    return value


def _require_database_url() -> str:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL 환경변수가 비어 있습니다. 메모리 SQLite로 대체 실행하지 않고 중단합니다. "
            "실제 DB 연결 문자열을 설정한 뒤 다시 실행해주세요."
        )
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return db_url

@st.cache_resource
def get_engine():
    db_url = _require_database_url()

    connect_args = {
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    }

    pool_size = _env_int("DASHBOARD_DB_POOL_SIZE", 5, min_value=1)
    max_overflow = _env_int("DASHBOARD_DB_MAX_OVERFLOW", 10, min_value=0)
    pool_timeout = _env_int("DASHBOARD_DB_POOL_TIMEOUT", 20, min_value=5)
    pool_recycle = _env_int("DASHBOARD_DB_POOL_RECYCLE", 1800, min_value=60)

    return create_engine(
        db_url,
        poolclass=QueuePool,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
        connect_args=connect_args,
        future=True,
    )

def db_ping(engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False

def table_exists(engine, table_name: str) -> bool:
    if "_table_names_cache" not in st.session_state:
        try:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'"))
                st.session_state["_table_names_cache"] = [r[0] for r in res]
        except Exception:
            return False
    return table_name in st.session_state.get("_table_names_cache", [])

def _validate_sql_identifier(name: str, label: str = "identifier") -> str:
    value = str(name or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"유효하지 않은 SQL {label}: {name}")
    return value

@st.cache_data(ttl=43200, max_entries=20, show_spinner=False)
def get_table_columns(_engine, table_name: str) -> list:
    safe_table_name = _validate_sql_identifier(table_name, "table name")
    for attempt in range(3):
        try:
            with _engine.connect() as conn:
                res = conn.execute(
                    text(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = :table_name
                          AND table_schema = 'public'
                        """
                    ),
                    {"table_name": safe_table_name},
                )
                return [r[0] for r in res]
        except (OperationalError, StatementError, InterfaceError):
            if attempt == 2:
                st.cache_resource.clear()
                st.error("데이터베이스 일시적 연결 오류. 페이지를 새로고침(F5) 해주세요.")
                st.stop()
            _engine.dispose()
            time.sleep(1.0)
        except Exception:
            return []

@st.cache_data(ttl=43200, max_entries=30, show_spinner=False)
def sql_read(_engine, query: str, params: dict = None) -> pd.DataFrame:
    last_error = None
    for attempt in range(3):
        try:
            with _engine.connect() as conn:
                return pd.read_sql(text(query), conn, params=params)
        except Exception as e:
            last_error = e
            time.sleep(1.0)
            
    st.cache_resource.clear()
    st.error(f"DB 연결이 지연되고 있습니다. 잠시 후 새로고침(F5) 해주세요. (사유: {last_error})")
    st.stop()

def sql_exec(_engine, query: str, params: dict = None) -> None:
    last_error = None
    for attempt in range(3):
        try:
            with _engine.begin() as conn:
                conn.execute(text(query), params or {})
            return
        except Exception as e:
            last_error = e
            time.sleep(1.0)
            
    st.cache_resource.clear()
    raise RuntimeError(f"쿼리 실행 실패 (사유: {last_error})")

def _normalize_filter_values(values) -> tuple:
    if not values:
        return tuple()
    normalized = []
    for value in values:
        value_str = str(value).strip()
        if value_str:
            normalized.append(value_str)
    return tuple(normalized)


def _sql_in_str_list(lst) -> str:
    """Legacy compatibility helper for existing view modules.

    Prefer parameterized filters for new code. This helper remains so existing
    view modules can still import cleanly until their SQL is migrated.
    """
    normalized = _normalize_filter_values(lst)
    if not normalized:
        return "''"
    return ",".join("'" + value.replace("'", "''") + "'" for value in normalized)

def _build_in_filter(column_sql: str, values, param_prefix: str) -> tuple[str, dict]:
    normalized = _normalize_filter_values(values)
    if not normalized:
        return "", {}

    placeholders = []
    params = {}
    for idx, value in enumerate(normalized):
        key = f"{param_prefix}_{idx}"
        placeholders.append(f":{key}")
        params[key] = value
    return f"AND {column_sql} IN ({', '.join(placeholders)})", params

def _build_campaign_type_filter(column_name: str, type_sel: tuple, param_prefix: str = "campaign_type") -> tuple[str, dict]:
    normalized_types = _normalize_filter_values(type_sel)
    if not normalized_types:
        return "", {}

    safe_column = _validate_sql_identifier(column_name, "column name")
    rev_map = {
        "파워링크": "WEB_SITE",
        "쇼핑검색": "SHOPPING",
        "파워컨텐츠": "POWER_CONTENTS",
        "브랜드검색": "BRAND_SEARCH",
        "플레이스": "PLACE",
    }
    db_types = [rev_map.get(t, t) for t in normalized_types]
    where_sql, params = _build_in_filter(f"c.{safe_column}", db_types, param_prefix)
    return where_sql, params

def _safe_limit(value, default: int, max_limit: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    if parsed <= 0:
        return default
    return min(parsed, max_limit)

# ==========================================
# 2. Metadata & Dimensions & Seeding
# ==========================================
DEFAULT_OPERATING_WEEKDAYS = "0,1,2,3,4,5,6"
_WEEKDAY_NAME_TO_INDEX = {
    "월": 0,
    "mon": 0,
    "monday": 0,
    "화": 1,
    "tue": 1,
    "tuesday": 1,
    "수": 2,
    "wed": 2,
    "wednesday": 2,
    "목": 3,
    "thu": 3,
    "thursday": 3,
    "금": 4,
    "fri": 4,
    "friday": 4,
    "토": 5,
    "sat": 5,
    "saturday": 5,
    "일": 6,
    "sun": 6,
    "sunday": 6,
}


def normalize_operating_weekdays(value) -> str:
    """Normalize operating weekdays to a stable CSV string such as '0,1,2,3,4'."""
    if isinstance(value, (list, tuple, set)):
        raw_parts = list(value)
    else:
        try:
            if pd.isna(value):
                return DEFAULT_OPERATING_WEEKDAYS
        except Exception:
            pass
        text_value = str(value or "").strip()
        if not text_value or text_value.lower() in {"nan", "none", "nat"}:
            return DEFAULT_OPERATING_WEEKDAYS
        compact = text_value.replace(" ", "").lower()
        if compact in {"all", "daily", "everyday", "매일", "매일운영", "전체", "전체요일"}:
            return DEFAULT_OPERATING_WEEKDAYS
        if compact in {"weekday", "weekdays", "평일", "평일운영"}:
            return "0,1,2,3,4"
        if compact in {"weekend", "weekends", "주말", "주말운영"}:
            return "5,6"
        raw_parts = [part for part in re.split(r"[,/|;·\s]+", text_value.replace("요일", "")) if part]

    selected: list[int] = []
    for part in raw_parts:
        token = str(part).strip().lower()
        if token in {"", "nan", "none", "nat"}:
            continue
        if re.fullmatch(r"[0-6]", token):
            selected.append(int(token))
            continue
        if token in _WEEKDAY_NAME_TO_INDEX:
            selected.append(_WEEKDAY_NAME_TO_INDEX[token])
            continue
        for weekday_name, weekday_idx in _WEEKDAY_NAME_TO_INDEX.items():
            if len(weekday_name) == 1 and weekday_name in token:
                selected.append(weekday_idx)

    selected = sorted({day for day in selected if 0 <= int(day) <= 6})
    if not selected:
        return DEFAULT_OPERATING_WEEKDAYS
    return ",".join(str(day) for day in selected)


def _is_operating_weekdays_column(c_clean: str) -> bool:
    return c_clean in [
        "operating_weekdays",
        "operatingweekdays",
        "operatingdays",
        "businessdays",
        "budget_operating_weekdays",
        "운영요일",
        "운영기준요일",
        "운영일",
        "예산운영요일",
    ]


def _normalize_customer_id_value(value) -> str:
    """Normalize customer_id read from Excel/DB so joins do not fall back to raw IDs.

    accounts.xlsx often stores custom IDs as numeric cells, which pandas can read as
    1234567.0 when the sheet has blank rows. Keep IDs as clean strings everywhere.
    """
    if pd.isna(value):
        return ""
    value_str = str(value).strip()
    if not value_str or value_str.lower() in {"nan", "none", "nat"}:
        return ""

    # Common Excel/pandas numeric representation: 3469289.0 -> 3469289
    if re.fullmatch(r"\d+\.0+", value_str):
        return value_str.split(".", 1)[0]

    # Defensive handling for scientific notation if Excel ever exposes it that way.
    if re.fullmatch(r"\d+(?:\.\d+)?[eE]\+\d+", value_str):
        try:
            as_float = float(value_str)
            if as_float.is_integer():
                return str(int(as_float))
        except Exception:
            pass

    return value_str


def _normalize_customer_id_series(series: pd.Series) -> pd.Series:
    return series.map(_normalize_customer_id_value)


def _prepare_accounts_meta_df(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize accounts.xlsx columns and remove invalid/blank rows."""
    if df is None:
        return pd.DataFrame()

    df = df.copy()
    rename_map = {}
    for c in df.columns:
        c_clean = str(c).replace(" ", "").lower()
        if c_clean in ["커스텀id", "customerid", "customer_id", "id", "고객id"]:
            rename_map[c] = "customer_id"
        elif c_clean in ["업체명", "accountname", "account_name", "name", "계정명"]:
            rename_map[c] = "account_name"
        elif c_clean in ["담당자", "manager"]:
            rename_map[c] = "manager"
        elif _is_operating_weekdays_column(c_clean):
            rename_map[c] = "operating_weekdays"

    df = df.rename(columns=rename_map)

    if "customer_id" not in df.columns or "account_name" not in df.columns:
        raise ValueError("accounts.xlsx에는 '업체명'과 '커스텀 ID' 컬럼이 필요합니다.")

    df["customer_id"] = _normalize_customer_id_series(df["customer_id"])
    df["account_name"] = df["account_name"].fillna("").astype(str).str.strip()

    if "manager" in df.columns:
        df["manager"] = df["manager"].fillna("미배정").astype(str).str.strip()
        df.loc[df["manager"].isin(["", "nan", "None", "NaN"]), "manager"] = "미배정"
    else:
        df["manager"] = "미배정"

    df = df[(df["customer_id"] != "") & (df["account_name"] != "")].copy()

    if "monthly_budget" in df.columns:
        df["monthly_budget"] = pd.to_numeric(df["monthly_budget"], errors="coerce").fillna(0).astype("int64")
    else:
        df["monthly_budget"] = 0

    if "operating_weekdays" in df.columns:
        df["operating_weekdays"] = df["operating_weekdays"].apply(normalize_operating_weekdays)
    else:
        df["operating_weekdays"] = DEFAULT_OPERATING_WEEKDAYS

    # Same customer_id can appear more than once after accidental duplicated rows.
    df = df.drop_duplicates(subset=["customer_id"], keep="last").reset_index(drop=True)
    return df


def _normalize_existing_dim_customer_table(_engine) -> None:
    """Repair previously synced dim_customer rows such as customer_id='3469289.0'."""
    if not table_exists(_engine, "dim_customer"):
        return

    try:
        df = sql_read(_engine, "SELECT * FROM dim_customer")
        if df.empty:
            return
        fixed_df = _prepare_accounts_meta_df(df)
        if fixed_df.empty:
            return
        fixed_df.to_sql("dim_customer", _engine, if_exists="replace", index=False)
        if "_table_names_cache" in st.session_state:
            del st.session_state["_table_names_cache"]
    except Exception:
        # Do not block page load; explicit upload sync will still fix the table.
        pass


def seed_from_accounts_xlsx(engine, df=None, file_buffer=None):
    try:
        if df is None and file_buffer is not None:
            df = pd.read_excel(file_buffer)
        if df is not None:
            df = _prepare_accounts_meta_df(df)

            if table_exists(engine, "dim_customer"):
                try:
                    old_df = sql_read(engine, "SELECT * FROM dim_customer")
                    if not old_df.empty:
                        old_df = _prepare_accounts_meta_df(old_df)
                        if "monthly_budget" in old_df.columns:
                            budget_map = dict(zip(old_df["customer_id"], old_df["monthly_budget"]))
                            df["monthly_budget"] = df["customer_id"].map(budget_map).fillna(df["monthly_budget"]).astype("int64")
                        if "operating_weekdays" in old_df.columns:
                            weekdays_map = dict(zip(old_df["customer_id"], old_df["operating_weekdays"]))
                            df["operating_weekdays"] = (
                                df["customer_id"]
                                .map(weekdays_map)
                                .fillna(df["operating_weekdays"])
                                .apply(normalize_operating_weekdays)
                            )
                except Exception:
                    pass

            df.to_sql("dim_customer", engine, if_exists="replace", index=False)
            if "_table_names_cache" in st.session_state:
                del st.session_state["_table_names_cache"]
            get_meta.clear()
            return {"meta": len(df)}
        return {"meta": 0}
    except Exception as e:
        st.error(f"업로드 실패: {e}")
        return {"meta": 0}


@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def get_meta(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_customer"):
        return pd.DataFrame()
        
    # ✨ SELECT 최적화 (꼭 필요한 컬럼만 추출)
    cols = get_table_columns(_engine, "dim_customer")
    target_cols = []
    for c in cols:
        c_clean = str(c).replace(" ", "").lower()
        if c_clean in ["커스텀id", "customerid", "customer_id", "id", "고객id", "업체명", "accountname", "account_name", "name", "계정명", "담당자", "manager", "monthly_budget"] or _is_operating_weekdays_column(c_clean):
            target_cols.append(f'"{c}"')
            
    select_str = ", ".join(target_cols) if target_cols else "*"
    df = sql_read(_engine, f"SELECT {select_str} FROM dim_customer")
    
    if not df.empty:
        rename_map = {}
        for c in df.columns:
            c_clean = str(c).replace(" ", "").lower()
            if c_clean in ["커스텀id", "customerid", "customer_id", "id", "고객id"]:
                rename_map[c] = "customer_id"
            elif c_clean in ["업체명", "accountname", "account_name", "name", "계정명"]:
                rename_map[c] = "account_name"
            elif c_clean in ["담당자", "manager"]:
                rename_map[c] = "manager"
            elif _is_operating_weekdays_column(c_clean):
                rename_map[c] = "operating_weekdays"
        df = df.rename(columns=rename_map)
        if "customer_id" in df.columns:
            df["customer_id"] = _normalize_customer_id_series(df["customer_id"])
        if "account_name" in df.columns:
            df["account_name"] = df["account_name"].fillna("").astype(str).str.strip()
        if "manager" in df.columns:
            df["manager"] = df["manager"].fillna("미배정").astype(str).str.strip()
        if "monthly_budget" in df.columns:
            df["monthly_budget"] = pd.to_numeric(df["monthly_budget"], errors="coerce").fillna(0)
        else:
            df["monthly_budget"] = 0
        if "operating_weekdays" in df.columns:
            df["operating_weekdays"] = df["operating_weekdays"].apply(normalize_operating_weekdays)
        else:
            df["operating_weekdays"] = DEFAULT_OPERATING_WEEKDAYS
    return df

@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame()
        
    # ✨ SELECT 최적화 (꼭 필요한 컬럼만 추출)
    cols = get_table_columns(_engine, "dim_campaign")
    target_cols = []
    for c in cols:
        c_clean = str(c).replace(" ", "").lower()
        if c_clean in ["customer_id", "campaign_id", "campaign_name", "campaign_tp", "campaign_type", "campaign_type_label", "status", "target_roas", "min_roas"]:
            target_cols.append(f'"{c}"')
            
    select_str = ", ".join(target_cols) if target_cols else "*"
    df = sql_read(_engine, f"SELECT {select_str} FROM dim_campaign")
    if not df.empty and "customer_id" in df.columns:
        df["customer_id"] = _normalize_customer_id_series(df["customer_id"])
    return df

@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def get_campaign_type_options_cached(_engine) -> list:
    """엔진 기준으로 캠페인 유형 옵션을 캐시해서 반환한다."""
    dim_campaign = load_dim_campaign(_engine)
    return get_campaign_type_options(dim_campaign)

def get_campaign_type_options(dim_campaign: pd.DataFrame) -> list:
    if dim_campaign is None or dim_campaign.empty:
        return ["파워링크", "쇼핑검색"]
    col_name = "campaign_tp" if "campaign_tp" in dim_campaign.columns else ("campaign_type_label" if "campaign_type_label" in dim_campaign.columns else "campaign_type")
    if col_name not in dim_campaign.columns:
        return ["파워링크", "쇼핑검색"]

    mapping = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENT": "파워컨텐츠", "POWER_CONTENTS": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
    raw_opts = [str(x) for x in dim_campaign[col_name].dropna().unique() if str(x).strip()]
    opts = list(set([mapping.get(x.upper(), x) for x in raw_opts]))
    return sorted(opts) if opts else ["파워링크", "쇼핑검색"]

def _map_campaign_types(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    if not df.empty and col_name in df.columns:
        mapping = {"WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENT": "파워컨텐츠", "POWER_CONTENTS": "파워컨텐츠", "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스"}
        df[col_name] = df[col_name].apply(lambda x: mapping.get(str(x).upper(), x) if pd.notna(x) else x)
    return df

@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def get_latest_dates(_engine) -> dict:
    dates = {}
    for tbl in ["fact_campaign_daily", "fact_adgroup_daily", "fact_keyword_daily", "fact_ad_daily", "fact_shopping_query_daily"]:
        if table_exists(_engine, tbl):
            df = sql_read(_engine, f"SELECT MAX(dt) as dt FROM {tbl}")
            if not df.empty and pd.notna(df.iloc[0]["dt"]):
                dates[tbl] = df.iloc[0]["dt"]
    return dates

# ==========================================
# 2-1. Platform Credential Storage
# ==========================================
def ensure_platform_credentials_table(_engine) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS platform_credentials (
        id BIGSERIAL PRIMARY KEY,
        platform VARCHAR(30) NOT NULL,
        account_label VARCHAR(120) NOT NULL,
        customer_id BIGINT NULL,
        account_id VARCHAR(120) NULL,
        access_token TEXT NULL,
        refresh_token TEXT NULL,
        app_id VARCHAR(200) NULL,
        app_secret TEXT NULL,
        extra_json JSONB DEFAULT '{}'::jsonb,
        is_active BOOLEAN DEFAULT TRUE,
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """
    sql_exec(_engine, sql)
    sql_exec(_engine, "CREATE INDEX IF NOT EXISTS idx_platform_credentials_platform ON platform_credentials(platform)")
    sql_exec(_engine, "CREATE INDEX IF NOT EXISTS idx_platform_credentials_customer_id ON platform_credentials(customer_id)")
    if "_table_names_cache" in st.session_state:
        del st.session_state["_table_names_cache"]

def _normalize_extra_json(extra_json) -> str:
    if extra_json is None:
        return "{}"
    if isinstance(extra_json, str):
        s = extra_json.strip()
        return s if s else "{}"
    try:
        return json.dumps(extra_json, ensure_ascii=False)
    except Exception:
        return "{}"

# 인증 캐시는 보안 및 토큰 만료를 위해 짧게 유지 (60초)
@st.cache_data(ttl=60, max_entries=20, show_spinner=False)
def get_platform_credentials(_engine, platform: str = "") -> pd.DataFrame:
    ensure_platform_credentials_table(_engine)
    if platform:
        df = sql_read(
            _engine,
            """
            SELECT *
            FROM platform_credentials
            WHERE platform = :platform
            ORDER BY is_active DESC, updated_at DESC, id DESC
            """,
            {"platform": platform},
        )
    else:
        df = sql_read(
            _engine,
            """
            SELECT *
            FROM platform_credentials
            ORDER BY platform, is_active DESC, updated_at DESC, id DESC
            """
        )
    if df.empty:
        return df

    if "extra_json" in df.columns:
        df["extra_json"] = df["extra_json"].apply(lambda x: x if isinstance(x, dict) else (json.loads(x) if isinstance(x, str) and str(x).strip().startswith("{") else {}))
    return df

def clear_platform_credentials_cache():
    try:
        get_platform_credentials.clear()
    except Exception:
        pass

def upsert_platform_credential(_engine, row: dict) -> None:
    ensure_platform_credentials_table(_engine)

    payload = {
        "id": row.get("id"),
        "platform": str(row.get("platform", "")).strip().lower(),
        "account_label": str(row.get("account_label", "")).strip(),
        "customer_id": None if str(row.get("customer_id", "")).strip() in ["", "None", "nan"] else int(row.get("customer_id")),
        "account_id": str(row.get("account_id", "")).strip(),
        "access_token": str(row.get("access_token", "")).strip(),
        "refresh_token": str(row.get("refresh_token", "")).strip(),
        "app_id": str(row.get("app_id", "")).strip(),
        "app_secret": str(row.get("app_secret", "")).strip(),
        "extra_json": _normalize_extra_json(row.get("extra_json")),
        "is_active": bool(row.get("is_active", True)),
    }

    if not payload["platform"] or not payload["account_label"]:
        raise ValueError("platform, account_label은 필수입니다.")

    if payload["id"]:
        sql_exec(
            _engine,
            """
            UPDATE platform_credentials
               SET platform=:platform,
                   account_label=:account_label,
                   customer_id=:customer_id,
                   account_id=:account_id,
                   access_token=:access_token,
                   refresh_token=:refresh_token,
                   app_id=:app_id,
                   app_secret=:app_secret,
                   extra_json=CAST(:extra_json AS JSONB),
                   is_active=:is_active,
                   updated_at=NOW()
             WHERE id=:id
            """,
            payload,
        )
    else:
        sql_exec(
            _engine,
            """
            INSERT INTO platform_credentials
                (platform, account_label, customer_id, account_id, access_token, refresh_token, app_id, app_secret, extra_json, is_active, updated_at)
            VALUES
                (:platform, :account_label, :customer_id, :account_id, :access_token, :refresh_token, :app_id, :app_secret, CAST(:extra_json AS JSONB), :is_active, NOW())
            """,
            payload,
        )
    clear_platform_credentials_cache()

def delete_platform_credential(_engine, row_id: int) -> None:
    ensure_platform_credentials_table(_engine)
    sql_exec(_engine, "DELETE FROM platform_credentials WHERE id = :id", {"id": int(row_id)})
    clear_platform_credentials_cache()

def toggle_platform_credential(_engine, row_id: int, is_active: bool) -> None:
    ensure_platform_credentials_table(_engine)
    sql_exec(
        _engine,
        "UPDATE platform_credentials SET is_active = :is_active, updated_at = NOW() WHERE id = :id",
        {"id": int(row_id), "is_active": bool(is_active)},
    )
    clear_platform_credentials_cache()

# ==========================================
# 3. Helper Functions (Math & Formatting)
# ==========================================
def pct_change(cur: float, base: float) -> float:
    if not base or base == 0:
        return 100.0 if cur and cur > 0 else 0.0
    return ((cur - base) / base) * 100.0

def pct_to_arrow(val) -> str:
    if val is None or pd.isna(val):
        return "-"
    if val > 0:
        return f"▲ {val:.1f}%"
    if val < 0:
        return f"▼ {abs(val):.1f}%"
    return "-"

def format_currency(val) -> str:
    try:
        return f"{int(float(val)):,}원"
    except (ValueError, TypeError):
        return "0원"

def format_number_commas(val) -> str:
    try:
        return f"{int(float(val)):,}"
    except (ValueError, TypeError):
        return "0"

def mask_secret(val: str) -> str:
    s = str(val or "").strip()
    if not s:
        return ""
    if len(s) <= 8:
        return "*" * len(s)
    return f"{s[:4]}{'*' * max(4, len(s) - 8)}{s[-4:]}"

# ==========================================
# 4. Data Aggregation Queries
# ==========================================
def ensure_target_roas_column(_engine):
    try:
        sql_exec(_engine, "ALTER TABLE dim_campaign ADD COLUMN target_roas NUMERIC DEFAULT 0")
    except Exception:
        pass
    try:
        sql_exec(_engine, "ALTER TABLE dim_campaign ADD COLUMN min_roas NUMERIC DEFAULT 0")
    except Exception:
        pass

def update_campaign_target_roas(_engine, cid, campaign_id, target_val, min_val):
    ensure_target_roas_column(_engine)

    t_val = float(target_val) if pd.notna(target_val) and str(target_val).strip() != "" else 0.0
    m_val = float(min_val) if pd.notna(min_val) and str(min_val).strip() != "" else 0.0

    query = """
        UPDATE dim_campaign
        SET target_roas = :t_val, min_roas = :m_val
        WHERE CAST(customer_id AS TEXT) = :cid
          AND CAST(campaign_id AS TEXT) = :camp_id
    """
    sql_exec(_engine, query, {
        "t_val": t_val,
        "m_val": m_val,
        "cid": str(cid),
        "camp_id": str(campaign_id)
    })

def _strict_conv_selects(fact_cols: list, alias: str = "") -> dict:
    prefix = f"{alias}." if alias else ""
    fact_col_set = set(fact_cols or [])
    has_cart = "cart_conv" in fact_col_set
    has_wish = "wishlist_conv" in fact_col_set

    def pick_expr(candidates: list[str]) -> str:
        picked = [f"{prefix}{col}" for col in candidates if col in fact_col_set]
        if not picked:
            return "0"
        if len(picked) == 1:
            return f"COALESCE({picked[0]}, 0)"
        return f"COALESCE({', '.join(picked)}, 0)"

    def max_expr(candidates: list[str]) -> str:
        picked = [f"{prefix}{col}" for col in candidates if col in fact_col_set]
        if not picked:
            return "0"
        if len(picked) == 1:
            return f"COALESCE({picked[0]}, 0)"
        safe_picked = [f"COALESCE({col}, 0)" for col in picked]
        return f"GREATEST({', '.join(safe_picked)})"

    return {
        "purchase_conv_expr": pick_expr(["primary_conv", "purchase_conv", "conv"]),
        "purchase_sales_expr": pick_expr(["primary_sales", "purchase_sales", "sales"]),
        "cart_conv_expr": f"COALESCE({prefix}cart_conv, 0)" if has_cart else "0",
        "cart_sales_expr": f"COALESCE({prefix}cart_sales, 0)" if has_cart else "0",
        "wish_conv_expr": f"COALESCE({prefix}wishlist_conv, 0)" if has_wish else "0",
        "wish_sales_expr": f"COALESCE({prefix}wishlist_sales, 0)" if has_wish else "0",
        "total_conv_expr": max_expr(["conv", "primary_conv", "purchase_conv"]),
        "total_sales_expr": max_expr(["sales", "primary_sales", "purchase_sales"]),
    }



def _resolve_campaign_type_column(_engine) -> tuple[list, str]:
    cols = get_table_columns(_engine, "dim_campaign")
    cp_col = "campaign_tp" if "campaign_tp" in cols else ("campaign_type_label" if "campaign_type_label" in cols else "campaign_type")
    return cols, cp_col


def _resolve_rank_column(_engine, fact_table: str) -> str | None:
    fact_cols = get_table_columns(_engine, fact_table)
    for candidate in ["avg_rank", "avg_rnk", "averageposition", "average_position", "avgrnk"]:
        if candidate in fact_cols:
            return candidate
    return None


def _build_rank_metric_sql(rank_col: str | None) -> tuple[str, str]:
    if not rank_col:
        return "", ""
    rank_agg_sql = f", CASE WHEN SUM(imp) > 0 THEN SUM(COALESCE({rank_col}, 0) * imp) / SUM(imp) ELSE NULL END as avg_rank"
    rank_select_sql = ", agg.avg_rank"
    return rank_agg_sql, rank_select_sql


def _build_bundle_metric_sql(fact_cols: list) -> dict:
    expr = _strict_conv_selects(fact_cols)
    return {
        "conv_agg_sql": f", SUM({expr['purchase_conv_expr']}) as conv, SUM({expr['purchase_sales_expr']}) as sales, SUM({expr['total_conv_expr']}) as tot_conv, SUM({expr['total_sales_expr']}) as tot_sales",
        "cart_agg_sql": f", SUM({expr['cart_conv_expr']}) as cart_conv, SUM({expr['cart_sales_expr']}) as cart_sales",
        "wish_agg_sql": f", SUM({expr['wish_conv_expr']}) as wishlist_conv, SUM({expr['wish_sales_expr']}) as wishlist_sales",
        "cart_select_sql": ", agg.cart_conv, agg.cart_sales",
        "wish_select_sql": ", agg.wishlist_conv, agg.wishlist_sales",
    }


def _build_dt_sql(include_dt: bool) -> tuple[str, str]:
    return (", dt", ", agg.dt") if include_dt else ("", "")


def _resolve_ad_dimension_selects(_engine) -> tuple[str, str, str]:
    ad_cols = get_table_columns(_engine, "dim_ad")
    url_select = "ad.pc_landing_url as landing_url" if "pc_landing_url" in ad_cols else "'' as landing_url"
    title_select = "ad.ad_title" if "ad_title" in ad_cols else "ad.ad_name as ad_title"
    image_select = "ad.image_url" if "image_url" in ad_cols else "'' as image_url"
    return url_select, title_select, image_select


def _bundle_limit_clause(topn_cost: int) -> str:
    try:
        if topn_cost is not None and int(topn_cost) < 0:
            return ""
    except Exception:
        pass
    limit_value = _safe_limit(topn_cost, default=10000, max_limit=10000)
    return f" ORDER BY agg.cost DESC LIMIT {limit_value}"


def _finalize_bundle_df(df: pd.DataFrame, campaign_type_col: str) -> pd.DataFrame:
    return _map_campaign_types(df, campaign_type_col)


def _read_fact_customer_summary(_engine, table: str, select_sql: str, d1: date, d2: date, where_cid: str, cid_params: dict) -> pd.DataFrame:
    sql = f"SELECT customer_id, {select_sql} FROM {table} WHERE dt BETWEEN :d1 AND :d2 {where_cid} GROUP BY customer_id"
    return sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params})


def _merge_customer_metric_frame(base_df: pd.DataFrame, metric_df: pd.DataFrame) -> pd.DataFrame:
    if metric_df.empty:
        return base_df
    metric_df = metric_df.copy()
    metric_df["customer_id"] = _normalize_customer_id_series(metric_df["customer_id"])
    return base_df.merge(metric_df, on="customer_id", how="left")


def _fill_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def _resolve_total_type_join(_engine, entity: str, type_sel: tuple) -> tuple[str, str, dict]:
    if not type_sel or not table_exists(_engine, "dim_campaign"):
        return "", "", {}

    dim_cols = get_table_columns(_engine, "dim_campaign")
    cp_col = "campaign_tp" if "campaign_tp" in dim_cols else ("campaign_type_label" if "campaign_type_label" in dim_cols else "campaign_type")
    fact_cols = get_table_columns(_engine, f"fact_{entity}_daily")
    if "campaign_id" not in fact_cols or cp_col not in dim_cols:
        return "", "", {}

    join_sql = "JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id"
    where_sql, params = _build_campaign_type_filter(cp_col, type_sel, f"{entity}_campaign_type")
    return join_sql, where_sql, params



def _read_budget_campaign_metrics(_engine, avg_d1: date, avg_d2: date, month_d1: date, month_d2: date, prev_month_d1: date, prev_month_d2: date, avg_days: int, where_cid: str, cid_params: dict) -> pd.DataFrame:
    outer_d1 = min(avg_d1, month_d1, prev_month_d1)
    outer_d2 = max(avg_d2, month_d2, prev_month_d2)

    def _run_budget_metric_query(table_name: str, sales_expr: str) -> pd.DataFrame:
        sql = f"""
            SELECT customer_id,
                   SUM(CASE WHEN dt BETWEEN :avg_d1 AND :avg_d2 THEN cost ELSE 0 END)/:avg_days as avg_cost,
                   SUM(CASE WHEN dt BETWEEN :month_d1 AND :month_d2 THEN cost ELSE 0 END) as current_month_cost,
                   SUM(CASE WHEN dt BETWEEN :month_d1 AND :month_d2 THEN {sales_expr} ELSE 0 END) as current_month_sales,
                   SUM(CASE WHEN dt BETWEEN :prev_month_d1 AND :prev_month_d2 THEN cost ELSE 0 END) as prev_month_cost
            FROM {table_name}
            WHERE dt BETWEEN :outer_d1 AND :outer_d2 {where_cid}
            GROUP BY customer_id
        """
        return sql_read(
            _engine,
            sql,
            {
                "avg_d1": str(avg_d1),
                "avg_d2": str(avg_d2),
                "month_d1": str(month_d1),
                "month_d2": str(month_d2),
                "prev_month_d1": str(prev_month_d1),
                "prev_month_d2": str(prev_month_d2),
                "outer_d1": str(outer_d1),
                "outer_d2": str(outer_d2),
                "avg_days": max(int(avg_days), 1),
                **cid_params,
            },
        )

    fact_df = pd.DataFrame()
    cache_df = pd.DataFrame()
    cache_is_fresh = False

    if table_exists(_engine, "fact_campaign_daily"):
        fact_df = _run_budget_metric_query("fact_campaign_daily", "COALESCE(sales, 0)")

    if table_exists(_engine, "overview_campaign_daily_cache"):
        latest_cache_df = sql_read(_engine, "SELECT MAX(dt) as dt FROM overview_campaign_daily_cache")
        latest_cache_dt = None if latest_cache_df.empty else latest_cache_df.iloc[0].get("dt")
        if pd.notna(latest_cache_dt):
            try:
                cache_is_fresh = pd.to_datetime(latest_cache_dt).date() >= outer_d2
            except Exception:
                cache_is_fresh = False
        cache_df = _run_budget_metric_query("overview_campaign_daily_cache", "COALESCE(tot_sales, sales, 0)")

    if fact_df.empty and cache_df.empty:
        return pd.DataFrame()

    if fact_df.empty:
        return cache_df

    if cache_df.empty or not cache_is_fresh:
        return fact_df

    merged = fact_df.merge(cache_df, on="customer_id", how="outer", suffixes=("_fact", "_cache"))
    for col in ["avg_cost", "current_month_cost", "current_month_sales", "prev_month_cost"]:
        cache_col = f"{col}_cache"
        fact_col = f"{col}_fact"
        merged[col] = merged[cache_col].where(merged[cache_col].notna(), merged[fact_col])
    return merged[["customer_id", "avg_cost", "current_month_cost", "current_month_sales", "prev_month_cost"]]

def _compute_total_ratio_metrics(row: dict) -> dict:
    imp = row.get("imp", 0) or 0
    clk = row.get("clk", 0) or 0
    cost = row.get("cost", 0) or 0
    sales = row.get("sales", 0) or 0
    cart_sales = row.get("cart_sales", 0) or 0
    wishlist_sales = row.get("wishlist_sales", 0) or 0

    row["ctr"] = (clk / imp * 100) if imp > 0 else 0
    row["cpc"] = (cost / clk) if clk > 0 else 0
    row["roas"] = (sales / cost * 100) if cost > 0 else 0
    row["cart_roas"] = (cart_sales / cost * 100) if cost > 0 else 0
    row["wishlist_roas"] = (wishlist_sales / cost * 100) if cost > 0 else 0
    return row

@st.cache_data(ttl=300, max_entries=10, show_spinner=False)
def query_budget_bundle(_engine, cids: tuple, yesterday: date, avg_d1: date, avg_d2: date, month_d1: date, month_d2: date, prev_month_d1: date, prev_month_d2: date, avg_days: int) -> pd.DataFrame:
    meta = get_meta(_engine)
    if meta.empty:
        return pd.DataFrame()

    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("CAST(customer_id AS TEXT)", cids_tuple, "budget_cid")

    df = meta.copy()
    df["customer_id"] = _normalize_customer_id_series(df["customer_id"])
    if cids_tuple:
        df = df[df["customer_id"].isin(cids_tuple)]

    metric_frames = [
        _read_budget_campaign_metrics(_engine, avg_d1, avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, avg_days, where_cid, cid_params),
    ]

    if table_exists(_engine, "fact_bizmoney_daily"):
        metric_frames.append(
            sql_read(
                _engine,
                f"""
                WITH latest AS (
                    SELECT CAST(customer_id AS TEXT) AS customer_id, MAX(dt) AS bizmoney_dt
                    FROM fact_bizmoney_daily
                    WHERE 1=1 {where_cid}
                    GROUP BY CAST(customer_id AS TEXT)
                )
                SELECT
                    CAST(f.customer_id AS TEXT) AS customer_id,
                    MAX(f.bizmoney_balance) AS bizmoney_balance,
                    MAX(f.dt) AS bizmoney_dt
                FROM fact_bizmoney_daily f
                JOIN latest l
                  ON CAST(f.customer_id AS TEXT) = l.customer_id
                 AND f.dt = l.bizmoney_dt
                GROUP BY CAST(f.customer_id AS TEXT)
                """,
                cid_params,
            )
        )

    for metric_df in metric_frames:
        df = _merge_customer_metric_frame(df, metric_df)

    df = _fill_numeric_columns(
        df,
        ["avg_cost", "current_month_cost", "current_month_sales", "prev_month_cost", "bizmoney_balance", "monthly_budget"],
    )

    if "manager" not in df.columns:
        df["manager"] = "미배정"
    if "account_name" not in df.columns:
        df["account_name"] = df["customer_id"].astype(str)
    return df

def update_monthly_budget(_engine, cid: int, val: int):
    try:
        cols = get_table_columns(_engine, "dim_customer")
        if "customer_id" not in cols:
            df = sql_read(_engine, "SELECT * FROM dim_customer")
            df = _prepare_accounts_meta_df(df)
            df.to_sql("dim_customer", _engine, if_exists="replace", index=False)
        else:
            if "monthly_budget" not in cols:
                sql_exec(_engine, "ALTER TABLE dim_customer ADD COLUMN monthly_budget BIGINT DEFAULT 0")
        cid_norm = _normalize_customer_id_value(cid)
        sql_exec(
            _engine,
            "UPDATE dim_customer SET monthly_budget = :val WHERE REGEXP_REPLACE(CAST(customer_id AS TEXT), '\\.0+$', '') = :cid",
            {"val": val, "cid": cid_norm},
        )
        get_table_columns.clear()
        get_meta.clear()
        query_budget_bundle.clear()
    except Exception as e:
        st.error(f"예산 업데이트 실패: {e}")


def update_customer_operating_weekdays(_engine, cid: int, weekdays: str):
    try:
        weekdays_norm = normalize_operating_weekdays(weekdays)
        cols = get_table_columns(_engine, "dim_customer")
        if "customer_id" not in cols:
            df = sql_read(_engine, "SELECT * FROM dim_customer")
            df = _prepare_accounts_meta_df(df)
            df.to_sql("dim_customer", _engine, if_exists="replace", index=False)
        else:
            if "operating_weekdays" not in cols:
                sql_exec(
                    _engine,
                    f"ALTER TABLE dim_customer ADD COLUMN operating_weekdays TEXT DEFAULT '{DEFAULT_OPERATING_WEEKDAYS}'",
                )
        cid_norm = _normalize_customer_id_value(cid)
        sql_exec(
            _engine,
            "UPDATE dim_customer SET operating_weekdays = :weekdays WHERE REGEXP_REPLACE(CAST(customer_id AS TEXT), '\\.0+$', '') = :cid",
            {"weekdays": weekdays_norm, "cid": cid_norm},
        )
        get_table_columns.clear()
        get_meta.clear()
        query_budget_bundle.clear()
    except Exception as e:
        st.error(f"운영 요일 업데이트 실패: {e}")


@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def query_campaign_off_log(_engine, d1: date, d2: date, cids: tuple) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_off_log"):
        return pd.DataFrame()
    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("customer_id", cids_tuple, "off_log_cid")
    # ✨ 데이터 로드 최적화 (LIMIT 5000)
    return sql_read(
        _engine,
        f"SELECT customer_id, campaign_id, off_time FROM fact_campaign_off_log WHERE dt BETWEEN :d1 AND :d2 {where_cid} LIMIT 5000",
        {"d1": str(d1), "d2": str(d2), **cid_params},
    )

@st.cache_data(ttl=43200, max_entries=20, show_spinner=False)
def get_entity_totals(_engine, entity: str, d1: date, d2: date, cids: tuple, type_sel: tuple) -> dict:
    if not table_exists(_engine, f"fact_{entity}_daily"):
        return {}

    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("f.customer_id", cids_tuple, f"{entity}_cid")
    type_join_sql, type_where_sql, type_params = _resolve_total_type_join(_engine, entity, type_sel)

    fact_cols = get_table_columns(_engine, f"fact_{entity}_daily")
    expr = _strict_conv_selects(fact_cols, alias="f")

    sql = f"""
        SELECT
            SUM(f.imp) as imp,
            SUM(f.clk) as clk,
            SUM(f.cost) as cost,
            SUM({expr['purchase_conv_expr']}) as conv,
            SUM({expr['purchase_sales_expr']}) as sales,
            SUM({expr['total_conv_expr']}) as tot_conv,
            SUM({expr['total_sales_expr']}) as tot_sales,
            SUM({expr['cart_conv_expr']}) as cart_conv,
            SUM({expr['cart_sales_expr']}) as cart_sales,
            SUM({expr['wish_conv_expr']}) as wishlist_conv,
            SUM({expr['wish_sales_expr']}) as wishlist_sales
        FROM fact_{entity}_daily f
        {type_join_sql}
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid} {type_where_sql}
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params, **type_params})
    if df.empty:
        return {}

    row = df.iloc[0].fillna(0).to_dict()
    row["tot_conv"] = row.get("tot_conv", 0)
    row["tot_sales"] = row.get("tot_sales", 0)
    return _compute_total_ratio_metrics(row)

@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def query_campaign_bundle(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple, topn_cost: int = 0) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()
    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("customer_id", cids_tuple, "campaign_bundle_cid")

    dim_cols, cp_col = _resolve_campaign_type_column(_engine)
    target_roas_select = ", c.target_roas" if "target_roas" in dim_cols else ", 0.0 as target_roas"
    min_roas_select = ", c.min_roas" if "min_roas" in dim_cols else ", 0.0 as min_roas"
    type_filter_sql, type_params = _build_campaign_type_filter(cp_col, type_sel, "campaign_bundle_type")

    camp_fact_cols = get_table_columns(_engine, "fact_campaign_daily")
    rank_agg_sql, rank_select_sql = _build_rank_metric_sql(_resolve_rank_column(_engine, "fact_campaign_daily"))
    metric_sql = _build_bundle_metric_sql(camp_fact_cols)

    sql = f"""
        WITH agg AS (
            SELECT customer_id, campaign_id,
                   SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost
                   {metric_sql['conv_agg_sql']}{rank_agg_sql}{metric_sql['cart_agg_sql']}{metric_sql['wish_agg_sql']}
            FROM fact_campaign_daily
            WHERE dt BETWEEN :d1 AND :d2 {where_cid}
            GROUP BY customer_id, campaign_id
        )
        SELECT
            agg.customer_id, agg.campaign_id,
            c.campaign_name, c.{cp_col} as campaign_type {target_roas_select} {min_roas_select},
            agg.imp, agg.clk, agg.cost, agg.conv, agg.sales, agg.tot_conv, agg.tot_sales{metric_sql['cart_select_sql']}{metric_sql['wish_select_sql']}{rank_select_sql}
        FROM agg
        JOIN dim_campaign c ON agg.campaign_id = c.campaign_id AND agg.customer_id = c.customer_id
        WHERE 1=1 {type_filter_sql}
    """
    sql += _bundle_limit_clause(topn_cost)

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params, **type_params})
    return _finalize_bundle_df(df, "campaign_type")

@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def query_keyword_bundle(_engine, d1: date, d2: date, cids, type_sel: tuple, topn_cost: int = 0, include_dt: bool = False) -> pd.DataFrame:
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame()
    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("customer_id", cids_tuple, "keyword_bundle_cid")

    _, cp_col = _resolve_campaign_type_column(_engine)
    type_filter_sql, type_params = _build_campaign_type_filter(cp_col, type_sel, "keyword_bundle_type")

    kw_fact_cols = get_table_columns(_engine, "fact_keyword_daily")
    rank_agg_sql, rank_select_sql = _build_rank_metric_sql(_resolve_rank_column(_engine, "fact_keyword_daily"))
    metric_sql = _build_bundle_metric_sql(kw_fact_cols)
    dt_group, dt_select = _build_dt_sql(include_dt)

    sql = f"""
        WITH agg AS (
            SELECT customer_id, keyword_id{dt_group},
                   SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost
                   {metric_sql['conv_agg_sql']}{rank_agg_sql}{metric_sql['cart_agg_sql']}{metric_sql['wish_agg_sql']}
            FROM fact_keyword_daily
            WHERE dt BETWEEN :d1 AND :d2 {where_cid}
            GROUP BY customer_id, keyword_id{dt_group}
        )
        SELECT
            agg.customer_id, a.campaign_id, k.adgroup_id, agg.keyword_id,
            c.campaign_name, c.{cp_col} as campaign_type_label,
            a.adgroup_name, k.keyword{dt_select},
            agg.imp, agg.clk, agg.cost, agg.conv, agg.sales, agg.tot_conv, agg.tot_sales{metric_sql['cart_select_sql']}{metric_sql['wish_select_sql']}{rank_select_sql}
        FROM agg
        JOIN dim_keyword k ON agg.keyword_id = k.keyword_id AND agg.customer_id = k.customer_id
        JOIN dim_adgroup a ON k.adgroup_id = a.adgroup_id AND agg.customer_id = a.customer_id
        JOIN dim_campaign c ON a.campaign_id = c.campaign_id AND agg.customer_id = c.customer_id
        WHERE 1=1 {type_filter_sql}
    """
    sql += _bundle_limit_clause(topn_cost)

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params, **type_params})
    return _finalize_bundle_df(df, "campaign_type_label")

@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def query_ad_bundle(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple, topn_cost: int = 0, top_k: int = 50, include_dt: bool = False) -> pd.DataFrame:
    if not table_exists(_engine, "fact_ad_daily"):
        return pd.DataFrame()
    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("customer_id", cids_tuple, "ad_bundle_cid")

    _, cp_col = _resolve_campaign_type_column(_engine)
    url_select, title_select, image_select = _resolve_ad_dimension_selects(_engine)
    type_filter_sql, type_params = _build_campaign_type_filter(cp_col, type_sel, "ad_bundle_type")

    ad_fact_cols = get_table_columns(_engine, "fact_ad_daily")
    rank_agg_sql, rank_select_sql = _build_rank_metric_sql(_resolve_rank_column(_engine, "fact_ad_daily"))
    metric_sql = _build_bundle_metric_sql(ad_fact_cols)
    dt_group, dt_select = _build_dt_sql(include_dt)

    sql = f"""
        WITH agg AS (
            SELECT customer_id, ad_id{dt_group},
                   SUM(imp) as imp, SUM(clk) as clk, SUM(cost) as cost
                   {metric_sql['conv_agg_sql']}{rank_agg_sql}{metric_sql['cart_agg_sql']}{metric_sql['wish_agg_sql']}
            FROM fact_ad_daily
            WHERE dt BETWEEN :d1 AND :d2 {where_cid}
            GROUP BY customer_id, ad_id{dt_group}
        )
        SELECT
            agg.customer_id, a.campaign_id, ad.adgroup_id, agg.ad_id,
            c.campaign_name, c.{cp_col} as campaign_type_label,
            a.adgroup_name, ad.ad_name, {title_select}, {image_select}, {url_select}{dt_select},
            agg.imp, agg.clk, agg.cost, agg.conv, agg.sales, agg.tot_conv, agg.tot_sales{metric_sql['cart_select_sql']}{metric_sql['wish_select_sql']}{rank_select_sql}
        FROM agg
        JOIN dim_ad ad ON agg.ad_id = ad.ad_id AND agg.customer_id = ad.customer_id
        JOIN dim_adgroup a ON ad.adgroup_id = a.adgroup_id AND agg.customer_id = a.customer_id
        JOIN dim_campaign c ON a.campaign_id = c.campaign_id AND agg.customer_id = c.customer_id
        WHERE 1=1 {type_filter_sql}
    """
    sql += _bundle_limit_clause(topn_cost)

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params, **type_params})
    return _finalize_bundle_df(df, "campaign_type_label")

@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def query_campaign_timeseries(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()
    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("f.customer_id", cids_tuple, "campaign_timeseries_cid")

    type_join_sql = ""
    type_where_sql = ""
    type_params = {}
    if type_sel and table_exists(_engine, "dim_campaign"):
        cols = get_table_columns(_engine, "dim_campaign")
        cp_col = "campaign_tp" if "campaign_tp" in cols else ("campaign_type_label" if "campaign_type_label" in cols else "campaign_type")
        type_join_sql = "JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id"
        type_where_sql, type_params = _build_campaign_type_filter(cp_col, type_sel, "campaign_timeseries_type")

    fact_cols = get_table_columns(_engine, "fact_campaign_daily")
    expr = _strict_conv_selects(fact_cols, alias="f")
    conv_select_sql = f", SUM({expr['purchase_conv_expr']}) as conv, SUM({expr['purchase_sales_expr']}) as sales, SUM({expr['total_conv_expr']}) as tot_conv, SUM({expr['total_sales_expr']}) as tot_sales"
    cart_select_sql = f", SUM({expr['cart_conv_expr']}) as cart_conv, SUM({expr['cart_sales_expr']}) as cart_sales"
    wish_select_sql = f", SUM({expr['wish_conv_expr']}) as wishlist_conv, SUM({expr['wish_sales_expr']}) as wishlist_sales"
    rank_col = _resolve_rank_column(_engine, "fact_campaign_daily")
    rank_select_sql = f", CASE WHEN SUM(f.imp) > 0 THEN SUM(COALESCE(f.{rank_col}, 0) * f.imp) / SUM(f.imp) ELSE NULL END as avg_rank" if rank_col else ""

    sql = f"""
        SELECT f.dt, SUM(f.imp) as imp, SUM(f.clk) as clk, SUM(f.cost) as cost{conv_select_sql}{cart_select_sql}{wish_select_sql}{rank_select_sql}
        FROM fact_campaign_daily f
        {type_join_sql}
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid} {type_where_sql}
        GROUP BY f.dt ORDER BY f.dt
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params, **type_params})
    if not df.empty:
        df["dt"] = pd.to_datetime(df["dt"])
    return df



def ensure_overview_report_source_cache(_engine) -> None:
    sql_exec(
        _engine,
        """
        CREATE TABLE IF NOT EXISTS overview_report_source_cache (
            dt DATE,
            customer_id TEXT,
            campaign_type TEXT,
            source_kind TEXT,
            source_text TEXT,
            metric_value DOUBLE PRECISION DEFAULT 0,
            sales_value BIGINT DEFAULT 0,
            rank_no INTEGER DEFAULT 0,
            PRIMARY KEY(dt, customer_id, campaign_type, source_kind, source_text)
        )
        """,
    )


@st.cache_data(ttl=3600, max_entries=200, show_spinner=False)
def query_overview_report_source_cache(_engine, source_kind: str, d1: date, d2: date, cids: tuple, type_sel: tuple, limit_n: int = 5) -> pd.DataFrame:
    safe_limit = _safe_limit(limit_n, 5, 50)
    try:
        ensure_overview_report_source_cache(_engine)
    except Exception:
        return pd.DataFrame()

    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("c.customer_id", cids_tuple, f"report_cache_cid_{source_kind}")
    normalized_types = _normalize_filter_values(type_sel)
    type_where_sql = ""
    type_params = {}
    if normalized_types:
        rev_map = {
            "파워링크": "WEB_SITE",
            "쇼핑검색": "SHOPPING",
            "파워컨텐츠": "POWER_CONTENTS",
            "브랜드검색": "BRAND_SEARCH",
            "플레이스": "PLACE",
        }
        db_types = tuple(rev_map.get(t, t) for t in normalized_types)
        type_where_sql, type_params = _build_in_filter("c.campaign_type", db_types, f"report_cache_type_{source_kind}")

    sql = f"""
        SELECT c.source_text, SUM(c.metric_value) as metric_value, SUM(c.sales_value) as sales_value
        FROM overview_report_source_cache c
        WHERE c.dt BETWEEN :d1 AND :d2
          AND c.source_kind = :source_kind
          {where_cid}
          {type_where_sql}
        GROUP BY c.source_text
        HAVING SUM(c.metric_value) > 0 OR SUM(c.sales_value) > 0
        ORDER BY SUM(c.metric_value) DESC, SUM(c.sales_value) DESC, c.source_text
        LIMIT {safe_limit}
    """
    return sql_read(
        _engine,
        sql,
        {"d1": str(d1), "d2": str(d2), "source_kind": str(source_kind), **cid_params, **type_params},
    )

@st.cache_data(ttl=43200, max_entries=10, show_spinner=False)
def query_shopping_search_terms(_engine, d1: date, d2: date, cids: tuple) -> pd.DataFrame:
    if not table_exists(_engine, "fact_shopping_query_daily"):
        return pd.DataFrame()
    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("f.customer_id", cids_tuple, "shopping_terms_cid")

    sql = f"""
        SELECT
            f.customer_id, c.campaign_name, a.adgroup_name, f.query_text,
            SUM(f.total_conv) as total_conv,
            SUM(f.total_sales) as total_sales,
            SUM(f.purchase_conv) as purchase_conv,
            SUM(f.purchase_sales) as purchase_sales,
            SUM(f.cart_conv) as cart_conv,
            SUM(f.cart_sales) as cart_sales,
            SUM(f.wishlist_conv) as wishlist_conv,
            SUM(f.wishlist_sales) as wishlist_sales
        FROM fact_shopping_query_daily f
        LEFT JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id
        LEFT JOIN dim_adgroup a ON f.adgroup_id = a.adgroup_id AND f.customer_id = a.customer_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid}
        GROUP BY f.customer_id, c.campaign_name, a.adgroup_name, f.query_text
        -- ✨ 성과가 있는 검색어 우선 정렬 및 로드 수 제한 최적화
        HAVING SUM(f.total_sales) > 0 OR SUM(f.total_conv) > 0
        ORDER BY SUM(f.purchase_sales) DESC, SUM(f.total_sales) DESC
        LIMIT 5000
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params})
    return df
