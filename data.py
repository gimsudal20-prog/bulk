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

from perf_utils import record_db_timing

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


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


DASHBOARD_DATA_CACHE_TTL = _env_int("DASHBOARD_DATA_CACHE_TTL", 300, min_value=30)


def _ensure_dashboard_indexes(engine) -> None:
    if not _env_bool("DASHBOARD_ENSURE_INDEXES", True):
        return

    index_sql = [
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_fact_campaign_customer_dt ON fact_campaign_daily (customer_id, dt)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_fact_campaign_campaign ON fact_campaign_daily (customer_id, campaign_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_fact_keyword_customer_dt ON fact_keyword_daily (customer_id, dt)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_fact_keyword_keyword ON fact_keyword_daily (customer_id, keyword_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_fact_ad_customer_dt ON fact_ad_daily (customer_id, dt)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_fact_ad_ad ON fact_ad_daily (customer_id, ad_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_fact_shop_customer_dt ON fact_shopping_query_daily (customer_id, dt)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_fact_shop_campaign ON fact_shopping_query_daily (customer_id, campaign_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_fact_campaign_hourly_customer_dt ON fact_campaign_hourly_daily (customer_id, dt)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_fact_campaign_age_customer_dt ON fact_campaign_age_daily (customer_id, dt)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_dim_campaign_customer_campaign ON dim_campaign (customer_id, campaign_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_dim_adgroup_customer_adgroup ON dim_adgroup (customer_id, adgroup_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_dim_keyword_customer_keyword ON dim_keyword (customer_id, keyword_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dash_dim_ad_customer_ad ON dim_ad (customer_id, ad_id)",
    ]
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text("SET lock_timeout TO '2s'"))
            conn.execute(text("SET statement_timeout TO '120s'"))
            for stmt in index_sql:
                try:
                    conn.execute(text(stmt))
                except Exception:
                    pass
    except Exception:
        pass

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

    engine = create_engine(
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
    _ensure_dashboard_indexes(engine)
    return engine

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

@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=20, show_spinner=False)
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

@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=30, show_spinner=False)
def sql_read(_engine, query: str, params: dict = None) -> pd.DataFrame:
    last_error = None
    for attempt in range(3):
        t0 = time.perf_counter()
        try:
            with _engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            record_db_timing("db", _query_label(query), elapsed_ms, rows=len(df.index), attempt=attempt + 1)
            return df
        except Exception as e:
            last_error = e
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            record_db_timing("db_error", _query_label(query), elapsed_ms, attempt=attempt + 1, error=type(e).__name__)
            time.sleep(1.0)
            
    st.cache_resource.clear()
    st.error(f"DB 연결이 지연되고 있습니다. 잠시 후 새로고침(F5) 해주세요. (사유: {last_error})")
    st.stop()


def _query_label(query: str) -> str:
    compact = " ".join(str(query or "").split())
    if not compact:
        return "empty query"
    upper = compact.upper()
    for marker in ["FROM ", "UPDATE ", "INSERT INTO ", "DELETE FROM "]:
        idx = upper.find(marker)
        if idx >= 0:
            return compact[idx:idx + 120]
    return compact[:120]

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

_CAMPAIGN_TYPE_ALIASES = {
    "파워링크": ["WEB_SITE", "파워링크"],
    "WEB_SITE": ["WEB_SITE", "파워링크"],
    "쇼핑검색": ["SHOPPING", "쇼핑검색"],
    "SHOPPING": ["SHOPPING", "쇼핑검색"],
    "파워컨텐츠": ["POWER_CONTENT", "POWER_CONTENTS", "파워컨텐츠"],
    "POWER_CONTENT": ["POWER_CONTENT", "POWER_CONTENTS", "파워컨텐츠"],
    "POWER_CONTENTS": ["POWER_CONTENT", "POWER_CONTENTS", "파워컨텐츠"],
    "브랜드검색": ["BRAND_SEARCH", "브랜드검색"],
    "BRAND_SEARCH": ["BRAND_SEARCH", "브랜드검색"],
    "플레이스": ["PLACE", "플레이스"],
    "PLACE": ["PLACE", "플레이스"],
    "네이버": ["WEB_SITE", "SHOPPING", "POWER_CONTENT", "POWER_CONTENTS", "BRAND_SEARCH", "PLACE", "파워링크", "쇼핑검색", "파워컨텐츠", "브랜드검색", "플레이스"],
    "NAVER": ["WEB_SITE", "SHOPPING", "POWER_CONTENT", "POWER_CONTENTS", "BRAND_SEARCH", "PLACE", "파워링크", "쇼핑검색", "파워컨텐츠", "브랜드검색", "플레이스"],
    "메타": ["META", "메타", "Meta", "FACEBOOK", "FACEBOOK_ADS", "facebook", "facebook_ads", "INSTAGRAM", "instagram"],
    "META": ["META", "메타", "Meta", "FACEBOOK", "FACEBOOK_ADS", "facebook", "facebook_ads", "INSTAGRAM", "instagram"],
    "FACEBOOK": ["META", "메타", "FACEBOOK", "FACEBOOK_ADS", "facebook", "facebook_ads"],
    "FACEBOOK_ADS": ["META", "메타", "FACEBOOK", "FACEBOOK_ADS", "facebook", "facebook_ads"],
    "구글": ["GOOGLE", "GOOGLE_ADS", "구글", "Google Ads", "PERFORMANCE_MAX", "PMAX", "P_MAX"],
    "GOOGLE": ["GOOGLE", "GOOGLE_ADS", "구글", "Google Ads", "PERFORMANCE_MAX", "PMAX", "P_MAX"],
    "GOOGLE_ADS": ["GOOGLE", "GOOGLE_ADS", "구글", "Google Ads", "PERFORMANCE_MAX", "PMAX", "P_MAX"],
    "PERFORMANCE_MAX": ["GOOGLE", "GOOGLE_ADS", "구글", "Google Ads", "PERFORMANCE_MAX", "PMAX", "P_MAX"],
    "PMAX": ["GOOGLE", "GOOGLE_ADS", "구글", "Google Ads", "PERFORMANCE_MAX", "PMAX", "P_MAX"],
}

_NAVER_CAMPAIGN_TYPE_FILTER_VALUES = _CAMPAIGN_TYPE_ALIASES["네이버"]
_BUDGET_PLATFORM_LABELS = ("네이버", "메타")
_BUDGET_NAVER_CAMPAIGN_TYPE_OPTIONS = ("파워링크", "쇼핑검색")
_BUDGET_CAMPAIGN_TYPE_OPTIONS = ("파워링크", "쇼핑검색", "메타")
_BUDGET_CAMPAIGN_TYPE_FILTER_VALUES = tuple(
    value
    for label in _BUDGET_CAMPAIGN_TYPE_OPTIONS
    for value in _CAMPAIGN_TYPE_ALIASES.get(label, [label])
)


def _expand_campaign_type_filter_values(type_sel: tuple | list) -> list[str]:
    normalized_types = _normalize_filter_values(type_sel)
    out: list[str] = []
    seen: set[str] = set()
    for raw in normalized_types:
        key = str(raw or "").strip()
        if not key:
            continue
        aliases = _CAMPAIGN_TYPE_ALIASES.get(key) or _CAMPAIGN_TYPE_ALIASES.get(key.upper()) or [key]
        for value in aliases:
            value = str(value or "").strip()
            if value and value not in seen:
                seen.add(value)
                out.append(value)
    return out


def _build_campaign_type_filter(column_name: str, type_sel: tuple, param_prefix: str = "campaign_type") -> tuple[str, dict]:
    db_types = _expand_campaign_type_filter_values(type_sel)
    if not db_types:
        return "", {}

    safe_column = _validate_sql_identifier(column_name, "column name")
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
    """Normalize customer_id/account_id read from Excel/DB.

    accounts.xlsx often stores custom IDs as numeric cells, which pandas can read as
    1234567.0 when the sheet has blank rows. Meta account IDs may also be saved as
    act_123... and Google IDs may contain dashes, so clean those shapes up before
    joining against fact-table customer_id values.
    """
    if pd.isna(value):
        return ""
    value_str = str(value).strip()
    if not value_str or value_str.lower() in {"nan", "none", "nat"}:
        return ""

    value_str = re.sub(r"^act_", "", value_str, flags=re.IGNORECASE).strip()

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

    compact = value_str.replace("-", "").replace(" ", "")
    if compact.isdigit():
        return compact

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
        elif c_clean in ["메타광고계정id", "metaadaccountid", "meta_ad_account_id", "adaccountid", "ad_account_id"]:
            rename_map[c] = "meta_ad_account_id"
        elif c_clean in ["platform", "플랫폼"]:
            rename_map[c] = "platform"
        elif c_clean in ["업체명", "accountname", "account_name", "name", "계정명"]:
            rename_map[c] = "account_name"
        elif c_clean in ["담당자", "manager"]:
            rename_map[c] = "manager"
        elif _is_operating_weekdays_column(c_clean):
            rename_map[c] = "operating_weekdays"

    df = df.rename(columns=rename_map)

    if "customer_id" not in df.columns and "meta_ad_account_id" in df.columns:
        df["customer_id"] = df["meta_ad_account_id"]

    if "customer_id" not in df.columns or "account_name" not in df.columns:
        raise ValueError("accounts.xlsx에는 '업체명'과 '커스텀 ID' 또는 '메타 광고 계정 ID' 컬럼이 필요합니다.")

    df["customer_id"] = _normalize_customer_id_series(df["customer_id"])
    if "meta_ad_account_id" in df.columns:
        meta_ids = _normalize_customer_id_series(df["meta_ad_account_id"]).str.replace(r"^act_", "", regex=True)
        df["customer_id"] = df["customer_id"].where(df["customer_id"].ne(""), meta_ids)
    df["customer_id"] = df["customer_id"].str.replace(r"^act_", "", regex=True)
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


@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=30, show_spinner=False)
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

@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=30, show_spinner=False)
def load_dim_campaign(_engine) -> pd.DataFrame:
    if not table_exists(_engine, "dim_campaign"):
        return pd.DataFrame()
        
    # ✨ SELECT 최적화 (꼭 필요한 컬럼만 추출)
    cols = get_table_columns(_engine, "dim_campaign")
    target_cols = []
    for c in cols:
        c_clean = str(c).replace(" ", "").lower()
        if c_clean in [
            "customer_id", "campaign_id", "campaign_name", "campaign_tp", "campaign_type",
            "campaign_type_label", "status", "target_roas", "min_roas",
            "daily_budget", "lifetime_budget", "budget_remaining", "spend_cap",
        ]:
            target_cols.append(f'"{c}"')
            
    select_str = ", ".join(target_cols) if target_cols else "*"
    df = sql_read(_engine, f"SELECT {select_str} FROM dim_campaign")
    if not df.empty and "customer_id" in df.columns:
        df["customer_id"] = _normalize_customer_id_series(df["customer_id"])
    return df

@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=30, show_spinner=False)
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

    mapping = {
        "WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENT": "파워컨텐츠", "POWER_CONTENTS": "파워컨텐츠",
        "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스",
        "META": "메타", "FACEBOOK": "메타", "FACEBOOK_ADS": "메타", "INSTAGRAM": "메타", "메타": "메타",
        "GOOGLE": "구글", "GOOGLE_ADS": "구글", "PERFORMANCE_MAX": "구글", "PMAX": "구글", "P_MAX": "구글", "구글": "구글",
    }
    raw_opts = [str(x) for x in dim_campaign[col_name].dropna().unique() if str(x).strip()]
    opts = list(set([mapping.get(x.upper(), mapping.get(x, x)) for x in raw_opts]))
    return sorted(opts) if opts else ["파워링크", "쇼핑검색"]

def _map_campaign_types(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    if not df.empty and col_name in df.columns:
        mapping = {
            "WEB_SITE": "파워링크", "SHOPPING": "쇼핑검색", "POWER_CONTENT": "파워컨텐츠", "POWER_CONTENTS": "파워컨텐츠",
            "BRAND_SEARCH": "브랜드검색", "PLACE": "플레이스",
            "META": "메타", "FACEBOOK": "메타", "FACEBOOK_ADS": "메타", "INSTAGRAM": "메타", "메타": "메타",
            "GOOGLE": "구글", "GOOGLE_ADS": "구글", "PERFORMANCE_MAX": "구글", "PMAX": "구글", "P_MAX": "구글", "구글": "구글",
        }
        df[col_name] = df[col_name].apply(lambda x: mapping.get(str(x).upper(), mapping.get(str(x), x)) if pd.notna(x) else x)
    return df

@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=30, show_spinner=False)
def get_latest_dates(_engine) -> dict:
    tables = [
        "fact_campaign_daily",
        "fact_adgroup_daily",
        "fact_keyword_daily",
        "fact_ad_daily",
        "fact_shopping_query_daily",
    ]
    existing = [tbl for tbl in tables if table_exists(_engine, tbl)]
    if not existing:
        return {}

    union_sql = "\nUNION ALL\n".join(
        f"SELECT '{tbl}' AS table_name, MAX(dt) AS dt FROM {tbl}"
        for tbl in existing
    )
    df = sql_read(_engine, union_sql)
    if df.empty:
        return {}
    dates = {}
    for _, row in df.iterrows():
        if pd.notna(row.get("dt")):
            dates[str(row.get("table_name"))] = row.get("dt")
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
        manager VARCHAR(120) NULL,
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
    sql_exec(_engine, "ALTER TABLE platform_credentials ADD COLUMN IF NOT EXISTS manager VARCHAR(120)")
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


# ==========================================
# Dashboard workflow tables
# ==========================================
def _json_dumps_payload(value) -> str:
    try:
        return json.dumps(value or {}, ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def ensure_dashboard_workflow_tables(_engine) -> None:
    sql_exec(
        _engine,
        """
        CREATE TABLE IF NOT EXISTS dashboard_action_items (
            id BIGSERIAL PRIMARY KEY,
            item_key TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL DEFAULT 'general',
            severity TEXT NOT NULL DEFAULT 'info',
            title TEXT NOT NULL,
            body TEXT DEFAULT '',
            manager TEXT DEFAULT '',
            account_name TEXT DEFAULT '',
            customer_id TEXT DEFAULT '',
            source_page TEXT DEFAULT '',
            source_ref TEXT DEFAULT '',
            status TEXT NOT NULL DEFAULT 'open',
            owner TEXT DEFAULT '',
            note TEXT DEFAULT '',
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            resolved_at TIMESTAMPTZ
        )
        """,
    )
    sql_exec(
        _engine,
        "CREATE INDEX IF NOT EXISTS idx_dashboard_action_status ON dashboard_action_items(status, severity, last_seen_at DESC)",
    )
    sql_exec(
        _engine,
        """
        CREATE TABLE IF NOT EXISTS dashboard_audit_log (
            id BIGSERIAL PRIMARY KEY,
            event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            actor TEXT NOT NULL DEFAULT 'dashboard',
            action_type TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id TEXT DEFAULT '',
            summary TEXT DEFAULT '',
            before_json JSONB DEFAULT '{}'::JSONB,
            after_json JSONB DEFAULT '{}'::JSONB
        )
        """,
    )
    sql_exec(
        _engine,
        "CREATE INDEX IF NOT EXISTS idx_dashboard_audit_time ON dashboard_audit_log(event_time DESC)",
    )
    sql_exec(
        _engine,
        """
        CREATE TABLE IF NOT EXISTS dashboard_filter_presets (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            payload JSONB NOT NULL DEFAULT '{}'::JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
    )


def _clear_dashboard_workflow_caches() -> None:
    for func_name in [
        "get_filter_presets",
        "query_action_items",
        "query_dashboard_audit_log",
        "query_collection_status",
    ]:
        func = globals().get(func_name)
        try:
            if func is not None:
                func.clear()
        except Exception:
            pass


def log_dashboard_audit(
    _engine,
    action_type: str,
    target_type: str,
    target_id: str = "",
    summary: str = "",
    before: dict | None = None,
    after: dict | None = None,
    actor: str = "dashboard",
) -> None:
    try:
        ensure_dashboard_workflow_tables(_engine)
        sql_exec(
            _engine,
            """
            INSERT INTO dashboard_audit_log
                (actor, action_type, target_type, target_id, summary, before_json, after_json)
            VALUES
                (:actor, :action_type, :target_type, :target_id, :summary, CAST(:before_json AS JSONB), CAST(:after_json AS JSONB))
            """,
            {
                "actor": str(actor or "dashboard"),
                "action_type": str(action_type or "update"),
                "target_type": str(target_type or "unknown"),
                "target_id": str(target_id or ""),
                "summary": str(summary or ""),
                "before_json": _json_dumps_payload(before),
                "after_json": _json_dumps_payload(after),
            },
        )
        _clear_dashboard_workflow_caches()
    except Exception:
        pass


@st.cache_data(ttl=60, max_entries=10, show_spinner=False)
def query_dashboard_audit_log(_engine, limit: int = 200) -> pd.DataFrame:
    ensure_dashboard_workflow_tables(_engine)
    safe_limit = max(10, min(int(limit or 200), 1000))
    return sql_read(
        _engine,
        """
        SELECT id, event_time, actor, action_type, target_type, target_id, summary, before_json, after_json
        FROM dashboard_audit_log
        ORDER BY event_time DESC
        LIMIT :limit
        """,
        {"limit": safe_limit},
    )


@st.cache_data(ttl=60, max_entries=10, show_spinner=False)
def get_filter_presets(_engine) -> pd.DataFrame:
    ensure_dashboard_workflow_tables(_engine)
    df = sql_read(
        _engine,
        """
        SELECT id, name, payload, created_at, updated_at
        FROM dashboard_filter_presets
        ORDER BY updated_at DESC, name ASC
        """,
    )
    if df.empty or "payload" not in df.columns:
        return df
    df["payload"] = df["payload"].apply(
        lambda x: x if isinstance(x, dict) else (json.loads(x) if isinstance(x, str) and str(x).strip().startswith("{") else {})
    )
    return df


def save_filter_preset(_engine, name: str, payload: dict) -> None:
    preset_name = str(name or "").strip()
    if not preset_name:
        raise ValueError("프리셋 이름을 입력해주세요.")
    ensure_dashboard_workflow_tables(_engine)
    payload_json = _json_dumps_payload(payload)
    sql_exec(
        _engine,
        """
        INSERT INTO dashboard_filter_presets (name, payload, updated_at)
        VALUES (:name, CAST(:payload AS JSONB), NOW())
        ON CONFLICT (name) DO UPDATE
           SET payload = EXCLUDED.payload,
               updated_at = NOW()
        """,
        {"name": preset_name, "payload": payload_json},
    )
    log_dashboard_audit(
        _engine,
        "save_filter_preset",
        "filter_preset",
        preset_name,
        f"필터 프리셋 저장: {preset_name}",
        after={"name": preset_name, "payload": payload},
    )
    _clear_dashboard_workflow_caches()


def delete_filter_preset(_engine, preset_id: int) -> None:
    ensure_dashboard_workflow_tables(_engine)
    sql_exec(_engine, "DELETE FROM dashboard_filter_presets WHERE id = :id", {"id": int(preset_id)})
    log_dashboard_audit(_engine, "delete_filter_preset", "filter_preset", str(preset_id), "필터 프리셋 삭제")
    _clear_dashboard_workflow_caches()


def upsert_action_items(_engine, items: list[dict]) -> int:
    if not items:
        return 0
    ensure_dashboard_workflow_tables(_engine)
    written = 0
    for item in items:
        item_key = str(item.get("item_key", "") or "").strip()
        title = str(item.get("title", "") or "").strip()
        if not item_key or not title:
            continue
        params = {
            "item_key": item_key,
            "category": str(item.get("category", "general") or "general"),
            "severity": str(item.get("severity", "info") or "info"),
            "title": title,
            "body": str(item.get("body", "") or ""),
            "manager": str(item.get("manager", "") or ""),
            "account_name": str(item.get("account_name", "") or ""),
            "customer_id": str(item.get("customer_id", "") or ""),
            "source_page": str(item.get("source_page", "") or ""),
            "source_ref": str(item.get("source_ref", "") or ""),
        }
        sql_exec(
            _engine,
            """
            INSERT INTO dashboard_action_items
                (item_key, category, severity, title, body, manager, account_name, customer_id, source_page, source_ref, last_seen_at)
            VALUES
                (:item_key, :category, :severity, :title, :body, :manager, :account_name, :customer_id, :source_page, :source_ref, NOW())
            ON CONFLICT (item_key) DO UPDATE
               SET category = EXCLUDED.category,
                   severity = EXCLUDED.severity,
                   title = EXCLUDED.title,
                   body = EXCLUDED.body,
                   manager = EXCLUDED.manager,
                   account_name = EXCLUDED.account_name,
                   customer_id = EXCLUDED.customer_id,
                   source_page = EXCLUDED.source_page,
                   source_ref = EXCLUDED.source_ref,
                   last_seen_at = NOW()
            """,
            params,
        )
        written += 1
    if written:
        _clear_dashboard_workflow_caches()
    return written


@st.cache_data(ttl=60, max_entries=40, show_spinner=False)
def query_action_items(_engine, status: str = "open", limit: int = 500, customer_ids=None) -> pd.DataFrame:
    ensure_dashboard_workflow_tables(_engine)
    safe_limit = max(20, min(int(limit or 500), 2000))
    safe_status = str(status or "").strip()
    cid_params = {}
    cid_filter_sql = ""
    if customer_ids is not None:
        cids_tuple = _normalize_filter_values(customer_ids)
        if cids_tuple:
            cid_filter_sql, cid_params = _build_in_filter(
                "REGEXP_REPLACE(CAST(customer_id AS TEXT), '\\.0+$', '')",
                cids_tuple,
                "action_item_cid",
            )
        else:
            cid_filter_sql = "AND 1=0"
    return sql_read(
        _engine,
        f"""
        SELECT
            id, item_key, category, severity, title, body, manager, account_name, customer_id,
            source_page, source_ref, status, owner, note, first_seen_at, last_seen_at, resolved_at
        FROM dashboard_action_items
        WHERE (:status = '' OR status = :status)
          {cid_filter_sql}
        ORDER BY
            CASE severity
                WHEN 'critical' THEN 0
                WHEN 'danger' THEN 1
                WHEN 'warning' THEN 2
                ELSE 3
            END,
            last_seen_at DESC
        LIMIT :limit
        """,
        {"status": safe_status, "limit": safe_limit, **cid_params},
    )


def update_action_item(_engine, item_id: int, status: str, owner: str = "", note: str = "") -> None:
    ensure_dashboard_workflow_tables(_engine)
    valid_statuses = {"open", "in_progress", "resolved", "skipped"}
    status_norm = str(status or "open").strip()
    if status_norm not in valid_statuses:
        status_norm = "open"
    sql_exec(
        _engine,
        """
        UPDATE dashboard_action_items
           SET status = :status,
               owner = :owner,
               note = :note,
               resolved_at = CASE
                   WHEN :status IN ('resolved', 'skipped') THEN COALESCE(resolved_at, NOW())
                   ELSE NULL
               END
         WHERE id = :id
        """,
        {"id": int(item_id), "status": status_norm, "owner": str(owner or ""), "note": str(note or "")},
    )
    log_dashboard_audit(
        _engine,
        "update_action_item",
        "action_item",
        str(item_id),
        f"조치 항목 상태 변경: {status_norm}",
        after={"status": status_norm, "owner": owner, "note": note},
    )
    _clear_dashboard_workflow_caches()


@st.cache_data(ttl=120, max_entries=20, show_spinner=False)
def query_collection_status(_engine, cids: tuple = tuple()) -> pd.DataFrame:
    cids_tuple = _normalize_filter_values(cids)
    sources = [
        ("fact_campaign_daily", "캠페인 일별"),
        ("fact_keyword_daily", "키워드 일별"),
        ("fact_ad_daily", "소재 일별"),
        ("fact_shopping_query_daily", "쇼핑 검색어"),
        ("fact_adgroup_placement_daily", "검색/콘텐츠 지면"),
        ("fact_campaign_hourly_daily", "시간대"),
        ("fact_campaign_age_daily", "연령대"),
        ("fact_campaign_device_daily", "디바이스"),
        ("fact_bizmoney_daily", "비즈머니"),
        ("fact_campaign_off_log", "OFF 로그"),
    ]
    frames = []
    for table_name, source_label in sources:
        if not table_exists(_engine, table_name):
            continue
        cols = get_table_columns(_engine, table_name)
        if "customer_id" not in cols or "dt" not in cols:
            continue
        where_cid, cid_params = _build_in_filter("CAST(customer_id AS TEXT)", cids_tuple, f"collection_{table_name}")
        df = sql_read(
            _engine,
            f"""
            SELECT
                CAST(customer_id AS TEXT) AS customer_id,
                MAX(dt)::date AS latest_dt,
                COUNT(*)::BIGINT AS row_count
            FROM {table_name}
            WHERE 1=1 {where_cid}
            GROUP BY CAST(customer_id AS TEXT)
            """,
            cid_params,
        )
        if df.empty:
            continue
        df["source_table"] = table_name
        df["source_label"] = source_label
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["customer_id", "source_table", "source_label", "latest_dt", "row_count", "stale_days"])
    out = pd.concat(frames, ignore_index=True)
    latest_dt = pd.to_datetime(out["latest_dt"], errors="coerce")
    today = pd.Timestamp(date.today())
    out["stale_days"] = (today - latest_dt).dt.days
    out["stale_days"] = out["stale_days"].fillna(9999).astype(int)
    return out


def upsert_platform_credential(_engine, row: dict) -> None:
    ensure_platform_credentials_table(_engine)

    payload = {
        "id": row.get("id"),
        "platform": str(row.get("platform", "")).strip().lower(),
        "account_label": str(row.get("account_label", "")).strip(),
        "manager": str(row.get("manager", "")).strip() or "미배정",
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
                   manager=:manager,
                   customer_id=:customer_id,
                   account_id=:account_id,
                   access_token=CASE
                       WHEN :access_token <> '' THEN :access_token
                       ELSE access_token
                   END,
                   refresh_token=CASE
                       WHEN :refresh_token <> '' THEN :refresh_token
                       ELSE refresh_token
                   END,
                   app_id=:app_id,
                   app_secret=CASE
                       WHEN :app_secret <> '' THEN :app_secret
                       ELSE app_secret
                   END,
                   extra_json=CASE
                       WHEN :extra_json <> '{}' THEN CAST(:extra_json AS JSONB)
                       ELSE extra_json
                   END,
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
                (platform, account_label, manager, customer_id, account_id, access_token, refresh_token, app_id, app_secret, extra_json, is_active, updated_at)
            VALUES
                (:platform, :account_label, :manager, :customer_id, :account_id, :access_token, :refresh_token, :app_id, :app_secret, CAST(:extra_json AS JSONB), :is_active, NOW())
            """,
            payload,
        )
    safe_payload = {
        k: v
        for k, v in payload.items()
        if k not in {"access_token", "refresh_token", "app_secret"}
    }
    log_dashboard_audit(
        _engine,
        "upsert_platform_credential",
        "platform_credential",
        str(payload.get("id") or payload.get("account_label") or ""),
        f"플랫폼 연결 저장: {payload.get('account_label')}",
        after=safe_payload,
    )
    clear_platform_credentials_cache()

def delete_platform_credential(_engine, row_id: int) -> None:
    ensure_platform_credentials_table(_engine)
    sql_exec(_engine, "DELETE FROM platform_credentials WHERE id = :id", {"id": int(row_id)})
    log_dashboard_audit(
        _engine,
        "delete_platform_credential",
        "platform_credential",
        str(row_id),
        "플랫폼 연결 삭제",
    )
    clear_platform_credentials_cache()

def toggle_platform_credential(_engine, row_id: int, is_active: bool) -> None:
    ensure_platform_credentials_table(_engine)
    sql_exec(
        _engine,
        "UPDATE platform_credentials SET is_active = :is_active, updated_at = NOW() WHERE id = :id",
        {"id": int(row_id), "is_active": bool(is_active)},
    )
    log_dashboard_audit(
        _engine,
        "toggle_platform_credential",
        "platform_credential",
        str(row_id),
        "플랫폼 연결 활성 상태 변경",
        after={"is_active": bool(is_active)},
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
    log_dashboard_audit(
        _engine,
        "update_target_roas",
        "campaign",
        f"{cid}:{campaign_id}",
        "캠페인 목표 ROAS 변경",
        after={"customer_id": str(cid), "campaign_id": str(campaign_id), "target_roas": t_val, "min_roas": m_val},
    )

def _strict_conv_selects(fact_cols: list, alias: str = "") -> dict:
    prefix = f"{alias}." if alias else ""
    fact_col_set = set(fact_cols or [])
    has_cart = "cart_conv" in fact_col_set
    has_wish = "wishlist_conv" in fact_col_set
    has_split_flag = "split_available" in fact_col_set

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

    purchase_conv_raw = pick_expr(["purchase_conv", "primary_conv"])
    purchase_sales_raw = pick_expr(["purchase_sales", "primary_sales"])
    if has_split_flag:
        purchase_conv_raw = f"CASE WHEN COALESCE({prefix}split_available, FALSE) THEN {purchase_conv_raw} ELSE 0 END"
        purchase_sales_raw = f"CASE WHEN COALESCE({prefix}split_available, FALSE) THEN {purchase_sales_raw} ELSE 0 END"

    return {
        # 구매완료는 명시적인 split 컬럼만 사용한다. conv/sales는 네이버 총전환일 수 있어
        # 구매완료 fallback으로 쓰면 장바구니/위시/기타 전환이 구매완료로 과대 집계된다.
        "purchase_conv_expr": purchase_conv_raw,
        "purchase_sales_expr": purchase_sales_raw,
        "cart_conv_expr": f"COALESCE({prefix}cart_conv, 0)" if has_cart else "0",
        "cart_sales_expr": f"COALESCE({prefix}cart_sales, 0)" if has_cart else "0",
        "wish_conv_expr": f"COALESCE({prefix}wishlist_conv, 0)" if has_wish else "0",
        "wish_sales_expr": f"COALESCE({prefix}wishlist_sales, 0)" if has_wish else "0",
        "total_conv_expr": max_expr(["total_conv", "tot_conv", "conv", "purchase_conv", "primary_conv"]),
        "total_sales_expr": max_expr(["total_sales", "tot_sales", "sales", "purchase_sales", "primary_sales"]),
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


def _build_bundle_metric_sql(fact_cols: list, *, purchase_fallback: bool = True) -> dict:
    expr = _strict_conv_selects(fact_cols)
    if purchase_fallback:
        purchase_conv_expr = expr["purchase_conv_expr"]
        purchase_sales_expr = expr["purchase_sales_expr"]
    else:
        scoped_cols = set(fact_cols or [])
        purchase_conv_expr = "COALESCE(purchase_conv, 0)" if "purchase_conv" in scoped_cols else "0"
        purchase_sales_expr = "COALESCE(purchase_sales, 0)" if "purchase_sales" in scoped_cols else "0"
    return {
        "conv_agg_sql": f", SUM({purchase_conv_expr}) as conv, SUM({purchase_sales_expr}) as sales, SUM({expr['total_conv_expr']}) as tot_conv, SUM({expr['total_sales_expr']}) as tot_sales",
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


def _infer_platform_label_from_campaign_type(value) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    key = raw.upper()
    if key in {"META", "메타", "FACEBOOK", "FACEBOOK_ADS", "INSTAGRAM"}:
        return "메타"
    if key in {"GOOGLE", "GOOGLE_ADS", "구글", "PERFORMANCE_MAX", "PMAX", "P_MAX"}:
        return "구글"
    if key in {
        "NAVER", "NAVER_ADS", "NAVER_SEARCH", "SEARCHAD", "SEARCH_AD", "GFA",
        "WEB_SITE", "SHOPPING", "POWER_CONTENT", "POWER_CONTENTS", "BRAND_SEARCH", "PLACE",
        "파워링크", "쇼핑검색", "파워컨텐츠", "브랜드검색", "플레이스", "네이버",
    }:
        return "네이버"
    return ""


def _finalize_bundle_df(df: pd.DataFrame, campaign_type_col: str) -> pd.DataFrame:
    df = _map_campaign_types(df, campaign_type_col)
    if df is not None and not df.empty and campaign_type_col in df.columns:
        inferred = df[campaign_type_col].apply(_infer_platform_label_from_campaign_type)
        if "platform" in df.columns:
            df["platform"] = df["platform"].where(df["platform"].astype(str).str.strip() != "", inferred)
        else:
            df["platform"] = inferred
        df["platform"] = df["platform"].replace("", "네이버")
    return df


def _read_fact_customer_summary(_engine, table: str, select_sql: str, d1: date, d2: date, where_cid: str, cid_params: dict) -> pd.DataFrame:
    sql = f"SELECT customer_id, {select_sql} FROM {table} WHERE dt BETWEEN :d1 AND :d2 {where_cid} GROUP BY customer_id"
    return sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params})


def _merge_customer_metric_frame(base_df: pd.DataFrame, metric_df: pd.DataFrame) -> pd.DataFrame:
    if metric_df.empty:
        return base_df
    metric_df = metric_df.copy()
    metric_df["customer_id"] = _normalize_customer_id_series(metric_df["customer_id"])
    return base_df.merge(metric_df, on="customer_id", how="left")


def _account_label_key(value) -> str:
    return re.sub(r"\s+", "", str(value or "").strip()).casefold()


def _first_nonblank(*values) -> str:
    for value in values:
        text = str(value or "").strip()
        if text and text.lower() not in {"nan", "none", "nat", "<na>"}:
            return text
    return ""


def _budget_display_account_name(account_name: object, platform: object) -> str:
    name = str(account_name or "").strip()
    label = str(platform or "").strip()
    if not name:
        return label or ""
    if label and label not in name:
        return f"{name} {label}"
    return name


def _budget_customer_platform_lookup(_engine) -> dict[str, str]:
    if not table_exists(_engine, "dim_campaign"):
        return {}
    try:
        dim_cols = get_table_columns(_engine, "dim_campaign")
        cp_col = "campaign_tp" if "campaign_tp" in dim_cols else ("campaign_type_label" if "campaign_type_label" in dim_cols else "campaign_type")
        if cp_col not in dim_cols:
            return {}
        df = sql_read(
            _engine,
            f"""
            SELECT CAST(customer_id AS TEXT) AS customer_id, {cp_col} AS campaign_type
            FROM dim_campaign
            WHERE COALESCE(CAST({cp_col} AS TEXT), '') <> ''
            """,
        )
    except Exception:
        return {}
    if df.empty:
        return {}

    out: dict[str, str] = {}
    for cid, group in df.groupby("customer_id"):
        labels = {
            _infer_platform_label_from_campaign_type(value)
            for value in group["campaign_type"].dropna().astype(str).tolist()
        }
        labels.discard("")
        cid_norm = _normalize_customer_id_value(cid)
        if "메타" in labels and "네이버" not in labels:
            out[cid_norm] = "메타"
        elif "네이버" in labels:
            out[cid_norm] = "네이버"
    return out


def _budget_account_scope_df(_engine) -> pd.DataFrame:
    """Return the account rows budget/Bizmoney should use.

    Platform connections are the authoritative media mapping. dim_customer remains
    a legacy fallback, while active Meta connections are included as budget rows
    instead of being hidden from the monthly budget view.
    """
    meta = get_meta(_engine)
    if meta is None:
        meta = pd.DataFrame()
    meta = meta.copy()
    for col in ["customer_id", "account_name", "manager", "monthly_budget", "operating_weekdays"]:
        if col not in meta.columns:
            meta[col] = "" if col not in {"monthly_budget"} else 0
    if not meta.empty:
        meta["customer_id"] = _normalize_customer_id_series(meta["customer_id"])
        meta["account_name"] = meta["account_name"].fillna("").astype(str).str.strip()
        meta["manager"] = meta["manager"].fillna("미배정").astype(str).str.strip()
        meta["monthly_budget"] = pd.to_numeric(meta["monthly_budget"], errors="coerce").fillna(0)
        meta["operating_weekdays"] = meta["operating_weekdays"].apply(normalize_operating_weekdays)

    try:
        conn_df = get_platform_credentials(_engine)
    except Exception:
        conn_df = pd.DataFrame()

    if conn_df is None or conn_df.empty:
        out = meta.copy()
        if "platform" not in out.columns:
            platform_lookup = _budget_customer_platform_lookup(_engine)
            out["platform"] = out["customer_id"].map(platform_lookup).fillna("네이버") if "customer_id" in out.columns else "네이버"
        return out

    conn = conn_df.copy()
    if "is_active" in conn.columns:
        conn = conn[conn["is_active"].fillna(False).astype(bool)].copy()
    for col in ["platform", "account_label", "manager", "customer_id", "account_id"]:
        if col not in conn.columns:
            conn[col] = ""
    conn["platform_norm"] = conn["platform"].fillna("").astype(str).str.strip().str.lower()
    conn["platform_label"] = conn["platform"].apply(_infer_platform_label_from_campaign_type)

    # Meta/Google 연동이 있다는 이유만으로 같은 광고주명의 dim_customer 행을
    # 제거하면 네이버 비즈머니용 customer_id까지 끊긴다. 외부 매체 account_id와
    # 실제로 동일한 dim_customer 행만 네이버 fallback에서 제외한다.
    external_ids = set()
    for _, conn_row in conn.iterrows():
        platform_label = str(conn_row.get("platform_label", "") or "").strip()
        if platform_label == "네이버":
            continue
        external_id = _normalize_customer_id_value(conn_row.get("account_id", ""))
        if external_id and str(external_id).isdigit():
            external_ids.add(str(external_id))

    meta_by_cid = {}
    meta_by_name = {}
    if not meta.empty:
        meta_by_cid = {str(row.get("customer_id", "")).strip(): row for _, row in meta.iterrows() if str(row.get("customer_id", "")).strip()}
        meta_by_name = {_account_label_key(row.get("account_name", "")): row for _, row in meta.iterrows() if _account_label_key(row.get("account_name", ""))}

    rows = []
    platform_lookup = _budget_customer_platform_lookup(_engine)
    budget_conn = conn[conn["platform_label"].isin(_BUDGET_PLATFORM_LABELS)].copy()
    for _, row in budget_conn.iterrows():
        cid = _normalize_customer_id_value(_first_nonblank(row.get("customer_id"), row.get("account_id")))
        if not cid or not str(cid).isdigit():
            continue
        label = _first_nonblank(row.get("account_label"), cid)
        meta_row = meta_by_cid.get(cid)
        if meta_row is None:
            meta_row = meta_by_name.get(_account_label_key(label))
        platform_label = _first_nonblank(row.get("platform_label"), "네이버")
        rows.append({
            "customer_id": cid,
            "account_name": _budget_display_account_name(label, platform_label),
            "manager": _first_nonblank(row.get("manager"), None if meta_row is None else meta_row.get("manager"), "미배정"),
            "monthly_budget": 0 if meta_row is None else pd.to_numeric(meta_row.get("monthly_budget", 0), errors="coerce"),
            "operating_weekdays": normalize_operating_weekdays(None if meta_row is None else meta_row.get("operating_weekdays", DEFAULT_OPERATING_WEEKDAYS)),
            "platform": platform_label,
        })

    # dim_customer is still the source for legacy Naver budget/Bizmoney rows.
    # Keep it even when the same account_name has Meta/Google credentials; only
    # remove rows whose customer_id is exactly an external media account_id.
    fallback = meta.copy()
    if not fallback.empty and external_ids:
        fallback["customer_id"] = _normalize_customer_id_series(fallback["customer_id"])
        fallback = fallback[~fallback["customer_id"].astype(str).isin(external_ids)].copy()
    if not fallback.empty:
        fallback["platform"] = fallback["customer_id"].map(platform_lookup).fillna("네이버")
        fallback = fallback[fallback["platform"].isin(_BUDGET_PLATFORM_LABELS)].copy()
        fallback["account_name"] = fallback.apply(
            lambda row: _budget_display_account_name(row.get("account_name"), row.get("platform")),
            axis=1,
        )
        rows.extend(fallback[["customer_id", "account_name", "manager", "monthly_budget", "operating_weekdays", "platform"]].to_dict("records"))

    if not rows:
        return pd.DataFrame(columns=["customer_id", "account_name", "manager", "monthly_budget", "operating_weekdays", "platform"])
    out = pd.DataFrame(rows)
    out["customer_id"] = _normalize_customer_id_series(out["customer_id"])
    out = out[(out["customer_id"].astype(str).str.strip() != "") & (out["account_name"].astype(str).str.strip() != "")].copy()
    out["monthly_budget"] = pd.to_numeric(out["monthly_budget"], errors="coerce").fillna(0)
    out["operating_weekdays"] = out["operating_weekdays"].apply(normalize_operating_weekdays)
    return out.drop_duplicates(subset=["customer_id", "account_name"], keep="last").reset_index(drop=True)


def _budget_naver_scope_df(_engine) -> pd.DataFrame:
    """Backward-compatible wrapper for older imports/tests."""
    return _budget_account_scope_df(_engine)


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

    def _budget_metric_scope_sql(table_name: str, alias: str = "f") -> tuple[str, str, dict]:
        """Keep budget metrics to supported media types: Naver Search/GFA and Meta."""
        try:
            table_cols = get_table_columns(_engine, table_name)
        except Exception:
            table_cols = []

        if table_name == "overview_campaign_daily_cache" and "campaign_type" in table_cols:
            direct_where, direct_params = _build_in_filter(f"{alias}.campaign_type", _BUDGET_CAMPAIGN_TYPE_FILTER_VALUES, f"{table_name}_budget_media_type")
            return "", direct_where, direct_params

        if "campaign_id" in table_cols and table_exists(_engine, "dim_campaign"):
            dim_cols = get_table_columns(_engine, "dim_campaign")
            cp_col = "campaign_tp" if "campaign_tp" in dim_cols else ("campaign_type_label" if "campaign_type_label" in dim_cols else "campaign_type")
            if cp_col in dim_cols:
                join_sql = f"JOIN dim_campaign c ON {alias}.campaign_id = c.campaign_id AND {alias}.customer_id = c.customer_id"
                type_where, type_params = _build_in_filter(f"c.{cp_col}", _BUDGET_CAMPAIGN_TYPE_FILTER_VALUES, f"{table_name}_budget_media_type")
                return join_sql, type_where, type_params

        # If no campaign type source exists, keep legacy behavior rather than dropping all budget rows.
        return "", "", {}

    def _run_budget_metric_query(table_name: str, sales_expr: str) -> pd.DataFrame:
        join_sql, budget_where_sql, budget_params = _budget_metric_scope_sql(table_name, alias="f")
        sql = f"""
            SELECT CAST(f.customer_id AS TEXT) AS customer_id,
                   SUM(CASE WHEN f.dt BETWEEN :avg_d1 AND :avg_d2 THEN f.cost ELSE 0 END)/:avg_days as avg_cost,
                   SUM(CASE WHEN f.dt BETWEEN :month_d1 AND :month_d2 THEN f.cost ELSE 0 END) as current_month_cost,
                   SUM(CASE WHEN f.dt BETWEEN :month_d1 AND :month_d2 THEN {sales_expr} ELSE 0 END) as current_month_sales,
                   SUM(CASE WHEN f.dt BETWEEN :prev_month_d1 AND :prev_month_d2 THEN f.cost ELSE 0 END) as prev_month_cost
            FROM {table_name} f
            {join_sql}
            WHERE f.dt BETWEEN :outer_d1 AND :outer_d2 {where_cid.replace('customer_id', 'f.customer_id')} {budget_where_sql}
            GROUP BY CAST(f.customer_id AS TEXT)
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
                **budget_params,
            },
        )

    fact_df = pd.DataFrame()
    cache_df = pd.DataFrame()
    cache_is_fresh = False

    if table_exists(_engine, "fact_campaign_daily"):
        fact_df = _run_budget_metric_query("fact_campaign_daily", "COALESCE(f.sales, 0)")

    if table_exists(_engine, "overview_campaign_daily_cache"):
        latest_cache_df = sql_read(_engine, "SELECT MAX(dt) as dt FROM overview_campaign_daily_cache")
        latest_cache_dt = None if latest_cache_df.empty else latest_cache_df.iloc[0].get("dt")
        if pd.notna(latest_cache_dt):
            try:
                cache_is_fresh = pd.to_datetime(latest_cache_dt).date() >= outer_d2
            except Exception:
                cache_is_fresh = False
        cache_df = _run_budget_metric_query("overview_campaign_daily_cache", "COALESCE(f.tot_sales, f.sales, 0)")

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

@st.cache_data(ttl=300, max_entries=30, show_spinner=False)
def query_budget_bundle(_engine, cids: tuple, yesterday: date, avg_d1: date, avg_d2: date, month_d1: date, month_d2: date, prev_month_d1: date, prev_month_d2: date, avg_days: int) -> pd.DataFrame:
    # Budget supports Naver and Meta. Use settings > platform connections first
    # so external media accounts keep their own platform identity.
    df = _budget_account_scope_df(_engine)
    if df.empty:
        return pd.DataFrame()

    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("CAST(customer_id AS TEXT)", cids_tuple, "budget_cid")

    df = df.copy()
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
    if "platform" not in df.columns:
        df["platform"] = ""
    df["platform"] = df["platform"].fillna("").replace("", "네이버")
    return df


def _budget_campaign_type_case_sql(type_sql: str) -> str:
    def _quote_sql_literal(value: object) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    clauses = []
    for label in _BUDGET_CAMPAIGN_TYPE_OPTIONS:
        values = ", ".join(_quote_sql_literal(value) for value in _CAMPAIGN_TYPE_ALIASES.get(label, [label]))
        clauses.append(f"WHEN {type_sql} IN ({values}) THEN '{label}'")
    return f"CASE {' '.join(clauses)} ELSE NULL END"


def _ensure_customer_type_budget_table(_engine) -> None:
    sql_exec(
        _engine,
        """
        CREATE TABLE IF NOT EXISTS dim_customer_type_budget (
            customer_id TEXT NOT NULL,
            campaign_type TEXT NOT NULL,
            monthly_budget BIGINT DEFAULT 0,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (customer_id, campaign_type)
        )
        """,
    )
    st.session_state.pop("_table_names_cache", None)
    get_table_columns.clear()


def _read_type_budget_settings(_engine, cids_tuple: tuple) -> pd.DataFrame:
    _ensure_customer_type_budget_table(_engine)
    where_cid, cid_params = _build_in_filter("REGEXP_REPLACE(CAST(customer_id AS TEXT), '\\.0+$', '')", cids_tuple, "type_budget_cid")
    where_type, type_params = _build_in_filter("campaign_type", _BUDGET_CAMPAIGN_TYPE_OPTIONS, "type_budget_campaign_type")
    return sql_read(
        _engine,
        f"""
        SELECT
            REGEXP_REPLACE(CAST(customer_id AS TEXT), '\\.0+$', '') AS customer_id,
            campaign_type,
            monthly_budget
        FROM dim_customer_type_budget
        WHERE 1=1 {where_type} {where_cid}
        """,
        {**cid_params, **type_params},
    )


def _read_budget_campaign_type_metrics(_engine, avg_d1: date, avg_d2: date, month_d1: date, month_d2: date, prev_month_d1: date, prev_month_d2: date, avg_days: int, where_cid: str, cid_params: dict) -> pd.DataFrame:
    outer_d1 = min(avg_d1, month_d1, prev_month_d1)
    outer_d2 = max(avg_d2, month_d2, prev_month_d2)

    def _run_type_metric_query(table_name: str, sales_expr: str) -> pd.DataFrame:
        try:
            table_cols = get_table_columns(_engine, table_name)
        except Exception:
            table_cols = []

        if table_name == "overview_campaign_daily_cache" and "campaign_type" in table_cols:
            type_expr = _budget_campaign_type_case_sql("f.campaign_type")
            join_sql = ""
            type_filter_sql, type_params = _build_in_filter("f.campaign_type", _BUDGET_CAMPAIGN_TYPE_FILTER_VALUES, f"{table_name}_budget_type")
        elif "campaign_id" in table_cols and table_exists(_engine, "dim_campaign"):
            dim_cols = get_table_columns(_engine, "dim_campaign")
            cp_col = "campaign_tp" if "campaign_tp" in dim_cols else ("campaign_type_label" if "campaign_type_label" in dim_cols else "campaign_type")
            if cp_col not in dim_cols:
                return pd.DataFrame()
            join_sql = f"JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id"
            type_expr = _budget_campaign_type_case_sql(f"c.{cp_col}")
            type_filter_sql, type_params = _build_in_filter(f"c.{cp_col}", _BUDGET_CAMPAIGN_TYPE_FILTER_VALUES, f"{table_name}_budget_type")
        else:
            return pd.DataFrame()

        sql = f"""
            SELECT
                CAST(f.customer_id AS TEXT) AS customer_id,
                {type_expr} AS campaign_type,
                SUM(CASE WHEN f.dt BETWEEN :avg_d1 AND :avg_d2 THEN f.cost ELSE 0 END)/:avg_days as avg_cost,
                SUM(CASE WHEN f.dt BETWEEN :month_d1 AND :month_d2 THEN f.cost ELSE 0 END) as current_month_cost,
                SUM(CASE WHEN f.dt BETWEEN :month_d1 AND :month_d2 THEN {sales_expr} ELSE 0 END) as current_month_sales,
                SUM(CASE WHEN f.dt BETWEEN :prev_month_d1 AND :prev_month_d2 THEN f.cost ELSE 0 END) as prev_month_cost
            FROM {table_name} f
            {join_sql}
            WHERE f.dt BETWEEN :outer_d1 AND :outer_d2 {where_cid.replace('customer_id', 'f.customer_id')} {type_filter_sql}
            GROUP BY CAST(f.customer_id AS TEXT), {type_expr}
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
                **type_params,
            },
        )

    fact_df = pd.DataFrame()
    cache_df = pd.DataFrame()
    cache_is_fresh = False

    if table_exists(_engine, "fact_campaign_daily"):
        fact_df = _run_type_metric_query("fact_campaign_daily", "COALESCE(f.sales, 0)")

    if table_exists(_engine, "overview_campaign_daily_cache"):
        latest_cache_df = sql_read(_engine, "SELECT MAX(dt) as dt FROM overview_campaign_daily_cache")
        latest_cache_dt = None if latest_cache_df.empty else latest_cache_df.iloc[0].get("dt")
        if pd.notna(latest_cache_dt):
            try:
                cache_is_fresh = pd.to_datetime(latest_cache_dt).date() >= outer_d2
            except Exception:
                cache_is_fresh = False
        cache_df = _run_type_metric_query("overview_campaign_daily_cache", "COALESCE(f.tot_sales, f.sales, 0)")

    if fact_df.empty and cache_df.empty:
        return pd.DataFrame()
    if fact_df.empty:
        return cache_df
    if cache_df.empty or not cache_is_fresh:
        return fact_df

    merged = fact_df.merge(cache_df, on=["customer_id", "campaign_type"], how="outer", suffixes=("_fact", "_cache"))
    for col in ["avg_cost", "current_month_cost", "current_month_sales", "prev_month_cost"]:
        cache_col = f"{col}_cache"
        fact_col = f"{col}_fact"
        merged[col] = merged[cache_col].where(merged[cache_col].notna(), merged[fact_col])
    return merged[["customer_id", "campaign_type", "avg_cost", "current_month_cost", "current_month_sales", "prev_month_cost"]]


def _merge_customer_type_metric_frame(base_df: pd.DataFrame, metric_df: pd.DataFrame) -> pd.DataFrame:
    if metric_df.empty:
        return base_df
    metric_df = metric_df.copy()
    metric_df["customer_id"] = _normalize_customer_id_series(metric_df["customer_id"])
    return base_df.merge(metric_df, on=["customer_id", "campaign_type"], how="left")


def _budget_type_options_for_platform(platform: object) -> tuple[str, ...]:
    label = str(platform or "").strip()
    if label == "메타":
        return ("메타",)
    return _BUDGET_NAVER_CAMPAIGN_TYPE_OPTIONS


@st.cache_data(ttl=300, max_entries=30, show_spinner=False)
def query_budget_type_bundle(_engine, cids: tuple, yesterday: date, avg_d1: date, avg_d2: date, month_d1: date, month_d2: date, prev_month_d1: date, prev_month_d2: date, avg_days: int) -> pd.DataFrame:
    df = _budget_account_scope_df(_engine)
    if df.empty:
        return pd.DataFrame()

    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("CAST(customer_id AS TEXT)", cids_tuple, "budget_type_cid")

    df = df.copy()
    df["customer_id"] = _normalize_customer_id_series(df["customer_id"])
    if cids_tuple:
        df = df[df["customer_id"].isin(cids_tuple)]
    if df.empty:
        return pd.DataFrame()

    account_base = df[["customer_id", "account_name", "manager", "operating_weekdays", "platform"]].drop_duplicates("customer_id").copy()
    type_records: list[dict] = []
    for _, row in account_base.iterrows():
        for campaign_type in _budget_type_options_for_platform(row.get("platform")):
            rec = row.to_dict()
            rec["campaign_type"] = campaign_type
            type_records.append(rec)
    base = pd.DataFrame(type_records)
    if base.empty:
        return pd.DataFrame()

    budget_settings = _read_type_budget_settings(_engine, cids_tuple)
    base = _merge_customer_type_metric_frame(base, budget_settings)
    if "monthly_budget" not in base.columns:
        base["monthly_budget"] = 0

    metric_frames = [
        _read_budget_campaign_type_metrics(_engine, avg_d1, avg_d2, month_d1, month_d2, prev_month_d1, prev_month_d2, avg_days, where_cid, cid_params),
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
        if not metric_df.empty and "campaign_type" in metric_df.columns:
            base = _merge_customer_type_metric_frame(base, metric_df)
        else:
            base = _merge_customer_metric_frame(base, metric_df)

    base = _fill_numeric_columns(
        base,
        ["avg_cost", "current_month_cost", "current_month_sales", "prev_month_cost", "bizmoney_balance", "monthly_budget"],
    )

    if "manager" not in base.columns:
        base["manager"] = "미배정"
    if "account_name" not in base.columns:
        base["account_name"] = base["customer_id"].astype(str)
    if "platform" not in base.columns:
        base["platform"] = ""
    base["platform"] = base["platform"].fillna("").replace("", "네이버")
    return base


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
            """
            INSERT INTO dim_customer (customer_id, account_name, monthly_budget, operating_weekdays)
            SELECT :cid, :cid, :val, :weekdays
            WHERE NOT EXISTS (
                SELECT 1
                FROM dim_customer
                WHERE REGEXP_REPLACE(CAST(customer_id AS TEXT), '\\.0+$', '') = :cid
            )
            """,
            {"val": val, "cid": cid_norm, "weekdays": DEFAULT_OPERATING_WEEKDAYS},
        )
        sql_exec(
            _engine,
            "UPDATE dim_customer SET monthly_budget = :val WHERE REGEXP_REPLACE(CAST(customer_id AS TEXT), '\\.0+$', '') = :cid",
            {"val": val, "cid": cid_norm},
        )
        get_table_columns.clear()
        get_meta.clear()
        query_budget_bundle.clear()
        log_dashboard_audit(
            _engine,
            "update_monthly_budget",
            "customer",
            cid_norm,
            "월 예산 변경",
            after={"customer_id": cid_norm, "monthly_budget": int(val or 0)},
        )
        return True
    except Exception as e:
        st.error(f"예산 업데이트 실패: {e}")
        return False


def update_monthly_budget_by_campaign_type(_engine, cid: int, campaign_type: str, val: int):
    try:
        type_norm = str(campaign_type or "").strip()
        if type_norm not in _BUDGET_CAMPAIGN_TYPE_OPTIONS:
            raise ValueError(f"지원하지 않는 예산 유형입니다: {campaign_type}")
        _ensure_customer_type_budget_table(_engine)
        cid_norm = _normalize_customer_id_value(cid)
        sql_exec(
            _engine,
            """
            INSERT INTO dim_customer_type_budget (customer_id, campaign_type, monthly_budget, updated_at)
            VALUES (:cid, :campaign_type, :val, NOW())
            ON CONFLICT (customer_id, campaign_type)
            DO UPDATE SET monthly_budget = EXCLUDED.monthly_budget, updated_at = NOW()
            """,
            {"cid": cid_norm, "campaign_type": type_norm, "val": int(val or 0)},
        )
        query_budget_type_bundle.clear()
        sql_read.clear()
        log_dashboard_audit(
            _engine,
            "update_monthly_budget_by_campaign_type",
            "customer",
            f"{cid_norm}:{type_norm}",
            "유형별 월 예산 변경",
            after={"customer_id": cid_norm, "campaign_type": type_norm, "monthly_budget": int(val or 0)},
        )
        return True
    except Exception as e:
        st.error(f"유형별 예산 업데이트 실패: {e}")
        return False


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
        log_dashboard_audit(
            _engine,
            "update_operating_weekdays",
            "customer",
            cid_norm,
            "운영 요일 변경",
            after={"customer_id": cid_norm, "operating_weekdays": weekdays_norm},
        )
    except Exception as e:
        st.error(f"운영 요일 업데이트 실패: {e}")


@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=30, show_spinner=False)
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

@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=60, show_spinner=False)
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



def _type_selection_includes_shopping(type_sel) -> bool:
    """Return True when the current campaign type filter can include shopping campaigns."""
    normalized = _normalize_filter_values(type_sel)
    if not normalized:
        return True
    expanded = {str(v).strip().upper() for v in _expand_campaign_type_filter_values(normalized)}
    shopping_aliases = {str(v).strip().upper() for v in _CAMPAIGN_TYPE_ALIASES.get("쇼핑검색", ["SHOPPING", "쇼핑검색"])}
    return bool(expanded.intersection(shopping_aliases))


def _shopping_query_metric_expr(sq_cols: set[str], candidates: list[str], alias: str, table_alias: str = "f") -> str:
    pieces = []
    for col in candidates:
        if col in sq_cols:
            pieces.append(f"COALESCE({table_alias}.{col}, 0)")
    if not pieces:
        return f"0 as {alias}"
    # Use the max of compatible columns so old/new schemas do not double-count the same value.
    if len(pieces) == 1:
        expr = pieces[0]
    else:
        expr = f"GREATEST({', '.join(pieces)})"
    return f"SUM({expr}) as {alias}"


def _shopping_query_total_expr(sq_cols: set[str], kind: str, table_alias: str = "f") -> str:
    """Return a row-safe total expression for fact_shopping_query_daily.

    total_conv/total_sales가 있으면 그것을 최우선으로 사용한다. explicit total이 없는
    legacy row만 split 합계 또는 conv/sales를 fallback으로 쓰며, conv를 purchase로 간주한 뒤
    cart/wishlist를 더하는 방식은 사용하지 않는다.
    """
    if kind == "conv":
        explicit = [c for c in ["total_conv", "tot_conv"] if c in sq_cols]
        split_cols = [c for c in ["purchase_conv", "primary_conv", "cart_conv", "wishlist_conv"] if c in sq_cols]
        legacy = [c for c in ["conv"] if c in sq_cols]
    else:
        explicit = [c for c in ["total_sales", "tot_sales"] if c in sq_cols]
        split_cols = [c for c in ["purchase_sales", "primary_sales", "cart_sales", "wishlist_sales"] if c in sq_cols]
        legacy = [c for c in ["sales"] if c in sq_cols]

    def max_or_zero(cols: list[str]) -> str:
        if not cols:
            return "0"
        exprs = [f"COALESCE({table_alias}.{c}, 0)" for c in cols]
        return exprs[0] if len(exprs) == 1 else f"GREATEST({', '.join(exprs)})"

    explicit_expr = max_or_zero(explicit)
    split_expr = " + ".join([f"COALESCE({table_alias}.{c}, 0)" for c in split_cols]) if split_cols else "0"
    legacy_expr = max_or_zero(legacy)
    # explicit total > split 합계 > legacy total 순서. NULL/0 explicit rows는 split으로 보완한다.
    return f"SUM(CASE WHEN {explicit_expr} > 0 THEN {explicit_expr} WHEN ({split_expr}) > 0 THEN ({split_expr}) ELSE {legacy_expr} END)"


@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=40, show_spinner=False)
def query_shopping_query_campaign_purchase_summary(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple = ()) -> pd.DataFrame:
    """Campaign-level shopping purchase summary from SHOPPINGKEYWORD_CONVERSION_DETAIL storage.

    fact_campaign_daily can contain campaign-summary conversion numbers that are not the
    shopping purchase-complete split.  For shopping purchase metrics, this query is the
    authoritative dashboard source because it is built from fact_shopping_query_daily.
    """
    if not _type_selection_includes_shopping(type_sel):
        return pd.DataFrame()
    if not table_exists(_engine, "fact_shopping_query_daily"):
        return pd.DataFrame()
    sq_cols = set(get_table_columns(_engine, "fact_shopping_query_daily"))
    if "campaign_id" not in sq_cols:
        return pd.DataFrame()
    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("f.customer_id", cids_tuple, "shop_campaign_purchase_cid")

    type_where_sql = ""
    type_params = {}
    if table_exists(_engine, "dim_campaign"):
        dim_cols = get_table_columns(_engine, "dim_campaign")
        cp_col = "campaign_tp" if "campaign_tp" in dim_cols else ("campaign_type_label" if "campaign_type_label" in dim_cols else "campaign_type")
        if cp_col in dim_cols:
            raw_type_where, type_params = _build_in_filter(f"c.{cp_col}", _CAMPAIGN_TYPE_ALIASES.get("쇼핑검색", ["SHOPPING", "쇼핑검색"]), "shop_campaign_purchase_type")
            if raw_type_where:
                type_where_sql = raw_type_where.replace("AND ", f"AND (c.{cp_col} IS NULL OR ", 1) + ")"

    purchase_conv_sql = _shopping_query_metric_expr(sq_cols, ["purchase_conv", "primary_conv"], "conv")
    purchase_sales_sql = _shopping_query_metric_expr(sq_cols, ["purchase_sales", "primary_sales"], "sales")
    cart_conv_sql = _shopping_query_metric_expr(sq_cols, ["cart_conv"], "cart_conv")
    cart_sales_sql = _shopping_query_metric_expr(sq_cols, ["cart_sales"], "cart_sales")
    wish_conv_sql = _shopping_query_metric_expr(sq_cols, ["wishlist_conv"], "wishlist_conv")
    wish_sales_sql = _shopping_query_metric_expr(sq_cols, ["wishlist_sales"], "wishlist_sales")
    total_conv_expr = _shopping_query_total_expr(sq_cols, "conv")
    total_sales_expr = _shopping_query_total_expr(sq_cols, "sales")

    sql = f"""
        SELECT
            f.customer_id,
            f.campaign_id,
            {purchase_conv_sql},
            {purchase_sales_sql},
            {total_conv_expr} as tot_conv,
            {total_sales_expr} as tot_sales,
            {cart_conv_sql},
            {cart_sales_sql},
            {wish_conv_sql},
            {wish_sales_sql},
            TRUE as shopping_query_purchase_source
        FROM fact_shopping_query_daily f
        LEFT JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid} {type_where_sql}
        GROUP BY f.customer_id, f.campaign_id
        HAVING {total_conv_expr} > 0 OR {total_sales_expr} > 0
    """
    return sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params, **type_params})


@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=40, show_spinner=False)
def query_shopping_query_adgroup_purchase_summary(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple = ()) -> pd.DataFrame:
    """Adgroup-level shopping purchase summary from fact_shopping_query_daily."""
    if not _type_selection_includes_shopping(type_sel):
        return pd.DataFrame()
    if not table_exists(_engine, "fact_shopping_query_daily"):
        return pd.DataFrame()
    sq_cols = set(get_table_columns(_engine, "fact_shopping_query_daily"))
    if not {"campaign_id", "adgroup_id"}.issubset(sq_cols):
        return pd.DataFrame()
    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("f.customer_id", cids_tuple, "shop_adgroup_purchase_cid")

    type_where_sql = ""
    type_params = {}
    if table_exists(_engine, "dim_campaign"):
        dim_cols = get_table_columns(_engine, "dim_campaign")
        cp_col = "campaign_tp" if "campaign_tp" in dim_cols else ("campaign_type_label" if "campaign_type_label" in dim_cols else "campaign_type")
        if cp_col in dim_cols:
            raw_type_where, type_params = _build_in_filter(f"c.{cp_col}", _CAMPAIGN_TYPE_ALIASES.get("쇼핑검색", ["SHOPPING", "쇼핑검색"]), "shop_adgroup_purchase_type")
            if raw_type_where:
                type_where_sql = raw_type_where.replace("AND ", f"AND (c.{cp_col} IS NULL OR ", 1) + ")"

    purchase_conv_sql = _shopping_query_metric_expr(sq_cols, ["purchase_conv", "primary_conv"], "conv")
    purchase_sales_sql = _shopping_query_metric_expr(sq_cols, ["purchase_sales", "primary_sales"], "sales")
    cart_conv_sql = _shopping_query_metric_expr(sq_cols, ["cart_conv"], "cart_conv")
    cart_sales_sql = _shopping_query_metric_expr(sq_cols, ["cart_sales"], "cart_sales")
    wish_conv_sql = _shopping_query_metric_expr(sq_cols, ["wishlist_conv"], "wishlist_conv")
    wish_sales_sql = _shopping_query_metric_expr(sq_cols, ["wishlist_sales"], "wishlist_sales")
    total_conv_expr = _shopping_query_total_expr(sq_cols, "conv")
    total_sales_expr = _shopping_query_total_expr(sq_cols, "sales")

    sql = f"""
        SELECT
            f.customer_id,
            f.campaign_id,
            f.adgroup_id,
            {purchase_conv_sql},
            {purchase_sales_sql},
            {total_conv_expr} as tot_conv,
            {total_sales_expr} as tot_sales,
            {cart_conv_sql},
            {cart_sales_sql},
            {wish_conv_sql},
            {wish_sales_sql},
            TRUE as shopping_query_purchase_source
        FROM fact_shopping_query_daily f
        LEFT JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid} {type_where_sql}
        GROUP BY f.customer_id, f.campaign_id, f.adgroup_id
        HAVING {total_conv_expr} > 0 OR {total_sales_expr} > 0
    """
    return sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params, **type_params})


def _is_shopping_campaign_rows(df: pd.DataFrame, campaign_type_col: str = "campaign_type") -> pd.Series:
    if df is None or df.empty or campaign_type_col not in df.columns:
        return pd.Series(False, index=df.index if df is not None else None)
    raw = df[campaign_type_col].fillna("").astype(str).str.strip().str.upper()
    return raw.isin({"SHOPPING", "쇼핑검색"}) | raw.str.contains("쇼핑", na=False)


def _override_with_shopping_query_purchase(df: pd.DataFrame, summary: pd.DataFrame, keys: list[str], campaign_type_col: str = "campaign_type") -> pd.DataFrame:
    if df is None or df.empty or summary is None or summary.empty:
        return df
    usable_keys = [k for k in keys if k in df.columns and k in summary.columns]
    if not usable_keys:
        return df
    out = df.copy()
    summary = summary.copy()
    for k in usable_keys:
        out[k] = out[k].astype(str)
        summary[k] = summary[k].astype(str)
    metric_cols = ["conv", "sales", "tot_conv", "tot_sales", "cart_conv", "cart_sales", "wishlist_conv", "wishlist_sales"]
    keep_cols = usable_keys + [c for c in metric_cols if c in summary.columns]
    merged = out.merge(summary[keep_cols].drop_duplicates(subset=usable_keys), on=usable_keys, how="left", suffixes=("", "__shopping_query"))
    shopping_mask = _is_shopping_campaign_rows(merged, campaign_type_col)
    source_available = pd.Series(False, index=merged.index)
    for c in metric_cols:
        sq_col = f"{c}__shopping_query"
        if sq_col in merged.columns:
            source_available = source_available | pd.to_numeric(merged[sq_col], errors="coerce").notna()
    mask = shopping_mask & source_available
    for c in metric_cols:
        sq_col = f"{c}__shopping_query"
        if sq_col in merged.columns:
            if c not in merged.columns:
                merged[c] = 0
            current = pd.to_numeric(merged[c], errors="coerce").fillna(0)
            replacement = pd.to_numeric(merged[sq_col], errors="coerce")
            merged.loc[mask & replacement.notna(), c] = replacement[mask & replacement.notna()]
    if "shopping_purchase_source" not in merged.columns:
        merged["shopping_purchase_source"] = ""
    merged.loc[mask, "shopping_purchase_source"] = "검색어상세 구매완료"
    drop_cols = [c for c in merged.columns if c.endswith("__shopping_query")]
    return merged.drop(columns=drop_cols)


@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=30, show_spinner=False)
def query_shopping_warning_source(_engine, d1: date, d2: date, cids: tuple) -> pd.DataFrame:
    """Read shopping search rows broadly enough for warning detection.

    The regular shopping search-term view keeps only rows with conversion value.
    Warning detection also needs high-cost/no-purchase and click/no-purchase rows,
    so this query intentionally keeps rows with impressions, clicks, cost, or any
    conversion split.
    """
    if not table_exists(_engine, "fact_shopping_query_daily"):
        return pd.DataFrame()
    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("f.customer_id", cids_tuple, "shopping_warning_cid")

    sq_cols = set(get_table_columns(_engine, "fact_shopping_query_daily"))

    def _sum_col(col: str, alias: str) -> str:
        if col in sq_cols:
            return f"SUM(COALESCE(f.{col}, 0)) as {alias}"
        return f"0 as {alias}"

    def _sum_expr(candidates: list[str], alias: str) -> str:
        picked = [f"f.{col}" for col in candidates if col in sq_cols]
        if not picked:
            return f"0 as {alias}"
        if len(picked) == 1:
            expr = f"COALESCE({picked[0]}, 0)"
        else:
            expr = f"COALESCE({', '.join(picked)}, 0)"
        return f"SUM({expr}) as {alias}"

    def _greatest_sum_expr(candidates: list[str], alias: str) -> str:
        picked = [f"COALESCE(f.{col}, 0)" for col in candidates if col in sq_cols]
        if not picked:
            return f"0 as {alias}"
        expr = picked[0] if len(picked) == 1 else f"GREATEST({', '.join(picked)})"
        return f"SUM({expr}) as {alias}"

    query_expr = "COALESCE(NULLIF(TRIM(CAST(f.query_text AS TEXT)), ''), '(검색어 미제공 영역)')"
    query_provided_select_sql = (
        "BOOL_AND(COALESCE(f.query_provided, TRUE)) as query_provided"
        if "query_provided" in sq_cols
        else f"BOOL_AND(CASE WHEN {query_expr} IN ('-', '(검색어 미제공 영역)') THEN FALSE ELSE TRUE END) as query_provided"
    )

    type_where_sql = ""
    type_params = {}
    if table_exists(_engine, "dim_campaign"):
        dim_cols = get_table_columns(_engine, "dim_campaign")
        cp_col = "campaign_tp" if "campaign_tp" in dim_cols else ("campaign_type_label" if "campaign_type_label" in dim_cols else "campaign_type")
        if cp_col in dim_cols:
            raw_type_where, type_params = _build_in_filter(
                f"c.{cp_col}",
                _CAMPAIGN_TYPE_ALIASES.get("쇼핑검색", ["SHOPPING", "쇼핑검색"]),
                "shopping_warning_type",
            )
            if raw_type_where:
                type_where_sql = raw_type_where.replace("AND ", f"AND (c.{cp_col} IS NULL OR ", 1) + ")"

    ad_join_sql = ""
    ad_name_sql = "'' as ad_name"
    ad_title_sql = "'' as ad_title"
    group_ad_cols = ""
    if table_exists(_engine, "dim_ad"):
        ad_cols = get_table_columns(_engine, "dim_ad")
        ad_name_expr = "ad.ad_name" if "ad_name" in ad_cols else "''"
        ad_title_expr = "ad.ad_title" if "ad_title" in ad_cols else ad_name_expr
        ad_name_sql = f"{ad_name_expr} as ad_name"
        ad_title_sql = f"{ad_title_expr} as ad_title"
        ad_join_sql = "LEFT JOIN dim_ad ad ON f.ad_id = ad.ad_id AND f.customer_id = ad.customer_id"
        group_ad_cols = f", {ad_name_expr}, {ad_title_expr}"

    metric_presence = []
    for col in ["imp", "clk", "cost", "purchase_conv", "primary_conv", "purchase_sales", "primary_sales", "total_conv", "tot_conv", "conv", "total_sales", "tot_sales", "sales"]:
        if col in sq_cols:
            metric_presence.append(f"SUM(COALESCE(f.{col}, 0)) > 0")
    having_sql = " OR ".join(metric_presence) if metric_presence else "COUNT(*) > 0"

    sql = f"""
        SELECT
            f.dt,
            f.customer_id,
            f.campaign_id,
            f.adgroup_id,
            f.ad_id,
            c.campaign_name,
            a.adgroup_name,
            {ad_name_sql},
            {ad_title_sql},
            {query_expr} AS query_text,
            {query_provided_select_sql},
            {_sum_col("imp", "imp")},
            {_sum_col("clk", "clk")},
            {_sum_col("cost", "cost")},
            {_sum_expr(["purchase_conv", "primary_conv"], "purchase_conv")},
            {_sum_expr(["purchase_sales", "primary_sales"], "purchase_sales")},
            {_greatest_sum_expr(["total_conv", "tot_conv", "conv", "purchase_conv", "primary_conv"], "total_conv")},
            {_greatest_sum_expr(["total_sales", "tot_sales", "sales", "purchase_sales", "primary_sales"], "total_sales")}
        FROM fact_shopping_query_daily f
        LEFT JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id
        LEFT JOIN dim_adgroup a ON f.adgroup_id = a.adgroup_id AND f.customer_id = a.customer_id
        {ad_join_sql}
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid} {type_where_sql}
        GROUP BY f.dt, f.customer_id, f.campaign_id, f.adgroup_id, f.ad_id, c.campaign_name, a.adgroup_name, {query_expr}{group_ad_cols}
        HAVING {having_sql}
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params, **type_params})
    if df is not None and not df.empty:
        df["dt"] = pd.to_datetime(df["dt"])
    return df


@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=40, show_spinner=False)
def query_campaign_bundle(_engine, d1: date, d2: date, cids: tuple, type_sel: tuple, topn_cost: int = 0) -> pd.DataFrame:
    if not table_exists(_engine, "fact_campaign_daily"):
        return pd.DataFrame()
    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("customer_id", cids_tuple, "campaign_bundle_cid")

    dim_cols, cp_col = _resolve_campaign_type_column(_engine)
    target_roas_select = ", c.target_roas" if "target_roas" in dim_cols else ", 0.0 as target_roas"
    min_roas_select = ", c.min_roas" if "min_roas" in dim_cols else ", 0.0 as min_roas"
    budget_select_sql = "".join(
        f", c.{col}" if col in dim_cols else f", 0 AS {col}"
        for col in ["daily_budget", "lifetime_budget", "budget_remaining", "spend_cap"]
    )
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
            c.campaign_name, c.{cp_col} as campaign_type {target_roas_select} {min_roas_select}{budget_select_sql},
            agg.imp, agg.clk, agg.cost, agg.conv, agg.sales, agg.tot_conv, agg.tot_sales{metric_sql['cart_select_sql']}{metric_sql['wish_select_sql']}{rank_select_sql}
        FROM agg
        JOIN dim_campaign c ON agg.campaign_id = c.campaign_id AND agg.customer_id = c.customer_id
        WHERE 1=1 {type_filter_sql}
    """
    sql += _bundle_limit_clause(topn_cost)

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params, **type_params})
    # 캠페인/오버뷰 구매완료는 fact_campaign_daily의 명시적 split만 사용한다.
    # 검색어 상세 테이블은 검색어 미제공/미매핑 버킷 때문에 캠페인 합계 대체 원천으로 쓰지 않는다.
    return _finalize_bundle_df(df, "campaign_type")

@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=40, show_spinner=False)
def query_keyword_bundle(_engine, d1: date, d2: date, cids, type_sel: tuple, topn_cost: int = 0, include_dt: bool = False) -> pd.DataFrame:
    if not table_exists(_engine, "fact_keyword_daily"):
        return pd.DataFrame()
    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("customer_id", cids_tuple, "keyword_bundle_cid")

    _, cp_col = _resolve_campaign_type_column(_engine)
    type_filter_sql, type_params = _build_campaign_type_filter(cp_col, type_sel, "keyword_bundle_type")
    shopping_exclude_values = _CAMPAIGN_TYPE_ALIASES.get("SHOPPING", ["SHOPPING", "쇼핑검색"])
    shopping_placeholders = []
    shopping_params = {}
    for idx, value in enumerate(shopping_exclude_values):
        key = f"keyword_bundle_exclude_shopping_{idx}"
        shopping_placeholders.append(f":{key}")
        shopping_params[key] = value
    shopping_exclude_sql = (
        f"AND COALESCE(CAST(c.{cp_col} AS TEXT), '') NOT IN ({', '.join(shopping_placeholders)})"
        if shopping_placeholders else ""
    )

    kw_fact_cols = get_table_columns(_engine, "fact_keyword_daily")
    rank_agg_sql, rank_select_sql = _build_rank_metric_sql(_resolve_rank_column(_engine, "fact_keyword_daily"))
    metric_sql = _build_bundle_metric_sql(kw_fact_cols, purchase_fallback=False)
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
        WHERE 1=1 {type_filter_sql} {shopping_exclude_sql}
    """
    sql += _bundle_limit_clause(topn_cost)

    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params, **type_params, **shopping_params})
    return _finalize_bundle_df(df, "campaign_type_label")

@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=40, show_spinner=False)
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

@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=40, show_spinner=False)
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
        db_type_values = []
        for t in normalized_types:
            key = rev_map.get(t, t)
            aliases = _CAMPAIGN_TYPE_ALIASES.get(t) or _CAMPAIGN_TYPE_ALIASES.get(key) or [key]
            db_type_values.extend(aliases)
        db_types = tuple(dict.fromkeys(str(v) for v in db_type_values if str(v).strip()))
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

@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=40, show_spinner=False)
def query_shopping_search_terms(_engine, d1: date, d2: date, cids: tuple) -> pd.DataFrame:
    if not table_exists(_engine, "fact_shopping_query_daily"):
        return pd.DataFrame()
    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("f.customer_id", cids_tuple, "shopping_terms_cid")

    sq_cols = set(get_table_columns(_engine, "fact_shopping_query_daily"))

    def _coalesce_expr(candidates: list[str]) -> str:
        picked = [f"f.{col}" for col in candidates if col in sq_cols]
        if not picked:
            return "0"
        if len(picked) == 1:
            return f"COALESCE({picked[0]}, 0)"
        return f"COALESCE({', '.join(picked)}, 0)"

    def _greatest_expr(candidates: list[str]) -> str:
        picked = [f"COALESCE(f.{col}, 0)" for col in candidates if col in sq_cols]
        if not picked:
            return "0"
        if len(picked) == 1:
            return picked[0]
        return f"GREATEST({', '.join(picked)})"

    # fact_shopping_query_daily has evolved over time.  Older rows may only have
    # primary_* or conv/sales, while newer rows have purchase_* and total_* split
    # columns.  Treat the purchase metric as the best available purchase-complete
    # source and never require one exact physical column name for the overview.
    purchase_conv_expr = _coalesce_expr(["purchase_conv", "primary_conv"])
    purchase_sales_expr = _coalesce_expr(["purchase_sales", "primary_sales"])
    total_conv_expr = _greatest_expr(["total_conv", "conv", "primary_conv", "purchase_conv"])
    total_sales_expr = _greatest_expr(["total_sales", "sales", "primary_sales", "purchase_sales"])

    def _sum_metric_expr(expr: str, alias: str) -> str:
        return f"SUM({expr}) as {alias}"

    def _sum_metric(col: str, alias: str) -> str:
        if col in sq_cols:
            return f"SUM(COALESCE(f.{col}, 0)) as {alias}"
        return f"0 as {alias}"

    def _sum_expr(col: str) -> str:
        return f"SUM(COALESCE(f.{col}, 0))" if col in sq_cols else "0"

    split_select_sql = "BOOL_OR(COALESCE(f.split_available, FALSE)) as split_available" if "split_available" in sq_cols else "FALSE as split_available"
    query_expr = "COALESCE(NULLIF(TRIM(CAST(f.query_text AS TEXT)), ''), '(검색어 미제공 영역)')"
    query_provided_select_sql = (
        "BOOL_AND(COALESCE(f.query_provided, TRUE)) as query_provided"
        if "query_provided" in sq_cols
        else f"CASE WHEN {query_expr} IN ('-', '(검색어 미제공 영역)') THEN FALSE ELSE TRUE END as query_provided"
    )
    query_bucket_conditions = ["BOOL_OR(COALESCE(NULLIF(TRIM(CAST(f.query_text AS TEXT)), ''), '-') = '-')"]
    if "query_bucket" in sq_cols:
        query_bucket_conditions.insert(0, "BOOL_OR(COALESCE(f.query_bucket, 'provided') = 'unprovided')")
    if "query_provided" in sq_cols:
        query_bucket_conditions.insert(0, "BOOL_OR(COALESCE(f.query_provided, TRUE) = FALSE)")
    query_bucket_select_sql = "CASE WHEN " + " OR ".join(query_bucket_conditions) + " THEN 'unprovided' ELSE 'provided' END as query_bucket"

    type_where_sql = ""
    type_params = {}
    if table_exists(_engine, "dim_campaign"):
        dim_cols = get_table_columns(_engine, "dim_campaign")
        cp_col = "campaign_tp" if "campaign_tp" in dim_cols else ("campaign_type_label" if "campaign_type_label" in dim_cols else "campaign_type")
        if cp_col in dim_cols:
            raw_type_where, type_params = _build_in_filter(f"c.{cp_col}", _CAMPAIGN_TYPE_ALIASES["쇼핑검색"], "shopping_terms_type")
            if raw_type_where:
                # fact_shopping_query_daily is already a shopping-source table.
                # Keep rows even when dim_campaign is missing/stale, while still excluding
                # non-shopping rows when the campaign type is available.
                type_where_sql = raw_type_where.replace("AND ", f"AND (c.{cp_col} IS NULL OR ", 1) + ")"

    sql = f"""
        SELECT
            f.customer_id,
            f.campaign_id,
            f.adgroup_id,
            f.ad_id,
            c.campaign_name,
            a.adgroup_name,
            {query_expr} AS query_text,
            {query_provided_select_sql},
            {query_bucket_select_sql},
            {_sum_metric_expr(total_conv_expr, "total_conv")},
            {_sum_metric_expr(total_sales_expr, "total_sales")},
            {_sum_metric_expr(purchase_conv_expr, "purchase_conv")},
            {_sum_metric_expr(purchase_sales_expr, "purchase_sales")},
            {_sum_metric("cart_conv", "cart_conv")},
            {_sum_metric("cart_sales", "cart_sales")},
            {_sum_metric("wishlist_conv", "wishlist_conv")},
            {_sum_metric("wishlist_sales", "wishlist_sales")},
            {split_select_sql}
        FROM fact_shopping_query_daily f
        LEFT JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id
        LEFT JOIN dim_adgroup a ON f.adgroup_id = a.adgroup_id AND f.customer_id = a.customer_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid} {type_where_sql}
        GROUP BY f.customer_id, f.campaign_id, f.adgroup_id, f.ad_id, c.campaign_name, a.adgroup_name, {query_expr}
        -- 성과가 있는 검색어 우선 정렬 및 로드 수 제한 최적화
        HAVING SUM({total_sales_expr}) > 0
            OR SUM({total_conv_expr}) > 0
            OR SUM({purchase_sales_expr}) > 0
            OR SUM({purchase_conv_expr}) > 0
        ORDER BY SUM({purchase_conv_expr}) DESC, SUM({purchase_sales_expr}) DESC, SUM({total_conv_expr}) DESC
        LIMIT 5000
    """
    df = sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params, **type_params})
    return df


@st.cache_data(ttl=DASHBOARD_DATA_CACHE_TTL, max_entries=40, show_spinner=False)
def query_shopping_placement_performance(_engine, d1: date, d2: date, cids: tuple) -> pd.DataFrame:
    if not table_exists(_engine, "fact_adgroup_placement_daily"):
        return pd.DataFrame()
    cols = set(get_table_columns(_engine, "fact_adgroup_placement_daily"))
    required = {"customer_id", "campaign_id", "adgroup_id", "device_name", "placement_type"}
    if not required.issubset(cols):
        return pd.DataFrame()

    cids_tuple = _normalize_filter_values(cids)
    where_cid, cid_params = _build_in_filter("f.customer_id", cids_tuple, "shopping_place_cid")

    def _sum_col(col: str, alias: str) -> str:
        return f"SUM(COALESCE(f.{col}, 0)) AS {alias}" if col in cols else f"0 AS {alias}"

    type_source_sql = "NULL"
    type_filter_source_sql = ""
    type_params = {}
    fact_type_expr = "NULLIF(CAST(f.campaign_type AS TEXT), '')" if "campaign_type" in cols else "NULL"
    if table_exists(_engine, "dim_campaign"):
        dim_cols = get_table_columns(_engine, "dim_campaign")
        cp_col = "campaign_tp" if "campaign_tp" in dim_cols else ("campaign_type_label" if "campaign_type_label" in dim_cols else "campaign_type")
        if cp_col in dim_cols:
            type_source_sql = f"COALESCE(NULLIF(CAST(c.{cp_col} AS TEXT), ''), {fact_type_expr})"
            type_filter_source_sql = type_source_sql
    if not type_filter_source_sql and "campaign_type" in cols:
        type_source_sql = fact_type_expr
        type_filter_source_sql = fact_type_expr
    type_label_sql = _budget_campaign_type_case_sql(type_source_sql)
    type_where_sql = ""
    if type_filter_source_sql:
        type_where_sql, type_params = _build_in_filter(type_filter_source_sql, _BUDGET_CAMPAIGN_TYPE_FILTER_VALUES, "placement_campaign_type")

    sql = f"""
        SELECT
            f.customer_id,
            f.campaign_id,
            f.adgroup_id,
            {type_label_sql} AS campaign_type_label,
            COALESCE(c.campaign_name, f.campaign_id) AS campaign_name,
            COALESCE(a.adgroup_name, f.adgroup_id) AS adgroup_name,
            COALESCE(NULLIF(TRIM(f.device_name), ''), 'UNKNOWN') AS device_name,
            COALESCE(NULLIF(TRIM(f.placement_type), ''), 'UNKNOWN') AS placement_type,
            {_sum_col("imp", "imp")},
            {_sum_col("clk", "clk")},
            {_sum_col("cost", "cost")},
            {_sum_col("conv", "conv")},
            {_sum_col("sales", "sales")},
            {_sum_col("purchase_conv", "purchase_conv")},
            {_sum_col("purchase_sales", "purchase_sales")}
        FROM fact_adgroup_placement_daily f
        LEFT JOIN dim_campaign c ON f.campaign_id = c.campaign_id AND f.customer_id = c.customer_id
        LEFT JOIN dim_adgroup a ON f.adgroup_id = a.adgroup_id AND f.customer_id = a.customer_id
        WHERE f.dt BETWEEN :d1 AND :d2 {where_cid} {type_where_sql}
        GROUP BY
            f.customer_id,
            f.campaign_id,
            f.adgroup_id,
            {type_label_sql},
            COALESCE(c.campaign_name, f.campaign_id),
            COALESCE(a.adgroup_name, f.adgroup_id),
            COALESCE(NULLIF(TRIM(f.device_name), ''), 'UNKNOWN'),
            COALESCE(NULLIF(TRIM(f.placement_type), ''), 'UNKNOWN')
        HAVING SUM(COALESCE(f.imp, 0)) > 0
            OR SUM(COALESCE(f.clk, 0)) > 0
            OR SUM(COALESCE(f.cost, 0)) > 0
            OR SUM(COALESCE(f.conv, 0)) > 0
            OR SUM(COALESCE(f.purchase_conv, 0)) > 0
        ORDER BY SUM(COALESCE(f.cost, 0)) DESC, SUM(COALESCE(f.clk, 0)) DESC
        LIMIT 10000
    """
    return sql_read(_engine, sql, {"d1": str(d1), "d2": str(d2), **cid_params, **type_params})
