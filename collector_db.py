# -*- coding: utf-8 -*-
from __future__ import annotations

import time
import os
from typing import Any, Dict, List

import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool, QueuePool

from device_collector_helpers import ensure_device_tables


def _log(msg: str) -> None:
    print(msg, flush=True)


def _exc_label(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _safe_rollback(raw_conn, *, ctx: str = ""):
    if not raw_conn:
        return
    try:
        if getattr(raw_conn, "closed", False):
            return
        raw_conn.rollback()
    except Exception as rollback_exc:
        if "closed" in str(rollback_exc).lower():
            return
        extra = f" | {ctx}" if ctx else ""
        _log(f"⚠️ 롤백 실패{extra} | {_exc_label(rollback_exc)}")


def _safe_close(resource, *, label: str, ctx: str = ""):
    if not resource:
        return
    try:
        resource.close()
    except Exception as close_exc:
        if "closed" in str(close_exc).lower():
            return
        extra = f" | {ctx}" if ctx else ""
        _log(f"⚠️ {label} close 실패{extra} | {_exc_label(close_exc)}")


def _log_retry_failure(action: str, attempt: int, total: int, exc: Exception, *, ctx: str = ""):
    extra = f" | {ctx}" if ctx else ""
    _log(f"⚠️ {action} 실패 {attempt}/{total}{extra} | {_exc_label(exc)}")


def _log_best_effort_failure(action: str, exc: Exception, *, ctx: str = ""):
    extra = f" | {ctx}" if ctx else ""
    _log(f"⚠️ {action} 무시됨{extra} | {_exc_label(exc)}")


def _raise_retry_failure(action: str, exc: Exception | None, *, ctx: str = ""):
    msg = f"{action} 최종 실패"
    if ctx:
        msg += f" | {ctx}"
    if exc is not None:
        msg += f" | {_exc_label(exc)}"
        raise RuntimeError(msg) from exc
    raise RuntimeError(msg)


def _best_effort_dispose(engine: Engine, *, ctx: str = "") -> None:
    try:
        engine.dispose()
    except Exception as dispose_exc:
        _log_best_effort_failure("engine.dispose", dispose_exc, ctx=ctx)


class _TableWriteSpec:
    def __init__(self, chunk_rows: int, page_size: int, statement_timeout_ms: int):
        self.chunk_rows = max(1, int(chunk_rows))
        self.page_size = max(1, int(page_size))
        self.statement_timeout_ms = max(30000, int(statement_timeout_ms))


def _table_write_spec(table: str, row_count: int) -> _TableWriteSpec:
    t = str(table or '').lower()
    if t == 'dim_keyword':
        if row_count >= 50000:
            return _TableWriteSpec(500, 100, 900000)
        if row_count >= 10000:
            return _TableWriteSpec(750, 150, 900000)
        return _TableWriteSpec(1000, 200, 900000)
    if t in {'dim_campaign', 'dim_adgroup', 'dim_ad'}:
        return _TableWriteSpec(1000, 250, 600000)
    if t.startswith('fact_'):
        return _TableWriteSpec(1000, 250, 600000)
    return _TableWriteSpec(1000, 250, 600000)


def _iter_chunks(seq, size: int):
    for i in range(0, len(seq), size):
        yield i, seq[i:i+size]


def _execute_values_in_chunks(engine: Engine, sql: str, tuples: list[tuple], *, table: str, ctx: str) -> None:
    if not tuples:
        return
    spec = _table_write_spec(table, len(tuples))
    total_chunks = (len(tuples) + spec.chunk_rows - 1) // spec.chunk_rows
    for chunk_idx0, chunk in _iter_chunks(tuples, spec.chunk_rows):
        chunk_no = chunk_idx0 // spec.chunk_rows + 1
        chunk_ctx = f"{ctx} chunk={chunk_no}/{total_chunks} chunk_rows={len(chunk)} page_size={spec.page_size}"
        last_err: Exception | None = None
        for attempt in range(1, 4):
            raw_conn = None
            cur = None
            try:
                raw_conn = engine.raw_connection()
                cur = raw_conn.cursor()
                cur.execute(f"SET statement_timeout TO {spec.statement_timeout_ms}")
                psycopg2.extras.execute_values(cur, sql, chunk, page_size=spec.page_size)
                raw_conn.commit()
                break
            except Exception as e:
                last_err = e
                _safe_rollback(raw_conn, ctx=chunk_ctx)
                _best_effort_dispose(engine, ctx=chunk_ctx)
                _log_retry_failure("DB 적재", attempt, 3, e, ctx=chunk_ctx)
                time.sleep(min(8, 2 + attempt))
            finally:
                _safe_close(cur, label="cursor", ctx=chunk_ctx)
                _safe_close(raw_conn, label="connection", ctx=chunk_ctx)
        else:
            _raise_retry_failure("DB 적재", last_err, ctx=chunk_ctx)


def _filter_nonzero_media_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        imp = int(round(float(r.get("imp", 0) or 0)))
        clk = int(round(float(r.get("clk", 0) or 0)))
        cost = int(round(float(r.get("cost", 0) or 0)))
        conv = float(r.get("conv", 0) or 0.0)
        sales = int(round(float(r.get("sales", 0) or 0)))
        if any([imp != 0, clk != 0, cost != 0, conv != 0.0, sales != 0]):
            out.append(r)
    return out


def get_engine(db_url: str) -> Engine:
    if not db_url:
        raise RuntimeError("DATABASE_URL이 설정되지 않았습니다. collector.py는 실제 DB 연결이 필요합니다.")
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    import os
    use_queue_pool = str(os.getenv("COLLECTOR_USE_QUEUEPOOL", "0") or "0").strip().lower() in {"1", "true", "yes", "y"}
    pool_size = max(1, int(os.getenv("COLLECTOR_DB_POOL_SIZE", "4") or 4))
    max_overflow = max(0, int(os.getenv("COLLECTOR_DB_MAX_OVERFLOW", "4") or 4))
    pool_timeout = max(5, int(os.getenv("COLLECTOR_DB_POOL_TIMEOUT", "30") or 30))
    pool_recycle = max(60, int(os.getenv("COLLECTOR_DB_POOL_RECYCLE", "300") or 300))
    connect_args = {
        "options": "-c lock_timeout=10000 -c statement_timeout=300000",
        "connect_timeout": int(os.getenv("COLLECTOR_DB_CONNECT_TIMEOUT", "15") or 15),
        "keepalives": 1,
        "keepalives_idle": int(os.getenv("COLLECTOR_DB_KEEPALIVES_IDLE", "30") or 30),
        "keepalives_interval": int(os.getenv("COLLECTOR_DB_KEEPALIVES_INTERVAL", "10") or 10),
        "keepalives_count": int(os.getenv("COLLECTOR_DB_KEEPALIVES_COUNT", "5") or 5),
    }
    if use_queue_pool:
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
    return create_engine(
        db_url,
        poolclass=NullPool,
        pool_pre_ping=True,
        connect_args=connect_args,
        future=True,
    )


def ensure_column(engine: Engine, table: str, column: str, datatype: str):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {datatype}"))
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "duplicate column" in msg or 'duplicatecolumn' in msg:
            return
        _log_best_effort_failure("ensure_column", e, ctx=f"table={table} column={column}")


def ensure_tables(engine: Engine):
    fast_mode = str(os.getenv("COLLECTOR_FAST_MODE", "0") or "0").strip().lower() in {"1", "true", "yes", "y"}
    for attempt in range(3):
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_account (customer_id TEXT PRIMARY KEY, account_name TEXT)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_campaign (customer_id TEXT, campaign_id TEXT, campaign_name TEXT, campaign_tp TEXT, status TEXT, PRIMARY KEY(customer_id, campaign_id))"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_adgroup (customer_id TEXT, adgroup_id TEXT, adgroup_name TEXT, campaign_id TEXT, status TEXT, PRIMARY KEY(customer_id, adgroup_id))"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS dim_keyword (customer_id TEXT, keyword_id TEXT, adgroup_id TEXT, keyword TEXT, status TEXT, PRIMARY KEY(customer_id, keyword_id))"))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS dim_ad (customer_id TEXT, ad_id TEXT, adgroup_id TEXT, ad_name TEXT, status TEXT, ad_title TEXT, ad_desc TEXT, pc_landing_url TEXT, mobile_landing_url TEXT, creative_text TEXT, image_url TEXT, PRIMARY KEY(customer_id, ad_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_campaign_daily (dt DATE, customer_id TEXT, campaign_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, campaign_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_keyword_daily (dt DATE, customer_id TEXT, keyword_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, keyword_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_ad_daily (dt DATE, customer_id TEXT, ad_id TEXT, imp BIGINT, clk BIGINT, cost BIGINT, conv DOUBLE PRECISION, sales BIGINT DEFAULT 0, roas DOUBLE PRECISION DEFAULT 0, avg_rnk DOUBLE PRECISION DEFAULT 0, PRIMARY KEY(dt, customer_id, ad_id))"""))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_shopping_query_daily (
                    dt DATE,
                    customer_id TEXT,
                    campaign_id TEXT,
                    adgroup_id TEXT,
                    ad_id TEXT,
                    query_text TEXT,
                    total_conv DOUBLE PRECISION,
                    total_sales BIGINT DEFAULT 0,
                    purchase_conv DOUBLE PRECISION,
                    purchase_sales BIGINT DEFAULT 0,
                    cart_conv DOUBLE PRECISION,
                    cart_sales BIGINT DEFAULT 0,
                    wishlist_conv DOUBLE PRECISION,
                    wishlist_sales BIGINT DEFAULT 0,
                    split_available BOOLEAN,
                    data_source TEXT,
                    PRIMARY KEY(dt, customer_id, adgroup_id, ad_id, query_text)
                )"""))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS fact_campaign_off_log (
                        dt DATE,
                        customer_id TEXT,
                        campaign_id TEXT,
                        off_time TEXT,
                        PRIMARY KEY(dt, customer_id, campaign_id)
                    )
                """))
                conn.execute(text("""CREATE TABLE IF NOT EXISTS fact_media_daily (
                    dt DATE,
                    customer_id TEXT,
                    campaign_type TEXT,
                    media_name TEXT,
                    region_name TEXT,
                    device_name TEXT DEFAULT '전체',
                    imp BIGINT,
                    clk BIGINT,
                    cost BIGINT,
                    conv DOUBLE PRECISION,
                    sales BIGINT DEFAULT 0,
                    data_source TEXT,
                    source_report TEXT,
                    PRIMARY KEY(dt, customer_id, campaign_type, media_name, region_name, device_name)
                )"""))
                conn.execute(text("""
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
                """))

            if not fast_mode:
                ensure_column(engine, "dim_ad", "ad_title", "TEXT")
                ensure_column(engine, "dim_ad", "ad_desc", "TEXT")
                ensure_column(engine, "dim_ad", "pc_landing_url", "TEXT")
                ensure_column(engine, "dim_ad", "mobile_landing_url", "TEXT")
                ensure_column(engine, "dim_ad", "creative_text", "TEXT")
                ensure_column(engine, "dim_ad", "image_url", "TEXT")

                for table in ["fact_campaign_daily", "fact_keyword_daily", "fact_ad_daily"]:
                    ensure_column(engine, table, "purchase_conv", "DOUBLE PRECISION")
                    ensure_column(engine, table, "purchase_sales", "BIGINT")
                    ensure_column(engine, table, "purchase_roas", "DOUBLE PRECISION")
                    ensure_column(engine, table, "cart_conv", "DOUBLE PRECISION")
                    ensure_column(engine, table, "cart_sales", "BIGINT")
                    ensure_column(engine, table, "cart_roas", "DOUBLE PRECISION")
                    ensure_column(engine, table, "wishlist_conv", "DOUBLE PRECISION")
                    ensure_column(engine, table, "wishlist_sales", "BIGINT")
                    ensure_column(engine, table, "wishlist_roas", "DOUBLE PRECISION")
                    ensure_column(engine, table, "primary_conv", "DOUBLE PRECISION")
                    ensure_column(engine, table, "primary_sales", "BIGINT")
                    ensure_column(engine, table, "primary_roas", "DOUBLE PRECISION")
                    ensure_column(engine, table, "split_available", "BOOLEAN")
                    ensure_column(engine, table, "data_source", "TEXT")

                ensure_column(engine, "fact_media_daily", "data_source", "TEXT")
                ensure_column(engine, "fact_media_daily", "source_report", "TEXT")

            ensure_device_tables(engine)
            break
        except Exception as e:
            time.sleep(3)
            if attempt == 2:
                raise e



def _get_table_columns(engine: Engine, table: str) -> list[str]:
    try:
        with engine.connect() as conn:
            res = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=:table
            """), {"table": str(table)})
            return [r[0] for r in res]
    except Exception:
        return []


def _pick_expr(cols: list[str], candidates: list[str], alias: str = 'f', default: str = '0') -> str:
    picked = [f"{alias}.{c}" for c in candidates if c in cols]
    if not picked:
        return default
    if len(picked) == 1:
        return f"COALESCE({picked[0]}, 0)"
    return f"COALESCE({', '.join(picked)}, 0)"


def refresh_overview_report_source_cache(engine: Engine, customer_id: str, d1, d2) -> None:
    ensure_tables(engine)
    kw_cols = _get_table_columns(engine, 'fact_keyword_daily')
    sq_cols = _get_table_columns(engine, 'fact_shopping_query_daily')
    camp_cols = _get_table_columns(engine, 'dim_campaign')
    cp_col = 'campaign_tp' if 'campaign_tp' in camp_cols else ('campaign_type' if 'campaign_type' in camp_cols else None)
    camp_type_sql = f"COALESCE(c.{cp_col}, 'WEB_SITE')" if cp_col else "'WEB_SITE'"
    kw_conv_expr = _pick_expr(kw_cols, ['primary_conv', 'purchase_conv', 'conv'], alias='f')
    kw_sales_expr = _pick_expr(kw_cols, ['primary_sales', 'purchase_sales', 'sales'], alias='f')
    sq_conv_expr = _pick_expr(sq_cols, ['total_conv', 'purchase_conv'], alias='f')
    sq_sales_expr = _pick_expr(sq_cols, ['total_sales', 'purchase_sales'], alias='f')
    ctx = f"customer_id={customer_id} d1={d1} d2={d2}"
    last_err = None
    for attempt in range(1, 4):
        try:
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM overview_report_source_cache WHERE customer_id=:cid AND dt BETWEEN :d1 AND :d2"), {"cid": str(customer_id), "d1": d1, "d2": d2})
                conn.execute(text(f"""
                    WITH agg AS (
                        SELECT
                            f.dt,
                            f.customer_id,
                            {camp_type_sql} AS campaign_type,
                            COALESCE(k.keyword, '') AS source_text,
                            SUM({kw_conv_expr}) AS metric_value,
                            SUM({kw_sales_expr}) AS sales_value
                        FROM fact_keyword_daily f
                        JOIN dim_keyword k ON f.customer_id=k.customer_id AND f.keyword_id=k.keyword_id
                        LEFT JOIN dim_adgroup a ON k.customer_id=a.customer_id AND k.adgroup_id=a.adgroup_id
                        LEFT JOIN dim_campaign c ON a.customer_id=c.customer_id AND a.campaign_id=c.campaign_id
                        WHERE f.customer_id=:cid AND f.dt BETWEEN :d1 AND :d2
                        GROUP BY f.dt, f.customer_id, {camp_type_sql}, COALESCE(k.keyword, '')
                    ), ranked AS (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY dt, customer_id, campaign_type ORDER BY metric_value DESC, sales_value DESC, source_text) AS rn
                        FROM agg
                        WHERE COALESCE(source_text, '') <> '' AND (metric_value > 0 OR sales_value > 0)
                    )
                    INSERT INTO overview_report_source_cache (
                        dt, customer_id, campaign_type, source_kind, source_text, metric_value, sales_value, rank_no
                    )
                    SELECT dt, customer_id, campaign_type, 'powerlink_keyword', source_text, metric_value, sales_value, rn
                    FROM ranked
                    WHERE rn <= 50 AND campaign_type <> 'SHOPPING'
                """), {"cid": str(customer_id), "d1": d1, "d2": d2})
                conn.execute(text(f"""
                    WITH agg AS (
                        SELECT
                            f.dt,
                            f.customer_id,
                            'SHOPPING' AS campaign_type,
                            COALESCE(f.query_text, '') AS source_text,
                            SUM({sq_conv_expr}) AS metric_value,
                            SUM({sq_sales_expr}) AS sales_value
                        FROM fact_shopping_query_daily f
                        WHERE f.customer_id=:cid AND f.dt BETWEEN :d1 AND :d2
                        GROUP BY f.dt, f.customer_id, COALESCE(f.query_text, '')
                    ), ranked AS (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY dt, customer_id, campaign_type ORDER BY metric_value DESC, sales_value DESC, source_text) AS rn
                        FROM agg
                        WHERE COALESCE(source_text, '') <> '' AND (metric_value > 0 OR sales_value > 0)
                    )
                    INSERT INTO overview_report_source_cache (
                        dt, customer_id, campaign_type, source_kind, source_text, metric_value, sales_value, rank_no
                    )
                    SELECT dt, customer_id, campaign_type, 'shopping_query', source_text, metric_value, sales_value, rn
                    FROM ranked
                    WHERE rn <= 50
                """), {"cid": str(customer_id), "d1": d1, "d2": d2})
            return
        except Exception as e:
            last_err = e
            _log_retry_failure('overview report cache refresh', attempt, 3, e, ctx=ctx)
            _best_effort_dispose(engine, ctx=ctx)
            time.sleep(min(5, 1 + attempt))
    _raise_retry_failure('overview report cache refresh', last_err, ctx=ctx)

def upsert_many(engine: Engine, table: str, rows: List[Dict[str, Any]], pk_cols: List[str]):
    if not rows:
        return
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep="last").sort_values(by=pk_cols).astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
    conflict_clause = (
        f'ON CONFLICT ({pk_str}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
        if update_cols else
        f'ON CONFLICT ({pk_str}) DO NOTHING'
    )
    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'

    tuples = list(df.itertuples(index=False, name=None))
    ctx = f"table={table} rows={len(tuples)} pk={pk_cols}"
    _execute_values_in_chunks(engine, sql, tuples, table=table, ctx=ctx)


def clear_fact_range(engine: Engine, table: str, customer_id: str, d1):
    last_err: Exception | None = None
    ctx = f"table={table} cid={customer_id} dt={d1}"
    for attempt in range(1, 4):
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {table} WHERE customer_id=:cid AND dt = :dt"), {"cid": str(customer_id), "dt": d1})
            return
        except Exception as e:
            last_err = e
            _log_retry_failure("fact 범위 삭제", attempt, 3, e, ctx=ctx)
            time.sleep(3)
    _raise_retry_failure("fact 범위 삭제", last_err, ctx=ctx)


def clear_fact_scope(engine: Engine, table: str, customer_id: str, d1, pk: str, ids: List[str]):
    ids = [str(x).strip() for x in (ids or []) if str(x).strip()]
    if not ids:
        return
    last_err: Exception | None = None
    ctx = f"table={table} cid={customer_id} dt={d1} pk={pk} ids={len(ids)}"
    for attempt in range(1, 4):
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(f'DELETE FROM {table} WHERE customer_id=:cid AND dt=:dt AND {pk} = ANY(:ids)'),
                    {"cid": str(customer_id), "dt": d1, "ids": ids},
                )
            return
        except Exception as e:
            last_err = e
            _log_retry_failure("fact 범위 삭제(scope)", attempt, 3, e, ctx=ctx)
            time.sleep(3)
    _raise_retry_failure("fact 범위 삭제(scope)", last_err, ctx=ctx)


def replace_fact_range(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1):
    clear_fact_range(engine, table, customer_id, d1)
    if not rows:
        return

    pk = "campaign_id" if "campaign" in table else ("keyword_id" if "keyword" in table else "ad_id")
    df = pd.DataFrame(rows).drop_duplicates(subset=["dt", "customer_id", pk], keep="last").sort_values(by=["dt", "customer_id", pk]).astype(object).where(pd.notnull, None)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in ["dt", "customer_id", pk]]
    col_names = ", ".join([f'"{c}"' for c in cols])

    if update_cols:
        conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
    else:
        conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO NOTHING'

    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    ctx = f"table={table} rows={len(tuples)}"
    _execute_values_in_chunks(engine, sql, tuples, table=table, ctx=ctx)


def replace_query_fact_range(engine: Engine, rows: List[Dict[str, Any]], customer_id: str, d1):
    table = "fact_shopping_query_daily"
    clear_fact_range(engine, table, customer_id, d1)
    if not rows:
        return

    pk_cols = ["dt", "customer_id", "adgroup_id", "ad_id", "query_text"]
    df = pd.DataFrame(rows).drop_duplicates(subset=pk_cols, keep="last").sort_values(by=pk_cols).astype(object).where(pd.notnull, None)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])

    if update_cols:
        conflict_clause = f'ON CONFLICT (dt, customer_id, adgroup_id, ad_id, query_text) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
    else:
        conflict_clause = f'ON CONFLICT (dt, customer_id, adgroup_id, ad_id, query_text) DO NOTHING'

    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    ctx = f"table={table} rows={len(tuples)}"
    _execute_values_in_chunks(engine, sql, tuples, table=table, ctx=ctx)


def replace_fact_scope(engine: Engine, table: str, rows: List[Dict[str, Any]], customer_id: str, d1, pk: str, ids: List[str]):
    clear_fact_scope(engine, table, customer_id, d1, pk, ids)
    if not rows:
        return

    df = pd.DataFrame(rows).drop_duplicates(subset=["dt", "customer_id", pk], keep="last").sort_values(by=["dt", "customer_id", pk]).astype(object).where(pd.notnull, None)

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in ["dt", "customer_id", pk]]
    col_names = ", ".join([f'"{c}"' for c in cols])

    if update_cols:
        conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
    else:
        conflict_clause = f'ON CONFLICT (dt, customer_id, {pk}) DO NOTHING'

    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    ctx = f"table={table} rows={len(tuples)}"
    _execute_values_in_chunks(engine, sql, tuples, table=table, ctx=ctx)


def _get_fact_media_daily_conflict_cols(engine: Engine) -> List[str]:
    expected = ["dt", "customer_id", "campaign_type", "media_name", "region_name", "device_name"]
    legacy = ["dt", "customer_id", "campaign_type", "media_name", "region_name"]
    sql = text("""
        SELECT
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            kcu.ordinal_position
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.table_name = 'fact_media_daily'
          AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
        ORDER BY
            CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN 0 ELSE 1 END,
            tc.constraint_name,
            kcu.ordinal_position
    """)
    try:
        with engine.begin() as conn:
            rows = conn.execute(sql).mappings().all()
    except Exception as e:
        _log(f"⚠️ fact_media_daily 제약조건 조회 실패 → 기본 PK 가정 사용 | {type(e).__name__}: {e}")
        return expected

    grouped: Dict[tuple[str, str], List[str]] = {}
    for row in rows:
        key = (str(row.get("constraint_name") or ""), str(row.get("constraint_type") or ""))
        grouped.setdefault(key, []).append(str(row.get("column_name") or "").strip())

    ordered_candidates: List[List[str]] = []
    for (_, constraint_type), cols in grouped.items():
        if constraint_type == "PRIMARY KEY":
            ordered_candidates.insert(0, cols)
        else:
            ordered_candidates.append(cols)

    for cols in ordered_candidates:
        if cols == expected:
            return expected
    for cols in ordered_candidates:
        if cols == legacy:
            return legacy
    for cols in ordered_candidates:
        if cols and all(c in cols for c in legacy):
            _log(f"⚠️ fact_media_daily 예상 외 제약조건 감지 | columns={cols}")
            return cols

    _log("⚠️ fact_media_daily PK/UNIQUE 제약조건을 찾지 못해 기본 PK 가정 사용")
    return expected


def _prepare_media_fact_rows_for_conflict(df: pd.DataFrame, conflict_cols: List[str]) -> pd.DataFrame:
    expected = ["dt", "customer_id", "campaign_type", "media_name", "region_name", "device_name"]
    legacy = ["dt", "customer_id", "campaign_type", "media_name", "region_name"]
    numeric_cols = ["imp", "clk", "cost", "conv", "sales"]

    if "device_name" not in df.columns:
        df["device_name"] = "전체"
    df["device_name"] = df["device_name"].map(lambda x: str(x).strip() if x is not None else "").replace("", "전체")

    if conflict_cols == expected:
        return df.drop_duplicates(subset=conflict_cols, keep="last").sort_values(by=conflict_cols)

    if conflict_cols == legacy:
        _log("⚠️ fact_media_daily가 구 PK(device_name 제외) 스키마입니다. device_name을 '전체'로 병합해 임시 적재합니다. 스키마 마이그레이션 후 백필이 필요합니다.")
        work = df.copy()
        work["device_name"] = "전체"
        if "data_source" in work.columns:
            work["data_source"] = "legacy_pk_schema_aggregated"
        if "source_report" in work.columns:
            work["source_report"] = work["source_report"].fillna("AD").replace("", "AD")

        agg_spec: Dict[str, Any] = {}
        for col in work.columns:
            if col in legacy:
                continue
            if col in numeric_cols:
                agg_spec[col] = "sum"
            else:
                agg_spec[col] = "last"
        grouped = work.groupby(legacy, dropna=False, as_index=False).agg(agg_spec)
        ordered = legacy + [c for c in work.columns if c not in legacy]
        grouped = grouped[ordered]
        return grouped.sort_values(by=legacy)

    _log(f"⚠️ fact_media_daily 예상 외 충돌키 사용 | {conflict_cols}")
    use_cols = [c for c in conflict_cols if c in df.columns]
    if not use_cols:
        use_cols = expected
    return df.drop_duplicates(subset=use_cols, keep="last").sort_values(by=use_cols)


def replace_media_fact_range(engine: Engine, rows: List[Dict[str, Any]], customer_id: str, d1, scoped_campaign_types: List[str] | None = None):
    table = "fact_media_daily"
    pk_cols = _get_fact_media_daily_conflict_cols(engine)
    input_rows = len(rows or [])
    rows = _filter_nonzero_media_rows(rows or [])
    dropped_zero_rows = max(0, input_rows - len(rows))
    if dropped_zero_rows:
        _log(f"ℹ️ fact_media_daily 0성과 행 제외 | cid={customer_id} dt={d1} dropped={dropped_zero_rows} kept={len(rows)}")
    last_delete_err: Exception | None = None
    delete_sql = text(
        f"DELETE FROM {table} WHERE customer_id=:cid AND dt=:dt" + (" AND campaign_type = ANY(:types)" if scoped_campaign_types else "")
    )

    for attempt in range(1, 4):
        try:
            with engine.begin() as conn:
                conn.execute(delete_sql, {"cid": str(customer_id), "dt": d1, "types": scoped_campaign_types or []})
            last_delete_err = None
            break
        except Exception as e:
            last_delete_err = e
            _log(f"⚠️ fact_media_daily 삭제 실패 {attempt}/3 | cid={customer_id} dt={d1} pk={pk_cols} | {type(e).__name__}: {e}")
            time.sleep(2)
    if last_delete_err is not None:
        raise RuntimeError(f"fact_media_daily 삭제 실패 | cid={customer_id} dt={d1} pk={pk_cols} | {type(last_delete_err).__name__}: {last_delete_err}") from last_delete_err

    if not rows:
        reason = "all_zero_filtered" if input_rows else "empty"
        _log(f"ℹ️ fact_media_daily 적재 대상 없음 | cid={customer_id} dt={d1} reason={reason} input_rows={input_rows}")
        return 0

    df = pd.DataFrame(rows).astype(object).where(pd.notnull, None)
    df = _prepare_media_fact_rows_for_conflict(df, pk_cols).astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    conflict_cols_sql = ", ".join(pk_cols)
    conflict_clause = (
        f'ON CONFLICT ({conflict_cols_sql}) DO UPDATE SET ' + ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
        if update_cols else
        f'ON CONFLICT ({conflict_cols_sql}) DO NOTHING'
    )
    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    last_upsert_err: Exception | None = None
    for attempt in range(1, 4):
        raw_conn = None
        cur = None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            _log(f"✅ fact_media_daily 적재 완료 | cid={customer_id} dt={d1} rows={len(df)} pk={pk_cols}")
            return len(df)
        except Exception as e:
            last_upsert_err = e
            if raw_conn:
                _safe_rollback(raw_conn, ctx=f"fact_media_daily upsert cid={customer_id} dt={d1}")
            _log(f"⚠️ fact_media_daily 적재 실패 {attempt}/3 | cid={customer_id} dt={d1} rows={len(df)} pk={pk_cols} | {type(e).__name__}: {e}")
            time.sleep(2)
        finally:
            _safe_close(cur, label="cursor", ctx=f"fact_media_daily upsert cid={customer_id} dt={d1}")
            _safe_close(raw_conn, label="connection", ctx=f"fact_media_daily upsert cid={customer_id} dt={d1}")
    raise RuntimeError(f"fact_media_daily 적재 최종 실패 | cid={customer_id} dt={d1} rows={len(df)} pk={pk_cols} | {type(last_upsert_err).__name__}: {last_upsert_err}") from last_upsert_err
