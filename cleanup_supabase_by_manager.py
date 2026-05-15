#!/usr/bin/env python3
"""
Supabase/Postgres cleanup helper for DA_ads.

기본은 DRY-RUN입니다. 실제 삭제는 --execute 옵션을 붙였을 때만 진행됩니다.
대상 담당자 기본값: 민아, 미혜, 정인
대상 계정은 account_master.xlsx 기준으로 추출합니다.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

DEFAULT_MANAGERS = ["민아", "미혜", "정인"]
DEFAULT_ACCOUNT_MASTER = "account_master.xlsx"

# customer_id 외 컬럼으로 연결되는 예외성 테이블 처리
SPECIAL_TABLES = {
    "fact_bizmoney_group_daily": {
        "id_columns": ["representative_customer_id"],
        "group_key_columns": ["bizmoney_group_key"],
    }
}


def die(msg: str, code: int = 1) -> None:
    print(f"❌ {msg}", file=sys.stderr)
    raise SystemExit(code)


def normalize_col(s: str) -> str:
    return str(s or "").strip().replace(" ", "").replace("_", "").lower()


def find_header_row(raw: pd.DataFrame) -> int:
    for i in range(min(len(raw), 20)):
        vals = {str(v).strip() for v in raw.iloc[i].tolist() if pd.notna(v)}
        if {"담당자", "업체그룹명", "계정표시명"}.issubset(vals):
            return i
    # accounts.xlsx 형태 대응
    for i in range(min(len(raw), 20)):
        vals = {str(v).strip() for v in raw.iloc[i].tolist() if pd.notna(v)}
        if {"담당자", "업체명"}.issubset(vals) and ("커스텀 ID" in vals or "커스텀ID" in vals):
            return i
    die("엑셀에서 담당자/계정 컬럼 헤더를 찾지 못했습니다.")


def load_target_accounts(path: Path, managers: list[str]) -> pd.DataFrame:
    if not path.exists():
        die(f"계정 마스터 파일이 없습니다: {path}")

    raw = pd.read_excel(path, header=None, dtype=str).fillna("")
    header_row = find_header_row(raw)
    df = pd.read_excel(path, header=header_row, dtype=str).fillna("")

    colmap = {}
    for c in df.columns:
        n = normalize_col(c)
        if n in {"담당자", "manager", "owner"}:
            colmap[c] = "manager"
        elif n in {"업체그룹명", "업체명", "clientgroupname"}:
            colmap[c] = "client_group_name"
        elif n in {"계정표시명", "accountname"}:
            colmap[c] = "account_name"
        elif n in {"네이버수집id", "커스텀id", "customerid", "customerid", "id"}:
            colmap[c] = "customer_id"
        elif n in {"비즈머니그룹키", "bizmoneygroupkey"}:
            colmap[c] = "bizmoney_group_key"
        elif n in {"사용여부", "useyn"}:
            colmap[c] = "use_yn"
    df = df.rename(columns=colmap)

    required = {"manager", "customer_id"}
    missing = required - set(df.columns)
    if missing:
        die(f"필수 컬럼이 없습니다: {sorted(missing)}")

    for c in ["client_group_name", "account_name", "bizmoney_group_key", "use_yn"]:
        if c not in df.columns:
            df[c] = ""

    managers_set = {m.strip() for m in managers if m.strip()}
    out = df[df["manager"].astype(str).str.strip().isin(managers_set)].copy()
    out["customer_id"] = out["customer_id"].astype(str).str.strip()
    out = out[out["customer_id"].ne("")]
    out = out[["manager", "client_group_name", "account_name", "customer_id", "bizmoney_group_key", "use_yn"]]
    out = out.drop_duplicates(subset=["customer_id", "account_name", "manager"], keep="last")
    return out


def get_engine() -> Engine:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        die("DATABASE_URL 환경변수가 비어 있습니다.")
    if not db_url.lower().startswith(("postgresql", "postgres://")):
        die("DATABASE_URL이 Postgres/Supabase URL 형식이 아닙니다.")
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    if "sslmode=" not in db_url:
        db_url += ("&" if "?" in db_url else "?") + "sslmode=require"
    return create_engine(db_url, poolclass=NullPool, pool_pre_ping=True, future=True)


def list_tables_with_column(engine: Engine, column_name: str) -> list[str]:
    sql = text("""
        SELECT table_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND column_name = :column_name
        ORDER BY table_name
    """)
    with engine.begin() as conn:
        return [r[0] for r in conn.execute(sql, {"column_name": column_name}).fetchall()]


def table_exists(engine: Engine, table_name: str) -> bool:
    sql = text("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = :table_name
        LIMIT 1
    """)
    with engine.begin() as conn:
        return conn.execute(sql, {"table_name": table_name}).first() is not None


def column_exists(engine: Engine, table_name: str, column_name: str) -> bool:
    sql = text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :table_name
          AND column_name = :column_name
        LIMIT 1
    """)
    with engine.begin() as conn:
        return conn.execute(sql, {"table_name": table_name, "column_name": column_name}).first() is not None


def count_by_ids(engine: Engine, table_name: str, column_name: str, ids: list[str]) -> int:
    stmt = text(f'SELECT COUNT(*) FROM public."{table_name}" WHERE "{column_name}" IN :ids')
    stmt = stmt.bindparams(bindparam("ids", expanding=True))
    with engine.begin() as conn:
        return int(conn.execute(stmt, {"ids": ids}).scalar() or 0)


def delete_by_ids(engine: Engine, table_name: str, column_name: str, ids: list[str]) -> int:
    stmt = text(f'DELETE FROM public."{table_name}" WHERE "{column_name}" IN :ids')
    stmt = stmt.bindparams(bindparam("ids", expanding=True))
    with engine.begin() as conn:
        result = conn.execute(stmt, {"ids": ids})
        return int(result.rowcount or 0)


def unique_nonempty(values: Iterable[str]) -> list[str]:
    return sorted({str(v).strip() for v in values if str(v).strip()})


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete Supabase rows for selected managers' accounts.")
    parser.add_argument("--account-master", default=DEFAULT_ACCOUNT_MASTER, help="account_master.xlsx path")
    parser.add_argument("--managers", default=",".join(DEFAULT_MANAGERS), help="comma-separated manager names")
    parser.add_argument("--execute", action="store_true", help="actually delete rows. Without this, dry-run only")
    parser.add_argument("--export-targets", default="cleanup_targets.csv", help="CSV path to save target account list")
    args = parser.parse_args()

    managers = [x.strip() for x in args.managers.split(",") if x.strip()]
    targets = load_target_accounts(Path(args.account_master), managers)
    if targets.empty:
        die(f"대상 담당자 계정을 찾지 못했습니다: {managers}")

    ids = unique_nonempty(targets["customer_id"])
    bizmoney_keys = unique_nonempty(targets["bizmoney_group_key"].tolist() + targets["client_group_name"].tolist())
    targets.to_csv(args.export_targets, index=False, encoding="utf-8-sig")

    print("=" * 80)
    print(f"대상 담당자: {', '.join(managers)}")
    print(f"대상 행 수: {len(targets):,} / 고유 customer_id: {len(ids):,}")
    print(f"대상 목록 저장: {args.export_targets}")
    print("실행 모드:", "DELETE" if args.execute else "DRY-RUN")
    print("=" * 80)
    print(targets.sort_values(["manager", "account_name", "customer_id"]).to_string(index=False))
    print("=" * 80)

    engine = get_engine()

    tables = list_tables_with_column(engine, "customer_id")
    if not tables:
        die("public 스키마에서 customer_id 컬럼이 있는 테이블을 찾지 못했습니다.")

    total = 0
    plan: list[tuple[str, str, int]] = []
    for table in tables:
        cnt = count_by_ids(engine, table, "customer_id", ids)
        plan.append((table, "customer_id", cnt))
        total += cnt

    # 특수 테이블: customer_id가 없지만 계정/그룹 기준으로 연동되는 테이블
    for table, rule in SPECIAL_TABLES.items():
        if not table_exists(engine, table):
            continue
        for col in rule.get("id_columns", []):
            if column_exists(engine, table, col):
                cnt = count_by_ids(engine, table, col, ids)
                plan.append((table, col, cnt))
                total += cnt
        for col in rule.get("group_key_columns", []):
            if column_exists(engine, table, col) and bizmoney_keys:
                cnt = count_by_ids(engine, table, col, bizmoney_keys)
                plan.append((table, col, cnt))
                total += cnt

    print("삭제 예정 row count")
    for table, col, cnt in plan:
        if cnt:
            print(f"- {table}.{col}: {cnt:,}")
    print(f"합계: {total:,}")

    if not args.execute:
        print("\nDRY-RUN만 완료했습니다. 실제 삭제하려면 --execute 옵션을 붙여 다시 실행하세요.")
        return

    print("\n삭제 시작...")
    deleted_total = 0
    for table, col, cnt in plan:
        if cnt <= 0:
            continue
        deleted = delete_by_ids(engine, table, col, bizmoney_keys if col == "bizmoney_group_key" else ids)
        deleted_total += deleted
        print(f"- {table}.{col}: {deleted:,} rows deleted")
    print(f"✅ 삭제 완료: {deleted_total:,} rows")


if __name__ == "__main__":
    main()
