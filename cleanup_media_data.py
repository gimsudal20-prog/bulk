from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool


def _database_url() -> str:
    load_dotenv()
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise SystemExit("DATABASE_URL is required")
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return db_url


def main() -> int:
    parser = argparse.ArgumentParser(description="Remove retired media placement facts.")
    parser.add_argument("--execute", action="store_true", help="Drop fact_media_daily. Without this flag, only print row counts.")
    args = parser.parse_args()

    engine = create_engine(_database_url(), poolclass=NullPool, future=True)
    with engine.begin() as conn:
        exists = bool(
            conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = current_schema()
                          AND table_name = 'fact_media_daily'
                    )
                    """
                )
            ).scalar()
        )
        if not exists:
            print("fact_media_daily does not exist")
            return 0

        rows = conn.execute(text("SELECT COUNT(*) FROM fact_media_daily")).scalar_one()
        if not args.execute:
            print(f"dry-run: fact_media_daily rows={rows}. Re-run with --execute to drop the table.")
            return 0

        conn.execute(text("DROP TABLE IF EXISTS fact_media_daily"))
        print(f"dropped fact_media_daily (previous rows={rows})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
