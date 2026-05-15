# -*- coding: utf-8 -*-
"""과거 구간(legacy) 단일 업체 SA 백필 전용 스크립트

용도
- 2026-03-11 이전 구간을 빠르게 백필할 때 사용
- 단일 업체만 대상으로 collector.py를 날짜별 호출
- 기본값은 모든 날짜에서 --skip_dim 사용(가장 빠름)
- 현재 collector.py는 2026-03-11 이전 날짜에 대해 purchase/cart/wishlist 분리 수집을 시도하지 않음

예시
python backfill_single_legacy_sa.py --start 2026-02-01 --end 2026-03-10 --account_name 3skbox
python backfill_single_legacy_sa.py --start 2026-02-01 --end 2026-03-10 --account_name 3skbox --workers 2
python backfill_single_legacy_sa.py --start 2026-02-01 --end 2026-03-10 --account_name 3skbox --sync_dim_first_day
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timedelta, date
from typing import List


def clean(v: str | None) -> str:
    return (v or "").strip()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="시작일 YYYY-MM-DD")
    p.add_argument("--end", required=True, help="종료일 YYYY-MM-DD")
    p.add_argument("--account_name", required=True, help="단일 업체명")
    p.add_argument("--workers", type=int, default=1, help="collector.py workers")
    p.add_argument(
        "--sync_dim_first_day",
        action="store_true",
        help="첫날만 구조 동기화, 이후 날짜는 --skip_dim (기본은 전 기간 skip_dim)",
    )
    p.add_argument("--shopping_only", action="store_true", help="쇼핑검색 캠페인만 백필")
    p.add_argument("--with_shop_ext", action="store_true", help="collector_shop_ext.py도 함께 실행")
    p.add_argument("--shop_ext_bucket", default="shopping", choices=["shopping", "non_shopping", "all"], help="확장소재 수집 버킷")
    args = p.parse_args()
    args.start = clean(args.start)
    args.end = clean(args.end)
    args.account_name = clean(args.account_name)
    return args


def run_cmd(cmd: List[str], label: str, day: str) -> None:
    print(f"▶ [{label}] {day} 실행: {' '.join(cmd)}", flush=True)
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        print(f"⚠️ [{label}] {day} 수집 중 오류 발생", flush=True)


def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def main() -> None:
    args = parse_args()

    api_key = clean(os.getenv("NAVER_ADS_API_KEY") or os.getenv("NAVER_API_KEY"))
    if not api_key:
        print("❌ [FATAL] NAVER_ADS_API_KEY / NAVER_API_KEY 환경변수가 없습니다.", flush=True)
        sys.exit(1)

    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError:
        print("❌ [FATAL] 날짜 형식은 YYYY-MM-DD 여야 합니다.", flush=True)
        sys.exit(1)

    if start_date > end_date:
        print("❌ [FATAL] 시작일이 종료일보다 늦습니다.", flush=True)
        sys.exit(1)

    if args.shopping_only:
        if args.workers != 1:
            print(f"⚠️ 쇼핑검색 백필은 workers=1로 고정합니다. (입력값 {args.workers} → 1)", flush=True)
        args.workers = 1
        args.with_shop_ext = True
        args.shop_ext_bucket = "shopping"

    print("=" * 64, flush=True)
    print("🚀 Legacy 단일 업체 SA 백필 시작", flush=True)
    print(f"- 기간: {start_date} ~ {end_date}", flush=True)
    print(f"- 업체: {args.account_name}", flush=True)
    print(f"- workers: {args.workers}", flush=True)
    print(f"- 구조 동기화: {'첫날만 수행' if args.sync_dim_first_day else '전 기간 skip_dim (가장 빠름)'}", flush=True)
    if args.shopping_only:
        print("- 쇼핑검색 전용 백필", flush=True)
    if args.with_shop_ext or args.shopping_only:
        print("- 쇼핑 확장소재 포함", flush=True)
    print("- purchase/cart/wishlist 분리: collector.py 로직상 2026-03-11 이전 날짜는 자동 미시도", flush=True)
    print("=" * 64, flush=True)

    first = True
    for d in daterange(start_date, end_date):
        d_str = d.strftime("%Y-%m-%d")
        cmd = [
            sys.executable,
            "collector.py",
            "--date",
            d_str,
            "--workers",
            str(args.workers),
            "--account_name",
            args.account_name,
        ]
        if args.shopping_only:
            cmd.append("--shopping_only")

        use_skip_dim = (not args.sync_dim_first_day) or (not first)
        if use_skip_dim:
            cmd.append("--skip_dim")
            mode = "stats only / skip_dim"
        else:
            mode = "full sync"

        print("\n" + "-" * 64, flush=True)
        print(f"📅 {d_str} | mode={mode}", flush=True)
        run_cmd(cmd, "collector", d_str)
        if args.with_shop_ext or args.shopping_only:
            ext_cmd = [sys.executable, "collector_shop_ext.py", "--date", d_str, "--account_name", args.account_name, "--ext_bucket", args.shop_ext_bucket]
            run_cmd(ext_cmd, "shop_ext", d_str)
        first = False

    print("\n" + "★" * 32, flush=True)
    print("🎉 Legacy 단일 업체 SA 백필 완료", flush=True)
    print("★" * 32, flush=True)


if __name__ == "__main__":
    main()
