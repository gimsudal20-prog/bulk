# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, List


CART_ENABLE_DATE = date(2026, 3, 11)

COLLECT_MODE_ALIASES = {
    "sa_only": "sa_only",
    "검색광고 전체만": "sa_only",
    "검색광고전체만": "sa_only",
    "device_only": "device_only",
    "기기만": "device_only",
    "sa_with_device": "sa_with_device",
    "검색광고 전체+기기": "sa_with_device",
    "검색광고전체+기기": "sa_with_device",
    "검색광고 전체 + 기기": "sa_with_device",
}

SA_SCOPE_ALIASES = {
    "full": "full",
    "전체": "full",
    "ad_only": "ad_only",
    "소재만": "ad_only",
}

RUN_TARGET_ALIASES = {
    "sa_only": "sa_only",
    "search_ads_only": "sa_only",
    "검색광고 전체만": "sa_only",
    "검색광고전체만": "sa_only",
    "shop_ext_only": "shop_ext_only",
    "ext_only": "shop_ext_only",
    "확장소재만": "shop_ext_only",
    "sa_and_shop_ext": "sa_and_shop_ext",
    "search_ads_and_ext": "sa_and_shop_ext",
    "검색광고 전체+확장소재": "sa_and_shop_ext",
    "검색광고전체+확장소재": "sa_and_shop_ext",
    "검색광고 전체 + 확장소재": "sa_and_shop_ext",
}

SHOP_EXT_BUCKET_ALIASES = {
    "shopping": "shopping",
    "쇼핑검색": "shopping",
    "쇼핑검색(SSA)": "shopping",
    "쇼핑검색 (SSA)": "shopping",
    "non_shopping": "non_shopping",
    "파워링크 외 검색광고": "non_shopping",
    "파워링크외": "non_shopping",
    "파워링크 외": "non_shopping",
    "all": "all",
    "전체": "all",
}


def clean(v: str | None) -> str:
    return (v or "").strip()


def _normalize_choice(value: str, alias_map: dict[str, str], field_name: str) -> str:
    raw = clean(value)
    if not raw:
        raise ValueError(f"{field_name} 값이 비어 있습니다.")
    if raw in alias_map:
        return alias_map[raw]
    lowered = raw.lower()
    if lowered in alias_map:
        return alias_map[lowered]
    raise ValueError(f"{field_name} 값이 올바르지 않습니다: {value}")


def _label_collect_mode(value: str) -> str:
    return {
        "sa_only": "검색광고 전체만",
        "device_only": "기기만",
        "sa_with_device": "검색광고 전체+기기",
    }.get(value, value)


def _label_sa_scope(value: str) -> str:
    return {
        "full": "전체",
        "ad_only": "소재만",
    }.get(value, value)


def _label_run_target(value: str) -> str:
    return {
        "sa_only": "검색광고 전체만",
        "shop_ext_only": "확장소재만",
        "sa_and_shop_ext": "검색광고 전체+확장소재",
    }.get(value, value)


def _label_shop_ext_bucket(value: str) -> str:
    return {
        "shopping": "쇼핑검색(SSA)",
        "non_shopping": "파워링크 외 검색광고",
        "all": "전체",
    }.get(value, value)


def _supports_cli_arg(filename: str, arg_name: str) -> bool:
    try:
        txt = Path(filename).read_text(encoding="utf-8")
        pattern = rf"add_argument\(\s*['\"]{re.escape(arg_name)}['\"]"
        return re.search(pattern, txt) is not None
    except Exception:
        return False


def _fmt_cmd(cmd: List[str]) -> str:
    if not cmd:
        return "-"
    try:
        return shlex.join(cmd)
    except Exception:
        return " ".join(cmd)


def _bool_label(value: bool) -> str:
    return "예" if value else "아니오"


def _status_icon(status: str) -> str:
    return {
        "ok": "✅",
        "failed": "❌",
        "skipped": "⏭️",
    }.get(status, "•")


def _md_escape(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", "<br>")


def _append_step_summary(markdown: str) -> None:
    path = clean(os.getenv("GITHUB_STEP_SUMMARY"))
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(markdown)
            if not markdown.endswith("\n"):
                fh.write("\n")
    except Exception as e:
        print(f"⚠️ Step Summary 기록 실패: {e}", flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="날짜 범위별 검색광고/GFA 백필 실행기")
    p.add_argument("--start", required=True, help="시작일 YYYY-MM-DD")
    p.add_argument("--end", required=True, help="종료일 YYYY-MM-DD")
    p.add_argument("--workers", type=int, default=1, help="collector.py workers")
    p.add_argument("--account_name", default="", help="단일 업체명")
    p.add_argument("--account_names", default="", help="여러 업체명 콤마구분")
    p.add_argument("--fast", action="store_true", help="collector.py 빠른 수집 모드")
    p.add_argument(
        "--sync_dim_first_day",
        action="store_true",
        help="첫날만 구조 동기화, 이후 날짜는 --skip_dim",
    )
    p.add_argument("--with_gfa", action="store_true", help="collector_gfa.py도 함께 실행")
    p.add_argument("--with_shop_ext", action="store_true", help="collector_shop_ext.py도 함께 실행")
    p.add_argument("--shopping_only", action="store_true", help="쇼핑검색(SSA) 캠페인만 수집/백필")
    p.add_argument("--collect_mode", default="검색광고 전체+기기", help="검색광고 전체만 / 기기만 / 검색광고 전체+기기")
    p.add_argument("--sa_scope", default="전체", help="검색광고 수집 범위: 전체 / 소재만")
    p.add_argument("--run_target", default="검색광고 전체만", help="실행 대상: 검색광고 전체만 / 확장소재만 / 검색광고 전체+확장소재")
    p.add_argument("--shop_ext_bucket", default="쇼핑검색(SSA)", help="확장소재 구분: 쇼핑검색(SSA) / 파워링크 외 검색광고 / 전체")
    args = p.parse_args()
    args.start = clean(args.start)
    args.end = clean(args.end)
    args.account_name = clean(args.account_name)
    args.account_names = clean(args.account_names)
    try:
        args.collect_mode = _normalize_choice(args.collect_mode, COLLECT_MODE_ALIASES, "collect_mode")
        args.sa_scope = _normalize_choice(args.sa_scope, SA_SCOPE_ALIASES, "sa_scope")
        args.run_target = _normalize_choice(args.run_target, RUN_TARGET_ALIASES, "run_target")
        args.shop_ext_bucket = _normalize_choice(args.shop_ext_bucket, SHOP_EXT_BUCKET_ALIASES, "shop_ext_bucket")
    except ValueError as e:
        p.error(str(e))
    return args


def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def run_cmd(cmd: List[str], label: str, day: str) -> tuple[bool, str]:
    print(f"   ▶ [{label}] 수집 진행 중...", flush=True)
    print(f"      실행: {_fmt_cmd(cmd)}", flush=True)
    try:
        subprocess.run(cmd, check=True)
        print(f"   ✅ [{label}] {day} 완료", flush=True)
        return True, ""
    except subprocess.CalledProcessError as e:
        reason = f"returncode={e.returncode}"
        print(f"   ❌ [{label}] {day} 실패 | {reason}", flush=True)
        return False, reason


def build_search_ads_cmd(args: argparse.Namespace, d_str: str, first: bool) -> List[str]:
    cmd: List[str] = [
        sys.executable,
        "collector.py",
        "--date",
        d_str,
        "--workers",
        str(args.workers),
        "--collect_mode",
        args.collect_mode,
    ]
    if args.account_name:
        cmd += ["--account_name", args.account_name]
    if args.account_names and args.account_names != args.account_name:
        cmd += ["--account_names", args.account_names]
    if args.shopping_only:
        cmd.append("--shopping_only")
    if args.sa_scope == "ad_only" and _supports_cli_arg("collector.py", "--sa_scope"):
        cmd += ["--sa_scope", args.sa_scope]

    if args.sync_dim_first_day:
        skip_dim_this_run = not first
    else:
        skip_dim_this_run = True

    if args.fast and skip_dim_this_run:
        cmd.append("--fast")
    elif args.fast and not skip_dim_this_run:
        print("   ℹ️ 첫날 구조 동기화 구간이라 --fast는 붙이지 않습니다.", flush=True)

    if skip_dim_this_run:
        cmd.append("--skip_dim")
    return cmd


def build_shop_ext_cmd(args: argparse.Namespace, d_str: str) -> List[str]:
    cmd: List[str] = [sys.executable, "collector_shop_ext.py", "--date", d_str]
    if args.account_name:
        cmd += ["--account_name", args.account_name]
    if args.account_names and args.account_names != args.account_name:
        cmd += ["--account_names", args.account_names]
    if _supports_cli_arg("collector_shop_ext.py", "--ext_bucket"):
        cmd += ["--ext_bucket", args.shop_ext_bucket]
    return cmd


def build_gfa_cmd(args: argparse.Namespace, d_str: str) -> List[str]:
    cmd: List[str] = [sys.executable, "collector_gfa.py", "--date", d_str]
    if args.account_name:
        cmd += ["--account_name", args.account_name]
    if args.account_names and args.account_names != args.account_name:
        cmd += ["--account_names", args.account_names]
    if _supports_cli_arg("collector_gfa.py", "--collect_mode"):
        cmd += ["--collect_mode", args.collect_mode]
    return cmd


def _build_effective_plan(args: argparse.Namespace) -> tuple[dict[str, Any], list[str], dict[str, bool]]:
    notes: list[str] = []
    support = {
        "collector_sa_scope": _supports_cli_arg("collector.py", "--sa_scope"),
        "shop_ext_bucket": _supports_cli_arg("collector_shop_ext.py", "--ext_bucket"),
        "gfa_collect_mode": _supports_cli_arg("collector_gfa.py", "--collect_mode"),
    }

    effective: dict[str, Any] = {
        "collect_mode": args.collect_mode,
        "sa_scope": args.sa_scope,
        "workers": args.workers,
        "fast": args.fast,
        "with_gfa": args.with_gfa,
        "with_shop_ext": args.with_shop_ext,
        "shopping_only": args.shopping_only,
        "shop_ext_bucket": args.shop_ext_bucket,
        "run_target": args.run_target,
        "sync_dim_first_day": args.sync_dim_first_day,
    }

    if effective["run_target"] in {"shop_ext_only", "sa_and_shop_ext"} and not effective["with_shop_ext"]:
        effective["with_shop_ext"] = True
        notes.append(f"run_target={_label_run_target(effective['run_target'])} 이므로 확장소재 수집을 자동 포함합니다.")

    if effective["shopping_only"]:
        if effective["fast"]:
            effective["fast"] = False
            notes.append("shopping_only=true 이므로 fast 모드는 해제됩니다.")
        if effective["workers"] != 1:
            notes.append(f"shopping_only=true 이므로 workers는 1로 고정됩니다. ({effective['workers']} → 1)")
            effective["workers"] = 1
        if not effective["with_shop_ext"]:
            effective["with_shop_ext"] = True
            notes.append("shopping_only=true 이므로 확장소재 수집을 자동 포함합니다.")
        if effective["shop_ext_bucket"] != "shopping":
            notes.append(
                f"shopping_only=true 이므로 확장소재 구분은 쇼핑검색(SSA)으로 고정됩니다. ({_label_shop_ext_bucket(effective['shop_ext_bucket'])} → 쇼핑검색(SSA))"
            )
            effective["shop_ext_bucket"] = "shopping"

    if effective["with_shop_ext"] and effective["run_target"] == "sa_only":
        effective["run_target"] = "sa_and_shop_ext"
        notes.append("with_shop_ext=true 이므로 실행 대상은 검색광고 전체+확장소재로 해석합니다.")

    if effective["sa_scope"] == "ad_only" and not support["collector_sa_scope"]:
        notes.append("현재 collector.py 는 --sa_scope 를 지원하지 않아 '소재만' 설정은 실제 실행에서 무시됩니다.")
    if effective["shop_ext_bucket"] != "shopping" and not support["shop_ext_bucket"]:
        notes.append("현재 collector_shop_ext.py 는 --ext_bucket 을 지원하지 않아 확장소재 구분 설정은 실제 실행에서 무시됩니다.")
    if effective["with_gfa"] and not support["gfa_collect_mode"]:
        notes.append("현재 collector_gfa.py 는 --collect_mode 를 지원하지 않아 GFA 실행에는 collect_mode 값이 전달되지 않습니다.")

    return effective, notes, support


def _print_plan_summary(
    args: argparse.Namespace,
    effective: dict[str, Any],
    notes: list[str],
    support: dict[str, bool],
    start_date: date,
    end_date: date,
) -> None:
    print(f"🚀 백필 작업 시작: {start_date} ~ {end_date}", flush=True)
    if args.account_name:
        print(f"🎯 단일 업체 필터: {args.account_name}", flush=True)
    if args.account_names:
        print(f"🎯 복수 업체 필터: {args.account_names}", flush=True)
    print(f"🧭 수집 모드: {_label_collect_mode(effective['collect_mode'])}", flush=True)
    print(f"🎯 검색광고 수집 범위: {_label_sa_scope(effective['sa_scope'])}", flush=True)
    print(f"🎬 실행 대상: {_label_run_target(effective['run_target'])}", flush=True)
    print(f"👷 workers: {effective['workers']} | fast: {_bool_label(effective['fast'])}", flush=True)
    print(f"🧱 첫날만 구조 수집: {_bool_label(effective['sync_dim_first_day'])}", flush=True)
    print(f"🛍️ 쇼핑검색(SSA) 전용: {_bool_label(effective['shopping_only'])}", flush=True)
    print(f"🧩 확장소재 포함: {_bool_label(effective['with_shop_ext'])} | 구분: {_label_shop_ext_bucket(effective['shop_ext_bucket'])}", flush=True)
    print(f"📺 GFA 포함: {_bool_label(effective['with_gfa'])}", flush=True)
    print(
        "🧪 CLI 지원 확인 | collector.py --sa_scope={} | collector_shop_ext.py --ext_bucket={} | collector_gfa.py --collect_mode={}".format(
            "지원" if support["collector_sa_scope"] else "미지원",
            "지원" if support["shop_ext_bucket"] else "미지원",
            "지원" if support["gfa_collect_mode"] else "미지원",
        ),
        flush=True,
    )
    if effective["fast"] and effective["collect_mode"] != "sa_only":
        print("⚠️ 빠른 수집 + 기기 조합입니다. PC/M 문제 분석 시에는 fast=false 를 권장합니다.", flush=True)
    for note in notes:
        print(f"⚠️ {note}", flush=True)
    print("=" * 60, flush=True)

    lines = [
        "## 02 수동 백필 실행 계획",
        "",
        f"- 기간: `{start_date}` ~ `{end_date}`",
        f"- 단일 업체: `{args.account_name or '-'}`",
        f"- 복수 업체: `{args.account_names or '-'}`",
        f"- 수집 모드: `{_label_collect_mode(effective['collect_mode'])}`",
        f"- 검색광고 수집 범위: `{_label_sa_scope(effective['sa_scope'])}`",
        f"- 실행 대상: `{_label_run_target(effective['run_target'])}`",
        f"- workers: `{effective['workers']}` / fast: `{_bool_label(effective['fast'])}` / 첫날만 구조 수집: `{_bool_label(effective['sync_dim_first_day'])}`",
        f"- 쇼핑검색 전용: `{_bool_label(effective['shopping_only'])}` / 확장소재 포함: `{_bool_label(effective['with_shop_ext'])}` / 확장소재 구분: `{_label_shop_ext_bucket(effective['shop_ext_bucket'])}` / GFA 포함: `{_bool_label(effective['with_gfa'])}`",
        "",
        "### CLI 지원 확인",
        "",
        f"- `collector.py --sa_scope`: {'지원' if support['collector_sa_scope'] else '미지원'}",
        f"- `collector_shop_ext.py --ext_bucket`: {'지원' if support['shop_ext_bucket'] else '미지원'}",
        f"- `collector_gfa.py --collect_mode`: {'지원' if support['gfa_collect_mode'] else '미지원'}",
    ]
    if notes:
        lines += ["", "### 자동 보정/주의사항", ""] + [f"- {n}" for n in notes]
    lines += ["", "---", ""]
    _append_step_summary("\n".join(lines))


def _render_execution_summary(records: list[dict[str, Any]], failed: bool) -> str:
    total = len(records)
    ok = sum(1 for r in records if r["status"] == "ok")
    skipped = sum(1 for r in records if r["status"] == "skipped")
    failed_count = sum(1 for r in records if r["status"] == "failed")
    lines = [
        "\n" + "★" * 32,
        "❌ 백필 작업 중단" if failed else "🎉 백필 작업 완료",
        f"총 단계: {total} | 성공: {ok} | 스킵: {skipped} | 실패: {failed_count}",
        "★" * 32,
    ]
    if records:
        lines.append("실행 요약:")
        for r in records:
            detail = f" | 사유: {r['reason']}" if r.get("reason") else ""
            lines.append(f"{_status_icon(r['status'])} {r['date']} | {r['label']} | {_fmt_cmd(r['cmd'])}{detail}")
    return "\n".join(lines) + "\n"


def _render_execution_summary_md(records: list[dict[str, Any]], failed: bool) -> str:
    total = len(records)
    ok = sum(1 for r in records if r["status"] == "ok")
    skipped = sum(1 for r in records if r["status"] == "skipped")
    failed_count = sum(1 for r in records if r["status"] == "failed")
    lines = [
        "## 02 수동 백필 실행 결과",
        "",
        f"- 최종 상태: {'실패' if failed else '성공'}",
        f"- 총 단계: `{total}` / 성공: `{ok}` / 스킵: `{skipped}` / 실패: `{failed_count}`",
        "",
        "| 날짜 | 단계 | 상태 | 비고 |",
        "|---|---|---|---|",
    ]
    for r in records:
        extra = r.get("reason") or _fmt_cmd(r["cmd"])
        lines.append(
            f"| {_md_escape(r['date'])} | {_md_escape(r['label'])} | {_md_escape(_status_icon(r['status']) + ' ' + r['status'])} | {_md_escape(extra)} |"
        )
    lines += ["", "---", ""]
    return "\n".join(lines)


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

    effective, notes, support = _build_effective_plan(args)
    args.fast = bool(effective["fast"])
    args.workers = int(effective["workers"])
    args.with_shop_ext = bool(effective["with_shop_ext"])
    args.shop_ext_bucket = str(effective["shop_ext_bucket"])
    args.run_target = str(effective["run_target"])

    _print_plan_summary(args, effective, notes, support, start_date, end_date)

    records: list[dict[str, Any]] = []
    failed = False
    first = True
    for d in daterange(start_date, end_date):
        d_str = d.strftime("%Y-%m-%d")
        print(f"\n📅 [ {d_str} ]", flush=True)

        if args.run_target != "shop_ext_only":
            cmd_search = build_search_ads_cmd(args, d_str, first)
            search_label = "쇼핑검색(SSA)" if args.shopping_only else "검색광고 전체"
            ok, reason = run_cmd(cmd_search, search_label, d_str)
            records.append({"date": d_str, "label": search_label, "status": "ok" if ok else "failed", "reason": reason, "cmd": cmd_search})
            if not ok:
                failed = True
                break
        else:
            reason = "run_target 설정으로 스킵"
            print(f"   ⏭️ [검색광고 전체] {reason}합니다.", flush=True)
            records.append({"date": d_str, "label": "검색광고 전체", "status": "skipped", "reason": reason, "cmd": []})

        if args.with_shop_ext:
            if os.path.exists("collector_shop_ext.py"):
                cmd_shop_ext = build_shop_ext_cmd(args, d_str)
                label = f"확장소재 | {_label_shop_ext_bucket(args.shop_ext_bucket)}"
                ok, reason = run_cmd(cmd_shop_ext, label, d_str)
                records.append({"date": d_str, "label": label, "status": "ok" if ok else "failed", "reason": reason, "cmd": cmd_shop_ext})
                if not ok:
                    failed = True
                    break
            else:
                reason = "collector_shop_ext.py 파일 없음"
                print(f"   ⏭️ [확장소재] {reason}으로 스킵합니다.", flush=True)
                records.append({"date": d_str, "label": "확장소재", "status": "skipped", "reason": reason, "cmd": []})

        if args.with_gfa:
            if os.path.exists("collector_gfa.py"):
                cmd_gfa = build_gfa_cmd(args, d_str)
                ok, reason = run_cmd(cmd_gfa, "GFA", d_str)
                records.append({"date": d_str, "label": "GFA", "status": "ok" if ok else "failed", "reason": reason, "cmd": cmd_gfa})
                if not ok:
                    failed = True
                    break
            else:
                reason = "collector_gfa.py 파일 없음"
                print(f"   ⏭️ [GFA] {reason}으로 스킵합니다.", flush=True)
                records.append({"date": d_str, "label": "GFA", "status": "skipped", "reason": reason, "cmd": []})

        first = False

    summary_text = _render_execution_summary(records, failed)
    print(summary_text, flush=True)
    _append_step_summary(_render_execution_summary_md(records, failed))
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
