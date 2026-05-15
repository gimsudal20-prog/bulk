# -*- coding: utf-8 -*-
"""Minimal regression checks for budget/bizmoney and parser contracts.

This intentionally avoids importing heavy app modules so it can run in light
CI/local environments.
"""
from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path


class RegressionFailure(Exception):
    pass


def _read_ast(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding='utf-8'), filename=str(path))


def _function_names(tree: ast.AST) -> set[str]:
    return {n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _find_call_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                names.add(func.id)
            elif isinstance(func, ast.Attribute):
                names.add(func.attr)
    return names


def check_budget_wrapper(root: Path) -> list[str]:
    messages: list[str] = []
    data_path = root / 'data.py'
    view_path = root / 'view_budget.py'
    if not data_path.exists() or not view_path.exists():
        raise RegressionFailure('data.py 또는 view_budget.py 가 없습니다')

    data_tree = _read_ast(data_path)
    view_tree = _read_ast(view_path)

    data_funcs = _function_names(data_tree)
    if 'query_budget_bundle' not in data_funcs:
        raise RegressionFailure('data.py 에 query_budget_bundle 공개 함수가 없습니다')
    messages.append('ok | data.py query_budget_bundle 공개 wrapper 존재')

    calls = _find_call_names(view_tree)
    if 'query_budget_bundle' not in calls:
        raise RegressionFailure('view_budget.py 가 query_budget_bundle 을 호출하지 않습니다')
    messages.append('ok | view_budget.py query_budget_bundle 호출 유지')
    return messages


def check_budget_cache_helpers(root: Path) -> list[str]:
    data_path = root / 'view_budget.py'
    tree = _read_ast(data_path)
    funcs = _function_names(tree)
    required = {
        '_cached_budget_bundle',
        'render_budget_editor',
        'render_alert_table',
    }
    missing = sorted(required - funcs)
    if missing:
        raise RegressionFailure(f'view_budget.py 필수 함수 누락: {", ".join(missing)}')
    return [f'ok | view_budget.py 필수 함수 유지 ({", ".join(sorted(required))})']




def check_backfill_public_contract(root: Path) -> list[str]:
    path = root / 'collector_backfill_recent_sa.py'
    if not path.exists():
        raise RegressionFailure('collector_backfill_recent_sa.py 가 없습니다')
    tree = _read_ast(path)
    funcs = _function_names(tree)
    required = {
        'process_account',
        'process_conversion_report',
        'parse_shopping_query_report',
        'parse_base_report',
        '_record_backfill_result',
        'emit_backfill_run_summary',
        '_log_backfill_db_failure',
    }
    missing = sorted(required - funcs)
    if missing:
        raise RegressionFailure(f'backfill 공개/핵심 함수 누락: {", ".join(missing)}')
    return [f"ok | backfill 핵심 함수 유지 ({', '.join(sorted(required))})"]


def check_backfill_parser_contract(root: Path) -> list[str]:
    path = root / 'collector_backfill_recent_sa.py'
    if not path.exists():
        raise RegressionFailure('collector_backfill_recent_sa.py 가 없습니다')
    tree = _read_ast(path)
    funcs = _function_names(tree)
    required = {
        '_conv_process_header_mode',
        '_conv_process_heuristic_mode',
        '_conv_collect_type_hits',
        '_conv_pick_numeric_payload',
        '_log_backfill_conv_diag',
    }
    missing = sorted(required - funcs)
    if missing:
        raise RegressionFailure(f'backfill 파서 helper 누락: {", ".join(missing)}')
    return [f"ok | backfill 파서 helper 유지 ({', '.join(sorted(required))})"]


def check_backfill_stage_logging(root: Path) -> list[str]:
    path = root / 'collector_backfill_recent_sa.py'
    if not path.exists():
        raise RegressionFailure('collector_backfill_recent_sa.py 가 없습니다')
    text = path.read_text(encoding='utf-8')
    required_tokens = [
        'result["stage"]',
        'stage=',
        'save_shopping_query_split',
        'save_stats_and_breakdowns',
        'resolve_split_payload',
    ]
    missing = [tok for tok in required_tokens if tok not in text]
    if missing:
        raise RegressionFailure(f'backfill stage/error 추적 토큰 누락: {", ".join(missing)}')
    return ['ok | backfill stage/error 추적 토큰 유지']

def check_sa_scope_contract(root: Path) -> list[str]:
    collector_path = root / 'collector.py'
    if not collector_path.exists():
        return ['note | collector.py 없음: sa_scope 계약 점검 스킵']
    tree = _read_ast(collector_path)
    funcs = _function_names(tree)
    msgs: list[str] = []
    if 'normalize_sa_scope' in funcs and 'label_sa_scope' in funcs and '--sa_scope' in collector_path.read_text(encoding='utf-8'):
        msgs.append('ok | collector.py sa_scope helper/옵션 유지')
    else:
        msgs.append('note | collector.py sa_scope 직접 지원은 현재 기준 미적용')
    return msgs


def main() -> int:
    parser = argparse.ArgumentParser(description='Run minimal regression checks.')
    parser.add_argument('--repo', default='.', help='repository root path')
    args = parser.parse_args()
    root = Path(args.repo).resolve()

    failures: list[str] = []
    notes: list[str] = []

    checks = [
        check_budget_wrapper,
        check_budget_cache_helpers,
        check_backfill_public_contract,
        check_backfill_parser_contract,
        check_backfill_stage_logging,
        check_sa_scope_contract,
    ]
    for fn in checks:
        try:
            notes.extend(fn(root))
        except RegressionFailure as exc:
            failures.append(f'{fn.__name__} 실패 | {exc}')
        except Exception as exc:  # pragma: no cover - unexpected infra failure
            failures.append(f'{fn.__name__} 예외 | {type(exc).__name__}: {exc}')

    print('=== regression check summary ===')
    print(f'repo: {root}')
    if notes:
        print('notes:')
        for msg in notes:
            print(f'- {msg}')
    if failures:
        print('failures:')
        for msg in failures:
            print(f'- {msg}')
        return 1
    print('all regression checks passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
