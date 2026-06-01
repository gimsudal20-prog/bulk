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


def check_conversion_keyword_mapping_contract(root: Path) -> list[str]:
    path = root / 'collector_parsers.py'
    runner_path = root / 'collector_runner.py'
    if not path.exists() or not runner_path.exists():
        raise RegressionFailure('collector_parsers.py 또는 collector_runner.py 가 없습니다')
    text = path.read_text(encoding='utf-8')
    runner_text = runner_path.read_text(encoding='utf-8')
    required = {
        'header keyword text index': 'kw_text_idx' in text and 'get_text_col_idx' in text,
        'header keyword lookup mapping': '_conv_resolve_keyword_object_id(' in text and 'keyword_unique_lookup' in text,
        'header campaign token fallback': 'row_campaign_id = extract_prefixed_token(vals, "cmp-")' in text,
        'AD/CRITERION 전환은 쇼핑 없이도 수집': 'split_enabled_for_date_fn(target_date) and collect_sa' in runner_text and 'split_candidate_reports = ["AD_CONVERSION", "CRITERION_CONVERSION"]' in runner_text,
        '쇼핑검색어는 keyword split 제외': 'raw_kw_map = merge_split_maps_fn(ad_kw_map, criterion_kw_map)' in runner_text and 'SHOPPINGKEYWORD_CONVERSION_DETAIL is a search-term report' in runner_text,
    }
    missing = [name for name, ok in required.items() if not ok]
    if missing:
        raise RegressionFailure(f'conversion keyword mapping 계약 누락: {", ".join(missing)}')
    return ['ok | 전환 리포트 키워드명→키워드ID 매핑 계약 유지']


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


def check_targeting_breakdown_contract(root: Path) -> list[str]:
    targeting_path = root / 'targeting_collector_helpers.py'
    collector_path = root / 'collector.py'
    runner_path = root / 'collector_runner.py'
    if not targeting_path.exists() or not collector_path.exists() or not runner_path.exists():
        raise RegressionFailure('targeting/collector 핵심 파일이 없습니다')

    targeting_text = targeting_path.read_text(encoding='utf-8')
    collector_text = collector_path.read_text(encoding='utf-8')
    runner_text = runner_path.read_text(encoding='utf-8')
    required = {
        'targeting ids 쉼표 파라미터': '"ids": ",".join(chunk)' in targeting_text,
        '연령대는 전체 수집 대상 캠페인 요청': 'age_ids = list(hour_ids)' in targeting_text and 'AGE_BREAKDOWN_CANDIDATES' in targeting_text,
        '시간대 24구간 zero-fill': 'for hour in range(24)' in targeting_text,
        '연령대 표준구간 zero-fill': all(x in targeting_text for x in ['"14세 미만"', '"14세 ~ 18세"', '"19세 ~ 24세"', '"60세 이상"', '"연령 알 수 없음"', '"해당 없음"']),
        '연령대 캠페인 id 기준 조회': 'age_entity_source": "campaign"' in targeting_text and 'for lookup_id in age_ids' in targeting_text,
        '캠페인 타입 맵 DB 로드': 'SELECT campaign_id, COALESCE(campaign_tp' in collector_text,
        'live 캠페인 타입 맵 병합': 'live_campaign_type_map' in runner_text and 'campaign_type_map.update(live_campaign_type_map)' in runner_text,
    }
    missing = [name for name, ok in required.items() if not ok]
    if missing:
        raise RegressionFailure(f'targeting breakdown 계약 누락: {", ".join(missing)}')
    return ['ok | 시간대/연령대 breakdown 수집 계약 유지']


def check_overview_keyword_purchase_contract(root: Path) -> list[str]:
    data_path = root / 'data.py'
    overview_path = root / 'view_overview.py'
    if not data_path.exists() or not overview_path.exists():
        raise RegressionFailure('data.py 또는 view_overview.py 가 없습니다')

    data_text = data_path.read_text(encoding='utf-8')
    overview_text = overview_path.read_text(encoding='utf-8')
    required = {
        'keyword bundle 구매완료 fallback 비활성': '_build_bundle_metric_sql(kw_fact_cols, purchase_fallback=False)' in data_text,
        'bundle metric fallback 옵션 유지': 'purchase_fallback: bool = True' in data_text,
        'overview keyword cache version 갱신': 'cache_version = 5' in overview_text,
        'keyword bundle 쇼핑검색 제외': 'keyword_bundle_exclude_shopping' in data_text and 'NOT IN' in data_text,
        '쇼핑검색 오버뷰 검색어상세 대체 금지': '요약 구매완료 기준' in overview_text and '검색어 상세가 아닌 캠페인 일별 split 기준' in overview_text and 'cur_shop_summary = _shopping_terms_purchase_summary' not in overview_text,
        '캠페인 번들 검색어상세 override 금지': 'shopping_summary = query_shopping_query_campaign_purchase_summary' not in data_text and '_override_with_shopping_query_purchase(df, shopping_summary' not in data_text,
        'overview keyword 전체 기준 정렬 컨트롤': '_render_overview_keyword_sort_controls' in overview_text and '_sort_overview_detail_frame' in overview_text,
        'overview keyword 미매핑 전환 행': '_append_unmapped_keyword_conversion_row' in overview_text and '키워드 미매핑 전환' in overview_text,
        'overview keyword 미매핑 중복 제거': '_UNMAPPED_KEYWORD_LABEL' in overview_text and '!= _UNMAPPED_KEYWORD_LABEL' in overview_text,
        'overview keyword 미매핑 campaign 기준 차감': '_filter_keyword_scope_for_campaigns' in overview_text and '"campaign_id"' in overview_text,
    }
    missing = [name for name, ok in required.items() if not ok]
    if missing:
        raise RegressionFailure(f'overview keyword 구매완료 계약 누락: {", ".join(missing)}')
    return ['ok | overview 키워드 상세 구매완료/총전환 분리 계약 유지']



def _get_function_def(tree: ast.AST, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def check_collector_runner_contract(root: Path) -> list[str]:
    collector_path = root / 'collector.py'
    runner_path = root / 'collector_runner.py'
    if not collector_path.exists() or not runner_path.exists():
        raise RegressionFailure('collector.py 또는 collector_runner.py 가 없습니다')

    collector_tree = _read_ast(collector_path)
    runner_tree = _read_ast(runner_path)
    runner_fn = _get_function_def(runner_tree, 'process_account')
    if runner_fn is None:
        raise RegressionFailure('collector_runner.py process_account 함수가 없습니다')

    accepted = {arg.arg for arg in runner_fn.args.args}
    accepted.update(arg.arg for arg in runner_fn.args.kwonlyargs)
    has_kwargs = runner_fn.args.kwarg is not None

    passed: set[str] = set()
    for node in ast.walk(collector_tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == 'process_account':
            base = func.value
            if isinstance(base, ast.Name) and base.id == 'collector_runner_mod':
                passed.update(kw.arg for kw in node.keywords if kw.arg)

    missing = sorted(k for k in passed if k not in accepted and not has_kwargs)
    if missing:
        raise RegressionFailure(f'collector.py → collector_runner.process_account 미지원 keyword: {", ".join(missing)}')

    collector_text = collector_path.read_text(encoding='utf-8')
    required = {
        '전 계정 오류 시 workflow 실패 처리': '_collection_run_has_fatal_errors' in collector_text and '수집이 전 계정에서 실패했습니다' in collector_text,
        'PC/M breakdown dependency 계약 유지': 'get_stats_breakdown_range_fn' in passed and 'get_stats_breakdown_range_fn' in accepted,
    }
    missing_contracts = [name for name, ok in required.items() if not ok]
    if missing_contracts:
        raise RegressionFailure(f'collector runner 계약 누락: {", ".join(missing_contracts)}')
    return ['ok | collector.py ↔ collector_runner.process_account keyword 계약 유지']

def check_device_breakdown_contract(root: Path) -> list[str]:
    device_path = root / 'device_collector_helpers.py'
    view_path = root / 'view_time_age.py'
    collector_path = root / 'collector.py'
    runner_path = root / 'collector_runner.py'
    backfill_path = root / 'collector_backfill_recent_sa.py'
    if not device_path.exists() or not view_path.exists() or not collector_path.exists() or not runner_path.exists() or not backfill_path.exists():
        raise RegressionFailure('기기별 수집/UI 핵심 파일이 없습니다')

    device_text = device_path.read_text(encoding='utf-8')
    view_text = view_path.read_text(encoding='utf-8')
    collector_text = collector_path.read_text(encoding='utf-8')
    runner_text = runner_path.read_text(encoding='utf-8')
    backfill_text = backfill_path.read_text(encoding='utf-8')
    required = {
        'device parser actual pcMobile version 갱신': 'pcm_v20260531_no_ad_report_conv_infer1' in device_text,
        'device metric 상대 위치 추론': '_infer_metric_indices_relative' in device_text and 'relative_metrics' in device_text,
        'pcMobileTp 헤더 alias': 'pcmobiletp' in device_text and 'PC/모바일 구분' in device_text,
        'criterion targeting id alias': 'targeting id' in device_text and '타겟팅 id' in device_text,
        '미분리 device total 유지': 'UNSEGMENTED' in device_text and 'PC/MOBILE' in device_text,
        'tuple key device filter 유지': '_stat_result_entity_id' in collector_text and 'isinstance(key, (tuple, list))' in collector_text,
        'actual pcMobile breakdown helper': 'build_pc_mobile_device_stat_from_stats' in device_text and 'PC_MOBILE_BREAKDOWN_KEY' in device_text,
        'collector uses pcMblTp breakdown': 'get_stats_breakdown_range_fn' in runner_text and 'pcMblTp' in runner_text and 'not_used_actual_pcMblTp' in runner_text,
        'backfill uses pcMblTp breakdown': 'get_stats_breakdown_range' in backfill_text and 'pcMblTp' in backfill_text and 'not_used_actual_pcMblTp' in backfill_text,
        'time_age 기기별 탭 추가': '_render_device_tab' in view_text and '기기별' in view_text,
        'time_age campaign/ad device 조회': '_query_device' in view_text and '_query_ad_device' in view_text,
        'time_age device 테이블 체크': 'fact_campaign_device_daily' in view_text and 'fact_ad_device_daily' in view_text,
    }
    missing = [name for name, ok in required.items() if not ok]
    if missing:
        raise RegressionFailure(f'기기별 breakdown 계약 누락: {", ".join(missing)}')
    return ['ok | 기기별 breakdown 수집/UI 계약 유지']


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
        check_conversion_keyword_mapping_contract,
        check_backfill_stage_logging,
        check_sa_scope_contract,
        check_targeting_breakdown_contract,
        check_overview_keyword_purchase_contract,
        check_device_breakdown_contract,
        check_collector_runner_contract,
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
