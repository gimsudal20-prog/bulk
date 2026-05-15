# -*- coding: utf-8 -*-
"""Lightweight smoke checks for local/GitHub Actions use.

Default checks:
- py_compile on every .py file under the repo
- optional YAML parse for .github/workflows/*.yml when PyYAML is installed
- local import contract checks for top-level repo modules
- selected key file presence/parsing checks

Optional extra checks:
- --with-help / --with-runtime-help: run selected CLI scripts with --help
- --with-regression: run regression_check.py when present
"""
from __future__ import annotations

import argparse
import ast
import os
import py_compile
import subprocess
import sys
from pathlib import Path


KEY_FILES = [
    'app.py',
    'pages.py',
    'data.py',
    'page_helpers.py',
    'streamlit_compat.py',
    'view_campaign.py',
    'view_overview.py',
    'view_media.py',
    'view_budget.py',
    'view_trend.py',
    'collector.py',
    'collector_backfill_recent_sa.py',
    'fast_backfill.py',
    'collector_shop_ext.py',
    'regression_check.py',
]

RUNTIME_HELP_SCRIPTS = [
    'collector.py',
    'collector_backfill_recent_sa.py',
    'fast_backfill.py',
    'collector_shop_ext.py',
    'backfill_single_legacy_sa.py',
]


def iter_python_files(root: Path):
    for path in root.rglob('*.py'):
        if any(part in {'.venv', 'venv', '__pycache__'} for part in path.parts):
            continue
        yield path


def run_py_compile(root: Path) -> list[str]:
    errors: list[str] = []
    for path in iter_python_files(root):
        try:
            py_compile.compile(str(path), doraise=True)
        except py_compile.PyCompileError as exc:
            errors.append(f'py_compile 실패 | {path.relative_to(root)} | {exc.msg}')
    return errors


def run_yaml_parse(root: Path) -> list[str]:
    workflows_dir = root / '.github' / 'workflows'
    if not workflows_dir.exists():
        return []

    try:
        import yaml  # type: ignore
    except Exception:
        return ['PyYAML 미설치: workflow YAML 파싱은 건너뜀']

    messages: list[str] = []
    for path in sorted(workflows_dir.glob('*.yml')):
        try:
            yaml.safe_load(path.read_text(encoding='utf-8'))
        except Exception as exc:
            messages.append(f'workflow 파싱 실패 | {path.relative_to(root)} | {exc}')
    return messages


def _module_public_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def run_local_import_checks(root: Path) -> list[str]:
    messages: list[str] = []
    module_cache: dict[Path, set[str]] = {}

    for path in iter_python_files(root):
        try:
            tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        except Exception as exc:
            messages.append(f'import 체크 스킵 | {path.relative_to(root)} | 파싱 실패: {exc}')
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    mod_name = alias.name.split('.')[0]
                    mod_path = root / f'{mod_name}.py'
                    if mod_path.exists() is False and (root / mod_name).is_dir() is False:
                        continue
                    if mod_path.exists() is False and (root / mod_name).is_dir():
                        init_path = root / mod_name / '__init__.py'
                        if not init_path.exists():
                            messages.append(
                                f'import 체크 실패 | {path.relative_to(root)} | import {mod_name} (패키지 __init__.py 없음)'
                            )
                continue

            if not isinstance(node, ast.ImportFrom):
                continue
            if node.level != 0 or not node.module or any(alias.name == '*' for alias in node.names):
                continue
            mod_rel = Path(*node.module.split('.'))
            mod_path = root / (str(mod_rel) + '.py')
            pkg_init = root / mod_rel / '__init__.py'
            if not mod_path.exists() and not pkg_init.exists():
                continue
            target_path = mod_path if mod_path.exists() else pkg_init
            if target_path not in module_cache:
                module_cache[target_path] = _module_public_names(target_path)
            exported = module_cache[target_path]
            for alias in node.names:
                imported_name = alias.name
                if imported_name not in exported:
                    messages.append(
                        f'import 체크 실패 | {path.relative_to(root)} | from {node.module} import {imported_name}'
                    )
    return messages


def run_key_file_checks(root: Path) -> list[str]:
    messages: list[str] = []
    for rel in KEY_FILES:
        path = root / rel
        if not path.exists():
            messages.append(f'핵심 파일 누락 | {rel}')
            continue
        try:
            ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        except Exception as exc:
            messages.append(f'핵심 파일 파싱 실패 | {rel} | {exc}')
    return messages


def _classify_help_failure(script_name: str, output: str) -> tuple[str, bool]:
    text = output.strip()
    last = text.splitlines()[-1] if text else '출력 없음'
    if 'ModuleNotFoundError' in text or 'No module named' in text:
        return (f'help 체크 참고 | {script_name} | 현재 환경 의존성 미설치: {last}', False)
    if '환경변수가 없습니다' in text:
        return (f'help 체크 실패 | {script_name} | 도움말 전에 환경변수 검사가 실행됨: {last}', True)
    return (f'help 체크 실패 | {script_name} | {last}', True)


def run_help_checks(root: Path) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    notes: list[str] = []
    for script_name in RUNTIME_HELP_SCRIPTS:
        script_path = root / script_name
        if not script_path.exists():
            notes.append(f'help 체크 스킵 | {script_name} 없음')
            continue
        proc = subprocess.run(
            [sys.executable, str(script_path), '--help'],
            cwd=str(root),
            capture_output=True,
            text=True,
            env=os.environ.copy(),
        )
        if proc.returncode == 0:
            continue
        message, is_failure = _classify_help_failure(script_name, (proc.stderr or '') + '\n' + (proc.stdout or ''))
        if is_failure:
            failures.append(message)
        else:
            notes.append(message)
    return failures, notes


def run_regression_check(root: Path) -> tuple[list[str], list[str]]:
    script_path = root / 'regression_check.py'
    if not script_path.exists():
        return [], ['regression 체크 스킵 | regression_check.py 없음']
    proc = subprocess.run(
        [sys.executable, str(script_path), '--repo', str(root)],
        cwd=str(root),
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    output = ((proc.stdout or '') + '\n' + (proc.stderr or '')).strip()
    lines = [ln.strip() for ln in output.splitlines() if ln.strip()]
    notes = [f'regression 참고 | {ln}' for ln in lines if ln.startswith('- ok') or ln.startswith('- note')]
    failures = [f'regression 실패 | {ln}' for ln in lines if ln.startswith('-') and ('실패' in ln or '예외' in ln)]
    if proc.returncode != 0 and not failures:
        failures.append(f'regression 실패 | 종료코드 {proc.returncode}')
    return failures, notes


def main() -> int:
    parser = argparse.ArgumentParser(description='Run lightweight smoke checks.')
    parser.add_argument('--repo', default='.', help='repository root path')
    parser.add_argument('--with-help', action='store_true', help='also run selected scripts with --help')
    parser.add_argument('--with-runtime-help', action='store_true', help='alias of --with-help')
    parser.add_argument('--with-regression', action='store_true', help='also run regression_check.py when present')
    args = parser.parse_args()

    root = Path(args.repo).resolve()
    failures: list[str] = []
    notes: list[str] = []

    compile_errors = run_py_compile(root)
    failures.extend(compile_errors)

    yaml_messages = run_yaml_parse(root)
    for msg in yaml_messages:
        if '실패' in msg:
            failures.append(msg)
        else:
            notes.append(msg)

    failures.extend(run_key_file_checks(root))

    import_messages = run_local_import_checks(root)
    failures.extend(import_messages)

    if args.with_help or args.with_runtime_help:
        help_failures, help_notes = run_help_checks(root)
        failures.extend(help_failures)
        notes.extend(help_notes)

    if args.with_regression:
        reg_failures, reg_notes = run_regression_check(root)
        failures.extend(reg_failures)
        notes.extend(reg_notes)

    print('=== smoke check summary ===')
    print(f'repo: {root}')
    print(f'py files checked: {sum(1 for _ in iter_python_files(root))}')
    print(f'key files checked: {len(KEY_FILES)}')
    if notes:
        print('notes:')
        for msg in notes:
            print(f'- {msg}')

    if failures:
        print('failures:')
        for msg in failures:
            print(f'- {msg}')
        return 1

    print('all checks passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
