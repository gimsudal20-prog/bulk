from __future__ import annotations

from pathlib import Path

OLD_REQUEST_JSON = '''def request_json(method: str, path: str, customer_id: str, params: dict | None = None, json_data: dict | None = None, raise_error=True) -> Tuple[int, Any]:
    url = BASE_URL + path
    max_retries = 8
    session = get_session()
    
    for attempt in range(max_retries):
        headers = make_headers(method, path, customer_id)
        try:
            r = session.request(method, url, headers=headers, params=params, json=json_data, timeout=TIMEOUT)
'''

NEW_REQUEST_JSON = '''def request_json(method: str, path: str, customer_id: str, params: dict | None = None, json_data: dict | None = None, raise_error=True) -> Tuple[int, Any]:
    url = BASE_URL + path
    is_ads_list = path == "/ncc/ads"
    max_retries = 3 if is_ads_list else 8
    req_timeout = 15 if is_ads_list else TIMEOUT
    session = get_session()
    
    for attempt in range(max_retries):
        headers = make_headers(method, path, customer_id)
        try:
            r = session.request(method, url, headers=headers, params=params, json=json_data, timeout=req_timeout)
'''

OLD_LIST_ADS = '''def list_ads(customer_id: str, adgroup_id: str) -> List[dict]:
    ok, data = safe_call("GET", "/ncc/ads", customer_id, {"nccAdgroupId": adgroup_id})
    if ok and isinstance(data, list) and data: return data
    ok_owner, data_owner = safe_call("GET", "/ncc/ads", customer_id, {"ownerId": adgroup_id})
    if ok_owner and isinstance(data_owner, list): return data_owner
    return data if ok and isinstance(data, list) else []
'''

NEW_LIST_ADS = '''def list_ads(customer_id: str, adgroup_id: str) -> List[dict]:
    gid = str(adgroup_id or "").strip()
    if not gid:
        return []

    ok, data = safe_call("GET", "/ncc/ads", customer_id, {"nccAdgroupId": gid})
    if ok and isinstance(data, list):
        return data

    if not ok:
        log(f"ℹ️ /ncc/ads 조회 실패로 ownerId fallback 생략 | customer_id={customer_id} adgroup_id={gid}")
        return []

    ok_owner, data_owner = safe_call("GET", "/ncc/ads", customer_id, {"ownerId": gid})
    if ok_owner and isinstance(data_owner, list):
        return data_owner
    return []
'''

OLD_REPLACE_FACT_RANGE = '''    last_err: Exception | None = None
    ctx = f"table={table} rows={len(tuples)}"
    for attempt in range(1, 4):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            return
'''

NEW_REPLACE_FACT_RANGE = '''    last_err: Exception | None = None
    page_size = 200 if table == "fact_ad_daily" else 5000
    local_stmt_timeout_ms = 900000 if table == "fact_ad_daily" else 300000
    ctx = f"table={table} rows={len(tuples)} page_size={page_size} stmt_timeout_ms={local_stmt_timeout_ms}"
    for attempt in range(1, 4):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            try:
                cur.execute(f"SET statement_timeout TO {int(local_stmt_timeout_ms)}")
            except Exception:
                pass
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=page_size)
            raw_conn.commit()
            return
'''

OLD_REPLACE_FACT_SCOPE = '''    last_err: Exception | None = None
    ctx = f"table={table} rows={len(tuples)}"
    for attempt in range(1, 4):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            return
'''

NEW_REPLACE_FACT_SCOPE = '''    last_err: Exception | None = None
    page_size = 200 if table == "fact_ad_daily" else 5000
    local_stmt_timeout_ms = 900000 if table == "fact_ad_daily" else 300000
    ctx = f"table={table} rows={len(tuples)} page_size={page_size} stmt_timeout_ms={local_stmt_timeout_ms}"
    for attempt in range(1, 4):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            try:
                cur.execute(f"SET statement_timeout TO {int(local_stmt_timeout_ms)}")
            except Exception:
                pass
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=page_size)
            raw_conn.commit()
            return
'''


def apply_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise RuntimeError(f"패치 대상 블록을 찾지 못했습니다: {label}")
    return text.replace(old, new, 1)


def main() -> None:
    target = Path("collector.py")
    if not target.exists():
        raise SystemExit("collector.py 파일을 찾지 못했습니다. 레포 루트에서 실행하세요.")

    src = target.read_text(encoding="utf-8")
    original = src
    src = apply_once(src, OLD_REQUEST_JSON, NEW_REQUEST_JSON, "request_json")
    src = apply_once(src, OLD_LIST_ADS, NEW_LIST_ADS, "list_ads")
    src = apply_once(src, OLD_REPLACE_FACT_RANGE, NEW_REPLACE_FACT_RANGE, "replace_fact_range")
    src = apply_once(src, OLD_REPLACE_FACT_SCOPE, NEW_REPLACE_FACT_SCOPE, "replace_fact_scope")

    if src == original:
        raise SystemExit("변경된 내용이 없습니다.")

    backup = target.with_name("collector_before_runtime_guard_patch.py")
    if not backup.exists():
        backup.write_text(original, encoding="utf-8")
    target.write_text(src, encoding="utf-8")
    print("patched: collector.py")
    print(f"backup: {backup.name}")


if __name__ == "__main__":
    main()
