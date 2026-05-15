# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from datetime import date, datetime
from typing import Any, Dict, List, Tuple

import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import text
from sqlalchemy.engine import Engine


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _exc_label(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _log_best_effort_failure(action: str, exc: Exception, *, ctx: str = ""):
    extra = f" | {ctx}" if ctx else ""
    log(f"⚠️ {action} 무시됨{extra} | {_exc_label(exc)}")


def _safe_rollback(raw_conn, *, ctx: str = ""):
    if not raw_conn:
        return
    try:
        raw_conn.rollback()
    except Exception as rollback_exc:
        extra = f" | {ctx}" if ctx else ""
        log(f"⚠️ 롤백 실패{extra} | {_exc_label(rollback_exc)}")


def _safe_close(resource, *, label: str, ctx: str = ""):
    if not resource:
        return
    try:
        resource.close()
    except Exception as close_exc:
        extra = f" | {ctx}" if ctx else ""
        log(f"⚠️ {label} close 실패{extra} | {_exc_label(close_exc)}")


MEDIA_HEADER_CANDIDATES = [
    "매체이름", "매체명", "매체", "노출매체", "지면", "노출지면", "media", "medianame", "mediatype", "placement", "network"
]
REGION_HEADER_CANDIDATES = [
    "지역", "지역명", "노출지역", "시도", "시군구", "행정구역", "region", "regionname", "location"
]
DEVICE_HEADER_CANDIDATES_LOCAL = [
    "pc mobile type", "pc_mobile_type", "pc/mobile type", "pcmobiletype",
    "device", "device_name", "devicename", "platform", "platform type",
    "기기", "디바이스", "노출기기", "노출 기기", "단말기", "플랫폼",
]
AD_HEADER_CANDIDATES_LOCAL = ["광고id", "소재id", "adid"]
IMP_HEADER_CANDIDATES_LOCAL = ["노출수", "impressions", "impcnt"]
CLK_HEADER_CANDIDATES_LOCAL = ["클릭수", "clicks", "clkcnt"]
COST_HEADER_CANDIDATES_LOCAL = ["총비용", "비용", "cost", "salesamt"]
CONV_HEADER_CANDIDATES_LOCAL = ["전환수", "conversions", "ccnt"]
SALES_HEADER_CANDIDATES_LOCAL = ["전환매출액", "전환매출", "conversionvalue", "sales", "convamt"]


def _m_normalize_header(v: Any) -> str:
    return str(v or '').lower().replace(' ', '').replace('_', '').replace('-', '').replace('"', '').replace("'", '')


def _m_get_col_idx(headers: List[str], candidates: List[str]) -> int:
    norm_headers = [_m_normalize_header(h) for h in headers]
    norm_candidates = [_m_normalize_header(c) for c in candidates]
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c == h:
                return i
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c and c in h:
                return i
    return -1


def _m_safe_float(v: Any) -> float:
    if pd.isna(v):
        return 0.0
    s = str(v).replace(',', '').strip()
    if not s or s == '-':
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _m_safe_text(v: Any, default: str = '전체') -> str:
    s = str(v or '').strip()
    if not s or s.lower() in {'nan', 'none'} or s == '-':
        return default
    return s


def _map_campaign_type_label(v: Any) -> str:
    s = str(v or '').strip()
    if not s:
        return '기타'
    up = s.upper()
    if up in {'WEB_SITE', 'WEBSITE', 'POWER_LINK'} or s == '파워링크':
        return '파워링크'
    if 'SHOPPING' in up or s == '쇼핑검색':
        return '쇼핑검색'
    if up in {'POWER_CONTENTS'} or s == '파워컨텐츠':
        return '파워컨텐츠'
    if up in {'BRAND_SEARCH'} or s == '브랜드검색':
        return '브랜드검색'
    if up in {'PLACE'} or s == '플레이스':
        return '플레이스'
    return s


def build_campaign_type_map(engine: Engine, customer_id: str) -> Dict[str, str]:
    sql = "SELECT campaign_id, campaign_tp FROM dim_campaign WHERE customer_id = :cid"
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {'cid': str(customer_id)}).fetchall()
        return {str(r[0]).strip(): _map_campaign_type_label(r[1]) for r in rows if str(r[0]).strip()}
    except Exception:
        return {}


def _get_fact_media_daily_conflict_cols(engine: Engine) -> List[str]:
    expected = ['dt', 'customer_id', 'campaign_type', 'media_name', 'region_name', 'device_name']
    legacy = ['dt', 'customer_id', 'campaign_type', 'media_name', 'region_name']
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
        log(f"⚠️ fact_media_daily 제약조건 조회 실패 → 기본 PK 가정 사용 | {type(e).__name__}: {e}")
        return expected

    grouped: Dict[Tuple[str, str], List[str]] = {}
    for row in rows:
        key = (str(row.get('constraint_name') or ''), str(row.get('constraint_type') or ''))
        grouped.setdefault(key, []).append(str(row.get('column_name') or '').strip())

    ordered_candidates: List[List[str]] = []
    for (_, constraint_type), cols in grouped.items():
        if constraint_type == 'PRIMARY KEY':
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
            log(f"⚠️ fact_media_daily 예상 외 제약조건 감지 | columns={cols}")
            return cols

    log("⚠️ fact_media_daily PK/UNIQUE 제약조건을 찾지 못해 기본 PK 가정 사용")
    return expected


def _prepare_media_fact_rows_for_conflict(df: pd.DataFrame, conflict_cols: List[str]) -> pd.DataFrame:
    expected = ['dt', 'customer_id', 'campaign_type', 'media_name', 'region_name', 'device_name']
    legacy = ['dt', 'customer_id', 'campaign_type', 'media_name', 'region_name']
    numeric_cols = ['imp', 'clk', 'cost', 'conv', 'sales']

    if 'device_name' not in df.columns:
        df['device_name'] = '전체'
    df['device_name'] = df['device_name'].map(lambda x: str(x).strip() if x is not None else '').replace('', '전체')

    if conflict_cols == expected:
        return df.drop_duplicates(subset=conflict_cols, keep='last').sort_values(by=conflict_cols)

    if conflict_cols == legacy:
        log("⚠️ fact_media_daily가 구 PK(device_name 제외) 스키마입니다. device_name을 '전체'로 병합해 임시 적재합니다. 스키마 마이그레이션 후 백필이 필요합니다.")
        work = df.copy()
        work['device_name'] = '전체'
        if 'data_source' in work.columns:
            work['data_source'] = 'legacy_pk_schema_aggregated'
        if 'source_report' in work.columns:
            work['source_report'] = work['source_report'].fillna('AD').replace('', 'AD')

        agg_spec: Dict[str, Any] = {}
        for col in work.columns:
            if col in legacy:
                continue
            if col in numeric_cols:
                agg_spec[col] = 'sum'
            else:
                agg_spec[col] = 'last'
        grouped = work.groupby(legacy, dropna=False, as_index=False).agg(agg_spec)
        ordered = legacy + [c for c in work.columns if c not in legacy]
        grouped = grouped[ordered]
        return grouped.sort_values(by=legacy)

    log(f"⚠️ fact_media_daily 예상 외 충돌키 사용 | {conflict_cols}")
    use_cols = [c for c in conflict_cols if c in df.columns]
    if not use_cols:
        use_cols = expected
    return df.drop_duplicates(subset=use_cols, keep='last').sort_values(by=use_cols)


def normalize_device_name(device_value: Any) -> str:
    v = str(device_value or '').strip().upper()
    if not v:
        return ''
    if v in {'M', 'MO', 'MOBILE', '모바일'}:
        return 'MO'
    if v in {'P', 'PC', 'DESKTOP', 'DESK', '컴퓨터'}:
        return 'PC'
    return v


def _has_media_metrics(imp: Any, clk: Any, cost: Any, conv: Any, sales: Any) -> bool:
    return any([
        int(round(_m_safe_float(imp))) != 0,
        int(round(_m_safe_float(clk))) != 0,
        int(round(_m_safe_float(cost))) != 0,
        float(_m_safe_float(conv)) != 0.0,
        int(round(_m_safe_float(sales))) != 0,
    ])


def _filter_nonzero_media_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in (rows or []):
        if _has_media_metrics(r.get('imp'), r.get('clk'), r.get('cost'), r.get('conv'), r.get('sales')):
            out.append(r)
    return out


def replace_media_fact_range(engine: Engine, rows: List[Dict[str, Any]], customer_id: str, d1: date, scoped_campaign_types: List[str] | None = None):
    table = 'fact_media_daily'
    pk_cols = _get_fact_media_daily_conflict_cols(engine)
    input_rows = len(rows or [])
    rows = _filter_nonzero_media_rows(rows or [])
    dropped_zero_rows = max(0, input_rows - len(rows))
    if dropped_zero_rows:
        log(f"ℹ️ fact_media_daily 0성과 행 제외 | cid={customer_id} dt={d1} dropped={dropped_zero_rows} kept={len(rows)}")
    last_delete_err: Exception | None = None
    delete_sql = text(
        f"DELETE FROM {table} WHERE customer_id=:cid AND dt=:dt" +
        (" AND campaign_type = ANY(:types)" if scoped_campaign_types else "")
    )

    for attempt in range(1, 4):
        try:
            with engine.begin() as conn:
                conn.execute(delete_sql, {'cid': str(customer_id), 'dt': d1, 'types': scoped_campaign_types or []})
            last_delete_err = None
            break
        except Exception as e:
            last_delete_err = e
            log(f"⚠️ fact_media_daily 삭제 실패 {attempt}/3 | cid={customer_id} dt={d1} pk={pk_cols} | {type(e).__name__}: {e}")
            time.sleep(2)
    if last_delete_err is not None:
        raise RuntimeError(f"fact_media_daily 삭제 실패 | cid={customer_id} dt={d1} pk={pk_cols} | {type(last_delete_err).__name__}: {last_delete_err}") from last_delete_err

    if not rows:
        reason = 'all_zero_filtered' if input_rows else 'empty'
        log(f"ℹ️ fact_media_daily 적재 대상 없음 | cid={customer_id} dt={d1} reason={reason} input_rows={input_rows}")
        return 0

    df = pd.DataFrame(rows).astype(object).where(pd.notnull, None)
    df = _prepare_media_fact_rows_for_conflict(df, pk_cols).astype(object).where(pd.notnull, None)
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    col_names = ", ".join([f'"{c}"' for c in cols])
    conflict_cols_sql = ", ".join(pk_cols)
    conflict_clause = (
        f'ON CONFLICT ({conflict_cols_sql}) DO UPDATE SET ' +
        ', '.join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])
        if update_cols else
        f'ON CONFLICT ({conflict_cols_sql}) DO NOTHING'
    )
    sql = f'INSERT INTO {table} ({col_names}) VALUES %s {conflict_clause}'
    tuples = list(df.itertuples(index=False, name=None))

    last_upsert_err: Exception | None = None
    for attempt in range(1, 4):
        raw_conn, cur = None, None
        try:
            raw_conn = engine.raw_connection()
            cur = raw_conn.cursor()
            psycopg2.extras.execute_values(cur, sql, tuples, page_size=5000)
            raw_conn.commit()
            log(f"✅ fact_media_daily 적재 완료 | cid={customer_id} dt={d1} rows={len(df)} pk={pk_cols}")
            return len(df)
        except Exception as e:
            last_upsert_err = e
            if raw_conn:
                _safe_rollback(raw_conn, ctx=f"fact_media_daily upsert cid={customer_id} dt={d1}")
            log(f"⚠️ fact_media_daily 적재 실패 {attempt}/3 | cid={customer_id} dt={d1} rows={len(df)} pk={pk_cols} | {type(e).__name__}: {e}")
            time.sleep(2)
        finally:
            _safe_close(cur, label="cursor", ctx=f"fact_media_daily upsert cid={customer_id} dt={d1}")
            _safe_close(raw_conn, label="connection", ctx=f"fact_media_daily upsert cid={customer_id} dt={d1}")
    raise RuntimeError(f"fact_media_daily 적재 최종 실패 | cid={customer_id} dt={d1} rows={len(df)} pk={pk_cols} | {type(last_upsert_err).__name__}: {last_upsert_err}") from last_upsert_err


def _log_media_parse_diag(diag: Dict[str, Any]):
    log(
        "📺 매체 파서 | "
        f"status={diag.get('status')} mode={diag.get('mode')} rows={diag.get('row_count', 0)} mapped={diag.get('mapped_rows', 0)} "
        f"detail={diag.get('detail_rows', 0)} summary={diag.get('summary_rows', 0)} distinct_media={diag.get('distinct_media_count', 0)}"
    )


def _resolve_media_campaign_id(row, ad_idx: int, camp_idx: int, ad_to_campaign: Dict[str, str]) -> str:
    campaign_id = ''
    if ad_idx != -1 and len(row) > ad_idx:
        ad_id = str(row.iloc[ad_idx]).strip()
        campaign_id = str(ad_to_campaign.get(ad_id, '') or '').strip()
    if not campaign_id and camp_idx != -1 and len(row) > camp_idx:
        campaign_id = str(row.iloc[camp_idx]).strip()
    return campaign_id


def _build_media_collect_meta(base_meta: Dict[str, Any] | None, *, status: str, selected_source: str, saved_rows: int) -> Dict[str, Any]:
    meta = dict(base_meta or {})
    meta['status'] = status
    meta['selected_source'] = selected_source
    meta['saved_rows'] = int(saved_rows or 0)
    return meta


def _log_media_collect_choice(customer_id: str, target_date: date, meta: Dict[str, Any]):
    log(
        "📺 매체 저장 선택 | "
        f"cid={customer_id} dt={target_date} status={meta.get('status')} selected={meta.get('selected_source')} saved={meta.get('saved_rows', 0)} "
        f"detail={meta.get('detail_rows', 0)} summary={meta.get('summary_rows', 0)} distinct_media={meta.get('distinct_media_count', 0)}"
    )


def _detect_media_header_idx(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return -1
    scan_limit = min(60, len(df))
    best_idx = -1
    best_score = -1
    for i in range(scan_limit):
        row_vals = [_m_normalize_header(x) for x in df.iloc[i].fillna('').tolist()]
        score = 0
        if any(c in row_vals for c in [_m_normalize_header(x) for x in AD_HEADER_CANDIDATES_LOCAL + ["캠페인id", "campaignid", "ncccampaignid"]]):
            score += 2
        if any(c in row_vals for c in [_m_normalize_header(x) for x in MEDIA_HEADER_CANDIDATES + REGION_HEADER_CANDIDATES + DEVICE_HEADER_CANDIDATES_LOCAL]):
            score += 2
        metric_hits = sum(1 for x in [_m_normalize_header(x) for x in IMP_HEADER_CANDIDATES_LOCAL + CLK_HEADER_CANDIDATES_LOCAL + COST_HEADER_CANDIDATES_LOCAL] if x in row_vals)
        score += min(metric_hits, 3)
        if score > best_score:
            best_score = score
            best_idx = i
        if score >= 5:
            return i
    return best_idx if best_score >= 3 else -1


def _finalize_media_rows(agg: Dict[Tuple[str, str, str, str], Dict[str, Any]], target_date: date, customer_id: str, data_source: str = 'unknown') -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    total_imp = total_clk = total_cost = total_sales = 0
    total_conv = 0.0
    for (campaign_type, media_name, region_name, device_name), bucket in agg.items():
        imp = int(round(float(bucket.get('imp', 0) or 0)))
        clk = int(round(float(bucket.get('clk', 0) or 0)))
        cost = int(round(float(bucket.get('cost', 0) or 0)))
        conv = float(bucket.get('conv', 0.0) or 0.0)
        sales = int(round(float(bucket.get('sales', 0) or 0)))
        rows.append({
            'dt': target_date,
            'customer_id': str(customer_id),
            'campaign_type': str(campaign_type or '기타'),
            'media_name': str(media_name or '전체'),
            'region_name': str(region_name or '전체'),
            'device_name': str(device_name or '전체'),
            'imp': imp,
            'clk': clk,
            'cost': cost,
            'conv': conv,
            'sales': sales,
            'data_source': data_source,
        })
        total_imp += imp
        total_clk += clk
        total_cost += cost
        total_conv += conv
        total_sales += sales
    diag = {
        'detail_rows': len(rows),
        'summary_rows': len(rows),
        'distinct_media_count': len({(r['media_name'], r['region_name'], r['device_name']) for r in rows}),
        'imp_sum': total_imp,
        'clk_sum': total_clk,
        'cost_sum': total_cost,
        'conv_sum': total_conv,
        'sales_sum': total_sales,
        'data_source': data_source,
    }
    return rows, diag


def _m_find_prefixed_value(values: List[str], prefixes: Tuple[str, ...]) -> str:
    for raw in values:
        s = str(raw or '').strip()
        sl = s.lower()
        if any(sl.startswith(p) for p in prefixes):
            return s
    return ''


def _m_numeric_candidates(values: List[str]) -> List[float]:
    out: List[float] = []
    for raw in values:
        s = str(raw or '').strip().replace(',', '')
        if not s or s == '-':
            continue
        try:
            out.append(float(s))
        except Exception:
            continue
    return out


def _m_guess_dim_tokens(values: List[str]) -> Tuple[str, str, str]:
    media_name = '전체'
    region_name = '전체'
    device_name = '전체'
    text_tokens: List[str] = []
    for raw in values:
        s = str(raw or '').strip()
        if not s or s == '-':
            continue
        sl = s.lower()
        if sl.startswith(('cmp-', 'nad-', 'grp-', 'nkw-', 'ad-')):
            continue
        try:
            float(s.replace(',', ''))
            continue
        except Exception:
            pass
        norm_device = normalize_device_name(s)
        if norm_device:
            device_name = norm_device
            continue
        text_tokens.append(s)
    if text_tokens:
        media_name = _m_safe_text(text_tokens[0], '전체')
    if len(text_tokens) >= 2:
        region_name = _m_safe_text(text_tokens[1], '전체')
    return media_name, region_name, device_name


def _build_media_rows_from_noheader(df: pd.DataFrame, target_date: date, customer_id: str, campaign_type_map: Dict[str, str], allowed_campaign_ids: set[str] | None = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if df is None or df.empty:
        diag = {'status': 'empty', 'mode': 'noheader'}
        _log_media_parse_diag(diag)
        return [], diag

    agg: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    row_count = 0
    mapped_rows = 0
    short_rows = 0
    invalid_campaign_rows = 0
    filtered_rows = 0

    for _, row in df.iterrows():
        row_count += 1
        values = [str(x).strip() for x in row.fillna('').tolist()]
        if not any(values):
            short_rows += 1
            continue

        campaign_id = _m_find_prefixed_value(values, ('cmp-',))
        if not campaign_id:
            invalid_campaign_rows += 1
            continue
        if allowed_campaign_ids is not None and campaign_id not in allowed_campaign_ids:
            filtered_rows += 1
            continue

        nums = _m_numeric_candidates(values)
        if len(nums) >= 5:
            imp, clk, cost, conv, sales = nums[-5:]
        elif len(nums) == 4:
            imp, clk, cost, conv = nums[-4:]
            sales = 0.0
        elif len(nums) == 3:
            imp, clk, cost = nums[-3:]
            conv, sales = 0.0, 0.0
        else:
            short_rows += 1
            continue

        media_name, region_name, device_name = _m_guess_dim_tokens(values)
        campaign_type = campaign_type_map.get(campaign_id, '기타')
        key = (campaign_type, media_name, region_name, device_name)
        bucket = agg.setdefault(key, {'imp': 0, 'clk': 0, 'cost': 0, 'conv': 0.0, 'sales': 0})
        bucket['imp'] += int(round(imp))
        bucket['clk'] += int(round(clk))
        bucket['cost'] += int(round(cost))
        bucket['conv'] += float(conv)
        bucket['sales'] += int(round(sales))
        mapped_rows += 1

    rows, agg_diag = _finalize_media_rows(agg, target_date, customer_id, data_source='ad_report_noheader')
    diag = {
        'status': 'ok' if rows else 'no_rows',
        'mode': 'noheader',
        'row_count': row_count,
        'mapped_rows': mapped_rows,
        'short_rows': short_rows,
        'invalid_campaign_rows': invalid_campaign_rows,
        'filtered_rows': filtered_rows,
    }
    diag.update(agg_diag)
    _log_media_parse_diag(diag)
    return rows, diag


def build_media_rows_from_campaign_device(target_date: date, customer_id: str, camp_device_stat: Dict[Tuple[str, str], Dict[str, Any]] | None, campaign_type_map: Dict[str, str], allowed_campaign_ids: set[str] | None = None) -> List[Dict[str, Any]]:
    agg: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for (campaign_id, raw_device), stat in (camp_device_stat or {}).items():
        cid = str(campaign_id or '').strip()
        if not cid:
            continue
        if allowed_campaign_ids is not None and cid not in allowed_campaign_ids:
            continue
        device_name = normalize_device_name(raw_device) or '전체'
        campaign_type = campaign_type_map.get(cid, '기타')
        key = (campaign_type, '전체', '전체', device_name)
        bucket = agg.setdefault(key, {'imp': 0, 'clk': 0, 'cost': 0, 'conv': 0.0, 'sales': 0})
        bucket['imp'] += int(round(float(stat.get('imp', 0) or 0)))
        bucket['clk'] += int(round(float(stat.get('clk', 0) or 0)))
        bucket['cost'] += int(round(float(stat.get('cost', 0) or 0)))
        bucket['conv'] += float(stat.get('conv', 0) or 0.0)
        bucket['sales'] += int(round(float(stat.get('sales', 0) or 0)))
    rows, _ = _finalize_media_rows(agg, target_date, customer_id, data_source='campaign_device_fallback')
    return rows


def build_media_rows_from_campaign_total_db(engine: Engine, customer_id: str, target_date: date, campaign_type_map: Dict[str, str], allowed_campaign_ids: set[str] | None = None) -> List[Dict[str, Any]]:
    sql = text("""
        SELECT campaign_id, imp, clk, cost, conv, sales
        FROM fact_campaign_daily
        WHERE customer_id = :cid AND dt = :dt
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {'cid': str(customer_id), 'dt': target_date}).mappings().all()
    except Exception as e:
        _log_best_effort_failure('campaign total fallback 조회', e, ctx=f'cid={customer_id} dt={target_date}')
        return []

    agg: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for r in rows:
        cid = str(r.get('campaign_id') or '').strip()
        if not cid:
            continue
        if allowed_campaign_ids is not None and cid not in allowed_campaign_ids:
            continue
        campaign_type = campaign_type_map.get(cid, '기타')
        key = (campaign_type, '전체', '전체', '전체')
        bucket = agg.setdefault(key, {'imp': 0, 'clk': 0, 'cost': 0, 'conv': 0.0, 'sales': 0})
        bucket['imp'] += int(round(float(r.get('imp', 0) or 0)))
        bucket['clk'] += int(round(float(r.get('clk', 0) or 0)))
        bucket['cost'] += int(round(float(r.get('cost', 0) or 0)))
        bucket['conv'] += float(r.get('conv', 0) or 0.0)
        bucket['sales'] += int(round(float(r.get('sales', 0) or 0)))
    rows, _ = _finalize_media_rows(agg, target_date, customer_id, data_source='campaign_total_fallback')
    return rows


def parse_media_report_rows(df: pd.DataFrame, target_date: date, customer_id: str, ad_to_campaign: Dict[str, str], campaign_type_map: Dict[str, str], allowed_campaign_ids: set[str] | None = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if df is None or df.empty:
        return [], {'status': 'empty'}

    raw_df = df.reset_index(drop=True).copy()
    header_idx = _detect_media_header_idx(raw_df)
    if header_idx == -1:
        return _build_media_rows_from_noheader(raw_df, target_date, customer_id, campaign_type_map, allowed_campaign_ids=allowed_campaign_ids)

    headers_raw = [str(x) for x in raw_df.iloc[header_idx].fillna('').tolist()]
    headers = [_m_normalize_header(x) for x in headers_raw]
    data_df = raw_df.iloc[header_idx + 1:].reset_index(drop=True)

    ad_idx = _m_get_col_idx(headers, AD_HEADER_CANDIDATES_LOCAL)
    camp_idx = _m_get_col_idx(headers, ["캠페인id", "campaignid", "ncccampaignid"])
    media_idx = _m_get_col_idx(headers, MEDIA_HEADER_CANDIDATES)
    region_idx = _m_get_col_idx(headers, REGION_HEADER_CANDIDATES)
    device_idx = _m_get_col_idx(headers, DEVICE_HEADER_CANDIDATES_LOCAL)
    imp_idx = _m_get_col_idx(headers, IMP_HEADER_CANDIDATES_LOCAL)
    clk_idx = _m_get_col_idx(headers, CLK_HEADER_CANDIDATES_LOCAL)
    cost_idx = _m_get_col_idx(headers, COST_HEADER_CANDIDATES_LOCAL)
    conv_idx = _m_get_col_idx(headers, CONV_HEADER_CANDIDATES_LOCAL)
    sales_idx = _m_get_col_idx(headers, SALES_HEADER_CANDIDATES_LOCAL)

    if ad_idx == -1 and camp_idx == -1:
        diag = {'status': 'no_ad_or_camp_id', 'mode': 'header', 'header_idx': header_idx, 'headers': headers_raw[:20]}
        _log_media_parse_diag(diag)
        return [], diag

    if media_idx == -1 and region_idx == -1 and device_idx == -1:
        diag = {'status': 'no_dimension', 'mode': 'header', 'header_idx': header_idx, 'headers': headers_raw[:20]}
        _log_media_parse_diag(diag)
        return [], diag

    agg: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    row_count = 0
    mapped_rows = 0
    short_rows = 0
    missing_campaign_rows = 0
    filtered_rows = 0
    max_idx = max([x for x in [ad_idx, camp_idx, media_idx, region_idx, device_idx, imp_idx, clk_idx, cost_idx, conv_idx, sales_idx] if x != -1], default=0)
    for _, row in data_df.iterrows():
        row_count += 1
        if len(row) <= max_idx:
            short_rows += 1
            continue

        campaign_id = _resolve_media_campaign_id(row, ad_idx, camp_idx, ad_to_campaign)
        if not campaign_id:
            missing_campaign_rows += 1
            continue
        if allowed_campaign_ids is not None and campaign_id not in allowed_campaign_ids:
            filtered_rows += 1
            continue

        mapped_rows += 1
        campaign_type = campaign_type_map.get(campaign_id, '기타')
        media_name = _m_safe_text(row.iloc[media_idx] if media_idx != -1 and len(row) > media_idx else '', '전체')
        region_name = _m_safe_text(row.iloc[region_idx] if region_idx != -1 and len(row) > region_idx else '', '전체')
        raw_device = row.iloc[device_idx] if device_idx != -1 and len(row) > device_idx else ''
        device_name = normalize_device_name(raw_device) or _m_safe_text(raw_device, '전체')
        key = (campaign_type, media_name, region_name, device_name)
        bucket = agg.setdefault(key, {'imp': 0, 'clk': 0, 'cost': 0, 'conv': 0.0, 'sales': 0})
        bucket['imp'] += int(round(_m_safe_float(row.iloc[imp_idx]) if imp_idx != -1 and len(row) > imp_idx else 0))
        bucket['clk'] += int(round(_m_safe_float(row.iloc[clk_idx]) if clk_idx != -1 and len(row) > clk_idx else 0))
        bucket['cost'] += int(round(_m_safe_float(row.iloc[cost_idx]) if cost_idx != -1 and len(row) > cost_idx else 0))
        bucket['conv'] += float(_m_safe_float(row.iloc[conv_idx]) if conv_idx != -1 and len(row) > conv_idx else 0)
        bucket['sales'] += int(round(_m_safe_float(row.iloc[sales_idx]) if sales_idx != -1 and len(row) > sales_idx else 0))

    rows, agg_diag = _finalize_media_rows(agg, target_date, customer_id, data_source='ad_report_dimension')
    diag = {
        'status': 'ok' if rows else 'no_rows',
        'mode': 'header',
        'header_idx': header_idx,
        'row_count': row_count,
        'mapped_rows': mapped_rows,
        'short_rows': short_rows,
        'missing_campaign_rows': missing_campaign_rows,
        'filtered_rows': filtered_rows,
        'dim_cols': {'media': media_idx, 'region': region_idx, 'device': device_idx},
    }
    diag.update(agg_diag)
    _log_media_parse_diag(diag)
    return rows, diag


def collect_media_fact(engine: Engine, customer_id: str, target_date: date, ad_report_df: pd.DataFrame | None, ad_to_campaign_map: Dict[str, str], campaign_type_map: Dict[str, str], camp_device_stat: Dict[Tuple[str, str], Dict[str, Any]] | None = None, allowed_campaign_ids: set[str] | None = None, scoped_campaign_types: List[str] | None = None) -> Tuple[int, Dict[str, Any]]:
    media_rows, meta = parse_media_report_rows(ad_report_df, target_date, customer_id, ad_to_campaign_map, campaign_type_map, allowed_campaign_ids=allowed_campaign_ids)
    if media_rows:
        saved = replace_media_fact_range(engine, media_rows, customer_id, target_date, scoped_campaign_types=scoped_campaign_types)
        meta = _build_media_collect_meta(meta, status=str(meta.get('status') or 'ok'), selected_source='report', saved_rows=saved)
        _log_media_collect_choice(customer_id, target_date, meta)
        return saved, meta

    if camp_device_stat:
        fb_rows = build_media_rows_from_campaign_device(target_date, customer_id, camp_device_stat, campaign_type_map, allowed_campaign_ids=allowed_campaign_ids)
        if fb_rows:
            saved = replace_media_fact_range(engine, fb_rows, customer_id, target_date, scoped_campaign_types=scoped_campaign_types)
            meta = _build_media_collect_meta(meta, status='fallback_device', selected_source='campaign_device_fallback', saved_rows=saved)
            _log_media_collect_choice(customer_id, target_date, meta)
            return saved, meta

    total_rows = build_media_rows_from_campaign_total_db(engine, customer_id, target_date, campaign_type_map, allowed_campaign_ids=allowed_campaign_ids)
    if total_rows:
        saved = replace_media_fact_range(engine, total_rows, customer_id, target_date, scoped_campaign_types=scoped_campaign_types)
        meta = _build_media_collect_meta(meta, status='fallback_total', selected_source='campaign_total_fallback', saved_rows=saved)
        _log_media_collect_choice(customer_id, target_date, meta)
        return saved, meta

    saved = replace_media_fact_range(engine, [], customer_id, target_date, scoped_campaign_types=scoped_campaign_types)
    meta = _build_media_collect_meta(meta, status='empty', selected_source='none', saved_rows=saved)
    _log_media_collect_choice(customer_id, target_date, meta)
    return saved, meta
