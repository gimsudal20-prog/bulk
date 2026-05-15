# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import os
import re
from datetime import date, datetime
from typing import Any, Dict, List, Tuple

import pandas as pd


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _exc_label(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _log_best_effort_failure(action: str, exc: Exception, *, ctx: str = ""):
    extra = f" | {ctx}" if ctx else ""
    log(f"⚠️ {action} 무시됨{extra} | {_exc_label(exc)}")


def normalize_header(v: str) -> str:
    return str(v).lower().replace(" ", "").replace("_", "").replace("-", "").replace('"', '').replace("'", "")


def normalize_keyword_text(v: str) -> str:
    s = str(v or "").strip().lower()
    if not s or s == "-":
        return ""
    out = []
    for ch in s:
        if ch.isalnum() or ('가' <= ch <= '힣'):
            out.append(ch)
    return "".join(out)


def extract_prefixed_token(vals, prefix: str) -> str:
    prefix_l = str(prefix).lower()
    p = re.compile(rf"\b{re.escape(prefix_l)}[a-z0-9-]+", re.I)
    for v in vals:
        s = str(v).strip()
        if s.lower().startswith(prefix_l):
            return s
        m = p.search(s)
        if m:
            return m.group(0)
    return ""


def keyword_text_candidates(kw_norm: str, rows: list[tuple[str, str]]) -> list[str]:
    if not kw_norm:
        return []
    hits = []
    for db_norm, kid in rows:
        if not db_norm or not kid:
            continue
        if db_norm == kw_norm or kw_norm in db_norm or db_norm in kw_norm:
            hits.append(kid)
    seen, out = set(), []
    for kid in hits:
        if kid not in seen:
            seen.add(kid)
            out.append(kid)
    return out


def get_col_idx(headers: List[str], candidates: List[str]) -> int:
    norm_headers = [normalize_header(h) for h in headers]
    norm_candidates = [normalize_header(c) for c in candidates]
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c == h:
                return i
    for c in norm_candidates:
        for i, h in enumerate(norm_headers):
            if c in h and "그룹" not in h:
                return i
    return -1


def safe_float(v) -> float:
    if pd.isna(v):
        return 0.0
    s = str(v).replace(",", "").strip()
    if not s or s == "-":
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def split_enabled_for_date(target_date: date, cart_enable_date: date) -> bool:
    return target_date >= cart_enable_date


def is_shopping_campaign_obj(camp: dict, shopping_hint_keys: tuple[str, ...]) -> bool:
    hay = " ".join([
        str(camp.get("campaignTp", "")),
        str(camp.get("campaignType", "")),
        str(camp.get("type", "")),
        str(camp.get("name", "")),
    ]).lower()
    return any(k in hay for k in shopping_hint_keys)


def merge_split_maps(*maps: dict) -> dict:
    out = {}
    for mp in maps:
        if not mp:
            continue
        for k, v in mp.items():
            if not k:
                continue
            b = out.setdefault(str(k), {
                "purchase_conv": 0.0,
                "purchase_sales": 0,
                "cart_conv": 0.0,
                "cart_sales": 0,
                "wishlist_conv": 0.0,
                "wishlist_sales": 0,
            })
            b["purchase_conv"] += float(v.get("purchase_conv", 0.0) or 0.0)
            b["purchase_sales"] += int(float(v.get("purchase_sales", 0) or 0))
            b["cart_conv"] += float(v.get("cart_conv", 0.0) or 0.0)
            b["cart_sales"] += int(float(v.get("cart_sales", 0) or 0))
            b["wishlist_conv"] += float(v.get("wishlist_conv", 0.0) or 0.0)
            b["wishlist_sales"] += int(float(v.get("wishlist_sales", 0) or 0))
    return out


def filter_split_map_excluding_ids(split_map: dict, excluded_ids: set[str] | None = None) -> dict:
    if not split_map:
        return {}
    excluded = {str(x).strip() for x in (excluded_ids or set()) if str(x).strip()}
    if not excluded:
        return dict(split_map)
    out = {}
    for k, v in split_map.items():
        ks = str(k).strip()
        if not ks or ks in excluded:
            continue
        out[ks] = v
    return out


def empty_split_summary() -> dict:
    return {
        "purchase_conv": 0.0,
        "purchase_sales": 0,
        "cart_conv": 0.0,
        "cart_sales": 0,
        "wishlist_conv": 0.0,
        "wishlist_sales": 0,
    }


def summarize_split_map(split_map: dict) -> dict:
    out = empty_split_summary()
    if not split_map:
        return out
    for v in split_map.values():
        if not isinstance(v, dict):
            continue
        out['purchase_conv'] += float(v.get('purchase_conv', 0.0) or 0.0)
        out['purchase_sales'] += int(float(v.get('purchase_sales', 0) or 0))
        out['cart_conv'] += float(v.get('cart_conv', 0.0) or 0.0)
        out['cart_sales'] += int(float(v.get('cart_sales', 0) or 0))
        out['wishlist_conv'] += float(v.get('wishlist_conv', 0.0) or 0.0)
        out['wishlist_sales'] += int(float(v.get('wishlist_sales', 0) or 0))
    return out


def validate_shopping_split_summary(summary: dict, ad_map: dict) -> tuple[bool, str]:
    if not split_summary_has_values(summary) or not ad_map:
        return True, ''
    map_sum = summarize_split_map(ad_map)
    checks = [
        ('purchase_conv', 0.6),
        ('purchase_sales', 0.15),
        ('cart_conv', 1.5),
        ('cart_sales', 0.20),
        ('wishlist_conv', 1.5),
        ('wishlist_sales', 0.20),
    ]
    mismatches = []
    for key, ratio_tol in checks:
        s_val = float(summary.get(key, 0) or 0)
        m_val = float(map_sum.get(key, 0) or 0)
        if s_val <= 0 and m_val <= 0:
            continue
        diff = abs(s_val - m_val)
        base = max(abs(s_val), abs(m_val), 1.0)
        if diff / base > ratio_tol:
            mismatches.append(f"{key} summary={s_val} ad_map={m_val}")
    return (len(mismatches) == 0, '; '.join(mismatches))


def add_split_summary(summary: dict, is_purchase: bool, is_cart: bool, is_wishlist: bool, c_val: float, s_val: int):
    if is_purchase:
        summary["purchase_conv"] += float(c_val or 0.0)
        summary["purchase_sales"] += int(s_val or 0)
    elif is_cart:
        summary["cart_conv"] += float(c_val or 0.0)
        summary["cart_sales"] += int(s_val or 0)
    elif is_wishlist:
        summary["wishlist_conv"] += float(c_val or 0.0)
        summary["wishlist_sales"] += int(s_val or 0)


def merge_split_summaries(*summaries: dict) -> dict:
    out = empty_split_summary()
    for s in summaries:
        if not s:
            continue
        out["purchase_conv"] += float(s.get("purchase_conv", 0.0) or 0.0)
        out["purchase_sales"] += int(float(s.get("purchase_sales", 0) or 0))
        out["cart_conv"] += float(s.get("cart_conv", 0.0) or 0.0)
        out["cart_sales"] += int(float(s.get("cart_sales", 0) or 0))
        out["wishlist_conv"] += float(s.get("wishlist_conv", 0.0) or 0.0)
        out["wishlist_sales"] += int(float(s.get("wishlist_sales", 0) or 0))
    return out


def split_summary_has_values(summary: dict) -> bool:
    if not summary:
        return False
    return any(float(summary.get(k, 0) or 0) > 0 for k in ["purchase_conv", "cart_conv", "wishlist_conv"])


def format_split_summary(summary: dict) -> str:
    def fmt(v):
        try:
            fv = float(v or 0)
            return str(int(fv)) if fv.is_integer() else f"{fv:.2f}".rstrip('0').rstrip('.')
        except Exception:
            return str(v)
    return (
        f"구매완료 {fmt(summary.get('purchase_conv', 0))}건 | "
        f"장바구니 {fmt(summary.get('cart_conv', 0))}건 | "
        f"위시리스트 {fmt(summary.get('wishlist_conv', 0))}건"
    )


def _conv_empty_maps_and_summary() -> tuple[dict, dict, dict, dict]:
    return {}, {}, {}, empty_split_summary()


def _conv_ensure_split_bucket(m_dict: dict, obj_id: str):
    if obj_id not in m_dict:
        m_dict[obj_id] = {
            "purchase_conv": 0.0,
            "purchase_sales": 0,
            "cart_conv": 0.0,
            "cart_sales": 0,
            "wishlist_conv": 0.0,
            "wishlist_sales": 0,
        }


def _conv_apply_row(m_dict: dict, obj_id: str, is_purchase: bool, is_cart: bool, is_wishlist: bool, c_val: float, s_val: int):
    obj_id = str(obj_id).strip()
    if not obj_id or obj_id == '-':
        return
    _conv_ensure_split_bucket(m_dict, obj_id)
    if is_purchase:
        m_dict[obj_id]["purchase_conv"] += c_val
        m_dict[obj_id]["purchase_sales"] += s_val
    elif is_cart:
        m_dict[obj_id]["cart_conv"] += c_val
        m_dict[obj_id]["cart_sales"] += s_val
    elif is_wishlist:
        m_dict[obj_id]["wishlist_conv"] += c_val
        m_dict[obj_id]["wishlist_sales"] += s_val


def _conv_classify_conversion_value(v) -> tuple[bool, bool, bool]:
    ctype = str(v).strip().lower()
    ctype_norm = ctype.replace('_', '').replace('-', '').replace(' ', '')
    is_purchase = (
        '구매완료' in ctype_norm or ctype_norm == '구매' or ctype_norm in {'1', 'purchase', 'purchasing'}
    )
    is_cart = (
        '장바구니담기' in ctype_norm or '장바구니' in ctype_norm or ctype_norm in {'3', 'cart', 'addtocart', 'addtocarts'}
    )
    is_wishlist = (
        '위시리스트추가' in ctype_norm or '위시리스트' in ctype_norm or '상품찜' in ctype_norm or ctype_norm in {'wishlist', 'addtowishlist', 'wishlistadd', 'wish'}
    )
    return is_purchase, is_cart, is_wishlist


def _conv_maybe_numeric(v: str) -> float | None:
    s = str(v).strip().replace(',', '')
    if not s or s == '-':
        return None
    if re.fullmatch(r'-?\d+(?:\.\d+)?', s):
        try:
            return float(s)
        except Exception:
            return None
    return None


def _conv_looks_like_id(v: str) -> bool:
    s = str(v).strip().lower()
    return s.startswith(('cmp-', 'grp-', 'nkw-', 'nad-', 'bsn-'))


def _conv_row_allowed(row_campaign_id: str | None, allowed_campaign_ids: set[str]) -> bool:
    if not allowed_campaign_ids:
        return True
    row_campaign_id = str(row_campaign_id or "").strip()
    return bool(row_campaign_id) and row_campaign_id in allowed_campaign_ids


def _conv_add_debug_row(debug_rows: list[dict], report_hint: str, debug_account_name: str, debug_target_date: str,
                        vals, parsed_type, c_val, s_val, kept, reason,
                        row_cid="", row_gid="", row_kid="", row_adid="", kw_text="", kw_obj_id=""):
    debug_rows.append({
        "report_tp": report_hint,
        "date": str(debug_target_date or ""),
        "account_name": str(debug_account_name or ""),
        "campaign_id": str(row_cid or ""),
        "adgroup_id": str(row_gid or ""),
        "keyword_id": str(row_kid or ""),
        "keyword_text": str(kw_text or ""),
        "keyword_mapped_id": str(kw_obj_id or ""),
        "ad_id": str(row_adid or ""),
        "parsed_type": str(parsed_type or ""),
        "parsed_count": c_val,
        "parsed_sales": s_val,
        "kept": 1 if kept else 0,
        "reason": reason,
        "row": " | ".join([str(x) for x in vals]),
    })


def _conv_flush_debug_rows(debug_rows: list[dict], report_hint: str, debug_account_name: str, debug_target_date: str, *, fast_mode: bool = False):
    if fast_mode or not debug_rows or not debug_account_name or not debug_target_date:
        return
    dbg_dir = os.path.join(os.getcwd(), "debug_split_rows")
    os.makedirs(dbg_dir, exist_ok=True)
    safe_name = re.sub(r'[^0-9A-Za-z가-힣._-]+', '_', str(debug_account_name))
    out_path = os.path.join(dbg_dir, f"{debug_target_date}_{safe_name}_{report_hint}.csv")
    fields = [
        "report_tp", "date", "account_name", "campaign_id", "adgroup_id", "keyword_id", "keyword_text", "keyword_mapped_id", "ad_id",
        "parsed_type", "parsed_count", "parsed_sales", "kept", "reason", "row"
    ]
    with open(out_path, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(debug_rows)


def _conv_guess_campaign_id_from_row(vals: list[str]) -> str:
    for v in vals:
        s = str(v).strip().lower()
        if s.startswith('cmp-'):
            return str(v).strip()
    return ""


def _conv_first_value_with_prefix(vals: list[str], prefix: str) -> str:
    for v in vals:
        s = str(v).strip()
        if s.lower().startswith(prefix):
            return s
    return ""


def _conv_value_from_idx_or_scan(vals: list[str], idx: int, prefix: str, allow_dash: bool = False) -> str:
    if 0 <= idx < len(vals):
        v = str(vals[idx]).strip()
        if v.lower().startswith(prefix):
            return v
        if allow_dash and v == '-':
            return v
    return _conv_first_value_with_prefix(vals, prefix)


def _conv_best_prefixed_idx(sample_rows, target_prefix: str, allow_dash: bool = False, preferred_after: int = -1) -> int:
    max_cols = max((len(r) for r in sample_rows), default=0)
    best_idx, best_score, best_prefix_hits = -1, -1, 0
    for i in range(max_cols):
        score = 0
        prefix_hits = 0
        dash_hits = 0
        for r in sample_rows:
            if len(r) <= i:
                continue
            v = str(r.iloc[i]).strip().lower()
            if v.startswith(target_prefix):
                score += 5
                prefix_hits += 1
            elif allow_dash and v == '-':
                dash_hits += 1
        if prefix_hits > 0:
            score += min(dash_hits, prefix_hits)
        if preferred_after >= 0 and i <= preferred_after:
            score -= 2
        if prefix_hits > best_prefix_hits or (prefix_hits == best_prefix_hits and score > best_score):
            best_idx, best_score, best_prefix_hits = i, score, prefix_hits
    return best_idx if best_prefix_hits > 0 else -1


def _conv_extract_header_rows(df: pd.DataFrame) -> tuple[int, list[str]]:
    header_idx = -1
    headers: list[str] = []
    for i in range(min(20, len(df))):
        row_vals = [normalize_header(str(x)) for x in df.iloc[i].fillna("")]
        if (
            'conversiontype' in row_vals or '전환유형' in row_vals or 'convtp' in row_vals or
            '총전환수' in row_vals or 'conversioncount' in row_vals
        ):
            header_idx = i
            headers = row_vals
            break
    return header_idx, headers


def _conv_resolve_header_indexes(headers: list[str]) -> dict[str, int]:
    return {
        'cid_idx': get_col_idx(headers, ['캠페인id', 'campaignid', 'ncccampaignid']),
        'kid_idx': get_col_idx(headers, ['키워드id', 'keywordid', 'ncckeywordid']),
        'adid_idx': get_col_idx(headers, ['광고id', '소재id', 'adid', 'nccadid']),
        'type_idx': get_col_idx(headers, ['전환유형', 'conversiontype', 'convtp']),
        'cnt_idx': get_col_idx(headers, ['총전환수', '전환수', 'conversions', 'conversioncount', 'ccnt']),
        'sales_idx': get_col_idx(headers, ['총전환매출액(원)', '전환매출액', 'conversionvalue', 'sales', 'salesbyconversion', 'convamt']),
    }


def _conv_try_header_mode(df: pd.DataFrame, allowed_campaign_ids: set[str], report_hint: str,
                          debug_account_name: str, debug_target_date: str, *, fast_mode: bool = False) -> tuple[dict, dict, dict, dict] | None:
    camp_map, kw_map, ad_map, summary = _conv_empty_maps_and_summary()
    debug_rows: list[dict] = []
    header_idx, headers = _conv_extract_header_rows(df)
    if header_idx == -1:
        return None
    idxs = _conv_resolve_header_indexes(headers)
    type_idx = idxs['type_idx']
    cnt_idx = idxs['cnt_idx']
    sales_idx = idxs['sales_idx']
    if type_idx == -1 or cnt_idx == -1:
        return None

    data_df = df.iloc[header_idx + 1:]
    for _, r in data_df.iterrows():
        need_max = max(type_idx, cnt_idx, sales_idx if sales_idx != -1 else -1)
        if len(r) <= need_max:
            continue
        row_campaign_id = r.iloc[idxs['cid_idx']] if idxs['cid_idx'] != -1 and len(r) > idxs['cid_idx'] else ''
        vals = [str(x) for x in r.tolist()]
        if not _conv_row_allowed(row_campaign_id, allowed_campaign_ids):
            _conv_add_debug_row(debug_rows, report_hint, debug_account_name, debug_target_date, vals, "", 0, 0, False, "campaign_filtered_header")
            continue
        is_purchase, is_cart, is_wishlist = _conv_classify_conversion_value(r.iloc[type_idx])
        if not (is_purchase or is_cart or is_wishlist):
            continue
        c_val = safe_float(r.iloc[cnt_idx])
        s_val = int(safe_float(r.iloc[sales_idx])) if sales_idx != -1 else 0
        add_split_summary(summary, is_purchase, is_cart, is_wishlist, c_val, s_val)
        _conv_add_debug_row(
            debug_rows, report_hint, debug_account_name, debug_target_date, vals,
            "purchase" if is_purchase else ("cart" if is_cart else "wishlist"), c_val, s_val, True, "header_keep"
        )
        if idxs['cid_idx'] != -1 and len(r) > idxs['cid_idx']:
            _conv_apply_row(camp_map, r.iloc[idxs['cid_idx']], is_purchase, is_cart, is_wishlist, c_val, s_val)
        if idxs['kid_idx'] != -1 and len(r) > idxs['kid_idx']:
            _conv_apply_row(kw_map, r.iloc[idxs['kid_idx']], is_purchase, is_cart, is_wishlist, c_val, s_val)
        if idxs['adid_idx'] != -1 and len(r) > idxs['adid_idx']:
            _conv_apply_row(ad_map, r.iloc[idxs['adid_idx']], is_purchase, is_cart, is_wishlist, c_val, s_val)

    if camp_map or kw_map or ad_map:
        _conv_flush_debug_rows(debug_rows, report_hint, debug_account_name, debug_target_date, fast_mode=fast_mode)
        return camp_map, kw_map, ad_map, summary
    return None


def _conv_detect_heuristic_indexes(df: pd.DataFrame, report_hint: str) -> dict[str, int]:
    sample_rows = [df.iloc[i].fillna("") for i in range(min(20, len(df)))]
    cid_idx = _conv_best_prefixed_idx(sample_rows, 'cmp-')
    gid_idx = _conv_best_prefixed_idx(sample_rows, 'grp-', preferred_after=cid_idx)
    kid_idx = _conv_best_prefixed_idx(sample_rows, 'nkw-', allow_dash=True, preferred_after=max(cid_idx, gid_idx))
    adid_idx = _conv_best_prefixed_idx(sample_rows, 'nad-', preferred_after=max(cid_idx, gid_idx, kid_idx))

    kw_text_idx = -1
    if report_hint.upper() == 'SHOPPINGKEYWORD_CONVERSION_DETAIL':
        candidate = gid_idx + 1 if gid_idx != -1 else -1
        max_cols = max((len(r) for r in sample_rows), default=0)
        if 0 <= candidate < max_cols:
            text_score = 0
            for r in sample_rows:
                if len(r) <= candidate:
                    continue
                v = str(r.iloc[candidate]).strip()
                if v and v != '-' and not _conv_looks_like_id(v) and _conv_maybe_numeric(v) is None:
                    text_score += 1
            if text_score > 0:
                kw_text_idx = candidate

    return {
        'cid_idx': cid_idx,
        'gid_idx': gid_idx,
        'kid_idx': kid_idx,
        'adid_idx': adid_idx,
        'kw_text_idx': kw_text_idx,
    }


def _conv_find_type_hits(vals: list[str], report_hint: str) -> list[tuple[int, bool, bool, bool]]:
    n = len(vals)
    text_type_hits = []
    numeric_type_hits = []
    for idx, v in enumerate(vals):
        s_raw = str(v).strip()
        is_purchase, is_cart, is_wishlist = _conv_classify_conversion_value(v)
        if not (is_purchase or is_cart or is_wishlist):
            continue
        if s_raw in {'1', '3'}:
            if idx >= max(0, n - 6):
                numeric_type_hits.append((idx, is_purchase, is_cart, is_wishlist))
        else:
            text_type_hits.append((idx, is_purchase, is_cart, is_wishlist))
    type_hits = text_type_hits if text_type_hits else numeric_type_hits
    if not type_hits and report_hint.upper() == 'SHOPPINGKEYWORD_CONVERSION_DETAIL':
        return []
    return type_hits


def _conv_pick_numeric_payload(vals: list[str], type_hits: list[tuple[int, bool, bool, bool]]) -> tuple[bool, bool, bool, float, int] | None:
    n = len(vals)
    for type_idx, is_purchase, is_cart, is_wishlist in type_hits:
        anchor_idx = type_idx
        anchor_is_purchase, anchor_is_cart, anchor_is_wishlist = is_purchase, is_cart, is_wishlist
        raw_tok = str(vals[type_idx]).strip().lower()
        if raw_tok in {'1', '2', '3'} and type_idx + 1 < n:
            n_is_purchase, n_is_cart, n_is_wishlist = _conv_classify_conversion_value(vals[type_idx + 1])
            if n_is_purchase or n_is_cart or n_is_wishlist:
                anchor_idx = type_idx + 1
                anchor_is_purchase, anchor_is_cart, anchor_is_wishlist = n_is_purchase, n_is_cart, n_is_wishlist

        numeric_right = []
        for j in range(anchor_idx + 1, n):
            vv = vals[j]
            if _conv_looks_like_id(vv):
                continue
            num = _conv_maybe_numeric(vv)
            if num is not None:
                numeric_right.append((j, num))
        if not numeric_right:
            continue

        c_val = float(numeric_right[0][1])
        s_val = int(numeric_right[1][1]) if len(numeric_right) >= 2 else 0
        return anchor_is_purchase, anchor_is_cart, anchor_is_wishlist, c_val, s_val
    return None


def _conv_resolve_keyword_object_id(row_kid: str, row_gid: str, kw_text_idx: int, vals: list[str],
                                    keyword_lookup: dict, live_keyword_resolver) -> tuple[str, str, str]:
    kw_obj_id = ""
    kw_text = ""
    row_kid_s = str(row_kid).strip()
    if row_kid_s not in {"", "-"} and row_kid_s.lower().startswith("nkw-"):
        kw_obj_id = row_kid_s
    elif kw_text_idx != -1 and kw_text_idx < len(vals) and row_gid:
        kw_text = str(vals[kw_text_idx]).strip()
        kw_norm = normalize_keyword_text(kw_text)
        kw_obj_id = (
            keyword_lookup.get((row_gid, kw_text), "")
            or keyword_lookup.get((row_gid, kw_text.lower()), "")
            or keyword_lookup.get((row_gid, kw_norm), "")
        )
        if not kw_obj_id:
            group_rows = keyword_lookup.get((row_gid, '__rows__'), [])
            cands = keyword_text_candidates(kw_norm, group_rows)
            if len(cands) == 1:
                kw_obj_id = cands[0]
        if not kw_obj_id and live_keyword_resolver:
            try:
                kw_obj_id = live_keyword_resolver(row_gid, kw_text) or ""
            except Exception as e:
                _log_best_effort_failure("live keyword resolve", e, ctx=f"row_gid={row_gid} kw_text={kw_text[:40]}")
                kw_obj_id = ""
    return kw_obj_id, kw_text, row_kid_s


def _conv_try_heuristic_mode(df: pd.DataFrame, allowed_campaign_ids: set[str], report_hint: str,
                             keyword_lookup: dict, live_keyword_resolver,
                             debug_account_name: str, debug_target_date: str, *, fast_mode: bool = False) -> tuple[dict, dict, dict, dict]:
    camp_map, kw_map, ad_map, summary = _conv_empty_maps_and_summary()
    debug_rows: list[dict] = []
    idxs = _conv_detect_heuristic_indexes(df, report_hint)

    for _, r in df.iterrows():
        vals = ["" if pd.isna(x) else str(x).strip() for x in r.tolist()]
        n = len(vals)
        if n < 2:
            continue

        type_hits = _conv_find_type_hits(vals, report_hint)
        if not type_hits:
            _conv_add_debug_row(debug_rows, report_hint, debug_account_name, debug_target_date, vals, "", 0, 0, False, "no_type_hit")
            continue

        row_campaign_id = _conv_guess_campaign_id_from_row(vals)
        if not _conv_row_allowed(row_campaign_id, allowed_campaign_ids):
            _conv_add_debug_row(debug_rows, report_hint, debug_account_name, debug_target_date, vals, "", 0, 0, False, "campaign_filtered")
            continue

        picked = _conv_pick_numeric_payload(vals, type_hits)
        if not picked:
            _conv_add_debug_row(debug_rows, report_hint, debug_account_name, debug_target_date, vals, "", 0, 0, False, "no_numeric_right")
            continue

        is_purchase, is_cart, is_wishlist, c_val, s_val = picked
        add_split_summary(summary, is_purchase, is_cart, is_wishlist, c_val, s_val)
        row_cid = _conv_value_from_idx_or_scan(vals, idxs['cid_idx'], 'cmp-') or extract_prefixed_token(vals, 'cmp-')
        row_gid = _conv_value_from_idx_or_scan(vals, idxs['gid_idx'], 'grp-') or extract_prefixed_token(vals, 'grp-')
        row_kid = _conv_value_from_idx_or_scan(vals, idxs['kid_idx'], 'nkw-', allow_dash=True)
        if row_kid in {'', '-'}:
            row_kid = extract_prefixed_token(vals, 'nkw-')
        row_adid = _conv_value_from_idx_or_scan(vals, idxs['adid_idx'], 'nad-') or extract_prefixed_token(vals, 'nad-')

        if row_cid:
            _conv_apply_row(camp_map, row_cid, is_purchase, is_cart, is_wishlist, c_val, s_val)

        kw_obj_id, kw_text, row_kid_s = _conv_resolve_keyword_object_id(
            row_kid, row_gid, idxs['kw_text_idx'], vals, keyword_lookup, live_keyword_resolver
        )
        if kw_obj_id:
            _conv_apply_row(kw_map, kw_obj_id, is_purchase, is_cart, is_wishlist, c_val, s_val)

        if row_adid:
            _conv_apply_row(ad_map, row_adid, is_purchase, is_cart, is_wishlist, c_val, s_val)

        _conv_add_debug_row(
            debug_rows,
            report_hint,
            debug_account_name,
            debug_target_date,
            vals,
            "purchase" if is_purchase else ("cart" if is_cart else "wishlist"),
            c_val,
            s_val,
            True,
            "keep",
            row_cid=row_cid,
            row_gid=row_gid,
            row_kid=row_kid_s,
            row_adid=row_adid,
            kw_text=kw_text,
            kw_obj_id=kw_obj_id,
        )

    _conv_flush_debug_rows(debug_rows, report_hint, debug_account_name, debug_target_date, fast_mode=fast_mode)
    return camp_map, kw_map, ad_map, summary


def process_conversion_report(df: pd.DataFrame, allowed_campaign_ids: set[str] | None = None, report_hint: str = "", keyword_lookup: dict | None = None, keyword_unique_lookup: dict | None = None, live_keyword_resolver=None, debug_account_name: str = "", debug_target_date: str = "", *, fast_mode: bool = False) -> Tuple[dict, dict, dict, dict]:
    allowed_campaign_ids = set(str(x).strip() for x in (allowed_campaign_ids or set()) if str(x).strip())
    keyword_lookup = keyword_lookup or {}
    keyword_unique_lookup = keyword_unique_lookup or {}
    if df is None or df.empty:
        return _conv_empty_maps_and_summary()

    header_result = _conv_try_header_mode(
        df,
        allowed_campaign_ids=allowed_campaign_ids,
        report_hint=report_hint,
        debug_account_name=debug_account_name,
        debug_target_date=debug_target_date,
        fast_mode=fast_mode,
    )
    if header_result is not None:
        return header_result

    return _conv_try_heuristic_mode(
        df,
        allowed_campaign_ids=allowed_campaign_ids,
        report_hint=report_hint,
        keyword_lookup=keyword_lookup,
        live_keyword_resolver=live_keyword_resolver,
        debug_account_name=debug_account_name,
        debug_target_date=debug_target_date,
        fast_mode=fast_mode,
    )


def _sq_classify_conversion_type(v) -> tuple[bool, bool, bool]:
    ctype = str(v).strip().lower()
    ctype_norm = ctype.replace('_', '').replace('-', '').replace(' ', '')
    is_purchase = ('구매완료' in ctype_norm or ctype_norm == '구매' or ctype_norm in {'1', 'purchase', 'purchasing'})
    is_cart = ('장바구니담기' in ctype_norm or '장바구니' in ctype_norm or ctype_norm in {'3', 'cart', 'addtocart', 'addtocarts'})
    is_wishlist = ('위시리스트추가' in ctype_norm or '위시리스트' in ctype_norm or '상품찜' in ctype_norm or ctype_norm in {'wishlist', 'addtowishlist', 'wishlistadd', 'wish'})
    return is_purchase, is_cart, is_wishlist


def _sq_best_prefixed_idx(sample_rows, target_prefix: str, preferred_after: int = -1) -> int:
    max_cols = max((len(r) for r in sample_rows), default=0)
    best_idx, best_score, best_prefix_hits = -1, -1, 0
    for i in range(max_cols):
        score = 0
        prefix_hits = 0
        for r in sample_rows:
            if len(r) <= i:
                continue
            v = str(r.iloc[i]).strip().lower()
            if v.startswith(target_prefix):
                score += 5
                prefix_hits += 1
        if preferred_after >= 0 and i <= preferred_after:
            score -= 2
        if prefix_hits > best_prefix_hits or (prefix_hits == best_prefix_hits and score > best_score):
            best_idx, best_score, best_prefix_hits = i, score, prefix_hits
    return best_idx if best_prefix_hits > 0 else -1


def _sq_detect_query_text_idx(sample_rows, gid_idx: int) -> int:
    candidate = gid_idx + 1 if gid_idx != -1 else -1
    max_cols = max((len(r) for r in sample_rows), default=0)
    if not (0 <= candidate < max_cols):
        return -1
    text_score = 0
    for r in sample_rows:
        if len(r) <= candidate:
            continue
        v = str(r.iloc[candidate]).strip()
        if v and v != '-' and not v.lower().startswith(('cmp-', 'grp-', 'nkw-', 'nad-', 'bsn-')):
            vv = v.replace(',', '')
            if not re.fullmatch(r'-?\d+(?:\.\d+)?', vv):
                text_score += 1
    return candidate if text_score > 0 else -1


def _sq_find_type_hits(vals: List[str]):
    text_type_hits = []
    numeric_type_hits = []
    n = len(vals)
    for idx, v in enumerate(vals):
        s_raw = str(v).strip()
        is_purchase, is_cart, is_wishlist = _sq_classify_conversion_type(v)
        if not (is_purchase or is_cart or is_wishlist):
            continue
        hit = (idx, is_purchase, is_cart, is_wishlist)
        if s_raw in {'1', '3'}:
            if idx >= max(0, n - 6):
                numeric_type_hits.append(hit)
        else:
            text_type_hits.append(hit)
    return text_type_hits if text_type_hits else numeric_type_hits


def _sq_extract_numeric_right(vals: List[str], anchor_idx: int):
    numeric_right = []
    for j in range(anchor_idx + 1, min(anchor_idx + 4, len(vals))):
        s = str(vals[j]).strip().replace(',', '')
        if re.fullmatch(r'-?\d+(?:\.\d+)?', s):
            try:
                numeric_right.append((j, float(s)))
            except ValueError:
                continue
    return numeric_right


def _log_shopping_query_parse_diag(diag: Dict[str, Any]):
    log(
        "🧩 SHOPPINGKEYWORD_CONVERSION_DETAIL 파서 | "
        f"rows={diag.get('rows', 0)} kept={diag.get('kept', 0)} unique={diag.get('unique', 0)} "
        f"short={diag.get('short', 0)} no_type={diag.get('no_type', 0)} no_numeric={diag.get('no_numeric', 0)} "
        f"missing_id={diag.get('missing_id', 0)} idx=(cid:{diag.get('cid_idx', -1)}, gid:{diag.get('gid_idx', -1)}, ad:{diag.get('adid_idx', -1)}, q:{diag.get('kw_text_idx', -1)})"
    )


def parse_shopping_query_report(df: pd.DataFrame, target_date: date, customer_id: str) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []

    rows_map: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    sample_rows = [df.iloc[i].fillna("") for i in range(min(20, len(df)))]
    cid_idx = _sq_best_prefixed_idx(sample_rows, 'cmp-')
    gid_idx = _sq_best_prefixed_idx(sample_rows, 'grp-', preferred_after=cid_idx)
    adid_idx = _sq_best_prefixed_idx(sample_rows, 'nad-', preferred_after=max(cid_idx, gid_idx))
    kw_text_idx = _sq_detect_query_text_idx(sample_rows, gid_idx)

    diag = {
        'rows': 0,
        'kept': 0,
        'short': 0,
        'no_type': 0,
        'no_numeric': 0,
        'missing_id': 0,
        'cid_idx': cid_idx,
        'gid_idx': gid_idx,
        'adid_idx': adid_idx,
        'kw_text_idx': kw_text_idx,
    }

    for _, r in df.iterrows():
        diag['rows'] += 1
        vals = ["" if pd.isna(x) else str(x).strip() for x in r.tolist()]
        if len(vals) < 2:
            diag['short'] += 1
            continue

        type_hits = _sq_find_type_hits(vals)
        if not type_hits:
            diag['no_type'] += 1
            continue

        anchor_idx, is_purchase, is_cart, is_wishlist = type_hits[-1]
        numeric_right = _sq_extract_numeric_right(vals, anchor_idx)
        if not numeric_right:
            diag['no_numeric'] += 1
            continue

        c_val = float(numeric_right[0][1])
        s_val = int(numeric_right[1][1]) if len(numeric_right) >= 2 else 0
        row_cid = vals[cid_idx].strip() if 0 <= cid_idx < len(vals) else ""
        row_gid = vals[gid_idx].strip() if 0 <= gid_idx < len(vals) else ""
        row_adid = vals[adid_idx].strip() if 0 <= adid_idx < len(vals) else ""
        query_text = vals[kw_text_idx].strip() if 0 <= kw_text_idx < len(vals) else ""
        if not row_gid or not row_adid or not query_text or query_text == '-':
            diag['missing_id'] += 1
            continue

        key = (row_cid, row_gid, row_adid, query_text)
        row = rows_map.setdefault(key, {
            "dt": target_date,
            "customer_id": str(customer_id),
            "campaign_id": row_cid,
            "adgroup_id": row_gid,
            "ad_id": row_adid,
            "query_text": query_text,
            "total_conv": 0.0,
            "total_sales": 0,
            "purchase_conv": 0.0,
            "purchase_sales": 0,
            "cart_conv": 0.0,
            "cart_sales": 0,
            "wishlist_conv": 0.0,
            "wishlist_sales": 0,
            "split_available": True,
            "data_source": "SHOPPINGKEYWORD_CONVERSION_DETAIL",
        })
        row["total_conv"] += c_val
        row["total_sales"] += s_val
        if is_purchase:
            row["purchase_conv"] += c_val
            row["purchase_sales"] += s_val
        elif is_cart:
            row["cart_conv"] += c_val
            row["cart_sales"] += s_val
        elif is_wishlist:
            row["wishlist_conv"] += c_val
            row["wishlist_sales"] += s_val
        diag['kept'] += 1

    diag['unique'] = len(rows_map)
    _log_shopping_query_parse_diag(diag)
    return list(rows_map.values())


def build_keyword_lookup_from_keyword_report(df: pd.DataFrame) -> tuple[dict, dict]:
    lookup = {}
    unique_lookup = {}
    if df is None or df.empty:
        return lookup, unique_lookup

    header_idx = -1
    pk_cands = ["키워드id", "keywordid", "ncckeywordid"]
    for i in range(min(20, len(df))):
        row_vals = [normalize_header(str(x)) for x in df.iloc[i].fillna("")]
        if any(c in row_vals for c in [normalize_header(x) for x in pk_cands]) or "노출수" in row_vals or "impressions" in row_vals:
            header_idx = i
            break

    if header_idx != -1:
        headers = [normalize_header(str(x)) for x in df.iloc[header_idx].fillna("")]
        data_df = df.iloc[header_idx + 1:]
        kid_idx = get_col_idx(headers, ["키워드id", "keywordid", "ncckeywordid"])
        gid_idx = get_col_idx(headers, ["광고그룹id", "adgroupid", "nccadgroupid"])
        kw_idx = get_col_idx(headers, ["키워드", "keyword", "연관검색어", "relkeyword", "검색어"])
    else:
        data_df = df.iloc[1:] if ("date" in str(df.iloc[0, 0]).lower() or "id" in str(df.iloc[0, 0]).lower()) else df
        gid_idx = 3
        kw_idx = 4
        kid_idx = 5

    rows = []
    text_freq = {}
    group_rows = {}
    for _, r in data_df.iterrows():
        vals = r.fillna("").tolist()
        if len(vals) <= max(kid_idx, gid_idx, kw_idx):
            continue
        kid = str(vals[kid_idx]).strip() if kid_idx != -1 and len(vals) > kid_idx else ""
        gid = str(vals[gid_idx]).strip() if gid_idx != -1 and len(vals) > gid_idx else ""
        kw = str(vals[kw_idx]).strip() if kw_idx != -1 and len(vals) > kw_idx else ""
        if not kid or kid == '-' or not gid or gid == '-' or not kw or kw == '-':
            continue
        kid_s = kid
        gid_s = gid
        kw_s = kw
        kw_l = kw_s.lower()
        kw_n = normalize_keyword_text(kw_s)
        lookup[(gid_s, kw_s)] = kid_s
        lookup[(gid_s, kw_l)] = kid_s
        lookup[(gid_s, kw_n)] = kid_s
        group_rows.setdefault(gid_s, []).append((kw_n, kid_s))
        if kw_n:
            text_freq[kw_n] = text_freq.get(kw_n, 0) + 1
            rows.append((kw_n, kid_s))
    for gid_s, rs in group_rows.items():
        lookup[(gid_s, '__rows__')] = rs
    for kw_n, kid_s in rows:
        if text_freq.get(kw_n) == 1:
            unique_lookup.setdefault(kw_n, []).append(kid_s)
    return lookup, unique_lookup


def _resolve_base_report_pk_candidates(report_tp: str) -> List[str]:
    if "CAMPAIGN" in report_tp:
        return ["캠페인id", "campaignid"]
    if "KEYWORD" in report_tp:
        return ["키워드id", "keywordid", "ncckeywordid"]
    if "AD" in report_tp:
        return ["광고id", "소재id", "adid"]
    return []


def _detect_base_report_layout(df: pd.DataFrame, report_tp: str) -> Dict[str, Any]:
    pk_cands = _resolve_base_report_pk_candidates(report_tp)
    header_idx = -1
    for i in range(min(20, len(df))):
        row_vals = [normalize_header(str(x)) for x in df.iloc[i].fillna("")]
        if any(c in row_vals for c in [normalize_header(x) for x in pk_cands]) or "노출수" in row_vals or "impressions" in row_vals:
            header_idx = i
            break
    if header_idx != -1:
        headers = [normalize_header(str(x)) for x in df.iloc[header_idx].fillna("")]
        return {
            'mode': 'header',
            'header_idx': header_idx,
            'data_df': df.iloc[header_idx + 1:],
            'pk_idx': get_col_idx(headers, pk_cands),
            'imp_idx': get_col_idx(headers, ["노출수", "impressions", "impcnt"]),
            'clk_idx': get_col_idx(headers, ["클릭수", "clicks", "clkcnt"]),
            'cost_idx': get_col_idx(headers, ["총비용", "cost", "salesamt"]),
            'conv_idx': get_col_idx(headers, ["전환수", "conversions", "ccnt"]),
            'sales_idx': get_col_idx(headers, ["전환매출액", "conversionvalue", "sales", "convamt"]),
            'rank_idx': get_col_idx(headers, ["평균노출순위", "averageposition", "avgrnk"]),
        }
    return {
        'mode': 'fallback',
        'header_idx': -1,
        'data_df': df.iloc[1:] if ("date" in str(df.iloc[0, 0]).lower() or "id" in str(df.iloc[0, 0]).lower()) else df,
        'pk_idx': 2 if "CAMPAIGN" in report_tp else 5,
        'imp_idx': 5 if "CAMPAIGN" in report_tp else 8,
        'clk_idx': 6 if "CAMPAIGN" in report_tp else 9,
        'cost_idx': 7 if "CAMPAIGN" in report_tp else 10,
        'conv_idx': 8 if "CAMPAIGN" in report_tp else 11,
        'sales_idx': 9 if "CAMPAIGN" in report_tp else 12,
        'rank_idx': 11 if "CAMPAIGN" in report_tp else 14,
    }


def _is_base_report_invalid_id(obj_id: str) -> bool:
    return (not obj_id or obj_id == '-' or obj_id.lower() in ['id', 'keywordid', 'adid', 'campaignid'])


def _log_base_report_diag(report_tp: str, diag: Dict[str, Any]):
    log(
        f"📊 {report_tp} 파서 | mode={diag.get('mode')} rows={diag.get('rows', 0)} kept={diag.get('kept', 0)} "
        f"short={diag.get('short', 0)} invalid_id={diag.get('invalid_id', 0)} split={diag.get('split_applied', 0)} "
        f"idx=(pk:{diag.get('pk_idx', -1)}, imp:{diag.get('imp_idx', -1)}, clk:{diag.get('clk_idx', -1)}, cost:{diag.get('cost_idx', -1)}, conv:{diag.get('conv_idx', -1)}, sales:{diag.get('sales_idx', -1)}, rank:{diag.get('rank_idx', -1)})"
    )


def parse_base_report(df: pd.DataFrame, report_tp: str, conv_map: dict | None = None, has_conv_report: bool = False) -> dict:
    if df is None or df.empty:
        return {}

    layout = _detect_base_report_layout(df, report_tp)
    data_df = layout['data_df']
    pk_idx = layout['pk_idx']
    imp_idx = layout['imp_idx']
    clk_idx = layout['clk_idx']
    cost_idx = layout['cost_idx']
    conv_idx = layout['conv_idx']
    sales_idx = layout['sales_idx']
    rank_idx = layout['rank_idx']

    diag = {
        'mode': layout.get('mode'),
        'rows': 0,
        'kept': 0,
        'short': 0,
        'invalid_id': 0,
        'split_applied': 0,
        'pk_idx': pk_idx,
        'imp_idx': imp_idx,
        'clk_idx': clk_idx,
        'cost_idx': cost_idx,
        'conv_idx': conv_idx,
        'sales_idx': sales_idx,
        'rank_idx': rank_idx,
    }

    res = {}
    for _, r in data_df.iterrows():
        diag['rows'] += 1
        if len(r) <= pk_idx:
            diag['short'] += 1
            continue

        obj_id = str(r.iloc[pk_idx]).strip()
        if _is_base_report_invalid_id(obj_id):
            diag['invalid_id'] += 1
            continue

        if obj_id not in res:
            res[obj_id] = {
                "imp": 0,
                "clk": 0,
                "cost": 0,
                "conv": 0.0,
                "sales": 0,
                "purchase_conv": 0.0 if has_conv_report else None,
                "purchase_sales": 0 if has_conv_report else None,
                "cart_conv": 0.0 if has_conv_report else None,
                "cart_sales": 0 if has_conv_report else None,
                "wishlist_conv": 0.0 if has_conv_report else None,
                "wishlist_sales": 0 if has_conv_report else None,
                "split_available": bool(has_conv_report),
                "rank_sum": 0.0,
                "rank_cnt": 0,
            }

        imp = int(safe_float(r.iloc[imp_idx])) if imp_idx != -1 and len(r) > imp_idx else 0
        res[obj_id]["imp"] += imp

        if clk_idx != -1 and len(r) > clk_idx:
            res[obj_id]["clk"] += int(safe_float(r.iloc[clk_idx]))
        if cost_idx != -1 and len(r) > cost_idx:
            res[obj_id]["cost"] += int(safe_float(r.iloc[cost_idx]))
        if conv_idx != -1 and len(r) > conv_idx:
            res[obj_id]["conv"] += safe_float(r.iloc[conv_idx])
        if sales_idx != -1 and len(r) > sales_idx:
            res[obj_id]["sales"] += int(safe_float(r.iloc[sales_idx]))

        if rank_idx != -1 and len(r) > rank_idx:
            rnk = safe_float(r.iloc[rank_idx])
            if rnk > 0 and imp > 0:
                res[obj_id]["rank_sum"] += (rnk * imp)
                res[obj_id]["rank_cnt"] += imp
        diag['kept'] += 1

    if has_conv_report and conv_map is not None:
        for obj_id, bucket in res.items():
            split = conv_map.get(obj_id)
            if split:
                bucket["purchase_conv"] = split.get("purchase_conv", 0.0)
                bucket["purchase_sales"] = split.get("purchase_sales", 0)
                bucket["cart_conv"] = split.get("cart_conv", 0.0)
                bucket["cart_sales"] = split.get("cart_sales", 0)
                bucket["wishlist_conv"] = split.get("wishlist_conv", 0.0)
                bucket["wishlist_sales"] = split.get("wishlist_sales", 0)
                diag['split_applied'] += 1

    _log_base_report_diag(report_tp, diag)
    return res
