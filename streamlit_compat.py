# -*- coding: utf-8 -*-
"""Compatibility shims for Streamlit versions used in lightweight deploys.

The app uses a few modern Streamlit APIs. This module keeps the runtime usable
when a deployment resolves an older version from requirements.txt.
"""

from __future__ import annotations

import inspect
from functools import wraps
from typing import Any, Callable

import streamlit as st


def _identity_fragment(func: Callable | None = None, **_kwargs):
    if func is None:
        def _decorator(inner: Callable) -> Callable:
            return inner
        return _decorator
    return func


def _supports_kw(fn: Callable, name: str) -> bool:
    try:
        sig = inspect.signature(fn)
    except Exception:
        return True
    if name in sig.parameters:
        return True
    return any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())


def _normalize_width_kwargs(fn: Callable, kwargs: dict[str, Any]) -> dict[str, Any]:
    out = dict(kwargs)
    if _supports_kw(fn, "width"):
        return out
    width = out.pop("width", None)
    if width == "stretch" and _supports_kw(fn, "use_container_width"):
        out.setdefault("use_container_width", True)
    return out


def _drop_unsupported_kwargs(fn: Callable, kwargs: dict[str, Any], names: tuple[str, ...]) -> dict[str, Any]:
    out = dict(kwargs)
    for name in names:
        if name in out and not _supports_kw(fn, name):
            out.pop(name, None)
    return out


def _wrap_streamlit_call(name: str, *, normalize_width: bool = False, drop: tuple[str, ...] = ()) -> None:
    original = getattr(st, name, None)
    if original is None or getattr(original, "_da_compat_wrapped", False):
        return

    @wraps(original)
    def _wrapped(*args, **kwargs):
        next_kwargs = dict(kwargs)
        if normalize_width:
            next_kwargs = _normalize_width_kwargs(original, next_kwargs)
        if drop:
            next_kwargs = _drop_unsupported_kwargs(original, next_kwargs, drop)
        return original(*args, **next_kwargs)

    _wrapped._da_compat_wrapped = True  # type: ignore[attr-defined]
    setattr(st, name, _wrapped)


def _fallback_segmented_control(label, options, default=None, key=None, label_visibility="visible", **kwargs):
    opts = list(options or [])
    if not opts:
        return None
    index = opts.index(default) if default in opts else 0
    return st.radio(
        label,
        opts,
        index=index,
        key=key,
        horizontal=True,
        label_visibility=label_visibility,
        help=kwargs.get("help"),
    )


def _fallback_pills(label, options, default=None, key=None, label_visibility="visible", **kwargs):
    return st.segmented_control(
        label,
        options,
        default=default,
        key=key,
        label_visibility=label_visibility,
        help=kwargs.get("help"),
    )


def _fallback_logo(image, **_kwargs) -> None:
    try:
        st.sidebar.image(image, use_container_width=True)
    except Exception:
        return


def apply_streamlit_compat() -> None:
    if not hasattr(st, "fragment"):
        st.fragment = _identity_fragment  # type: ignore[attr-defined]
    if not hasattr(st, "segmented_control"):
        st.segmented_control = _fallback_segmented_control  # type: ignore[attr-defined]
    if not hasattr(st, "pills"):
        st.pills = _fallback_pills  # type: ignore[attr-defined]
    if not hasattr(st, "logo"):
        st.logo = _fallback_logo  # type: ignore[attr-defined]

    _wrap_streamlit_call("dataframe", normalize_width=True)
    _wrap_streamlit_call("data_editor", normalize_width=True)
    _wrap_streamlit_call("download_button", normalize_width=True, drop=("icon",))
    _wrap_streamlit_call("button", drop=("icon",))


apply_streamlit_compat()
