"""Dustmaps utilities and caching."""

import os
from typing import Any

from dustmaps.config import config as dust_config

DUST_QUERY_CACHE: dict[str, Any] = {}


def _make_dust_query(name: str):
    name = (name or "sfd").strip().lower()
    if name == "sfd":
        from dustmaps.sfd import SFDQuery
        return SFDQuery()
    if name.startswith("bayestar"):
        from dustmaps.bayestar import BayestarQuery
        return BayestarQuery()
    if name == "planck":
        from dustmaps.planck import PlanckQuery
        return PlanckQuery()
    # generic fallback: try to import dustmaps.<name>.<*Query>
    import importlib

    mod = importlib.import_module(f"dustmaps.{name}")
    for attr in dir(mod):
        if attr.lower().endswith("query"):
            return getattr(mod, attr)()
    raise ValueError(f"Unsupported dustmap '{name}'")


def get_dust_query(dust_cfg: dict[str, Any]):
    name = (dust_cfg.get("use_dustmap") or "sfd").strip().lower()
    if name in DUST_QUERY_CACHE:
        return DUST_QUERY_CACHE[name]
    dq = _make_dust_query(name)
    DUST_QUERY_CACHE[name] = dq
    return dq


def configure_dustmaps_path(dust_cfg: dict[str, Any]):
    path = dust_cfg.get("path_to_dustmaps")
    if path:
        dust_config["data_dir"] = path
        os.environ.setdefault("DUSTMAPS_PATH", path)


__all__ = ["get_dust_query", "configure_dustmaps_path", "DUST_QUERY_CACHE"]
