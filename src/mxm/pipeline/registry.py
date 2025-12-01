"""
Demo registry for mxm-pipeline flows.

This module exposes a minimal, in-process flow catalog used by the CLI during
M2.1. It lazily builds and caches a single demo flow under the key "demo".
"""

from __future__ import annotations

from mxm.pipeline.spec import FlowSpec

__all__ = ["get_flow", "get_flows"]

# Internal lazy cache of flow specs (demo-only).
_cache: dict[str, FlowSpec] | None = None


def get_flows() -> dict[str, FlowSpec]:
    """
    Return the available flows in the demo registry.

    The registry is populated lazily on first call and cached for the lifetime
    of the process. Keys are normalized to lowercase.

    Returns
    -------
    Dict[str, FlowSpec]
        Mapping of flow-name -> FlowSpec. Currently only {"demo": ...}.
    """
    global _cache
    if _cache is None:
        from .demos.prefect_demo import build_demo_flow

        _cache = {"demo": build_demo_flow()}
    return _cache


def get_flow(name: str) -> FlowSpec:
    """
    Retrieve a single flow by name (case-insensitive).

    Parameters
    ----------
    name : str
        The flow identifier to resolve.

    Returns
    -------
    FlowSpec
        The resolved flow specification.

    Raises
    ------
    KeyError
        If the requested flow name is not present in the registry.
    """
    flows = get_flows()
    key = name.strip().lower()
    if key not in flows:
        raise KeyError(f"Unknown flow: {name}")
    return flows[key]
