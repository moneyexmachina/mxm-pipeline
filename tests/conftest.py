# pyright: reportUnusedFunction=false

from __future__ import annotations

import os
import typing as _t

import pytest


@pytest.fixture(scope="session", autouse=True)
def _prefect_quiet_session() -> _t.Iterator[None]:
    """
    Make Prefect behave quietly in tests and avoid Rich writing to a closed stream
    at interpreter shutdown. Only affects tests; restores original state on exit.
    """
    # 1) Env knobs before Prefect import (kept minimal)
    os.environ.setdefault("PREFECT_API_ENABLE", "false")
    os.environ.setdefault("PREFECT_UI_ENABLED", "false")
    os.environ.setdefault("PREFECT_LOGGING_TO_CONSOLE", "false")
    os.environ.setdefault("PREFECT_LOGGING_LEVEL", "WARNING")

    # 2) Patch Prefect's ConsoleHandler.emit to swallow only the teardown ValueError.
    original_emit = None  # type: ignore[assignment]
    try:
        from prefect.logging.handlers import ConsoleHandler  # type: ignore

        original_emit = ConsoleHandler.emit  # type: ignore[attr-defined]

        def safe_emit(self, record):  # type: ignore[no-redef]
            try:
                return original_emit(self, record)  # type: ignore[misc]
            except ValueError:
                # Swallow "I/O operation on closed file" from Rich during pytest teardown.
                return None

        ConsoleHandler.emit = safe_emit  # type: ignore[assignment]
    except Exception:
        original_emit = None  # Prefect not imported/available; nothing to patch

    # Run tests
    yield

    # Restore patched method
    if original_emit is not None:
        from prefect.logging.handlers import ConsoleHandler  # type: ignore

        ConsoleHandler.emit = original_emit  # type: ignore[assignment]
