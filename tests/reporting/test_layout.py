from __future__ import annotations

from pathlib import Path

from mxm.pipeline.reporting.layout import ReportingLayout


def test_reporting_dir() -> None:
    root = Path("mxm-root")
    layout = ReportingLayout(root=root)

    assert layout.reporting_dir() == root / "reporting"


def test_sqlite_db_path() -> None:
    root = Path("mxm-root")
    layout = ReportingLayout(root=root)

    assert layout.sqlite_db_path() == root / "reporting" / "reporting.sqlite3"
