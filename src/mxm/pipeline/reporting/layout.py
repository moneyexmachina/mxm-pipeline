from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ReportingLayout:
    """
    Filesystem layout for MXM pipeline reporting store.

    We keep this layout pipeline-local. It can be extracted later if reporting
    storage becomes its own package.

    Layout principles (V1):
    - reporting owns a single SQLite database
    - reporting data is pipeline-internal rather than vendor-rooted
    - current-state tables and append-only event logs live inside that DB
    - layout remains minimal until additional reporting artifacts exist
      (for example exported reports or archived event bundles)
    """

    root: Path

    def reporting_dir(self) -> Path:
        """
        Root directory for pipeline reporting storage.
        """
        return self.root / "reporting"

    def sqlite_db_path(self) -> Path:
        """
        Path to the single reporting SQLite database.

        This DB is owned by pipeline reporting and stores:
        - materialised current-state tables for FlowRun / TaskRun / TaskAttempt
        - append-only execution event log
        - append-only semantic event log
        """
        return self.reporting_dir() / "reporting.sqlite3"
