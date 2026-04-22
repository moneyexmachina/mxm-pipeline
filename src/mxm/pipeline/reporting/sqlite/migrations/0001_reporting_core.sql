-- 0001_reporting_core.sql
--
-- MXM Pipeline reporting SQLite schema:
-- - Materialised current-state tables for FlowRun / TaskRun / TaskAttempt
-- - Append-only operational execution event log
-- - Append-only semantic event log
--
-- Design notes:
-- - Current-state tables are keyed by the reporting object IDs and are upserted.
-- - Event tables are append-only ledgers keyed by event_id.
-- - Timestamps are stored as ISO8601 UTC with Z, lexicographically sortable.
-- - JSON payloads are stored as canonical JSON text.

PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------------------
-- A) Current-state tables
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS flow_runs (
    flow_run_id TEXT PRIMARY KEY,
    flow_name TEXT NOT NULL,
    status TEXT NOT NULL,

    started_at TEXT,
    finished_at TEXT,

    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_flow_runs_flow_name
ON flow_runs (flow_name);

CREATE INDEX IF NOT EXISTS idx_flow_runs_status
ON flow_runs (status);

CREATE INDEX IF NOT EXISTS idx_flow_runs_started_at
ON flow_runs (started_at);


CREATE TABLE IF NOT EXISTS task_runs (
    task_run_id TEXT PRIMARY KEY,
    flow_run_id TEXT NOT NULL,
    task_name TEXT NOT NULL,
    status TEXT NOT NULL,

    started_at TEXT,
    finished_at TEXT,

    attempt_count INTEGER NOT NULL,

    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),

    FOREIGN KEY(flow_run_id) REFERENCES flow_runs(flow_run_id)
);

CREATE INDEX IF NOT EXISTS idx_task_runs_flow_run_id
ON task_runs (flow_run_id);

CREATE INDEX IF NOT EXISTS idx_task_runs_task_name
ON task_runs (task_name);

CREATE INDEX IF NOT EXISTS idx_task_runs_status
ON task_runs (status);

CREATE INDEX IF NOT EXISTS idx_task_runs_flow_task
ON task_runs (flow_run_id, task_name);


CREATE TABLE IF NOT EXISTS task_attempts (
    task_attempt_id TEXT PRIMARY KEY,
    task_run_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,

    started_at TEXT,
    finished_at TEXT,

    error_summary TEXT,
    log_ref TEXT,

    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),

    FOREIGN KEY(task_run_id) REFERENCES task_runs(task_run_id)
);

CREATE INDEX IF NOT EXISTS idx_task_attempts_task_run_id
ON task_attempts (task_run_id);

CREATE INDEX IF NOT EXISTS idx_task_attempts_status
ON task_attempts (status);

CREATE INDEX IF NOT EXISTS idx_task_attempts_task_run_attempt_number
ON task_attempts (task_run_id, attempt_number);


-- ---------------------------------------------------------------------------
-- B) Append-only execution event log
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS execution_events (
    event_id TEXT PRIMARY KEY,

    event_type TEXT NOT NULL,
    event_ts TEXT NOT NULL,

    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,

    flow_run_id TEXT,
    task_run_id TEXT,
    task_attempt_id TEXT,

    payload_json TEXT NOT NULL,

    ingested_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),

    FOREIGN KEY(flow_run_id) REFERENCES flow_runs(flow_run_id),
    FOREIGN KEY(task_run_id) REFERENCES task_runs(task_run_id),
    FOREIGN KEY(task_attempt_id) REFERENCES task_attempts(task_attempt_id)
);

CREATE INDEX IF NOT EXISTS idx_execution_events_event_ts
ON execution_events (event_ts);

CREATE INDEX IF NOT EXISTS idx_execution_events_event_type
ON execution_events (event_type);

CREATE INDEX IF NOT EXISTS idx_execution_events_entity
ON execution_events (entity_type, entity_id);

CREATE INDEX IF NOT EXISTS idx_execution_events_flow_run_id_event_ts
ON execution_events (flow_run_id, event_ts);

CREATE INDEX IF NOT EXISTS idx_execution_events_task_run_id_event_ts
ON execution_events (task_run_id, event_ts);

CREATE INDEX IF NOT EXISTS idx_execution_events_task_attempt_id_event_ts
ON execution_events (task_attempt_id, event_ts);


-- ---------------------------------------------------------------------------
-- C) Append-only semantic event log
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS semantic_events (
    event_id TEXT PRIMARY KEY,

    task_attempt_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_ts TEXT NOT NULL,

    domain_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,

    ingested_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),

    FOREIGN KEY(task_attempt_id) REFERENCES task_attempts(task_attempt_id)
);

CREATE INDEX IF NOT EXISTS idx_semantic_events_task_attempt_id_event_ts
ON semantic_events (task_attempt_id, event_ts);

CREATE INDEX IF NOT EXISTS idx_semantic_events_event_type
ON semantic_events (event_type);

CREATE INDEX IF NOT EXISTS idx_semantic_events_domain_key
ON semantic_events (domain_key);

CREATE INDEX IF NOT EXISTS idx_semantic_events_domain_key_event_type
ON semantic_events (domain_key, event_type);
