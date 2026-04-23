-- 0001_semantic_events.sql
--
-- MXM Pipeline semantic-event SQLite schema.
--
-- Scope:
-- - Append-only semantic event log only
-- - Operational execution truth is owned by Prefect, not mirrored here
--
-- Design notes:
-- - One row per emitted SemanticEvent
-- - Timestamps are stored as ISO8601 UTC with trailing Z
-- - JSON payloads are stored as canonical JSON text
-- - Prefect runtime identifiers are stored as plain text correlation keys

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS semantic_events (
    event_id TEXT PRIMARY KEY,

    flow_run_id TEXT NOT NULL,
    task_run_id TEXT NOT NULL,

    event_type TEXT NOT NULL,
    event_ts TEXT NOT NULL,

    domain_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,

    ingested_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_semantic_events_flow_run_id_event_ts
ON semantic_events (flow_run_id, event_ts);

CREATE INDEX IF NOT EXISTS idx_semantic_events_task_run_id_event_ts
ON semantic_events (task_run_id, event_ts);

CREATE INDEX IF NOT EXISTS idx_semantic_events_event_type
ON semantic_events (event_type);

CREATE INDEX IF NOT EXISTS idx_semantic_events_domain_key
ON semantic_events (domain_key);

CREATE INDEX IF NOT EXISTS idx_semantic_events_domain_key_event_type
ON semantic_events (domain_key, event_type);

CREATE INDEX IF NOT EXISTS idx_semantic_events_flow_task_event_ts
ON semantic_events (flow_run_id, task_run_id, event_ts);
