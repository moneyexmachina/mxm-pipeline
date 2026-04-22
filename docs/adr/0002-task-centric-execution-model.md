# ADR 0002 — Task-Centric Execution Model with Semantic Events

**Date:** 2026-04-15  
**Status:** Accepted  
**Version:** v0.2.0 (planned)  
**Package:** `mxm-pipeline`

## Context

Session 36 establishes the first deployed runtime for MXM V1 and prompts a re-evaluation of the execution and reporting model of the system.

Earlier versions of `mxm-pipeline` introduced an optional asset layer (`ADR 0001`) to enable future asset-centric orchestration. However, practical system design and comparison with established frameworks (Prefect, Airflow, Dagster, Temporal) revealed:

- Standard orchestration models (flow → task → run → attempt) are sufficient for MXM
- Domain-level meaning should not be embedded as a first-class orchestration primitive
- A separate “job” abstraction is unnecessary if the task abstraction is sufficiently rich
- Semantic meaning is best represented as **events emitted during execution**, not as declarations in the task specification

We therefore require:

- a **canonical executable unit** that is reusable across CLI, orchestration, and testing
- a **clean separation between execution mechanics and domain meaning**
- a **structured, queryable reporting layer** for execution and semantics
- independence from any specific orchestration backend

## Decision

`mxm-pipeline` adopts a **task-centric execution model**, with the following structure:

```text
FlowRun
  TaskRun
    TaskAttempt(s)
    SemanticEvent(s)
```

### Key elements

#### 1. Task as the canonical executable unit

- `TaskSpec` defines the executable unit of work
- Tasks encapsulate:
  - config resolution
  - dependency construction / injection
  - execution logic
  - telemetry hooks
  - semantic event emission

Tasks can be executed:

- standalone (CLI / local)
- within a flow
- in tests or ad hoc contexts

A standalone execution is equivalent to a **degenerate FlowRun containing a single TaskRun**.

#### 2. Flow as composition

- `FlowSpec` composes multiple `TaskSpec`s into a DAG
- Defines dependencies and execution topology
- No additional execution semantics beyond composition

#### 3. Execution records (second-order operational layer)

The execution substrate records:

- **FlowRun**: lifecycle of a flow execution
- **TaskRun**: orchestration-level scheduling and state
- **TaskAttempt**: actual execution attempts (including retries)

These records are:

- structured
- minimal
- queryable
- persisted via `mxm-pipeline`

They represent **what happened**.

#### 4. Semantic events (domain layer)

Tasks emit **SemanticEvent** records during execution.

These represent:

- domain-level outcomes
- dataset changes
- materializations
- observations
- degradations or no-op results

Minimal structure:

```text
event_id
task_attempt_id
event_type
event_ts
domain_key
payload
```

Semantic events:

- are emitted via execution context
- are domain-defined (e.g. in `mxm-v1`)
- are not owned by orchestration logic
- represent **what execution meant**

#### 5. Logging as execution trace

Logging (`stdout`, file logs) remains separate:

- provides narrative/debug context
- not used as a source of truth
- linked to execution records via IDs

Principle:

> records say **what happened**  
> semantic events say **what it meant**  
> logs say **how it unfolded**

## Consequences

### Positive

- Aligns MXM with standard orchestration models (flows, tasks, runs, attempts)
- Eliminates redundant “job” abstraction layer
- Provides a single canonical executable unit (Task)
- Enables reuse across CLI, orchestration, and tests
- Clean separation of:
  - execution mechanics
  - domain semantics
- Flexible semantic model via event emission (supports partial, degraded, noop outcomes)
- Backend independence preserved (Prefect, local runner, etc.)

### Neutral / Deferred

- Advanced semantic event cataloging and querying are out of scope for v0.2.0
- Rich reporting UIs and dashboards are deferred
- Persistence backends may evolve beyond initial SQLite/file-based implementations

### Negative

- Tasks become slightly richer abstractions (must handle execution context)
- Responsibility for semantic event consistency shifts to domain code
- No built-in asset abstraction; requires interpretation of semantic events

## Alternatives Considered

| Option | Pros | Cons | Outcome |
|--------|------|------|---------|
| **Separate job layer** | Explicit execution membrane; reuse outside orchestration | Redundant abstraction; additional complexity | Rejected |
| **Declarative asset model (ADR 0001)** | Strong asset semantics; forward compatibility | Over-constrains domain; less flexible than events | Superseded |
| **Full asset framework (Dagster)** | Rich lineage, partitioning, tooling | Heavy; reduced control; slower iteration | Rejected |
| **Task-centric + semantic events (chosen)** | Simple, flexible, aligned with standards | Requires discipline in event emission | **Accepted** |

## Implementation Notes

- `TaskSpec` is extended to support execution context and event emission
- `ExecutionContext` provides:
  - run identifiers
  - logging hooks
  - `emit_semantic_event(...)`
- `mxm-pipeline` implements:
  - runners (sequential, parallel)
  - execution record persistence
  - CLI integration
- Domain packages (`mxm-v1`) define:
  - concrete tasks and flows
  - semantic event types and payloads

## Relationship to ADR 0001

ADR 0001 introduced an optional asset declaration model.

This ADR supersedes that approach:

- Asset declarations in `TaskSpec` are removed
- Asset semantics are replaced by `SemanticEvent` emission
- Asset catalogs, if introduced later, will be built by interpreting semantic events

## Status & Next Steps

- Implement execution models (`FlowRun`, `TaskRun`, `TaskAttempt`)
- Implement execution context and semantic event emission
- Upgrade runner to support parallel DAG execution
- Deploy first runtime (5-product × 2-task DAG) on `monolith`
- Validate model on real MXM V1 workflows

**Decision Owner:** Dr Pendryl Coinwright  
**Reviewers:** MXM Core Maintainers  
**Last Updated:** 2026-04-15
