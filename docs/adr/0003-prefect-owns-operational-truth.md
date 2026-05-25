# ADR 0003 — Prefect Owns Operational Truth

**Date:** 2026-05-25  
**Status:** Accepted  
**Version:** v0.2.0 (planned)  
**Package:** `mxm-pipeline`  
**Supersedes:** Portions of ADR 0002 — Task-Centric Execution Model with Semantic Events

## Context

ADR 0002 introduced a task-centric execution model together with structured execution records and semantic event emission.

During implementation and runtime integration work for MXM V1, it became clear that the original design still retained too much ownership of orchestration semantics inside `mxm-pipeline`.

In particular, the following concepts risked duplicating responsibilities already handled by modern orchestration systems such as Prefect:

- FlowRun persistence
- TaskRun persistence
- TaskAttempt persistence
- retry lifecycle management
- orchestration state tracking
- operational execution reporting

At the same time, the practical deployment requirements of MXM became clearer:

- the system requires reliable orchestration and retries
- operational state must remain externally inspectable
- semantic/domain meaning must remain queryable
- execution semantics must remain simple and comprehensible
- local execution and deployment workflows must remain lightweight

The implementation experience demonstrated that Prefect already provides a mature operational execution substrate, including:

- flow and task lifecycle management
- retries
- scheduling
- orchestration state
- runtime metadata
- operational UI and APIs

Attempting to recreate a parallel operational execution model inside `mxm-pipeline` would therefore:

- duplicate orchestration logic
- increase maintenance burden
- introduce conceptual drift
- weaken interoperability with the underlying orchestration engine

We therefore require a stricter separation between:

- operational execution truth
- domain-level semantic meaning

## Decision

`mxm-pipeline` adopts the following architectural principle:

> Prefect owns operational execution truth.  
> MXM owns semantic execution meaning.

Operational orchestration state is delegated entirely to Prefect.

`mxm-pipeline` will not implement or persist an independent operational execution model.

Instead, `mxm-pipeline` provides:

- declarative flow specifications
- compilation into Prefect flows
- execution-context injection
- semantic event emission
- semantic event persistence
- thin runtime convenience wrappers

## Operational vs Semantic Responsibilities

### Prefect Responsibilities

Prefect is the canonical source of truth for:

- flow runs
- task runs
- retries
- scheduling
- orchestration state
- execution lifecycle
- runtime metadata
- operational logs and UI

Operational inspection should use Prefect-native tooling whenever possible.

### MXM Responsibilities

`mxm-pipeline` owns:

- `FlowSpec` and `TaskSpec`
- deterministic flow compilation
- runtime execution context injection
- semantic event emission APIs
- semantic event persistence
- domain-level execution semantics

Semantic events represent what execution *meant* from the perspective of the domain.

Examples:

- marketdata successfully materialized
- partial coverage obtained
- degraded upstream availability
- no-op execution due to unchanged source state
- validation failure
- stale reference data detection

## Execution Model

The runtime execution model becomes:

```text
FlowSpec
  ↓
Prefect FlowRun
  ↓
Prefect TaskRun
  ↓
MXM ExecutionContext
  ↓
SemanticEvent(s)
```

The `ExecutionContext` is injected into task functions during runtime execution.

The context provides:

- Prefect-derived runtime identifiers
- logging access
- semantic event emission
- lightweight execution metadata

The context does not own orchestration state.

## Semantic Events

Semantic events are append-only domain records emitted during execution.

They are intentionally separate from:

- operational orchestration records
- logs
- metrics

Principle:

> operational records say what happened  
> semantic events say what it meant  
> logs say how it unfolded

Semantic events may later support:

- reporting
- observability
- lineage reconstruction
- downstream triggers
- operator dashboards
- execution summaries

without requiring MXM to own orchestration itself.

## Consequences

### Positive

- Eliminates duplication of orchestration concerns
- Aligns MXM with established orchestration systems
- Keeps `mxm-pipeline` conceptually small
- Preserves semantic richness without operational complexity
- Simplifies deployment and runtime reasoning
- Enables direct use of Prefect operational tooling
- Reduces maintenance burden
- Clarifies ownership boundaries

### Neutral / Deferred

- Semantic event visualization is deferred
- Rich semantic dashboards are deferred
- Alternative orchestration backends remain theoretically possible but are not a current design priority

### Negative

- MXM becomes operationally coupled to Prefect concepts and runtime identifiers
- Some orchestration semantics become backend-specific
- Semantic event correlation depends on Prefect runtime context

## Alternatives Considered

| Option | Pros | Cons | Outcome |
|--------|------|------|---------|
| MXM-owned operational execution model | Full control; backend independence | Large complexity; duplication of Prefect | Rejected |
| Full Prefect-native implementation without semantic layer | Simpler implementation | Weak domain semantics; reduced reporting expressiveness | Rejected |
| Thin semantic layer over Prefect (chosen) | Clear ownership; rich semantics; low complexity | Some backend coupling | Accepted |

## Relationship to ADR 0002

ADR 0002 introduced the distinction between execution mechanics and semantic meaning.

This ADR refines that model further by removing MXM ownership of operational execution records.

Specifically:

- Prefect owns execution lifecycle state
- MXM owns semantic event meaning
- `ExecutionContext` becomes a semantic augmentation layer rather than an execution substrate

The conceptual distinction introduced in ADR 0002 remains valid:

> operational records say what happened  
> semantic events say what it meant  
> logs say how it unfolded

However, operational records are now explicitly delegated to Prefect.

## Implementation Notes

The implementation includes:

- `ExecutionContext`
- `SemanticEventSink`
- `ReportingSemanticEventSink`
- `SemanticEventsStore`
- Prefect runtime context extraction
- deterministic `FlowSpec` compilation into Prefect flows

Removed or avoided concepts include:

- MXM-owned `FlowRun`
- MXM-owned `TaskRun`
- MXM-owned `TaskAttempt`
- independent orchestration scheduler
- independent retry engine

## Status & Next Steps

Current priorities:

- finalize runtime substrate
- document local execution workflow
- integrate semantic event reporting into MXM V1
- deploy local marketdata flows
- establish operator-facing runtime smoke workflows

Future work may include:

- semantic event dashboards
- semantic event querying APIs
- execution summaries
- deployment helpers
- production runtime patterns

**Decision Owner:** Dr Pendryl Coinwright  
**Reviewers:** MXM Core Maintainers  
**Last Updated:** 2026-05-25
