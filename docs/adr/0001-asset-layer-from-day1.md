# ADR 0001 — Introduce Optional Asset Layer from Day 1

**Date:** 2025-11-11  
**Status:** Accepted  
**Version:** v0.1.0  
**Package:** `mxm-pipeline`

---

## Context

The Money Ex Machina system requires a unified orchestration layer to define, run, and monitor data pipelines.  
Two conceptual models are possible:

1. **Task-centric orchestration** – traditional DAGs connecting parameterized Python callables.
2. **Asset-centric orchestration** – assets as first-class entities that can be materialized, partitioned, and versioned (e.g. Dagster-style).

Task-centric design is simpler and immediately productive; asset-centric design provides stronger semantics for reproducibility, provenance, and incremental re-computation.

We require rapid progress for early data-flows (e.g. `justETF`) **without committing to a heavy asset framework**, while also preserving the ability to evolve naturally toward one later.

---

## Decision

`mxm-pipeline` v0.1.0 will:

* Implement **Prefect 2.x** as the execution backend, using the standard DAG model.
* Introduce an **optional `AssetDecl`** dataclass in the pipeline specification.
* For any task that produces durable output, an accompanying **`_asset.json` sidecar** will be written through `mxm-dataio.write_asset_meta()`.

This design makes every producing task *asset-aware* but keeps the orchestration logic *task-driven*.

---

## Consequences

### Positive
* ✅ Enables **immediate productivity** using Prefect flows.
* ✅ Creates a **uniform provenance trail**: `_meta.json` for run context + `_asset.json` for persistent state.
* ✅ Establishes **forward compatibility** with a future asset catalog and materialization planner.
* ✅ Keeps the system **local-first and backend-agnostic**; the adapter interface can later target Dagster or others.

### Neutral / Deferred
* Catalog ingestion, lineage visualization, and partition management are explicitly out of scope for v0.1.0.

### Negative
* Slight additional boilerplate for producing tasks.
* Risk of conceptual drift if asset metadata is under-used or inconsistent—mitigated by schema validation in `mxm-dataio`.

---

## Alternatives Considered

| Option | Pros | Cons | Outcome |
|--------|------|------|---------|
| **Pure task model** | Simplest now; minimal metadata | Harder future migration; weak reproducibility | Rejected |
| **Full asset framework (Dagster)** | Strong lineage, partitioning | Heavy dependencies; slower setup | Rejected for v0.1.0 |
| **Hybrid (chosen)** | Productive now; extensible later | Slight extra metadata burden | **Accepted** |

---

## Implementation Notes

* `TaskSpec.produces: Optional[AssetDecl]` marks an asset-producing task.
* `mxm-dataio.write_asset_meta()` writes a JSON record beside artifacts.
* The Prefect adapter ignores asset semantics for now.
* Future versions (`≥ v0.3.0`) will collect these JSON records into an asset catalog for lineage and re-materialization planning.

---

## Status & Next Steps

* Implemented in `v0.1.0` scaffold.  
* Will be validated on the reference flow `justETF`.  
* Catalog ingestion and visualization planned for `v0.3.0`.

---

**Decision Owner:** Dr Pendryl Coinwright
**Reviewers:** MXM Core Maintainers  
**Last Updated:** 2025-11-11
