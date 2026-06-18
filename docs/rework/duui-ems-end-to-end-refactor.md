# DUUI EMS End-to-End Refactor: duui-base + duui-core

## Scope
This document catalogs every drift between the semantic EMS design and the current implementation in both `duui-base` and `duui-core`. Each drift is scoped by concept with exact target semantics and file-level action items.

---

## 1. Global State Elimination

### Drift
Current code uses `DUUIEntityManager.global()` as a static singleton for entity registration, relationship indexing, and task index access. The semantic design requires all EMS state to be runtime-scoped: `DUUIRuntime` creates and owns services; entities are registered through the runtime's entity manager, not a global singleton.

### Violations Found

| File | Pattern | Action |
|------|---------|--------|
| `duui-core/.../DUUIPipeline.java:105` | `DUUIEntityManager.global().register(this)` |{