# DUUI Trait-Driven Rework — Gap Analysis & Action Plan

> **Generated**: 2026-06-25 | **Status**: ~30% complete
> **Source**: Honest assessment of migrated code vs architecture spec

---

## Completion Overview

| Layer | Done | Gap |
|-------|------|-----|
| Trait interfaces (74 files, 8 families) | ✅ | — |
| `DUUIEntity<T>` abstract base class | ✅ | Trait auto-discovery missing |
| `DUUIManager<T>` singleton base class | ✅ | — |
| `DUUIDescription`/`DUIBuilder`/`DUIConfiguration` types | ✅ | No YAML parser, no concrete builder |
| Legacy EMS bridge (`DUUITrait`, `DUUIEntity`, `DUUITraits`) | ✅ | Dual storage fragile |
| `DUUIWorker` migrated | ✅ | — |
| `DUUIAnnotator`/`DUUIComponent<C>` migrated | ✅ | — |
| `DUUIExecutorService`/`DUUITask<R>` migrated | ✅ | — |
| `DUUIScheduler` migrated | ✅ | — |
| Pipeline builder (`DUUIPipeline.fromDescription()`) | ❌ | Entirely missing |
| `@Phase` AOP integration with lifecycle traits | ❌ | Two parallel systems |
| Auto-telemetry wiring (scan for `Measurable`/`Observable`) | ❌ | Traits exist, nothing collects |
| Wrapper/lambda injection for telemetry & control flow | ❌ | `DUUIAnnotationWrapper` not integrated |
| Lifecycle state machine enforcement | ❌ | Traits override `currentState()` with hardcoded defaults |
| Compile-time trait validation | ❌ | `T extends DUUITrait` is too loose |
| Concrete `DUUIV1Annotator` (HTTP, serialization, protocol) | ❌ | `initializeProtocol()` is a stub |
| Type parameter runtime retention | ❌ | Erasure means `Slot` == `Pooled` at runtime |

---

## Issues — One by One

### 1. 🔴 DUUITraits Dual Storage Drift

**Problem**: `legacyValues` (`Set<ems.DUUITrait>`) and `newValues` (`Set<trait.DUUITrait>`) are separate sets. Calling `of()` (legacy factory) fills only legacy. Calling `fromNew()` fills only new with wrapping. If someone mixes factories, the sets diverge.

**Fix**: Collapse to single `Set<trait.DUUITrait>` internally. Keep `asSet()` returning `Set<ems.DUUITrait>` via a lazy-computed view.

**File**: `duui-base/.../ems/traits/DUUITraits.java`

---

### 2. 🔴 Name Collision: Two `DUUIEntity` Types

**Problem**: `ems.DUUIEntity` (legacy interface) and `entity.DUUIEntity<T>` (new abstract class) coexist. Both are imported in migrated files. The legacy interface has `gid()`, `kind()`, etc. The new abstract class has `has()`, `get()`, `describe()`, etc. They are different types.

**Fix**: Rename the new abstract class to `DUUIBaseEntity<T>` or make the legacy interface extend the new abstract class's contract via a superinterface. Or deprecate the legacy interface and have all concrete classes extend the new abstract class directly.

**Files**: `duui-base/.../ems/DUUIEntity.java`, `duui-base/.../entity/DUUIEntity.java`

---

### 3. 🔴 Pipeline Builder Missing

**Problem**: `DUUIPipeline` (898 lines) is untouched legacy. No `fromDescription()`, no fluent `withComponent()`, no `DUUIV1ComponentBuilder` in the new design. The user-facing API to build a pipeline does not exist.

**Fix**: Implement `DUUIPipeline.fromDescription(DUUIDescription)` with a `PipelineBuilder` that:
- Parses YAML → `DUUIDescription`
- Creates `DUUIStage<C>` instances with trait composition
- Wires `DUUIComponent<C>` via `DUUIComponentDescription.traitIds()`
- Connects stages with checkpoints

**Files**: `duui-core/.../workflow/DUUIPipeline.java` (needs rewrite)

---

### 4. 🔴 @Phase AOP Not Integrated with Lifecycle Traits

**Problem**: Two parallel lifecycle systems:
- **New**: `Startable.start()`, `Stoppable.stop()`, `Failable.fail(Throwable)` — trait interfaces with no instrumentation
- **Legacy**: `@Phase(DUUIStatus.STARTED)` → `DUUIPhaseAspect` (AspectJ) → `DUUIPhaseWrapper` → `DUUITimeline`

Calling `worker.start()` creates no `DUUIPhase`. The AOP doesn't know about lifecycle traits.

**Fix**: Either:
- (A) Make lifecycle trait methods `@Phase`-annotated so the AspectJ aspect intercepts them
- (B) Replace AOP with explicit `timeline().create()` calls in trait default methods
- (C) Create a `DUUIPhaseWrapper` adapter that wraps any lifecycle trait method

**Files**: `duui-base/.../trait/lifecycle/*.java`, `duui-base/.../ems/lifecycle/DUUIPhaseAspect.java`

---

### 5. 🔴 Lifecycle State Machine Not Enforced

**Problem**: Each lifecycle trait overrides `currentState()` with a hardcoded default (`"RUNNING"`, `"STOPPED"`, etc.). There's no:
- Validation that transitions are legal (can't `stop()` a `Creatable` that hasn't started)
- State that persists across trait boundaries
- Diamond resolution when `Creatable & Startable` both override `currentState()`

**Fix**: Remove `currentState()` from individual traits. Make `DUUILifecycle` carry an `AtomicReference<String>` state. Each trait method checks `validTransitions()` before transitioning:

```java
public interface DUUILifecycle extends DUUITrait {
    AtomicReference<String> state();
    Set<String> validTransitions();
    
    default boolean transition(String target) {
        if (!validTransitions().contains(state().get() + "→" + target))
            return false;
        state().set(target);
        return true;
    }
}
```

**Files**: `duui-base/.../trait/lifecycle/*.java`

---

### 6. 🔴 Auto-Telemetry Wiring Missing

**Problem**: `Measurable`, `Observable`, `Loggable`, `Traceable` traits exist but nothing scans entities and auto-creates instruments. A `DUUIComponent<Pooled>` declares `metricNames()` but no `Timer`/`Counter`/`Gauge` is created.

**Fix**: `DUUITelemetryService.initialize()` must scan the `DUUIEntityRegistry` for entities composing telemetry traits and auto-create `DUUITelemetryInstrument` instances:

```java
for (var entity : registry.all()) {
    if (entity.has(Measurable.class)) {
        for (var metric : entity.get(Measurable.class).get().metricNames()) {
            createInstrument(entity.gid(), metric);
        }
    }
}
```

**File**: `duui-base/.../telemetry/DUUITelemetryService.java`

---

### 7. 🔴 Wrapper/Lambda Injection Not Implemented

**Problem**: The architecture promises:

```java
entity.wrap(phase -> {
    phase.before(() -> logger.metric("start"));
    phase.after(() -> logger.metric("end"));
    phase.onFailure(e -> logger.error("failed", e));
}).execute(() -> doWork());
```

This doesn't exist. `DUUIAnnotationWrapper<A>` is in the legacy code but not integrated with entities.

**Fix**: Add to `DUUIEntity<T>`:

```java
public final <R> R wrap(Function<DUUIPhaseWrapper, DUUIPhaseWrapper> config,
                         Supplier<R> work) {
    var phase = timeline().create(DUUIStatus.PROFILE_BEGIN, List.of(this));
    var wrapper = config.apply(new DUUIPhaseWrapper(phase));
    try {
        wrapper.before();
        R result = work.get();
        wrapper.after();
        phase.complete();
        return result;
    } catch (Exception e) {
        wrapper.onFailure(e);
        phase.fail(e);
        throw e;
    }
}
```

**Files**: `duui-base/.../entity/DUUIEntity.java`, new `DUUIPhaseWrapper.java`

---

### 8. 🟡 Compile-Time Trait Validation Too Loose

**Problem**: `DUUIEntity<T extends DUUITrait>` accepts any trait. You can write `DUUIEntity<HTTP & RetryPolicy>` even though `HTTP` belongs in protocol family and `RetryPolicy` in policy family. The constraint only ensures `T` is *some* trait.

**Fix**: Not fixable in Java's type system without sealed interfaces per family. Accept this as a limitation. Mitigation: add a `validate()` method in `DUUIConfiguration` that checks trait family consistency at construction time.

---

### 9. 🟡 Type Parameter Erasure

**Problem**: `DUUIComponent<Slot>` and `DUUIComponent<Pooled>` are identical at runtime. Can't `instanceof` check the concurrency model.

**Fix**: Store the trait `Class<?>` at construction:

```java
public abstract class DUUIEntity<T extends DUUITrait> {
    private final Class<?> primaryTraitClass;
    
    protected DUUIEntity(GID gid, DUUITraits traits, Class<?> primaryTraitClass) {
        this.primaryTraitClass = primaryTraitClass;
    }
    
    public boolean is(Class<?> traitClass) {
        return primaryTraitClass.equals(traitClass);
    }
}

// Usage:
var c = new DUUIComponent<Slot>(..., Slot.class);
c.is(Slot.class); // true
```

**Files**: `duui-base/.../entity/DUUIEntity.java`, all concrete entity constructors

---

### 10. 🟡 `describe()`/`builder()`/`configuration()` Must Be Implemented Per Entity

**Problem**: These are `abstract` — every entity class must write the same boilerplate. The spec promises auto-generation from trait composition.

**Fix**: Make them default methods in `DUUIEntity<T>` that derive from `traits()`:

```java
public DUIDescription describe() {
    Set<String> ids = new LinkedHashSet<>();
    for (var t : traits().asNewSet()) ids.add(t.traitId());
    return new DUIDescription(
        getClass().getName(), ids, collectAttributes(), List.of(),
        DUIDescription.CURRENT_VERSION
    );
}
```

`collectAttributes()` would scan the entity for fields annotated with `@ConfigAttribute` or similar.

**File**: `duui-base/.../entity/DUUIEntity.java`

---

### 11. 🟡 Concrete `DUUIV1Annotator` Is Hollow

**Problem**: `initializeProtocol()` only logs and creates empty timeline entries. No:
- `HttpClient` GET to `/v1/documentation`, `/v1/communication_layer`, `/v1/typesystem`
- `XmiCasSerializer`/`XmiCasDeserializer` wiring
- `TypeSystemDescription` merging
- `serialize()` → `analyse()` → `deserialize()` pipeline with `DUUIBridge`

**Fix**: Port the real initialization from the legacy `DUUIV1Annotator` (which is in the old `duui-core` code at ~500 lines). Wire HTTP client, communication layer selection, type system merging.

**File**: `duui-core/.../component/DUUIV1Annotator.java`

---

### 12. 🟡 `DUUILogger` Static Factory Missing

**Problem**: Migrated code calls `DUUILogger.of(this)` but the legacy `DUUILogger` is a `final class` that expects a `DUUITelemetryService` binding. The static factory method may not exist.

**Fix**: Add `DUUILogger.of(DUUIEntity)` that auto-resolves the telemetry service from the entity's runtime context.

**File**: `duui-base/.../telemetry/DUUILogger.java`

---

### 13. 🟢 `DUUIResourceContext.of()` Factory Missing

**Problem**: Same as logger — migrated code calls `DUUIResourceContext.of(gid, traits)` which may not exist as a static factory.

**Fix**: Add the factory method.

**File**: `duui-base/.../telemetry/DUUIResourceContext.java`

---

### 14. 🟢 `DUUIRelationship` Import Typo

**Problem**: Migrated code imports `DUIRelationship` (missing one 'I' — should be `DUUIRelationship`).

**Fix**: Fix all import statements.

**Files**: Multiple files across `duui-base` and `duui-core`

---

## Priority Order

| # | Issue | Priority | Effort |
|---|-------|----------|--------|
| 3 | Pipeline Builder Missing | 🔴 P0 | Large |
| 4 | @Phase AOP Integration | 🔴 P0 | Medium |
| 5 | Lifecycle State Machine | 🔴 P0 | Medium |
| 1 | DUUITraits Dual Storage | 🔴 P1 | Small |
| 2 | Name Collision Two DUUIEntity | 🔴 P1 | Medium |
| 6 | Auto-Telemetry Wiring | 🔴 P1 | Medium |
| 11 | Concrete DUUIV1Annotator | 🟡 P2 | Large |
| 7 | Wrapper/Lambda Injection | 🟡 P2 | Medium |
| 10 | describe()/builder() Auto-Gen | 🟡 P2 | Small |
| 9 | Type Parameter Erasure | 🟡 P2 | Small |
| 12 | DUUILogger Factory | 🟡 P2 | Small |
| 13 | DUUIResourceContext Factory | 🟡 P3 | Small |
| 14 | Import Typos | 🟢 P3 | Tiny |
| 8 | Compile-Time Validation | 🟢 P3 | Cannot fix (JVM limit) |

---

## Target End State

After all issues are resolved, the ergonomics should be:

```java
// 1. Define a custom annotator — boilerplate is auto-wired
public final class Tokenizer extends DUUIV1Annotator {
    public Tokenizer(String endpoint) {
        super(endpoint); // auto: GID, traits(UIMA+HTTP+Measurable+RetryPolicy), timeline, logger
    }
}

// 2. Build a full pipeline from YAML
var pipeline = DUUIPipeline.fromYAML("""
    source:
      type: directory
      path: /data/input
    stages:
      - component: tokenizer
        image: docker.io/my/tokenizer:latest
        concurrency: Slot
        replicas: 4
      - component: ner
        endpoint: http://ner:8080
        concurrency: Pooled
        replicas: 2
    target:
      type: xmi
      path: /data/output
    """).build(runtime);

// 3. Custom telemetry — auto-wired by the framework
pipeline.wrap(phase -> phase
    .before(() -> System.out.println("Starting stage"))
    .after(() -> System.out.println("Stage complete"))
).run();

// 4. Lifecycle is consistent — all transitions go through timeline
pipeline.start();   // → timeline.create(STARTED) → state=RUNNING
pipeline.stop();    // → timeline.create(STOPPED) → state=STOPPED
```
