# Signal & Channel — Research Report

## Sources
- Java `Exchanger<V>` (java.util.concurrent)
- Java `CompletableFuture<T>` / `CompletionStage<T>` (java.util.concurrent)
- Go channels (CSP model — goroutines + buffered channels)
- CSP (Communicating Sequential Processes, Hoare 1978)

---

## Java Exchanger<V>

Synchronization point where two threads rendezvous and swap objects.
Thread A calls `exchange(x)` → blocks until Thread B calls `exchange(y)`.
Thread A gets `y`, Thread B gets `x`. Both continue.

Properties:
- Synchronous: both sides must arrive simultaneously
- Bidirectional swap (not unidirectional send)
- No buffering — always rendezvous
- Supports timeout variant

Use case: buffer swapping between producer/consumer threads. Genuinely useful for
pipeline handoff where both sides have something to give.

---

## Go Channels

Typed conduit. Created with `make(chan T)` or `make(chan T, n)` (buffered).

Properties:
- Unidirectional: `ch <- v` (send), `v := <-ch` (receive)
- Blocking by default: send blocks until receive, receive blocks until send
- Buffered variant: non-blocking until buffer full/empty
- Close signal: `close(ch)` — receivers detect via `v, ok := <-ch`
- Range over channel: `for v := range ch`
- Select on multiple channels: `select { case <-ch1: ... case <-ch2: ... }`

The CSP model in Go: "Don't communicate by sharing memory; share memory by
communicating." Channels ARE the synchronization primitive.

---

## CompletableFuture<T>

Single-value async computation. Complete once, consumed by dependents.
Pipeline of transformations: `thenApply`, `thenCompose`, `thenCombine`,
`thenAccept`, `exceptionally`, `whenComplete`.

Properties:
- Settable once: `complete(value)` or `completeExceptionally(ex)`
- Chainable: `future.thenApply(f).thenAccept(g)`
- Combinable: `a.thenCombine(b, (x,y) -> ...)`
- Any/all: `CompletableFuture.allOf(...)`, `anyOf(...)`
- Non-blocking polling: `isDone()`, `getNow(default)`
- Timeout: `orTimeout(...)`, `completeOnTimeout(...)`

NOT a channel — it's a single-result future. But its chaining and combining
operators model dataflow well.

---

## CSP (Hoare)

Formal algebra. Primitives: events, processes. Operators: prefix (`a→P`),
choice (`◻` external, `⊓` internal), interleave (`|||`), interface parallel
(`|[{X}]|`), hiding (`\X`), sequential composition.

Key insight: processes communicate through synchronized events on named
channels. The rendezvous model — both sides must be ready.

Influenced: Go, Erlang, Occam, Clojure core.async, VerilogCSP, Ada.

---

## Patterns Across All Four

### Rendezvous (Synchronous Exchange)
- Exchanger: `exchanger.exchange(x)` — blocks until partner
- Go channel: `ch <- v` with unbuffered channel — blocks until receiver reads
- CSP: interface parallel `P |[{a}]| Q` — both must agree on event `a`
- DUUI equivalent: a signal that must be acknowledged before proceeding

### Buffered (Asynchronous Send)
- Go channel: `make(chan T, 100)` — sends non-blocking until buffer full
- DUUI equivalent: Checkpoint with QUEUE mode

### Backpressure (Block When Full)
- Go: buffered channel naturally blocks sender when full
- CompletableFuture: no backpressure concept (single value)
- DUUI equivalent: Checkpoint with BACKPRESSURE mode

### Composition
- CompletableFuture: `thenCombine`, `thenCompose`, `allOf`, `anyOf`
- Go: `select` over multiple channels
- CSP: interleave `|||`, interface parallel `|[{X}]|`
- DUUI: Channel<S,R> chains, Signal projection chains

### Completion Signal
- Go: `close(ch)` — receivers get zero value + `ok=false`
- CompletableFuture: `complete(value)` — dependents fire
- CSP: STOP process (deadlock) or SKIP (immediate termination)
- DUUI equivalent: Channel.complete(result), Pipeline completion

---

## Principles for DUUI Signal & Channel

Based on the research, the cleanest possible design:

### 1. Signal = Immutable Timestamped Value + Projection

A signal IS a value at a point in time. It is lazy (projection) — you don't
need to materialize it until consumption. It carries metadata (when it was
emitted, attributes). It is EXACT (the value is known) or COMPUTED (derived
from a Future) or UNAVAILABLE (not yet produced).

This is what `DUUISignal<T>` already does. Keep it.

### 2. Channel = Single-Producer, Single-Consumer Rendezvous with Futures

A channel connects exactly one input signal (source) to one output signal
(result). It wraps a `CompletableFuture<R>`. The producer calls `complete(result)`.
The consumer calls `join()` or chains via `thenApply`. The channel IS the
synchronization point.

This is what `DUUIChannel<S,R>` already does. Keep it, but simplify.

### 3. Remove Source and Type Parameters from Channel

`DUUIChannel<S,R>` has a `source` field (`DUUIProjection<S>`) and two type
parameters. This is unnecessary complexity. A channel should be:

```java
public final class Channel<R> {
    private final CompletableFuture<R> future;
    public void complete(R value) { future.complete(value); }
    public void fail(Throwable t) { future.completeExceptionally(t); }
    public R join() { return future.join(); }
    public Channel<R> then(Function<R, ?> fn) { ... }
}
```

Single type parameter. No source reference. The pipeline connects channels —
the channel doesn't need to know what feeds it.

### 4. Stream = Unbounded Queue + Completion

`DUUIStream<T>` already is a thread-safe unbounded stream with completion
signaling. This is the Go buffered channel equivalent. Keep it.

### 5. Flow = Future + Phase

`DUUIFlow<T>` pairs a `CompletableFuture<T>` with a `DUUIPhase`. This binds
the result of an async computation to a lifecycle event. Keep it — it's
the right abstraction for pipeline telemetry.

### 6. Projection = Lazy Value Access

`DUUIProjection<T>` with `DUUIAvailability` states (EXACT, COMPUTED,
UNAVAILABLE, UNSUPPORTED, STALE). This is good. The lazy pattern means
consumers can check availability before blocking. Keep it.

---

## Proposed Simplifications

| Current | Proposed | Reason |
|---------|----------|--------|
| `DUUIChannel<S,R>` with 2 type params + source | `Channel<R>` with 1 type param, no source | Simpler. Source tracking is the pipeline's job, not the channel's. |
| `DUUISignal<T>` extends `DUUICarrier<T>` | Flat `Signal<T>` record | No need for Carrier base class. Signal IS a carrier. |
| `DUUICarrier<T>` with GID | Remove GID from carriers | Signals don't need identity — only entities do. |
| `DUUIFlow<T>` with `started` boolean | `Flow<T>` = `(CompletableFuture<T>, DUUIPhase)` | The `started` flag is noise. Phase tracks lifecycle. |
| `DUUIProjection<T>` with 5 availability states | Keep availability states, add `orElse(T)` | `get().orElse(default)` is more ergonomic than manual checks. |
