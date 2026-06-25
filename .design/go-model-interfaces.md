# Go Model Interfaces — Channel/Signal Primitives

## Go Model → Java Interface Mapping

```
Go                           Java Interface
──                           ──────────────
chan T                       Channel<T>
make(chan T)                 Channel.of()
make(chan T, n)              Channel.buffered(n)
ch <- v                      channel.send(v)              // block until received
v := <-ch                    channel.receive()            // block until sent
v, ok := <-ch                channel.tryReceive()         // Optional<T>
close(ch)                    channel.close()              // no more sends
for v := range ch            channel.forEach(consumer)
select { case <-ch: ... }    Selector.of(ch1, ch2).await()
go func() { ... }            Executor.execute(runnable)
sync.WaitGroup               Gate
context.Context              Context (cancellation + deadline)
```

## All Interfaces

### Channel<T>

```java
/**
 * Go chan T analogue. Unbuffered by default (rendezvous — send blocks until receive).
 * Buffered variant: Channel.buffered(n) — send non-blocking until buffer full.
 */
public interface Channel<T> extends AutoCloseable {

    /** Unbuffered rendezvous channel. */
    static <T> Channel<T> of() { ... }

    /** Buffered channel with capacity n. Send non-blocking until full. */
    static <T> Channel<T> buffered(int capacity) { ... }

    /** Send value. Blocks if unbuffered and no receiver ready. */
    void send(T value) throws InterruptedException;

    /** Try send, non-blocking. Returns false if buffer full or no receiver. */
    boolean trySend(T value);

    /** Receive value. Blocks until sender provides or channel closed. */
    T receive() throws InterruptedException;

    /** Try receive, non-blocking. Empty if nothing available. */
    Optional<T> tryReceive();

    /** Close the channel. No more sends. Receivers drain remaining, then get empty. */
    void close();

    /** True if closed and drained. */
    boolean isClosed();

    /** Number of buffered items waiting. */
    int size();
}
```

### Selector

```java
/**
 * Go select statement analogue. Waits for the first ready channel among multiple.
 * Returns the case index (0-based) and the received value.
 */
public interface Selector {

    /** Build a select over multiple receives. */
    static Selector over(ReceiveCase<?>... cases) { ... }

    /** Build a select over mixed send/receive. */
    static Selector over(SelectCase... cases) { ... }

    /** Block until one case is ready. Returns the index + result. */
    Selection await() throws InterruptedException;

    /** Block with timeout. */
    Optional<Selection> await(Duration timeout) throws InterruptedException;

    /** Non-blocking — return first ready or empty. */
    Optional<Selection> trySelect();

    /** A single case in the select. */
    sealed interface SelectCase permits ReceiveCase, SendCase {}

    record ReceiveCase<T>(Channel<T> channel) implements SelectCase {}
    record SendCase<T>(Channel<T> channel, T value) implements SelectCase {}

    /** Result of a select. */
    record Selection(int index, Object value) {}
}
```

### Gate

```java
/**
 * Go sync.WaitGroup analogue. Tracks N pending operations.
 * Add before, done after, await until zero.
 */
public interface Gate {

    /** Increment counter. */
    void add(int n);

    /** Decrement counter. Called when one operation completes. */
    void done();

    /** Block until counter reaches zero. */
    void await() throws InterruptedException;

    /** Block with timeout. Returns false if timed out. */
    boolean await(Duration timeout) throws InterruptedException;
}
```

### Context

```java
/**
 * Go context.Context analogue. Carries cancellation signal, deadline, and key-value values.
 * Used to propagate cancellation through a pipeline.
 */
public interface Context {

    /** Background context — never cancelled, no deadline. */
    static Context background() { ... }

    /** Create a cancellable context. */
    static Context withCancel(Context parent) { ... }

    /** Create a context with deadline. */
    static Context withDeadline(Context parent, Instant deadline) { ... }

    /** Create a context with timeout. */
    static Context withTimeout(Context parent, Duration timeout) { ... }

    /** True if context is done. */
    boolean isDone();

    /** Blocks until cancelled or deadline reached. */
    void awaitDone() throws InterruptedException;

    /** The cancellation cause, or null. */
    Throwable error();

    /** Request cancellation. */
    void cancel();

    /** Deadline, or empty if none. */
    Optional<Instant> deadline();

    /** Stored value by key. */
    <T> Optional<T> value(Object key);

    /** Store a value (returns new context, immutable). */
    <T> Context withValue(Object key, T value);
}
```

### Work

```java
/**
 * A unit of work — wraps a function from input to output.
 * Executed by the pipeline, not called directly.
 */
@FunctionalInterface
public interface Work<I, O> {
    O apply(I input) throws Exception;
}
```

### Source

```java
/**
 * Produces a stream of work items from a data source.
 * Implements the pipeline source stage.
 */
public interface Source<O> {

    /** Feed items into the output channel until exhausted. */
    void produce(Channel<O> output);

    /** Called before production starts. */
    default void open() {}

    /** Called after production finishes. */
    default void close() {}
}
```

### Sink

```java
/**
 * Consumes a stream of work items and writes them to a destination.
 * Implements the pipeline sink/target stage.
 */
public interface Sink<I> {

    /** Consume one item. */
    void consume(I item) throws Exception;

    /** Called before consumption starts. */
    default void open() {}

    /** Called after all items consumed. */
    default void close() {}
}

### Stage

```java
/**
 * A pipeline stage — wraps a Work<I,O> with concurrency control.
 */
public interface Stage<I, O> {

    /** The processing function. */
    Work<I, O> work();

    /** How many concurrent workers. 1 = serial, N = pool, 0 = virtual-thread-per-item. */
    int concurrency();

    /** Timeout per work item. Empty = no timeout. */
    Optional<Duration> timeout();

    /** Retry configuration. */
    RetryConfig retry();
}
```

### Pipeline

```java
/**
 * The orchestrator. Connects sources through stages to sinks via channels.
 * Each stage boundary is a channel. Concurrency is automatic.
 */
public interface Pipeline {

    /** Define a new pipeline. */
    static PipelineBuilder define(String name) { ... }

    /** Run the pipeline. Blocks until all work processed. */
    void run() throws Exception;

    /** Run with a context for cancellation. */
    void run(Context ctx) throws Exception;
}
```

### PipelineBuilder

```java
/**
 * Fluent builder. Source → stages → sink. Build creates channels.
 */
public interface PipelineBuilder {

    /** Register a source stage. */
    <O> PipelineBuilder source(Source<O> source);

    /** Register a processing stage. */
    <I, O> PipelineBuilder stage(Work<I, O> work);

    /** Configure the most recently added stage. */
    PipelineBuilder concurrency(int n);
    PipelineBuilder timeout(Duration d);
    PipelineBuilder retry(int maxAttempts, Duration backoff);

    /** Register a sink stage. */
    <I> PipelineBuilder sink(Sink<I> sink);

    /** Build the pipeline — creates all channels, connects stages. */
    Pipeline build();
}
```

## Minimum Functional Units

The minimum set to implement the Go model:

```
1. Channel<T>      — typed conduit (buffered + unbuffered)
2. Selector         — wait for first ready among multiple
3. Gate             — await N completions
4. Context          — cancellation + deadline propagation
5. Work<I,O>        — function from I to O
6. Source<O>        — produce items into a channel
7. Sink<I>          — consume items from processing
8. Stage<I,O>       — Work + concurrency + timeout + retry
9. Pipeline         — connect Source → Stages → Sink via Channels
10. PipelineBuilder — fluent construction
```

That's 10 interfaces. Everything else (concurrency pools, backpressure, scheduling policies, checkpoints, telemetry sinks) is implementation detail behind these interfaces.

## Semantics Mapping

| Go Concept | Interface Method | Behavior |
|------------|-----------------|----------|
| `ch := make(chan T)` | `Channel.of()` | Unbuffered. Send blocks until receive. |
| `ch := make(chan T, n)` | `Channel.buffered(n)` | Buffered. Send non-blocking < n. |
| `ch <- v` | `channel.send(v)` | Blocks if unbuffered + no receiver. |
| `v := <-ch` | `channel.receive()` | Blocks until sender. |
| `v, ok := <-ch` | `channel.tryReceive()` | Non-blocking Optional. |
| `close(ch)` | `channel.close()` | No more sends. Drain remaining. |
| `for v := range ch` | `channel.forEach(c)` | Consume until closed. |
| `select { case <-ch1: case <-ch2: }` | `Selector.over(ch1, ch2).await()` | First ready wins. |
| `go func()` | `executor.execute(...)` | Concurrency via Executor. |
| `wg.Add(n); ...; wg.Done(); wg.Wait()` | `gate.add(n); ...; gate.done(); gate.await()` | Wait group. |
| `ctx, cancel := context.WithCancel(...)` | `Context.withCancel(parent)` | Cancellation propagation. |
| `ctx, cancel := context.WithTimeout(...)` | `Context.withTimeout(parent, d)` | Timeout. |
