---
layout: default
---

# DUUIRayParallelDriver

`DUUIRayParallelDriver` integrates DUUI with [Ray](https://www.ray.io/) to run NLP components that distribute their internal work across multiple Ray workers. Unlike `DUUIDockerDriver` (which replicates containers) or `DUUIRayDriver` (Bayesian HPO), this driver manages a Ray cluster directly from the Java pipeline and submits a FastAPI-based component as a Ray job.

---

## When to Use This Driver

| Use Case | Recommended Driver |
|---|---|
| Pre-built Docker image | `DUUIDockerDriver` |
| Already-running HTTP service | `DUUIRemoteDriver` |
| Ray-based ML training + HPO | `DUUIRayDriver` |
| **Ray-parallel NLP component (custom Python script)** | **`DUUIRayParallelDriver`** |

Choose `DUUIRayParallelDriver` when your Python component uses `ray.remote` tasks or `ray.data` internally and needs a managed Ray cluster started alongside the pipeline.

---

## Architecture

```
DUUIComposer (Java)
  │
  ├─ instantiate()
  │     ├── ray start --head          (starts Ray head node)
  │     ├── ray start --address=...   (starts N worker nodes)
  │     ├── ray job submit --no-wait  (launches FastAPI script as Ray job)
  │     └── poll GET /v1/communication_layer until HTTP 200
  │
  ├─ run()  [once per document]
  │     ├── Lua serialize(jCas) → JSON bytes
  │     ├── HTTP POST bytes → /v1/process
  │     └── Lua deserialize(response) → jCas
  │
  └─ destroy()
        └── ray stop  (unless keepAlive=true)
```

The Java driver controls the full cluster lifecycle. The Python component is a standard DUUI FastAPI service — it just happens to use Ray internally.

---

## Python Component Contract

The Python script must implement the standard DUUI REST endpoints:

```python
from fastapi import FastAPI
import ray

ray.init(address="auto")   # connects to the cluster the driver started
app = FastAPI()

@app.get("/v1/communication_layer")
def communication_layer():
    # return the Lua script as plain text
    with open("communication.lua") as f:
        return f.read()

@app.get("/v1/typesystem")
def typesystem():
    # return UIMA TypeSystem XML (or an empty TypeSystem)
    return "<typeSystemDescription/>"

@app.post("/v1/process")
def process(request: dict):
    # use Ray internally; return result as dict/JSON
    ...
```

The Lua script at `/v1/communication_layer` defines `serialize` and `deserialize` exactly as for any other DUUI driver. The `params` map passed to `serialize` will contain all component parameters **except** the Ray infrastructure keys (`ray_parallel_component`, `cpus_per_worker`, `gpus_per_worker`, `head_node_port`, `dashboard_port`, `processing_timeout`, `ray_executable`, `keep_alive`, `working_dir`, `entrypoint`, `python_executable`). `num_workers` is intentionally forwarded so the Lua script can embed it in the request body if needed.

---

## Java API

### Minimal Example

```java
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRayDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRayParallelDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.LuaConsts;

DUUIComposer composer = new DUUIComposer()
        .withLuaContext(LuaConsts.getJSON())
        .withScale(1);

DUUIRayDriver rayDriver = new DUUIRayDriver();
composer.

addDriver(rayDriver);

composer.

add(
    new DUUIRayDriver.Component(
                "/home/user/my_component",   // working directory
            "letter_counter.py"          // entrypoint script
)
    .

withTaskUrl("http://127.0.0.1:8000")
    .

withNumWorkers(4)
    .

withCPUsPerWorker(2)
    .

build()
);

        composer.

run(reader);
composer.

shutdown();
```

### Full Configuration Reference

```java
new DUUIRayParallelDriver.Component(workingDir, entrypoint)
    // FastAPI component URL
    .withTaskUrl("http://127.0.0.1:25590")  // default: "http://127.0.0.1:25590"

    // Ray cluster
    .withNumWorkers(2)              // Ray worker nodes (default: 2; 0 = head-only)
    .withCPUsPerWorker(1)           // CPUs per worker (default: 1)
    .withGPUsPerWorker(0)           // GPUs per worker (default: 0)
    .withHeadNodePort(6379)         // Ray GCS port (default: 6379)
    .withDashboardPort(8265)        // Ray Dashboard port (default: 8265)

    // Executables
    .withRayExecutable("ray")       // ray binary (default: "ray" on PATH)
    .withPythonExecutable("python3")// Python interpreter (default: "python3")

    // Processing
    .withProcessingTimeout(300)     // seconds to wait for /v1/process (default: 300)
    .withKeepAlive(false)           // keep cluster alive after pipeline ends (default: false)

    // Standard DUUI parameters
    .withParameter("num_workers", "4")  // forwarded to Lua params map
    .withSourceView("_InitialView")
    .withTargetView("_InitialView")
    .withName("MyRayComponent")
    .build()
```

### Driver-Level Ray Executable Override

When `ray` is not on the system `PATH` (e.g., inside a conda environment), set the executable once at the driver level rather than per-component:

```java
DUUIRayParallelDriver rayDriver = new DUUIRayParallelDriver()
    .withRaySource("/opt/conda/envs/myenv/bin/ray");
composer.addDriver(rayDriver);
```

This takes priority over the component-level `.withRayExecutable(...)`.

---

## Head-Only Mode (`numWorkers=0`)

With `withNumWorkers(0)`, the driver starts only the Ray head node. The head node itself acts as the sole worker — useful for development or single-machine deployments where worker overhead is undesirable:

```java
new DUUIRayParallelDriver.Component("/path/to/component", "main.py")
    .withNumWorkers(0)
    .build()
```

---

## Keep-Alive Mode

By default the Ray cluster is stopped (`ray stop`) when the last component is destroyed. Set `withKeepAlive(true)` to leave the cluster running after the pipeline finishes, for example when another process needs it:

```java
.withKeepAlive(true)
```

The cluster must then be stopped manually with `ray stop`.

---

## CAS View Routing

Like all DUUI drivers, `DUUIRayParallelDriver` respects view routing:

```java
.withSourceView("InputView")   // CAS view read before serialisation
.withTargetView("OutputView")  // CAS view written after deserialisation
```

---

## Concurrency Notes

- DUUI's `withScale(N)` on the **composer** controls document-level parallelism (how many documents are processed simultaneously). Set this to `1` for `DUUIRayParallelDriver` because Ray already parallelises internally — multiple DUUI threads would conflict on the same Ray ports.
- The builder enforces `withScale(1)` automatically.
- The driver uses a `LinkedBlockingQueue<ComponentInstance>` to coordinate the single HTTP instance among composer worker threads without busy-waiting.

---

## Cluster Lifecycle

| Event | Cluster State |
|---|---|
| First `instantiate()` call | Head + N workers started; Ray job submitted |
| Subsequent `instantiate()` calls (same driver) | Cluster already running — skipped |
| `destroy(uuid)` when `activeComponents > 0` | Cluster kept running |
| `destroy(uuid)` when `activeComponents == 0` and `keepAlive=false` | `ray stop` issued |
| `shutdown()` when `keepAlive=false` | `ray stop` issued |
| `keepAlive=true` at any point | Cluster never stopped by driver |

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `IOException: Failed to start Ray head node (exit 1)` | `ray` not on PATH | Use `.withRayExecutable("/full/path/to/ray")` or driver-level `.withRaySource(...)` |
| `Ray parallel component did not become responsive within N ms` | FastAPI server slow to start | Increase `withProcessingTimeout(...)` or the driver constructor timeout |
| `HTTP 500` from `/v1/process` | Error inside the Python component | Check the Ray job logs via the Dashboard at `http://localhost:8265` |
| Workers crash on startup with `FileNotFoundError` | `--working-dir` packaging issue | Do **not** pass `--working-dir` to `ray job submit`; the driver already avoids this |
| Port conflict on `6379` or `8265` | Another Ray cluster is running | Call `ray stop` first, or change ports with `.withHeadNodePort(...)` / `.withDashboardPort(...)` |
