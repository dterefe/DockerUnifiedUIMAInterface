# DUUI Gateway

Separate Maven module for running DUUI as a managed Java service.

## Build

```bash
cd DockerUnifiedUIMAInterface
mvn -DskipTests install

cd ../duui-gateway
mvn -DskipTests package
```

## Run

```bash
cd duui-gateway
mvn exec:java \
  -Dduui.gateway.port=8788 \
  -Dduui.dashboard.dir=../annotator-testbench/frontend/dist
```

If `frontend/dist` is not present, the gateway falls back to the frontend source root for development. Production should point at the built Svelte bundle.

The annotator testbench compose file now routes all dashboard API traffic to this Java gateway. The Python compatibility backend is not part of the normal dashboard path.

## Current API

- `GET /api/gateway/status`
- `GET /api/gateway/annotators`
- `POST /api/gateway/annotators`
- `GET /api/gateway/components`
- `POST /api/gateway/components`
- `GET /api/gateway/pipelines`
- `POST /api/gateway/pipelines`
- `GET /api/gateway/experiments`
- `POST /api/gateway/experiments`
- `GET /api/gateway/runs`
- `POST /api/gateway/runs`
- `GET /api/gateway/orchestrator/inspect`
- `GET /...` static dashboard assets

The gateway owns state for annotators, components, pipelines, experiments, service declarations, DUUI event/metric capture, and live run snapshots. DUUI core owns the pipeline/orchestrator execution path.
