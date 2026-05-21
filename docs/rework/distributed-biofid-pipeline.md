# Distributed BioFID-Style DUUI Orchestrator Pipeline

This harness exercises the new scoped DUUI orchestrator with a BioFID/NOVA-shaped v1 workflow:

```text
XMI JCas
  -> spacy
  -> geonames
  -> gnfinder
  -> taxonerd
  -> XMI target
```

The scheduler remains final and only selects the next checkpoint artifact. Execution behavior is set on stages with `DUUIDispatchPolicy.IO`, so artifact processing runs through DUUI virtual-thread execution while component capacity is controlled by v1 replica/slot configuration.

## Runtime Modes

- `remote`: uses fixed v1 endpoints.
- `podman`: starts local DUUI v1 containers through `DUUIPodmanDriver`, resolves their HTTP endpoints, then runs them as normal v1 annotator nodes.
- `kubernetes`: starts DUUI v1 deployments/services through `DUUIKubernetesDriver`, resolves service endpoints, then runs them as normal v1 annotator nodes.

The environment creates addressable resources. The pipeline still owns v1 annotators, nodes, concurrency slots, and task execution.

## Build Local duui-py Images

```bash
cd /home/stud_homes/s0424382/projects/ttlab/duui/DockerUnifiedUIMAInterface
scripts/rework/build-duui-py-biofid-images.sh
```

## Run Likely Optimal Podman Configuration

```bash
cd /home/stud_homes/s0424382/projects/ttlab/duui/DockerUnifiedUIMAInterface
scripts/rework/run-distributed-biofid-pipeline.sh podman
```

Defaults:

- dispatch parallelism: `256`
- each component: `scale=2`, `concurrency=16`
- image tags:
  - `localhost/duui-py-spacy-lua-msgpack:dev`
  - `localhost/duui-py-geonames-msgpack-lua:dev`
  - `localhost/duui-py-gnfinder-msgpack-lua:dev`
  - `localhost/duui-py-taxonerd-msgpack-lua:dev`

## Run Remote Configuration

```bash
DUUI_SPACY_ENDPOINT=http://127.0.0.1:19715 \
DUUI_GEONAMES_ENDPOINT=http://127.0.0.1:19716 \
DUUI_GNFINDER_ENDPOINT=http://127.0.0.1:19714 \
DUUI_TAXONERD_ENDPOINT=http://127.0.0.1:19717 \
scripts/rework/run-distributed-biofid-pipeline.sh remote
```

## Run Kubernetes Configuration

```bash
DUUI_KUBERNETES_LABELS=disktype=all \
scripts/rework/run-distributed-biofid-pipeline.sh kubernetes
```

## Configuration Matrix

```bash
scripts/rework/run-distributed-biofid-matrix.sh podman
scripts/rework/run-distributed-biofid-matrix.sh kubernetes
```

Matrix rows:

- `likely-optimal`: high IO dispatch, moderate scale, high per-replica concurrency.
- `low-pressure`: conservative baseline.
- `high-io`: more replicas with the same slots per replica.
- `slot-pressure`: fewer replicas with more slots per replica.

Primary indicators:

- end-to-end runtime
- per-stage DUUI events
- component node borrow pressure
- remote v1 telemetry logs/metrics
- failures/timeouts/retries
- output XMI existence and basic annotation presence
