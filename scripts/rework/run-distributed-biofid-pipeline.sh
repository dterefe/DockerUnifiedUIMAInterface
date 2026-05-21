#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MODE="${1:-${DUUI_DISTRIBUTED_MODE:-podman}}"

cd "$ROOT"

common=(
  -Dtest=org.texttechnologylab.duui.rework.DUUIDistributedBiofidPipelineTest#biofidStylePipelineRunsOnConfiguredDistributedEnvironment
  -Dmaven.test.skip=false
  -Dduui.distributed.enabled=true
  -Dduui.distributed.mode="$MODE"
  -Dduui.distributed.dispatch.parallelism="${DUUI_DISPATCH_PARALLELISM:-256}"
  -Dduui.distributed.spacy.scale="${DUUI_SPACY_SCALE:-2}"
  -Dduui.distributed.spacy.concurrency="${DUUI_SPACY_CONCURRENCY:-16}"
  -Dduui.distributed.geonames.scale="${DUUI_GEONAMES_SCALE:-2}"
  -Dduui.distributed.geonames.concurrency="${DUUI_GEONAMES_CONCURRENCY:-16}"
  -Dduui.distributed.gnfinder.scale="${DUUI_GNFINDER_SCALE:-2}"
  -Dduui.distributed.gnfinder.concurrency="${DUUI_GNFINDER_CONCURRENCY:-16}"
  -Dduui.distributed.taxonerd.scale="${DUUI_TAXONERD_SCALE:-2}"
  -Dduui.distributed.taxonerd.concurrency="${DUUI_TAXONERD_CONCURRENCY:-16}"
)

case "$MODE" in
  remote|REMOTE)
    mvn -q \
      "${common[@]}" \
      -Dduui.distributed.spacy.endpoint="${DUUI_SPACY_ENDPOINT:?}" \
      -Dduui.distributed.geonames.endpoint="${DUUI_GEONAMES_ENDPOINT:?}" \
      -Dduui.distributed.gnfinder.endpoint="${DUUI_GNFINDER_ENDPOINT:?}" \
      -Dduui.distributed.taxonerd.endpoint="${DUUI_TAXONERD_ENDPOINT:?}" \
      test
    ;;
  podman|PODMAN)
    mvn -q "${common[@]}" test
    ;;
  kubernetes|KUBERNETES)
    mvn -q \
      "${common[@]}" \
      -Dduui.distributed.kubernetes.labels="${DUUI_KUBERNETES_LABELS:-}" \
      test
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    exit 2
    ;;
esac
