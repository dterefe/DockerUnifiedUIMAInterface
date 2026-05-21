#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUN="$ROOT/scripts/rework/run-distributed-biofid-pipeline.sh"
MODE="${1:-${DUUI_DISTRIBUTED_MODE:-podman}}"

configs=(
  "likely-optimal:256:2:16:2:16:2:16:2:16"
  "low-pressure:64:1:8:1:8:1:8:1:8"
  "high-io:512:4:16:4:16:4:16:4:16"
  "slot-pressure:512:2:32:2:32:2:32:2:32"
)

for config in "${configs[@]}"; do
  IFS=: read -r name dispatch spacy_scale spacy_con geonames_scale geonames_con gnfinder_scale gnfinder_con taxonerd_scale taxonerd_con <<< "$config"
  echo "=== distributed-biofid $MODE $name ==="
  DUUI_DISPATCH_PARALLELISM="$dispatch" \
  DUUI_SPACY_SCALE="$spacy_scale" \
  DUUI_SPACY_CONCURRENCY="$spacy_con" \
  DUUI_GEONAMES_SCALE="$geonames_scale" \
  DUUI_GEONAMES_CONCURRENCY="$geonames_con" \
  DUUI_GNFINDER_SCALE="$gnfinder_scale" \
  DUUI_GNFINDER_CONCURRENCY="$gnfinder_con" \
  DUUI_TAXONERD_SCALE="$taxonerd_scale" \
  DUUI_TAXONERD_CONCURRENCY="$taxonerd_con" \
  "$RUN" "$MODE"
done
