#!/usr/bin/env bash
set -euo pipefail

cd /home/stud_homes/s0424382/projects/ttlab/duui-alpha/DockerUnifiedUIMAInterface

CP="duui-dua/dua-core/target/classes:duui-base/target/classes:duui-core/target/classes:$(cat /tmp/dua-core.cp)"

exec java \
  --enable-preview \
  --enable-native-access=ALL-UNNAMED \
  --add-opens java.base/java.nio=ALL-UNNAMED \
  --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
  -cp "$CP" \
  org.texttechnologylab.duui.dua.benchmarks.DUAImporterDashboardBenchmark \
  /storage/projects/BIOfid/code/dterefe/artifacts/nertools-benchmark \
  90 \
  17931 \
  32 \
  /tmp/dua-service-17931-dterefe90 \
  SPAN_GRAPH_SELECT
