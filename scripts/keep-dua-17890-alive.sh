#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PORT="${PORT:-17890}"
WS_PORT="$((PORT + 1))"
XMI_DIR="${XMI_DIR:-/storage/projects/BIOfid/code/dterefe/artifacts/nertools-benchmark}"
LIMIT="${LIMIT:-90}"
SOURCE_PARALLELISM="${SOURCE_PARALLELISM:-16}"
LMDB_DIR="${LMDB_DIR:-/tmp/dua-importer-dashboard-live}"
PROFILE="${PROFILE:-SPAN_GRAPH_SELECT}"

if command -v java >/dev/null 2>&1; then
  JAVA_BIN="$(command -v java)"
else
  JAVA_BIN="/home/stud_homes/s0424382/.local/opt/jdk21-extracted/usr/lib/jvm/java-21-openjdk-amd64/bin/java"
fi

LOG_FILE="${REPO_ROOT}/logs/dua-17890-live.log"
PID_FILE="${REPO_ROOT}/logs/dua-17890-live.pid"
mkdir -p "${REPO_ROOT}/logs"

build_classpath() {
  local cp
  cp="$(cd "$REPO_ROOT" && mvn -pl duui-dua/dua-core -q -DincludeScope=runtime dependency:build-classpath -Dmdep.outputFile=/tmp/dua-dua-core-classpath.txt 2>/dev/null || true)"
  cp="$(cat /tmp/dua-dua-core-classpath.txt 2>/dev/null | tr '\n' ':')"
  if [[ -z "${cp}" ]]; then
    echo "classpath generation failed" >&2
    exit 1
  fi
  echo "${REPO_ROOT}/duui-dua/dua-core/target/classes:${REPO_ROOT}/duui-dua/dua-core/target/test-classes:${cp}"
}

CLASSPATH="$(build_classpath)"

is_listener_active() {
  ss -ltnp '( sport = :'${PORT}' )' 2>/dev/null | awk 'NR>1 {print}' | rg -q "pid="
}

is_websocket_active() {
  ss -ltnp '( sport = :'${WS_PORT}' )' 2>/dev/null | awk 'NR>1 {print}' | rg -q "pid="
}

is_http_healthy() {
  curl -fsS --max-time 1 "http://127.0.0.1:${PORT}/health" > /dev/null 2>&1
}

is_process_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

cleanup_stale_processes() {
  local pid
  if [[ -f "$PID_FILE" ]]; then
    pid="$(cat "$PID_FILE")"
    if is_process_running "$pid"; then
      kill "$pid" 2>/dev/null || true
      sleep 1
      if is_process_running "$pid"; then
        kill -9 "$pid" 2>/dev/null || true
      fi
    fi
    rm -f "$PID_FILE"
  fi

  local stale_pids
  stale_pids="$(pgrep -f "org.texttechnologylab.duui.dua.benchmarks.DUAImporterDashboardBenchmark" || true)"
  if [[ -n "$stale_pids" ]]; then
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      if is_process_running "$pid"; then
        kill "$pid" 2>/dev/null || true
        sleep 1
        is_process_running "$pid" && kill -9 "$pid" 2>/dev/null || true
      fi
    done <<< "$stale_pids"
  fi
}

is_service_healthy() {
  is_http_healthy && is_listener_active
}

start_server() {
  cleanup_stale_processes
  is_listener_active && is_websocket_active && {
    echo "[$(date --iso-8601=seconds)] WARN: listener detected on clean start; forcing cleanup before launch" >> "${LOG_FILE}"
    cleanup_stale_processes
  }

  echo "[$(date --iso-8601=seconds)] INFO: launching DUA dashboard on port ${PORT}" >> "${LOG_FILE}"
  "$JAVA_BIN" \
    --enable-preview \
    --enable-native-access=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "${CLASSPATH}" \
    org.texttechnologylab.duui.dua.benchmarks.DUAImporterDashboardBenchmark \
    "${XMI_DIR}" "${LIMIT}" "${PORT}" "${SOURCE_PARALLELISM}" "${LMDB_DIR}" "${PROFILE}" \
    >> "${LOG_FILE}" 2>&1 &

  local server_pid=$!
  echo "$server_pid" > "${PID_FILE}"
  echo "[$(date --iso-8601=seconds)] INFO: java pid ${server_pid}" >> "${LOG_FILE}"

  local attempts=0
  while [[ $attempts -lt 120 ]]; do
    sleep 1
    attempts=$((attempts + 1))
    if is_service_healthy; then
      echo "[$(date --iso-8601=seconds)] INFO: dashboard is live on 127.0.0.1:${PORT}" >> "${LOG_FILE}"
      return 0
    fi
    if ! is_process_running "$server_pid"; then
      if grep -q "Address already in use" "${LOG_FILE}" || true; then
        echo "[$(date --iso-8601=seconds)] WARN: startup failed due bind conflict; killing stale holders" >> "${LOG_FILE}"
      fi
      return 1
    fi
  done
  echo "[$(date --iso-8601=seconds)] WARN: startup timed out waiting for dashboard health on ${PORT}" >> "${LOG_FILE}"
  return 1
}

while true; do
  if is_service_healthy; then
    echo "[$(date --iso-8601=seconds)] INFO: port ${PORT} healthy; keeping alive" >> "${LOG_FILE}"
  else
    start_server || {
      cleanup_stale_processes
      echo "[$(date --iso-8601=seconds)] WARN: service failed to come up, retrying" >> "${LOG_FILE}"
    }
  fi
  sleep 1
done
