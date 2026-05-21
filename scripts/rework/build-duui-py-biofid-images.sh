#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PY_EXAMPLES="$ROOT/duui-py/examples"
ENGINE="${CONTAINER_ENGINE:-podman}"

"$ENGINE" build -f "$PY_EXAMPLES/spacy-lua-msgpack/Dockerfile" -t localhost/duui-py-spacy-lua-msgpack:dev "$ROOT/duui-py"
"$ENGINE" build -f "$PY_EXAMPLES/geonames-msgpack-lua/Dockerfile" -t localhost/duui-py-geonames-msgpack-lua:dev "$ROOT/duui-py"
"$ENGINE" build -f "$PY_EXAMPLES/gnfinder-msgpack-lua/Dockerfile" -t localhost/duui-py-gnfinder-msgpack-lua:dev "$ROOT/duui-py"
"$ENGINE" build -f "$PY_EXAMPLES/taxonerd-msgpack-lua/Dockerfile" -t localhost/duui-py-taxonerd-msgpack-lua:dev "$ROOT/duui-py"
