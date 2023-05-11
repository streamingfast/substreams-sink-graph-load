#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

main() {
  cd "$ROOT"

  set -e

  out="${OUT_DIR}:-"$ROOT/csv"}"
  sink="$ROOT/../graphload"

  echo dsn
  set -x

  $sink run \
    ${out} \
    "api-unstable.streamingfast.io:443" \
    "https://github.com/streamingfast/substreams-uniswap-v3/releases/download/v0.1.5-beta/uniswap-v3-v0.1.5-beta.spkg" \
    "graph_out" \
    12369900 \
    "$@"
}

main "$@"
