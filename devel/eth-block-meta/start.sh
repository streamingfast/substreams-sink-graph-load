#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

main() {
  cd "$ROOT"

  set -e

  out="${OUT_DIR}:-"$ROOT/csv"}"
  sink="$ROOT/../substreams-sink-graphcsv"

  echo dsn
  set -x

  $sink run \
    ${out} \
    "api-unstable.streamingfast.io:443" \
    "https://github.com/streamingfast/substreams-eth-block-meta/releases/download/v0.4.0/substreams-eth-block-meta-v0.4.0.spkg" \
    "graph_out" \
    10000 \
    "$@"
}

main "$@"
