#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 || ($1 == -timeline && ($# != 2 || ${2:-} == -)) ]]; then
  echo "Usage: $0 EXPERIMENT_ID [RUN_ID] | -timeline FILE" >&2
  exit 2
fi

if [[ $1 == -timeline ]]; then
  source_args=(-timeline "$2")
else
  source_args=(-id "$1")
  if [[ $# == 2 ]]; then
    source_args+=(-run "$2")
  fi
fi

profile trace "${source_args[@]}" -analysis request-phases
profile trace "${source_args[@]}" -analysis admission-wait -scope leader -tickets 31 -limit 3
profile trace "${source_args[@]}" -analysis slow-path -limit 1
profile trace "${source_args[@]}" -analysis fetch-delay -scope round -limit 10
