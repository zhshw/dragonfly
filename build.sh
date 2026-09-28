#!/usr/bin/env bash
set -euo pipefail

# Usage: ./build.sh [target ...] [ninja options]
# Examples: ./build.sh -j 4
#           ./build.sh generic_family_test
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

./helio/blaze.sh -DWITH_AWS=OFF -DWITH_GCP=OFF

if [[ $# -eq 0 || $1 == -* ]]; then
  set -- dragonfly "$@"
fi

ninja -C build-dbg "$@"
