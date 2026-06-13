#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
  exit 0
fi

cd "$(git rev-parse --show-toplevel)"
uv run ty check "$@"
