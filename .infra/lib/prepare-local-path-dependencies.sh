#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)

if [[ $# -eq 1 && "$1" == --check-locked ]]; then
    exec "$project_root/.infra/lib/check-locked-local-path-dependencies.sh"
fi

exec "$project_root/.infra/bin/prepare-local-path-dependencies.sh" "$@"
