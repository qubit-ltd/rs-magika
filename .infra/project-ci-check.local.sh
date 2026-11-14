#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
python3 "$project_root/.infra/check-operation-pin.py"
exec python3 "$project_root/.infra/check-doc-versions.py" "$@"
