#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
bash "$project_root/tests/infra/prepare-local-path-dependencies.sh"
python3 "$project_root/.infra/check-doc-links.py" --self-test
python3 "$project_root/.infra/check-readme-examples.py" --self-test
python3 "$project_root/.infra/check-managed-guide-examples.py" --self-test
python3 "$project_root/.infra/check-doc-versions.py" --self-test
python3 "$project_root/.infra/check-doc-links.py"
python3 "$project_root/.infra/check-readme-examples.py"
python3 "$project_root/.infra/check-managed-guide-examples.py"
exec python3 "$project_root/.infra/check-doc-versions.py" "$@"
