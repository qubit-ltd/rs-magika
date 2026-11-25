#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
bash "$project_root/tests/infra/prepare-local-path-dependencies.sh"
package_list=$(mktemp)
trap 'rm -f "$package_list"' EXIT
cargo +1.94.0 package --list --locked --allow-dirty --manifest-path "$project_root/Cargo.toml" > "$package_list"
python3 "$project_root/.infra/check-cargo-matrix-legacy.py" --self-test
python3 "$project_root/.infra/check-cargo-matrix-legacy.py"
python3 "$project_root/.infra/check-doc-links.py" --self-test
python3 "$project_root/.infra/record-consumer-ci-evidence.py" --self-test
python3 "$project_root/.infra/check-readme-examples.py" --self-test
python3 "$project_root/.infra/check-managed-guide-examples.py" --self-test
python3 "$project_root/.infra/check-doc-versions.py" --self-test
python3 "$project_root/.infra/check-doc-links.py" --package-list "$package_list"
python3 "$project_root/.infra/check-doc-links.py"
python3 "$project_root/.infra/check-readme-examples.py"
python3 "$project_root/.infra/check-managed-guide-examples.py"
python3 "$project_root/.infra/check-doc-versions.py" "$@"
