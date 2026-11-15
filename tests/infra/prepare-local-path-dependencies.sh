#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT

git init --bare "$temporary_root/source.git" >/dev/null
git init "$temporary_root/working" >/dev/null
git -C "$temporary_root/working" config user.name "Infra Test"
git -C "$temporary_root/working" config user.email infra-test@example.invalid
printf 'fixture\n' > "$temporary_root/working/fixture.txt"
git -C "$temporary_root/working" add fixture.txt
git -C "$temporary_root/working" commit -m fixture >/dev/null
git -C "$temporary_root/working" branch -M main
git -C "$temporary_root/working" remote add origin "$temporary_root/source.git"
git -C "$temporary_root/working" push -u origin main >/dev/null
revision=$(git -C "$temporary_root/working" rev-parse HEAD)

mkdir -p "$temporary_root/project/.infra/ci" "$temporary_root/project/.infra/lib"
cp "$repo_root/.infra/lib/prepare-local-path-dependencies.sh" "$temporary_root/project/.infra/lib/"
printf '../rs-operation\t%s\t%s\n' "$temporary_root/source.git" "$revision" \
    > "$temporary_root/project/.infra/ci/local-path-dependencies.tsv"
(cd "$temporary_root/project" && .infra/lib/prepare-local-path-dependencies.sh)
test "$(git -C "$temporary_root/project/../rs-operation" rev-parse HEAD)" = "$revision"

rm -rf "$temporary_root/project/../rs-operation"
printf '../rs-operation\t%s\tmain\n' "$temporary_root/source.git" \
    > "$temporary_root/project/.infra/ci/local-path-dependencies.tsv"
(cd "$temporary_root/project" && .infra/lib/prepare-local-path-dependencies.sh)
test "$(git -C "$temporary_root/project/../rs-operation" rev-parse HEAD)" = "$revision"
