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
(cd "$temporary_root/project" && .infra/lib/prepare-local-path-dependencies.sh --check-locked)

printf 'dirty\n' > "$temporary_root/project/../rs-operation/untracked.txt"
before_status=$(git -C "$temporary_root/project/../rs-operation" status --porcelain)
if (cd "$temporary_root/project" && .infra/lib/prepare-local-path-dependencies.sh --check-locked) \
    > "$temporary_root/dirty.log" 2>&1; then
    echo "expected dirty dependency check to fail" >&2
    exit 1
fi
grep -F "dirty at $revision" "$temporary_root/dirty.log" >/dev/null
test "$(git -C "$temporary_root/project/../rs-operation" status --porcelain)" = "$before_status"
rm "$temporary_root/project/../rs-operation/untracked.txt"

printf '../rs-operation\t%s\t%s\n' "$temporary_root/source.git" \
    0000000000000000000000000000000000000000 \
    > "$temporary_root/project/.infra/ci/local-path-dependencies.tsv"
if (cd "$temporary_root/project" && .infra/lib/prepare-local-path-dependencies.sh --check-locked) \
    > "$temporary_root/mismatch.log" 2>&1; then
    echo "expected dependency revision check to fail" >&2
    exit 1
fi
grep -F "actual $revision; expected 0000000000000000000000000000000000000000" \
    "$temporary_root/mismatch.log" >/dev/null
test "$(git -C "$temporary_root/project/../rs-operation" rev-parse HEAD)" = "$revision"
test -z "$(git -C "$temporary_root/project/../rs-operation" status --porcelain)"

real_git=$(command -v git)
mkdir -p "$temporary_root/bin"
cat > "$temporary_root/bin/git" <<EOF
#!/usr/bin/env bash
if [[ "\$*" == *"status --porcelain"* ]]; then
    echo "simulated git status failure" >&2
    exit 23
fi
exec "$real_git" "\$@"
EOF
chmod +x "$temporary_root/bin/git"
if (cd "$temporary_root/project" && PATH="$temporary_root/bin:$PATH" \
    .infra/lib/prepare-local-path-dependencies.sh --check-locked) \
    > "$temporary_root/status-failure.log" 2>&1; then
    echo "expected git status failure to fail locked dependency check" >&2
    exit 1
fi
grep -F "working-tree status for '../rs-operation' (exit 23): simulated git status failure" \
    "$temporary_root/status-failure.log" >/dev/null

rm -rf "$temporary_root/project/../rs-operation"
printf '../rs-operation\t%s\tmain\n' "$temporary_root/source.git" \
    > "$temporary_root/project/.infra/ci/local-path-dependencies.tsv"
(cd "$temporary_root/project" && .infra/lib/prepare-local-path-dependencies.sh)
test "$(git -C "$temporary_root/project/../rs-operation" rev-parse HEAD)" = "$revision"
