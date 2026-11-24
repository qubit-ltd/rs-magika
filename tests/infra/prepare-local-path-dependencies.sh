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
(cd "$temporary_root/project" && .infra/lib/prepare-local-path-dependencies.sh)
test "$(git -C "$temporary_root/project/../rs-operation" status --porcelain)" = "$before_status"
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
printf '../rs-operation\t%s\t%s\n' "$temporary_root/source.git" "$revision" \
    > "$temporary_root/project/.infra/ci/local-path-dependencies.tsv"
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
    "$temporary_root/status-failure.log" >/dev/null || {
    cat "$temporary_root/status-failure.log" >&2
    exit 1
}

printf '../rs-retry\t%s\t%s\n' "$temporary_root/source.git" "$revision" \
    > "$temporary_root/project/.infra/ci/local-path-dependencies.tsv"
cat > "$temporary_root/bin/git" <<EOF
#!/usr/bin/env bash
if [[ "\$*" == *" fetch "* ]]; then
    echo "simulated fetch failure" >&2
    exit 31
fi
exec "$real_git" "\$@"
EOF
chmod +x "$temporary_root/bin/git"
if (cd "$temporary_root/project" && PATH="$temporary_root/bin:$PATH" \
    .infra/lib/prepare-local-path-dependencies.sh) > "$temporary_root/fetch-failure.log" 2>&1; then
    echo "expected fetch failure to fail dependency preparation" >&2
    exit 1
fi
grep -F "simulated fetch failure" "$temporary_root/fetch-failure.log" >/dev/null
test ! -e "$temporary_root/rs-retry"
test -z "$(find "$temporary_root" -maxdepth 1 -name '.rs-retry.prepare.*' -print -quit)"
test ! -e "$temporary_root/.rs-retry.prepare-publish.lock"
rm "$temporary_root/bin/git"
(cd "$temporary_root/project" && .infra/lib/prepare-local-path-dependencies.sh)
test "$(git -C "$temporary_root/rs-retry" rev-parse HEAD)" = "$revision"

printf '../rs-existing\t%s\t%s\n' "$temporary_root/source.git" "$revision" \
    > "$temporary_root/project/.infra/ci/local-path-dependencies.tsv"
mkdir "$temporary_root/rs-existing"
printf 'preserve me\n' > "$temporary_root/rs-existing/marker"
if (cd "$temporary_root/project" && .infra/lib/prepare-local-path-dependencies.sh) \
    > "$temporary_root/existing.log" 2>&1; then
    echo "expected existing non-checkout path to be preserved and rejected" >&2
    exit 1
fi
grep -F "refusing to replace existing dependency path '../rs-existing'" \
    "$temporary_root/existing.log" >/dev/null
test "$(cat "$temporary_root/rs-existing/marker")" = 'preserve me'

printf '../rs-dangling\t%s\t%s\n' "$temporary_root/source.git" "$revision" \
    > "$temporary_root/project/.infra/ci/local-path-dependencies.tsv"
ln -s "$temporary_root/missing-target" "$temporary_root/rs-dangling"
if (cd "$temporary_root/project" && .infra/lib/prepare-local-path-dependencies.sh) \
    > "$temporary_root/dangling.log" 2>&1; then
    echo "expected dangling symlink path to be preserved and rejected" >&2
    exit 1
fi
grep -F "refusing to replace existing dependency path '../rs-dangling'" \
    "$temporary_root/dangling.log" >/dev/null
test -L "$temporary_root/rs-dangling"

printf '../rs-publish-race\t%s\t%s\n' "$temporary_root/source.git" "$revision" \
    > "$temporary_root/project/.infra/ci/local-path-dependencies.tsv"
real_mv=$(command -v mv)
cat > "$temporary_root/bin/mv" <<EOF
#!/usr/bin/env bash
mkdir "\$4"
printf 'published first\n' > "\$4/marker"
exec "$real_mv" "\$@"
EOF
chmod +x "$temporary_root/bin/mv"
if (cd "$temporary_root/project" && PATH="$temporary_root/bin:$PATH" \
    .infra/lib/prepare-local-path-dependencies.sh) > "$temporary_root/publish-race.log" 2>&1; then
    echo "expected concurrent destination appearance to fail publication" >&2
    exit 1
fi
grep -F "appeared during publication; preserving it" "$temporary_root/publish-race.log" >/dev/null || {
    cat "$temporary_root/publish-race.log" >&2
    exit 1
}
test "$(cat "$temporary_root/rs-publish-race/marker")" = 'published first'
test -z "$(find "$temporary_root" -maxdepth 1 -name '.rs-publish-race.prepare.*' -print -quit)"
test ! -e "$temporary_root/.rs-publish-race.prepare-publish.lock"
rm "$temporary_root/bin/mv"

printf '../rs-race\t%s\t%s\n' "$temporary_root/source.git" "$revision" \
    > "$temporary_root/project/.infra/ci/local-path-dependencies.tsv"
mkdir "$temporary_root/barrier"
cat > "$temporary_root/bin/git" <<EOF
#!/usr/bin/env bash
if [[ "\$*" == *" fetch "* ]]; then
    touch "$temporary_root/barrier/\$\$"
    for _ in {1..500}; do
        count=\$(find "$temporary_root/barrier" -type f | wc -l)
        [[ "\$count" -ge 2 ]] && break
        sleep 0.01
    done
fi
exec "$real_git" "\$@"
EOF
chmod +x "$temporary_root/bin/git"
(
    cd "$temporary_root/project"
    PATH="$temporary_root/bin:$PATH" .infra/lib/prepare-local-path-dependencies.sh \
        > "$temporary_root/race-1.log" 2>&1
) &
first_pid=$!
(
    cd "$temporary_root/project"
    PATH="$temporary_root/bin:$PATH" .infra/lib/prepare-local-path-dependencies.sh \
        > "$temporary_root/race-2.log" 2>&1
) &
second_pid=$!
first_status=0
second_status=0
wait "$first_pid" || first_status=$?
wait "$second_pid" || second_status=$?
test "$first_status" -eq 0 -o "$second_status" -eq 0
test "$first_status" -ne 0 -o "$second_status" -ne 0
test "$(git -C "$temporary_root/rs-race" rev-parse HEAD)" = "$revision"
test -z "$(git -C "$temporary_root/rs-race" status --porcelain)"
test -z "$(find "$temporary_root" -maxdepth 1 \( -name '.rs-race.prepare.*' -o \
    -name '.rs-race.prepare-publish.lock' \) -print -quit)"

rm -rf "$temporary_root/project/../rs-operation"
printf '../rs-operation\t%s\tmain\n' "$temporary_root/source.git" \
    > "$temporary_root/project/.infra/ci/local-path-dependencies.tsv"
(cd "$temporary_root/project" && .infra/lib/prepare-local-path-dependencies.sh)
test "$(git -C "$temporary_root/project/../rs-operation" rev-parse HEAD)" = "$revision"
