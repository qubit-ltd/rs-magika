#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
shared_root=${RS_INFRA_TEST_SHARED_ROOT:-${RS_INFRA_SHARED_ROOT:-}}
unset RS_INFRA_PROJECT_ROOT
[[ -n "$shared_root" ]] || {
    echo "error: set RS_INFRA_SHARED_ROOT or RS_INFRA_TEST_SHARED_ROOT to an rs-infra-tools checkout" >&2
    exit 2
}

temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT

project="$temporary_root/project"
bin="$temporary_root/bin"
revision=0123456789abcdef0123456789abcdef01234567
mkdir -p "$project/.infra/bin" "$project/.infra/lib" "$project/.infra/ci" "$bin"
cp "$repo_root/.infra/bootstrap.sh" "$project/.infra/"
cp "$repo_root/.infra/bin/prepare-local-path-dependencies.sh" \
    "$repo_root/.infra/bin/align-ci.sh" "$repo_root/.infra/bin/ci-check.sh" \
    "$project/.infra/bin/"
cp "$repo_root/.infra/lib/prepare-local-path-dependencies.sh" "$project/.infra/lib/"
printf '../rs-operation\t%s\t%s\n' /mock/rs-operation.git "$revision" \
    > "$project/.infra/ci/local-path-dependencies.tsv"

cat > "$bin/rs-infra-tools" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$ENTRY_CALL_LOG"
EOF
cat > "$bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ "$*" == *" fetch "* && -n "${MOCK_FETCH_FAIL_ONCE:-}" && ! -e "$MOCK_FETCH_FAIL_ONCE" ]]; then
    touch "$MOCK_FETCH_FAIL_ONCE"
    echo 'simulated fetch failure' >&2
    exit 31
fi

if [[ "$1" == init ]]; then
    mkdir -p "$2"
    mkdir -p "$2/.git"
    printf 'mock source\n' > "$2/fixture.txt"
    exit 0
fi

if [[ "$1" == -C ]]; then
    checkout=$2
    shift 2
    case "$1" in
        remote|checkout) exit 0 ;;
        rev-parse)
            if [[ "${2:-}" == --show-toplevel ]]; then
                cursor=$checkout
                while [[ "$cursor" != / && ! -e "$cursor/.git" ]]; do cursor=$(dirname "$cursor"); done
                printf '%s\n' "$cursor"
                exit 0
            fi
            [[ -f "$checkout/fixture.txt" ]] || exit 128
            printf '%s\n' "$MOCK_GIT_HEAD"
            exit 0
            ;;
        status)
            if [[ -e "$checkout/untracked.txt" ]]; then
                printf '?? untracked.txt\n'
            fi
            exit 0
            ;;
    esac
fi

exit 0
EOF
chmod +x "$bin/rs-infra-tools" "$bin/git"
export ENTRY_CALL_LOG="$temporary_root/entry-calls.log"
export MOCK_GIT_HEAD="$revision"
export MOCK_FETCH_FAIL_ONCE="$temporary_root/fetch-failed-once"
export RS_INFRA_SHARED_ROOT="$shared_root"
export RS_INFRA_TOOLS_BIN="$bin/rs-infra-tools"
export RS_INFRA_NETWORK_MAX_ATTEMPTS=1

if (cd "$project" && PATH="$bin:$PATH" .infra/bin/align-ci.sh --dry-run) \
    > "$temporary_root/first-entry.log" 2>&1; then
    echo "expected normal align-ci entrypoint to fail on the simulated fetch" >&2
    exit 1
fi
grep -F 'simulated fetch failure' "$temporary_root/first-entry.log" >/dev/null
test ! -e "$temporary_root/rs-operation"
test -z "$(find "$temporary_root" -maxdepth 1 -name '.rs-operation.prepare.*' -print -quit)"
test ! -e "$temporary_root/.rs-operation.prepare-publish.lock"

(cd "$project" && PATH="$bin:$PATH" .infra/bin/align-ci.sh --dry-run)
test "$(cat "$temporary_root/rs-operation/fixture.txt")" = 'mock source'
test -z "$(find "$temporary_root" -maxdepth 1 -name '.rs-operation.prepare.*' -print -quit)"

# The local compatibility helper forwards to the formal bootstrap entrypoint.
(cd "$project" && PATH="$bin:$PATH" .infra/lib/prepare-local-path-dependencies.sh --check-locked)
printf 'dirty\n' > "$temporary_root/rs-operation/untracked.txt"
if (cd "$project" && PATH="$bin:$PATH" .infra/bin/prepare-local-path-dependencies.sh --check-locked) \
    > "$temporary_root/dirty.log" 2>&1; then
    echo 'expected strict locked check to reject a dirty checkout' >&2
    exit 1
fi
grep -F "dirty at $revision" "$temporary_root/dirty.log" >/dev/null
rm "$temporary_root/rs-operation/untracked.txt"

printf '../rs-operation\t%s\t%s\n' /mock/rs-operation.git 0000000000000000000000000000000000000000 \
    > "$project/.infra/ci/local-path-dependencies.tsv"
if (cd "$project" && PATH="$bin:$PATH" .infra/bin/prepare-local-path-dependencies.sh --check-locked) \
    > "$temporary_root/mismatch.log" 2>&1; then
    echo 'expected strict locked check to reject a mismatched HEAD' >&2
    exit 1
fi
grep -F "actual $revision; expected 0000000000000000000000000000000000000000" \
    "$temporary_root/mismatch.log" >/dev/null

printf '../occupied\t%s\t%s\n' /mock/occupied.git "$revision" \
    > "$project/.infra/ci/local-path-dependencies.tsv"
mkdir "$temporary_root/occupied"
printf 'preserve me\n' > "$temporary_root/occupied/marker"
if (cd "$project" && PATH="$bin:$PATH" .infra/bin/prepare-local-path-dependencies.sh) \
    > "$temporary_root/occupied.log" 2>&1; then
    echo 'expected occupied destination to be preserved and rejected' >&2
    exit 1
fi
grep -F "refusing to replace existing dependency path '../occupied'" \
    "$temporary_root/occupied.log" >/dev/null
test "$(cat "$temporary_root/occupied/marker")" = 'preserve me'

printf '../rs-operation\t%s\t%s\n' /mock/rs-operation.git "$revision" \
    > "$project/.infra/ci/local-path-dependencies.tsv"
(cd "$project" && PATH="$bin:$PATH" .infra/bin/ci-check.sh)
grep -F "latest --project $project --tool rs-infra-style" "$ENTRY_CALL_LOG" >/dev/null
grep -F "latest --project $project" "$ENTRY_CALL_LOG" >/dev/null
