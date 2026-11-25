#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
temporary_root=$(mktemp -d)
cleanup() {
    if [[ "${KEEP_TEST_TMP:-}" == true ]]; then
        echo "test fixture retained at $temporary_root" >&2
    else
        rm -rf "$temporary_root"
    fi
}
trap cleanup EXIT

project="$temporary_root/project"
dependency="$temporary_root/dependency"
cache="$temporary_root/cache"
bin="$temporary_root/bin"
mkdir -p "$project/.infra/ci" "$project/target/debug" "$project/target/release" \
    "$project/coverage" "$cache" "$bin" "$dependency"

real_git=$(command -v git)
git -c init.defaultBranch=main -C "$dependency" init -q
git -C "$dependency" -c user.name=fixture -c user.email=fixture@example.invalid \
    commit --allow-empty -qm fixture
revision=$(git -C "$dependency" rev-parse HEAD)
printf '../dependency\tfixture://dependency\t%s\n' "$revision" \
    > "$project/.infra/ci/local-path-dependencies.tsv"
printf 'debug sentinel\n' > "$project/target/debug/sentinel"
printf 'release sentinel\n' > "$project/target/release/sentinel"
printf 'coverage sentinel\n' > "$project/coverage/sentinel"

cat > "$bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$GIT_CALL_LOG"
args=("$@")
index=0
subcommand=''
while (( index < ${#args[@]} )); do
    case "${args[index]}" in
        --no-optional-locks) ((index += 1)) ;;
        -C|-c|--git-dir|--work-tree|--namespace|--exec-path)
            (( index + 1 < ${#args[@]} )) || { echo "cannot parse Git global option ${args[index]} without its value" >&2; exit 93; }
            ((index += 2)) ;;
        -*) echo "cannot parse unexpected Git global option ${args[index]}" >&2; exit 93 ;;
        *) subcommand=${args[index]}; break ;;
    esac
done
[[ -n "$subcommand" ]] || { echo 'cannot parse Git call without a subcommand' >&2; exit 93; }
printf '%s\n' "$subcommand" >> "$GIT_SUBCOMMAND_LOG"
if [[ "${GIT_PARSE_ONLY:-}" == true ]]; then printf '%s\n' "$subcommand"; exit 0; fi
if [[ "${GIT_FAIL_STATUS:-}" == true && "$subcommand" == status ]]; then
    echo 'simulated git status failure' >&2
    exit 47
fi
exec "$REAL_GIT" "$@"
EOF
chmod +x "$bin/git"
export REAL_GIT="$real_git"
export GIT_CALL_LOG="$temporary_root/git-calls.log"
export GIT_SUBCOMMAND_LOG="$temporary_root/git-subcommands.log"
export RS_INFRA_PROJECT_ROOT="$project"
export RS_INFRA_CACHE_DIR="$cache"

snapshot() {
    find "$project" "$cache" -type f -print0 | sort -z | xargs -0 sha256sum
}
is_mutating_git_subcommand() {
    grep -E -x 'fetch|checkout|clone|init|reset|clean|update-index|add|commit|gc|maintenance|worktree' \
        <<< "$1" >/dev/null
}
before=$(snapshot)
index_before=$(sha256sum "$dependency/.git/index")
worktree_before=$(git -C "$dependency" status --porcelain)

# Cold and warm cache runs must take only read-only Git paths and preserve sentinels.
PATH="$bin:$PATH" "$repo_root/.infra/lib/prepare-local-path-dependencies.sh" --check-locked
test "$before" = "$(snapshot)"
test -z "$(find "$cache" -type f -print -quit)"
test "$index_before" = "$(sha256sum "$dependency/.git/index")"
test "$worktree_before" = "$(git -C "$dependency" status --porcelain)"
printf 'cache sentinel\n' > "$cache/sentinel"
warm_before=$(snapshot)
PATH="$bin:$PATH" "$repo_root/.infra/lib/check-locked-local-path-dependencies.sh"
test "$warm_before" = "$(snapshot)"
test "$index_before" = "$(sha256sum "$dependency/.git/index")"
test "$worktree_before" = "$(git -C "$dependency" status --porcelain)"
while IFS= read -r subcommand; do
    if is_mutating_git_subcommand "$subcommand"; then
        echo "strict check invoked mutating Git command '$subcommand'" >&2
        exit 1
    fi
done < "$GIT_SUBCOMMAND_LOG"
grep -F -- '--no-optional-locks' "$GIT_CALL_LOG" >/dev/null

assert_synthetic_mutating() {
    local actual_subcommand
    actual_subcommand=$(GIT_PARSE_ONLY=true "$bin/git" "$@")
    if ! is_mutating_git_subcommand "$actual_subcommand"; then
        echo "expected mutating Git subcommand, got '$actual_subcommand'" >&2
        exit 1
    fi
}
assert_synthetic_read_only() {
    local actual_subcommand
    actual_subcommand=$(GIT_PARSE_ONLY=true "$bin/git" "$@")
    if is_mutating_git_subcommand "$actual_subcommand"; then
        echo "expected read-only Git subcommand, got '$actual_subcommand'" >&2
        exit 1
    fi
    test "$actual_subcommand" = status
}

# The same grep decision rejects each synthetic write command and accepts
# status even when a single -C path argument contains spaces and command names.
assert_synthetic_mutating fetch origin
assert_synthetic_mutating checkout abc
assert_synthetic_mutating update-index --refresh
assert_synthetic_mutating gc
assert_synthetic_read_only --no-optional-locks -C '/tmp/status fetch gc update-index checkout' \
    status --porcelain=v1
assert_synthetic_read_only -c safe.directory=fixture -C '/tmp/gc update-index checkout fetch' \
    status --porcelain=v1
if GIT_PARSE_ONLY=true "$bin/git" --unrecognized-global-option status > "$temporary_root/unparseable.log" 2>&1; then
    echo 'expected fixture to reject an unparseable Git invocation' >&2
    exit 1
fi
grep -F 'cannot parse unexpected Git global option' "$temporary_root/unparseable.log" >/dev/null

# A dirty worktree, mismatched HEAD, missing checkout, or Git status failure fails closed.
printf 'dirty\n' > "$dependency/untracked"
if PATH="$bin:$PATH" "$repo_root/.infra/lib/check-locked-local-path-dependencies.sh" \
    > "$temporary_root/dirty.log" 2>&1; then
    echo 'expected strict check to reject a dirty checkout' >&2
    exit 1
fi
grep -F 'dirty at ' "$temporary_root/dirty.log" >/dev/null
rm "$dependency/untracked"

printf '../dependency\tfixture://dependency\t%s\n' \
    0000000000000000000000000000000000000000 > "$project/.infra/ci/local-path-dependencies.tsv"
if PATH="$bin:$PATH" "$repo_root/.infra/lib/check-locked-local-path-dependencies.sh" \
    > "$temporary_root/mismatch.log" 2>&1; then
    echo 'expected strict check to reject a mismatched HEAD' >&2
    exit 1
fi
grep -F 'HEAD mismatch' "$temporary_root/mismatch.log" >/dev/null

printf '../missing\tfixture://missing\t%s\n' "$revision" > "$project/.infra/ci/local-path-dependencies.tsv"
if PATH="$bin:$PATH" "$repo_root/.infra/lib/check-locked-local-path-dependencies.sh" \
    > "$temporary_root/missing.log" 2>&1; then
    echo 'expected strict check to reject a missing checkout' >&2
    exit 1
fi
grep -F 'is missing' "$temporary_root/missing.log" >/dev/null

printf '../dependency\tfixture://dependency\t%s\n' "$revision" \
    > "$project/.infra/ci/local-path-dependencies.tsv"
if GIT_FAIL_STATUS=true PATH="$bin:$PATH" \
    "$repo_root/.infra/lib/check-locked-local-path-dependencies.sh" \
    > "$temporary_root/git-failure.log" 2>&1; then
    echo 'expected strict check to reject a failed Git status query' >&2
    exit 1
fi
grep -F 'simulated git status failure' "$temporary_root/git-failure.log" >/dev/null
grep -F 'exit 47' "$temporary_root/git-failure.log" >/dev/null

test -f "$project/target/debug/sentinel"
test -f "$project/target/release/sentinel"
test -f "$project/coverage/sentinel"
test -f "$cache/sentinel"
test "$index_before" = "$(sha256sum "$dependency/.git/index")"
test -z "$(git -C "$dependency" status --porcelain)"
grep -E -n '^[[:space:]]+run: \.infra/lib/check-locked-local-path-dependencies\.sh$' \
    "$repo_root/.github/workflows/ci.yml" >/dev/null

# Exercise ordinary preparation against the shared implementation with a fake Git
# transport. The helper stages and publishes a synthetic checkout without cloning.
shared_root=${RS_INFRA_TEST_SHARED_ROOT:-${RS_INFRA_SHARED_ROOT:-}}
[[ -n "$shared_root" ]] || {
    echo 'error: set RS_INFRA_TEST_SHARED_ROOT or RS_INFRA_SHARED_ROOT' >&2
    exit 2
}
normal_project="$temporary_root/normal-project"
prep_bin="$temporary_root/prep-bin"
mkdir -p "$normal_project/.infra/ci" "$prep_bin"
prep_revision=0123456789abcdef0123456789abcdef01234567
printf '../prepared\tfixture://prepared\t%s\n' "$prep_revision" \
    > "$normal_project/.infra/ci/local-path-dependencies.tsv"
cat > "$prep_bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$PREP_GIT_LOG"
if [[ "$1" == init ]]; then
    mkdir -p "$2/.git"
    printf '%s\n' "$MOCK_GIT_HEAD" > "$2/.mock-head"
    exit 0
fi
if [[ "$1" == -c ]]; then shift 2; fi
if [[ "$1" != -C ]]; then exit 91; fi
checkout=$2
shift 2
case "$1" in
    remote) exit 0 ;;
    fetch)
        test -d "$(dirname "$checkout")"
        if [[ "${CHECK_PREPARED_ABSENT:-}" == true ]]; then
            test ! -e "$PREPARED_TARGET"
        fi
        if [[ ! -e "$MOCK_FETCH_FAIL_ONCE" ]]; then
            touch "$MOCK_FETCH_FAIL_ONCE"
            echo 'simulated fetch failure' >&2
            exit 31
        fi
        exit 0
        ;;
    checkout) exit 0 ;;
    rev-parse)
        if [[ "${2:-}" == --show-toplevel ]]; then printf '%s\n' "$checkout"; exit 0; fi
        cat "$checkout/.mock-head"
        ;;
    status) exit 0 ;;
    *) exit 92 ;;
esac
EOF
chmod +x "$prep_bin/git"
export PREP_GIT_LOG="$temporary_root/prep-git-calls.log"
export MOCK_GIT_HEAD="$prep_revision"
export MOCK_FETCH_FAIL_ONCE="$temporary_root/fetch-failed-once"
export NORMAL_PROJECT="$normal_project"
export PREPARED_TARGET="$temporary_root/prepared"
export CHECK_PREPARED_ABSENT=true
RS_INFRA_PROJECT_ROOT="$normal_project" RS_INFRA_SHARED_ROOT="$shared_root" \
    RS_INFRA_NETWORK_MAX_ATTEMPTS=2 RS_INFRA_NETWORK_RETRY_DELAY_SECONDS=0 \
    PATH="$prep_bin:$PATH" "$shared_root/.infra/lib/prepare-local-path-dependencies.sh" \
    > "$temporary_root/prepare.log" 2>&1
grep -F 'simulated fetch failure' "$temporary_root/prepare.log" >/dev/null
export CHECK_PREPARED_ABSENT=false
test -d "$PREPARED_TARGET/.git"
test "$(cat "$PREPARED_TARGET/.mock-head")" = "$prep_revision"
test -e "$MOCK_FETCH_FAIL_ONCE"
test -z "$(find "$temporary_root" -maxdepth 1 -name '.prepared.prepare.*' -print -quit)"
test ! -e "$temporary_root/.prepared.prepare-publish.lock"

printf '../occupied\tfixture://occupied\t%s\n' "$prep_revision" \
    > "$normal_project/.infra/ci/local-path-dependencies.tsv"
mkdir "$temporary_root/occupied"
printf 'preserve me\n' > "$temporary_root/occupied/marker"
if RS_INFRA_PROJECT_ROOT="$normal_project" RS_INFRA_SHARED_ROOT="$shared_root" \
    PATH="$prep_bin:$PATH" "$shared_root/.infra/lib/prepare-local-path-dependencies.sh" \
    > "$temporary_root/occupied.log" 2>&1; then
    echo 'expected ordinary preparation to preserve and reject an occupied path' >&2
    exit 1
fi
grep -F "refusing to replace existing dependency path '../occupied'" \
    "$temporary_root/occupied.log" >/dev/null
test "$(cat "$temporary_root/occupied/marker")" = 'preserve me'

printf '../concurrent\tfixture://concurrent\t%s\n' "$prep_revision" \
    > "$normal_project/.infra/ci/local-path-dependencies.tsv"
mkdir "$temporary_root/.concurrent.prepare-publish.lock"
if RS_INFRA_PROJECT_ROOT="$normal_project" RS_INFRA_SHARED_ROOT="$shared_root" \
    PATH="$prep_bin:$PATH" "$shared_root/.infra/lib/prepare-local-path-dependencies.sh" \
    > "$temporary_root/concurrent.log" 2>&1; then
    echo 'expected ordinary preparation to reject an active publisher lock' >&2
    exit 1
fi
grep -F "another dependency preparation is publishing '../concurrent'" \
    "$temporary_root/concurrent.log" >/dev/null
test -d "$temporary_root/.concurrent.prepare-publish.lock"
test -z "$(find "$temporary_root" -maxdepth 1 -name '.concurrent.prepare.*' -print -quit)"
echo 'strict dependency checks preserved the worktree, index, cache, and build sentinels'
