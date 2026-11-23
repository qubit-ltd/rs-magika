#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
config="$project_root/.infra/ci/local-path-dependencies.tsv"
[ "$#" -le 1 ] || { echo "usage: $0 [--check-locked]" >&2; exit 2; }
mode=${1:-prepare}
[[ "$mode" == prepare || "$mode" == --check-locked ]] || {
    echo "usage: $0 [--check-locked]" >&2; exit 2;
}
if [[ ! -f "$config" ]]; then
    [[ "$mode" != --check-locked ]] && exit 0
    echo "error: locked dependency list is missing: $config" >&2
    exit 1
fi

while IFS=$'\t' read -r relative_path repository_url revision; do
    [[ -z "$relative_path" || "$relative_path" == \#* ]] && continue
    revision=${revision%$'\r'}
    [[ "$relative_path" == ../* && "$relative_path" != *$'\t'* ]] || {
        echo "error: invalid local dependency path '$relative_path'" >&2; exit 1;
    }
    [[ -n "$repository_url" && -n "$revision" ]] || {
        echo "error: incomplete local dependency entry '$relative_path'" >&2; exit 1;
    }
    target="$project_root/$relative_path"
    if [[ "$mode" == --check-locked ]]; then
        if [[ ! -e "$target/.git" ]]; then
            echo "error: locked dependency '$relative_path' is missing (expected $revision)" >&2
            exit 1
        fi
        actual_revision=$(git -C "$target" rev-parse HEAD 2>&1) || {
            echo "error: cannot read HEAD for '$relative_path': $actual_revision" >&2
            exit 1
        }
        if [[ "$actual_revision" != "$revision" ]]; then
            echo "error: locked dependency '$relative_path' HEAD mismatch: actual $actual_revision; expected $revision" >&2
            exit 1
        fi
        if actual_status=$(git -C "$target" status --porcelain 2>&1); then
            :
        else
            status_exit=$?
            echo "error: cannot read working-tree status for '$relative_path' (exit $status_exit): $actual_status" >&2
            exit 1
        fi
        if [[ -n "$actual_status" ]]; then
            echo "error: locked dependency '$relative_path' is dirty at $actual_revision; expected clean checkout at $revision" >&2
            printf 'actual status:\n%s\n' "$actual_status" >&2
            exit 1
        fi
        continue
    fi
    [ -e "$target/.git" ] && continue
    mkdir -p "$(dirname "$target")"
    if [[ "$revision" =~ ^[0-9a-fA-F]{40}$ ]]; then
        git init "$target"
        git -C "$target" remote add origin "$repository_url"
        git -C "$target" fetch --depth 1 origin "$revision"
        git -C "$target" checkout --detach FETCH_HEAD
    else
        git clone --depth 1 --branch "$revision" "$repository_url" "$target"
    fi
done < "$config"
