#!/usr/bin/env python3
"""Validate pinned sibling checkouts without entering bootstrap or preparing them."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


def git(checkout: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["GIT_OPTIONAL_LOCKS"] = "0"
    return subprocess.run(
        ["git", "--no-optional-locks", "-C", str(checkout), *arguments],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )


def fail(message: str) -> int:
    print(f"error: {message}", file=sys.stderr)
    return 1


def main() -> int:
    project_root = Path(
        os.environ.get("RS_INFRA_PROJECT_ROOT", Path(__file__).resolve().parents[2])
    ).resolve()
    config = project_root / ".infra/ci/local-path-dependencies.tsv"
    try:
        lines = config.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        return fail(f"locked dependency list is unavailable: {config}: {error}")

    entries = 0
    for line_number, line in enumerate(lines, start=1):
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) != 3:
            return fail(f"invalid locked dependency entry at {config}:{line_number}")
        relative_path, repository_url, expected_revision = fields
        path_parts = Path(relative_path).parts
        if (
            len(path_parts) != 2
            or path_parts[0] != ".."
            or path_parts[1] in {"", ".", ".."}
            or not repository_url
            or not expected_revision
        ):
            return fail(f"invalid locked dependency entry '{relative_path}' at line {line_number}")

        target = project_root / relative_path
        if not target.exists():
            return fail(
                f"locked dependency '{relative_path}' is missing "
                f"(expected {expected_revision})"
            )
        if not target.is_dir():
            return fail(
                f"locked dependency '{relative_path}' is not a Git checkout root "
                f"(expected {expected_revision})"
            )

        root_result = git(target, "rev-parse", "--show-toplevel")
        if root_result.returncode != 0:
            detail = root_result.stderr.strip() or root_result.stdout.strip()
            return fail(
                f"locked dependency '{relative_path}' is not a Git checkout root "
                f"(expected {expected_revision}); Git exited {root_result.returncode}: {detail}"
            )
        try:
            checkout_root = Path(root_result.stdout.strip()).resolve(strict=True)
            canonical_target = target.resolve(strict=True)
        except OSError as error:
            return fail(f"cannot resolve Git checkout root for '{relative_path}': {error}")
        if checkout_root != canonical_target:
            return fail(
                f"locked dependency '{relative_path}' is not a Git checkout root "
                f"(expected {expected_revision})"
            )

        head_result = git(target, "rev-parse", "--verify", "HEAD")
        if head_result.returncode != 0:
            detail = head_result.stderr.strip() or head_result.stdout.strip()
            return fail(
                f"cannot read HEAD for '{relative_path}' (exit {head_result.returncode}): {detail}"
            )
        actual_revision = head_result.stdout.strip()
        if actual_revision != expected_revision:
            return fail(
                f"locked dependency '{relative_path}' HEAD mismatch: actual {actual_revision}; "
                f"expected {expected_revision}"
            )

        status_result = git(target, "status", "--porcelain=v1", "--untracked-files=all")
        if status_result.returncode != 0:
            detail = status_result.stderr.strip() or status_result.stdout.strip()
            return fail(
                f"cannot read working-tree status for '{relative_path}' "
                f"(exit {status_result.returncode}): {detail}"
            )
        if status_result.stdout:
            return fail(
                f"locked dependency '{relative_path}' is dirty at {actual_revision}; "
                f"expected clean checkout at {expected_revision}\n"
                f"actual status:\n{status_result.stdout.rstrip()}"
            )
        entries += 1

    if entries == 0:
        return fail(f"locked dependency list contains no entries: {config}")
    print(f"all {entries} locked local path dependencies are clean and at their pinned commits")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
