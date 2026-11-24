#!/usr/bin/env python3
"""Keep legacy Cargo matrix constraints not enforced by rs-infra-ci."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def check(path: Path) -> list[str]:
    try:
        document: Any = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        return [f"{path}: cannot read matrix JSON: {error}"]

    if not isinstance(document, dict):
        return []  # rs-infra-ci reports the matrix schema error.
    checks = document.get("checks")
    if not isinstance(checks, list):
        return []  # rs-infra-ci reports the matrix schema error.

    if not checks:
        return [f"{path}: checks must contain at least one check"]

    errors: list[str] = []
    for index, entry in enumerate(checks):
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            continue  # rs-infra-ci reports missing or non-string names.
        name = entry["name"]
        if not NAME_PATTERN.fullmatch(name):
            errors.append(
                f"{path}: checks[{index}].name {name!r} is invalid; "
                "expected ^[A-Za-z0-9][A-Za-z0-9._-]*$"
            )
    return errors


def self_test() -> list[str]:
    failures: list[str] = []
    valid = {"version": 1, "checks": [{"name": "no-default-features"}]}
    with tempfile.TemporaryDirectory(prefix="magika-cargo-matrix-check-") as temporary:
        path = Path(temporary) / "cargo-matrix.json"

        path.write_text(json.dumps(valid))
        if check(path):
            failures.append("valid matrix was rejected")

        path.write_text(json.dumps({"version": 1, "checks": []}))
        if not any("checks must contain at least one check" in error for error in check(path)):
            failures.append("empty checks array was accepted")

        path.write_text(json.dumps({"version": 1, "checks": [{"name": "with+plus"}]}))
        if not any("checks[0].name" in error for error in check(path)):
            failures.append("legacy-invalid check name was accepted")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=Path(".infra/ci/cargo-matrix.json"))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    errors = self_test() if args.self_test else check(args.matrix)
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
