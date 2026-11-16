#!/usr/bin/env python3
"""Check that the dynamic loader fixture uses only the dynamic ORT features."""

from __future__ import annotations

import argparse
import re
import sys


FORBIDDEN = (
    "download-binaries",
    "copy-dylibs",
    "download-binaries-check",
    "tls-rustls",
    "tls-rustls-no-provider",
)


def check(tree: str) -> list[str]:
    errors: list[str] = []
    feature_lines = [line for line in tree.splitlines() if re.search(r"\bort feature \"", line)]
    if not any(re.search(r"\bort v", line) for line in tree.splitlines()):
        return ["cargo tree output contains no ort package/features"]
    if not any('feature "load-dynamic"' in line for line in feature_lines):
        errors.append("ort load-dynamic feature is not enabled")
    for forbidden in FORBIDDEN:
        if any(f'feature "{forbidden}"' in line for line in feature_lines):
            errors.append(f"forbidden ort feature is enabled: {forbidden}")
    return errors


def self_test() -> None:
    valid = "├── ort v2.0.0-rc.12\n│   ├── ort feature \"load-dynamic\"\n│   └── ort feature \"std\""
    assert not check(valid)
    assert "load-dynamic" in " ".join(check("├── ort v2.0.0-rc.12\n│   └── ort feature \"std\""))
    assert "download-binaries" in " ".join(check(valid + "\n│   └── ort feature \"download-binaries\""))
    assert "tls-rustls" in " ".join(check(valid + "\n│   └── ort feature \"tls-rustls\""))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        print("dynamic loader feature checker self-tests passed")
        return 0
    errors = check(sys.stdin.read())
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("dynamic loader feature selection is valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
