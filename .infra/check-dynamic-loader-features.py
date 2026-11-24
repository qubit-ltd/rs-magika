#!/usr/bin/env python3
"""Check that a Cargo metadata graph uses dynamic ORT linking safely."""

from __future__ import annotations

import argparse
import sys

from _dynamic_loader_feature_policy import check_dynamic_metadata, parse_metadata, run_self_tests


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        run_self_tests()
        print("dynamic loader feature checker self-tests passed")
        return 0
    try:
        errors = check_dynamic_metadata(parse_metadata(sys.stdin.read()))
    except ValueError as error:
        errors = [str(error)]
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("dynamic loader feature selection is valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
