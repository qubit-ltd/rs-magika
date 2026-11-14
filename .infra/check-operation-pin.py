#!/usr/bin/env python3
"""Check that CI and local dependency setup use the same immutable operation revision."""

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parent.parent
EXPECTED_PATH = "../rs-operation"
EXPECTED_URL = "https://github.com/qubit-ltd/rs-operation.git"
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


def main() -> int:
    rows = []
    for line in (ROOT / ".infra/ci/local-path-dependencies.tsv").read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        columns = line.split("\t")
        if columns[0] == EXPECTED_PATH:
            rows.append(columns)
    if len(rows) != 1 or len(rows[0]) != 3 or rows[0][1] != EXPECTED_URL:
        print("error: expected exactly one rs-operation local dependency entry", file=sys.stderr)
        return 1

    local_revision = rows[0][2]
    if not SHA_PATTERN.fullmatch(local_revision):
        print("error: rs-operation local dependency revision must be a full 40-character SHA", file=sys.stderr)
        return 1

    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    match = re.search(
        r"- uses: actions/checkout@[^\n]+\n        with:\n          repository: qubit-ltd/rs-operation\n          ref: ([^\n]+)\n          path: rs-operation",
        workflow,
    )
    if match is None:
        print("error: unable to locate rs-operation checkout ref in CI workflow", file=sys.stderr)
        return 1
    workflow_revision = match.group(1).strip()
    if not SHA_PATTERN.fullmatch(workflow_revision):
        print("error: rs-operation CI ref must be a full 40-character SHA", file=sys.stderr)
        return 1
    if local_revision != workflow_revision:
        print("error: rs-operation revisions differ between CI and local dependency config", file=sys.stderr)
        return 1
    print(f"rs-operation revision is consistent: {local_revision}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
