#!/usr/bin/env python3
"""Check repository-relative Markdown links in public project documents."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


DOCUMENTS = (
    "README.md",
    "README.zh_CN.md",
    "doc/user_guide.md",
    "doc/user_guide.zh_CN.md",
    "doc/design-0.19.0.zh_CN.md",
)
LINK = re.compile(r"!?(?:\[[^\]]*\])\(([^)]+)\)")
REFERENCE = re.compile(r"^\s*\[[^\]]+\]:\s*(\S+)", re.MULTILINE)


def markdown_without_fences(markdown: str) -> str:
    lines: list[str] = []
    in_fence = False
    for line in markdown.splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            lines.append("")
        elif not in_fence:
            lines.append(line)
        else:
            lines.append("")
    return "\n".join(lines)


def local_targets(markdown: str) -> list[str]:
    content = markdown_without_fences(markdown)
    targets = [match.group(1).strip().split()[0].strip("<>") for match in LINK.finditer(content)]
    targets.extend(match.group(1).strip().strip("<>") for match in REFERENCE.finditer(content))
    return [target.split("#", 1)[0] for target in targets if target and not target.startswith(("http://", "https://", "mailto:", "#", "data:"))]


def find_errors(root: Path) -> list[str]:
    errors: list[str] = []
    for relative in DOCUMENTS:
        document = root / relative
        if not document.is_file():
            errors.append(f"missing document: {relative}")
            continue
        for target in local_targets(document.read_text(encoding="utf-8")):
            resolved = (document.parent / target).resolve()
            try:
                resolved.relative_to(root.resolve())
            except ValueError:
                continue
            if not resolved.exists():
                errors.append(f"{relative}: missing local target: {target}")
    return errors


def self_test() -> None:
    sample = "[good](guide.md#part) [web](https://example.com)\n```md\n[ignored](missing.md)\n```"
    assert local_targets(sample) == ["guide.md"]
    with_missing = "[bad](missing.md)"
    assert local_targets(with_missing) == ["missing.md"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        print("documentation link checker self-tests passed")
        return 0
    root = Path(__file__).resolve().parent.parent
    errors = find_errors(root)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("repository-local Markdown links are valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
