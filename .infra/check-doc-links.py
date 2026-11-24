#!/usr/bin/env python3
"""Check repository-relative Markdown links in public project documents."""

from __future__ import annotations

import argparse
from collections import Counter
import re
import sys
import tempfile
from urllib.parse import unquote, urlsplit
from pathlib import Path


DOCUMENTS = (
    "README.md",
    "README.zh_CN.md",
    "doc/user_guide.md",
    "doc/user_guide.zh_CN.md",
    "doc/design-0.19.0.zh_CN.md",
)
SOURCE_FIXTURE_LINKS = (
    ("doc/user_guide.md", "tests/fixtures/readme_consumer/Cargo.toml"),
    ("doc/user_guide.md", "tests/fixtures/dynamic_loader/Cargo.toml"),
    ("doc/user_guide.md", "tests/fixtures/dynamic_loader/Cargo.toml"),
    ("doc/user_guide.zh_CN.md", "tests/fixtures/readme_consumer/Cargo.toml"),
    ("doc/user_guide.zh_CN.md", "tests/fixtures/dynamic_loader/Cargo.toml"),
    ("doc/user_guide.zh_CN.md", "tests/fixtures/dynamic_loader/Cargo.toml"),
)
LINK_START = re.compile(r"!?(?:\[[^\]]*\])\(")
REFERENCE = re.compile(r"^\s*\[[^\]]+\]:\s*(\S+)", re.MULTILINE)


def markdown_without_fences(markdown: str) -> str:
    lines: list[str] = []
    fence_char: str | None = None
    fence_length = 0
    for line in markdown.splitlines():
        marker = re.match(r"^ {0,3}(`{3,}|~{3,})", line)
        if marker and fence_char is None:
            fence_char = marker.group(1)[0]
            fence_length = len(marker.group(1))
            lines.append("")
        elif (
            marker
            and marker.group(1)[0] == fence_char
            and len(marker.group(1)) >= fence_length
            and not line[marker.end() :].strip()
        ):
            fence_char = None
            fence_length = 0
            lines.append("")
        elif fence_char is None:
            lines.append(line)
        else:
            lines.append("")
    return "\n".join(lines)


def inline_targets(content: str) -> list[str]:
    targets: list[str] = []
    for match in LINK_START.finditer(content):
        depth = 1
        cursor = match.end()
        destination: list[str] = []
        while cursor < len(content) and depth:
            char = content[cursor]
            if char == "\\" and cursor + 1 < len(content):
                destination.extend((char, content[cursor + 1]))
                cursor += 2
                continue
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    break
            destination.append(char)
            cursor += 1
        if depth:
            continue
        raw = "".join(destination).strip()
        if raw.startswith("<") and ">" in raw:
            raw = raw[1 : raw.index(">")]
        else:
            raw = re.split(r"\s+[\"']", raw, maxsplit=1)[0]
        if raw:
            targets.append(raw)
    return targets


def local_targets(markdown: str) -> list[str]:
    content = markdown_without_fences(markdown)
    targets = inline_targets(content)
    targets.extend(match.group(1).strip().strip("<>") for match in REFERENCE.finditer(content))
    return [target.split("#", 1)[0] for target in targets if target and not target.startswith(("http://", "https://", "mailto:", "#", "data:"))]


def package_errors(root: Path, package_paths: set[str]) -> list[str]:
    """Check relative links in packaged Markdown against Cargo's package file list."""
    errors: list[str] = []
    for relative in sorted(package_paths):
        if not relative.lower().endswith(".md"):
            continue
        document = root / relative
        if not document.is_file():
            errors.append(f"{relative}: packaged Markdown file is missing from source tree")
            continue
        content = markdown_without_fences(document.read_text(encoding="utf-8"))
        targets = inline_targets(content)
        targets.extend(match.group(1).strip().strip("<>") for match in REFERENCE.finditer(content))
        for target in targets:
            parsed = urlsplit(target)
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            path = unquote(parsed.path)
            if path.startswith("/"):
                continue
            resolved = (document.parent / path).resolve()
            try:
                resolved_relative = resolved.relative_to(root.resolve()).as_posix()
            except ValueError:
                resolved_relative = ""
            if not resolved_relative or resolved_relative not in package_paths:
                errors.append(f"{relative}: package is missing link target: {target}")
    return errors


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
                errors.append(f"{relative}: target outside repository: {target}")
                continue
            if not resolved.exists():
                errors.append(f"{relative}: missing local target: {target}")
    return errors


def self_test() -> None:
    sample = (
        "[good](guide.md#part) [nested](sub(dir)/guide.md) [web](https://example.com)\n"
        "```md\n[backtick-ignored](also-missing.md)\n```\n"
        "~~~md\n[tilde-ignored](missing.md)\n~~~"
        "\n```\n```md\n[still-ignored](after-fake-close.md)\n```\n[good-again](after.md)"
    )
    assert local_targets(sample) == ["guide.md", "sub(dir)/guide.md", "after.md"]
    with_missing = "[bad](missing.md)"
    assert local_targets(with_missing) == ["missing.md"]

    with tempfile.TemporaryDirectory() as temporary_directory:
        root = Path(temporary_directory) / "repository"
        root.mkdir()
        for relative in DOCUMENTS:
            document = root / relative
            document.parent.mkdir(parents=True, exist_ok=True)
            document.write_text("", encoding="utf-8")
        (root / "README.md").write_text("repository root", encoding="utf-8")
        design = root / "doc/design-0.19.0.zh_CN.md"
        design.write_text(
            "[outside](../../sibling/existing.md) "
            "[back](../README.md) [web](https://example.com)",
            encoding="utf-8",
        )
        sibling_target = root.parent / "sibling/existing.md"
        sibling_target.parent.mkdir(parents=True)
        sibling_target.write_text("exists", encoding="utf-8")
        assert find_errors(root) == [
            "doc/design-0.19.0.zh_CN.md: target outside repository: ../../sibling/existing.md"
        ]

        legacy_links = {
            "doc/user_guide.md": [
                "../tests/fixtures/readme_consumer/Cargo.toml",
                "../tests/fixtures/dynamic_loader/Cargo.toml",
                "../tests/fixtures/dynamic_loader/Cargo.toml",
            ],
            "doc/user_guide.zh_CN.md": [
                "../tests/fixtures/readme_consumer/Cargo.toml",
                "../tests/fixtures/dynamic_loader/Cargo.toml",
                "../tests/fixtures/dynamic_loader/Cargo.toml",
            ],
        }
        packaged_markdown = set(legacy_links)
        for document, targets in legacy_links.items():
            path = root / document
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(" ".join(f"[fixture]({target})" for target in targets), encoding="utf-8")
            for target in targets:
                fixture = (path.parent / target).resolve()
                fixture.relative_to(root.resolve())
                fixture.parent.mkdir(parents=True, exist_ok=True)
                fixture.write_text("[package]", encoding="utf-8")
        expected = [
            f"{document}: package is missing link target: {target}"
            for document, targets in legacy_links.items()
            for target in targets
        ]
        assert package_errors(root, packaged_markdown) == expected

    repository_root = Path(__file__).resolve().parent.parent
    for (document, target), expected_count in Counter(SOURCE_FIXTURE_LINKS).items():
        markdown = (repository_root / document).read_text(encoding="utf-8")
        source_url = f"https://github.com/qubit-ltd/rs-magika/blob/main/{target}"
        actual_count = markdown.count(f"]({source_url})")
        assert actual_count == expected_count, (
            f"{document}: expected {expected_count} links to {target}, found {actual_count}"
        )
        assert (repository_root / target).is_file(), f"source fixture target is missing: {target}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--package-list", type=Path, help="file containing cargo package --list output")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        print("documentation link checker self-tests passed")
        return 0
    root = Path(__file__).resolve().parent.parent
    if args.package_list:
        package_paths = {
            line.strip().removeprefix("./")
            for line in args.package_list.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
        errors = package_errors(root, package_paths)
    else:
        errors = find_errors(root)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    if args.package_list:
        print("packaged Markdown relative links are valid")
    else:
        print("repository-local Markdown links are valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
