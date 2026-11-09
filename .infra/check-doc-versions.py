#!/usr/bin/env python3
"""Check dependency versions in the bilingual Magika guides against Cargo.toml."""

from __future__ import annotations

import argparse
import re
import shutil
import sys
import tempfile
import tomllib
from pathlib import Path

GUIDES = ("doc/user_guide.md", "doc/user_guide.zh_CN.md")
DOC_DEPENDENCIES = ("qubit-magika", "qubit-mime", "qubit-fs", "qubit-spi", "ort")
STALE_REFERENCES = {
    "qubit-magika": ("0.15", "0.17"),
    "qubit-mime": ("0.18", "0.20"),
    "qubit-fs": ("0.2",),
    "qubit-spi": ("0.13",),
}


def expected_versions(root: Path) -> dict[str, str]:
    cargo = tomllib.loads((root / "Cargo.toml").read_text())
    package = cargo["package"]
    result = {"qubit-magika": ".".join(package["version"].split("+", 1)[0].split(".")[:2])}
    deps = cargo.get("dependencies", {})
    for name in DOC_DEPENDENCIES[1:]:
        entry = deps.get(name)
        if entry is None:
            entry = cargo.get("dependencies", {}).get(name)
        if entry is None and name == "ort":
            entry = cargo.get("dependencies", {}).get("ort")
        if entry is None:
            raise ValueError(f"Cargo.toml has no direct dependency declaration for {name}")
        version = entry if isinstance(entry, str) else entry["version"]
        result[name] = ("=" if version.startswith("=") else "") + version.removeprefix("=").removeprefix("^")
    return result


def toml_blocks(text: str):
    for match in re.finditer(r"```toml\s*\n(.*?)\n```", text, re.DOTALL):
        yield match.group(1)


def check(root: Path) -> list[str]:
    errors: list[str] = []
    expected = expected_versions(root)
    for relative in GUIDES:
        path = root / relative
        text = path.read_text()
        seen: set[str] = set()
        for block_number, block in enumerate(toml_blocks(text), start=1):
            try:
                data = tomllib.loads(block)
            except tomllib.TOMLDecodeError as error:
                errors.append(f"{relative}: TOML dependency block {block_number} is invalid: {error}")
                continue
            for name in DOC_DEPENDENCIES:
                entry = data.get(name)
                if entry is None:
                    entry = data.get("dependencies", {}).get(name)
                if entry is None:
                    continue
                version = entry if isinstance(entry, str) else entry.get("version")
                if version is None:
                    errors.append(f"{relative}: {name} has no version in dependency block {block_number}")
                    continue
                seen.add(name)
                if version != expected[name]:
                    errors.append(
                        f"{relative}: {name} version {version!r} in TOML block {block_number}; "
                        f"Cargo.toml requires {expected[name]!r}"
                    )
        required = set(DOC_DEPENDENCIES)
        missing = required - seen
        if missing:
            errors.append(f"{relative}: dependency blocks do not declare {', '.join(sorted(missing))}")
        for inline_version in re.findall(
            r'qubit-magika\s*=\s*\{\s*version\s*=\s*"([^"]+)"', text
        ):
            if inline_version != expected["qubit-magika"]:
                errors.append(
                    f"{relative}: inline inventory version {inline_version!r}; "
                    f"Cargo.toml requires {expected['qubit-magika']!r}"
                )
        for name, stale_versions in STALE_REFERENCES.items():
            for version in stale_versions:
                pattern = rf"{re.escape(name)}(?:\s+|[^\n`]*?\s){re.escape(version)}(?:\b|\")"
                if re.search(pattern, text):
                    errors.append(f"{relative}: stale prose version {name} {version}")
    return errors


def self_test(root: Path) -> list[str]:
    failures: list[str] = []
    with tempfile.TemporaryDirectory(prefix="magika-doc-check-") as temporary:
        fixture_root = Path(temporary)
        shutil.copy2(root / "Cargo.toml", fixture_root / "Cargo.toml")
        (fixture_root / "doc").mkdir()
        for relative in GUIDES:
            shutil.copy2(root / relative, fixture_root / relative)
        if check(fixture_root):
            return ["self-test baseline fixture unexpectedly failed"]
        expected = expected_versions(fixture_root)
        for name in ("qubit-magika", "qubit-mime", "qubit-fs", "qubit-spi", "ort"):
            path = fixture_root / "doc/user_guide.md"
            original = path.read_text()
            pattern = rf'({re.escape(name)}\s*=\s*(?:\{{\s*version\s*=\s*)?\")({re.escape(expected[name])})(\")'
            changed = original
            count = 0
            for block in toml_blocks(original):
                mutated, count = re.subn(pattern, rf"\g<1>9.99\3", block, count=1)
                if count:
                    changed = changed.replace(block, mutated, 1)
                    break
            if count != 1:
                failures.append(f"self-test could not mutate {name} independently")
                continue
            path.write_text(changed)
            if not check(fixture_root):
                failures.append(f"self-test failed to reject changed {name} version")
            path.write_text(original)
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="test the checker using temporary fixtures")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    arguments = parser.parse_args()
    try:
        errors = check(arguments.root.resolve())
        if arguments.self_test:
            errors.extend(self_test(arguments.root.resolve()))
    except (OSError, KeyError, ValueError, tomllib.TOMLDecodeError) as error:
        errors = [str(error)]
    if errors:
        print("Documentation version check failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("Documentation dependency versions match Cargo.toml; self-test passed." if arguments.self_test
          else "Documentation dependency versions match Cargo.toml.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
