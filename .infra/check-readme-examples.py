#!/usr/bin/env python3
"""Compile the dependency and Quick Start examples from both Magika READMEs."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path


READMES = {
    "README.md": ("Installation", "Quick Start"),
    "README.zh_CN.md": ("安装", "快速开始"),
}
REQUIRED_DEPENDENCIES = ("qubit-magika", "qubit-mime", "qubit-spi", "qubit-operation")


def section(markdown: str, title: str, filename: str) -> str:
    headings = list(re.finditer(r"(?m)^##\s+(.+?)\s*$", markdown))
    matches = [index for index, match in enumerate(headings) if match.group(1) == title]
    if len(matches) != 1:
        raise ValueError(f"{filename}: expected exactly one '## {title}' section")
    index = matches[0]
    start = headings[index].end()
    end = headings[index + 1].start() if index + 1 < len(headings) else len(markdown)
    return markdown[start:end]


def extract_readme(markdown: str, filename: str) -> tuple[dict[str, object], str]:
    try:
        installation_title, quick_start_title = READMES[filename]
    except KeyError as error:
        raise ValueError(f"unsupported README fixture: {filename}") from error

    installation = section(markdown, installation_title, filename)
    toml_blocks = re.findall(r"```toml\s*\n(.*?)\n```", installation, re.DOTALL)
    if len(toml_blocks) != 1:
        raise ValueError(f"{filename}: expected exactly one TOML block in {installation_title}")
    try:
        parsed = tomllib.loads(toml_blocks[0])
    except tomllib.TOMLDecodeError as error:
        raise ValueError(f"{filename}: invalid installation TOML: {error}") from error
    dependencies = parsed.get("dependencies")
    if not isinstance(dependencies, dict):
        raise ValueError(f"{filename}: installation TOML must declare [dependencies]")

    quick_start = section(markdown, quick_start_title, filename)
    rust_blocks = re.findall(r"```rust(?:,[^\n]*)?\s*\n(.*?)\n```", quick_start, re.DOTALL)
    if len(rust_blocks) != 1 or not rust_blocks[0].strip():
        raise ValueError(f"{filename}: expected exactly one non-empty Rust block in {quick_start_title}")
    return dependencies, rust_blocks[0]


def dependency_version(entry: object) -> str | None:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        version = entry.get("version")
        return version if isinstance(version, str) else None
    return None


def expected_versions(project_root: Path) -> dict[str, str]:
    manifest = tomllib.loads((project_root / "Cargo.toml").read_text(encoding="utf-8"))
    package = manifest["package"]
    dependencies = manifest.get("dependencies", {})
    expected = {
        "qubit-magika": ".".join(package["version"].split("+", 1)[0].split(".")[:2])
    }
    for name in REQUIRED_DEPENDENCIES[1:]:
        declaration = dependencies.get(name)
        if declaration is None:
            raise ValueError(f"Cargo.toml has no direct dependency declaration for {name}")
        version = dependency_version(declaration)
        if version is None:
            raise ValueError(f"Cargo.toml has no version for {name}")
        expected[name] = version.removeprefix("=").removeprefix("^")
    return expected


def validate_dependencies(
    dependencies: dict[str, object], expected: dict[str, str], filename: str
) -> None:
    for name in REQUIRED_DEPENDENCIES:
        declaration = dependencies.get(name)
        if declaration is None:
            raise ValueError(f"{filename}: installation dependencies are missing {name}")
        actual = dependency_version(declaration)
        if actual is None:
            raise ValueError(f"{filename}: {name} has no version requirement")
        normalized = actual.removeprefix("=").removeprefix("^")
        if normalized != expected[name]:
            raise ValueError(
                f"{filename}: {name} version requirement {actual!r}; "
                f"Cargo.toml requires {expected[name]!r}"
            )


def toml_value(value: object) -> str:
    if isinstance(value, dict):
        return "{ " + ", ".join(
            f"{key} = {toml_value(item)}" for key, item in value.items()
        ) + " }"
    return json.dumps(value, ensure_ascii=False)


def consumer_manifest(
    dependencies: dict[str, object], project_root: Path, filename: str | None
) -> str:
    entries: list[str] = []
    local_paths = {
        "qubit-magika": project_root,
        "qubit-mime": project_root.parent / "rs-mime",
        "qubit-operation": project_root.parent / "rs-operation",
    }
    for package, original in dependencies.items():
        value = original
        if filename is not None and package in local_paths:
            if isinstance(value, dict):
                value = dict(value)
                if "path" in value:
                    raise ValueError(f"{filename}: {package} must not use a README path override")
            else:
                value = {"version": value}
            value["path"] = str(local_paths[package])
        entries.append(f"{package} = {toml_value(value)}")
    return (
        "[package]\n"
        f'name = "magika-readme-example-{"zh" if filename and filename.endswith("zh_CN.md") else "en"}"\n'
        'version = "0.0.0"\n'
        'edition = "2024"\n\n'
        "[workspace]\n\n"
        "[dependencies]\n"
        + "\n".join(entries)
        + "\n"
    )


def validate_registry_manifest(manifest_text: str) -> None:
    manifest = tomllib.loads(manifest_text)
    for package, entry in manifest.get("dependencies", {}).items():
        if isinstance(entry, dict) and "path" in entry:
            raise ValueError(f"registry consumer dependency {package} must not use a path")
    if manifest.get("patch"):
        raise ValueError("registry consumer must not declare a patch")
    workspace = manifest.get("workspace", {})
    if workspace.get("members"):
        raise ValueError("registry consumer must not declare workspace members")


def run_self_tests() -> None:
    expected = {"qubit-magika": "0.18", "qubit-mime": "0.22", "qubit-spi": "0.14", "qubit-operation": "0.3.2"}
    fixtures = {
        "README.md": """## Installation\n```toml\n[dependencies]\nqubit-magika = \"0.18\"\nqubit-mime = \"0.22\"\nqubit-spi = \"0.14\"\nqubit-operation = \"0.3.2\"\n```\n## Quick Start\n```rust\nfn main() {}\n```\n""",
        "README.zh_CN.md": """## 安装\n```toml\n[dependencies]\nqubit-magika = \"0.18\"\nqubit-mime = \"0.22\"\nqubit-spi = \"0.14\"\nqubit-operation = \"0.3.2\"\n```\n## 快速开始\n```rust\nfn main() {}\n```\n""",
    }
    for filename, fixture in fixtures.items():
        _, quick_start_title = READMES[filename]
        dependencies, example = extract_readme(fixture, filename)
        validate_dependencies(dependencies, expected, filename)
        assert example == "fn main() {}"
        manifest = consumer_manifest(dependencies, Path("/project"), filename)
        assert 'version = "0.22"' in manifest
        assert "[workspace]" in manifest
        relative_manifest = consumer_manifest(
            dependencies, Path("layout/rs-magika"), filename
        )
        assert (
            'qubit-operation = { version = "0.3.2", path = "layout/rs-operation" }'
            in relative_manifest
        )
        registry_manifest = consumer_manifest(dependencies, Path("/project"), None)
        validate_registry_manifest(registry_manifest)
        assert "path =" not in registry_manifest
        assert "[patch." not in registry_manifest
        for invalid_registry_manifest in (
            registry_manifest.replace('qubit-magika = "0.18"', 'qubit-magika = { version = "0.18", path = "../rs-magika" }'),
            registry_manifest + "\n[patch.crates-io]\nqubit-magika = { path = \"../rs-magika\" }\n",
            registry_manifest.replace("[workspace]", '[workspace]\nmembers = ["../member"]'),
        ):
            try:
                validate_registry_manifest(invalid_registry_manifest)
            except ValueError:
                pass
            else:
                raise AssertionError("accepted registry consumer with a local override or member")
        installation_end = fixture.index(f"## {quick_start_title}")
        duplicate_toml = (
            fixture[:installation_end]
            + '```toml\n[dependencies]\nqubit-magika = "0.18"\nqubit-mime = "0.22"\nqubit-spi = "0.14"\n```\n'
            + fixture[installation_end:]
        )
        quick_start_end = fixture.index("```rust") + len("```rust")
        rust_end = fixture.index("```", quick_start_end)
        duplicate_rust = fixture[:rust_end + 3] + "\n```rust\nfn main() {}\n```" + fixture[rust_end + 3:]
        for invalid, message in (
            (fixture.replace("[dependencies]", "[other]", 1), "[dependencies]"),
            (fixture.replace("```toml", "```text", 1), "TOML block"),
            (duplicate_toml, "exactly one TOML"),
            (fixture + f"\n## {quick_start_title}\n```rust\nfn main() {{}}\n```", "exactly one"),
            (fixture.replace("```rust", "```text", 1), "Rust block"),
            (duplicate_rust, "exactly one non-empty Rust"),
        ):
            try:
                extract_readme(invalid, filename)
            except ValueError as error:
                assert message in str(error), str(error)
            else:
                raise AssertionError(f"{filename}: accepted invalid extraction fixture")

        old_mime = dict(dependencies, **{"qubit-mime": "0.21"})
        try:
            validate_dependencies(old_mime, expected, filename)
        except ValueError as error:
            assert "qubit-mime" in str(error)
        else:
            raise AssertionError(f"{filename}: accepted old qubit-mime version")
        old_manifest = consumer_manifest(old_mime, Path("/project"), filename)
        assert 'qubit-mime = { version = "0.21", path = "/rs-mime" }' in old_manifest
        old_operation = dict(dependencies, **{"qubit-operation": "0.3"})
        try:
            validate_dependencies(old_operation, expected, filename)
        except ValueError as error:
            assert "qubit-operation" in str(error)
        else:
            raise AssertionError(f"{filename}: accepted qubit-operation below manifest minimum")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="run extraction and version checks")
    parser.add_argument(
        "--registry-only",
        action="store_true",
        help="check README examples using a clean registry consumer with no local path overrides",
    )
    args = parser.parse_args()
    if args.self_test:
        run_self_tests()
        print("README extraction and dependency self-tests passed.")
        return 0

    project_root = Path(__file__).resolve().parent.parent
    target_dir = project_root / "target" / "readme-example-check"
    target_dir.mkdir(parents=True, exist_ok=True)
    cargo_env = os.environ.copy()
    cargo_env["CARGO_TARGET_DIR"] = str(target_dir)
    try:
        expected = expected_versions(project_root)
        with tempfile.TemporaryDirectory(prefix="magika-readme-check-") as temporary:
            if args.registry_only:
                cargo_env["CARGO_HOME"] = str(Path(temporary) / "cargo-home")
            for filename in READMES:
                readme_path = project_root / filename
                dependencies, example = extract_readme(readme_path.read_text(encoding="utf-8"), filename)
                validate_dependencies(dependencies, expected, filename)
                consumer_root = Path(temporary) / filename.replace(".md", "")
                consumer_root.mkdir(parents=True, exist_ok=True)
                (consumer_root / "src").mkdir()
                generated_manifest = consumer_manifest(
                    dependencies,
                    project_root,
                    filename if not args.registry_only else None,
                )
                if args.registry_only:
                    validate_registry_manifest(generated_manifest)
                    if (consumer_root / "Cargo.lock").exists():
                        raise ValueError("registry consumer unexpectedly has a borrowed Cargo.lock")
                (consumer_root / "Cargo.toml").write_text(
                    generated_manifest, encoding="utf-8"
                )
                (consumer_root / "src/main.rs").write_text(example + "\n", encoding="utf-8")
                manifest_path = consumer_root / "Cargo.toml"
                mode = "registry-only" if args.registry_only else "fixed-source"
                print(f"Checking {filename} ({mode}) with the README dependency requirements...", flush=True)
                if args.registry_only:
                    metadata = subprocess.run(
                        ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(manifest_path)],
                        env=cargo_env,
                        check=False,
                    )
                    if metadata.returncode:
                        print(f"{filename}: registry-only cargo metadata exited with {metadata.returncode}", file=sys.stderr)
                        return metadata.returncode if metadata.returncode > 0 else 1
                command = ["cargo", "check", "--manifest-path", str(manifest_path)]
                result = subprocess.run(command, env=cargo_env, check=False)
                if result.returncode:
                    print(f"{filename}: cargo check exited with {result.returncode}", file=sys.stderr)
                    return result.returncode if result.returncode > 0 else 1
        print("Both README consumer examples compiled successfully.")
    except (OSError, KeyError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"README example check failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
