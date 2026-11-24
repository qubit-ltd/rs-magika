#!/usr/bin/env python3
"""Compile managed examples using only each guide's declared dependencies."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path


START = "<!-- managed-guide-example-start -->"
END = "<!-- managed-guide-example-end -->"
PROVIDER_START = "<!-- provider-runtime-example-start -->"
PROVIDER_END = "<!-- provider-runtime-example-end -->"
DEPENDENCIES_START = "<!-- managed-dependencies-start -->"
DEPENDENCIES_END = "<!-- managed-dependencies-end -->"
GUIDES = ("doc/user_guide.md", "doc/user_guide.zh_CN.md")
VERSIONED_DEPENDENCIES = ("qubit-mime", "qubit-operation", "qubit-fs")
REQUIRED_IMPORTS = (
    "use qubit_magika::MagikaMimeDetector;",
    "use qubit_operation::{GroupOptions, ManagerOptions, OperationManager, OperationOptions, OperationOutcome, ShutdownMode};",
)


def extract_example(
    markdown: str,
    guide_name: str,
    start_marker: str = START,
    end_marker: str = END,
    required_imports: tuple[str, ...] = REQUIRED_IMPORTS,
) -> str:
    if markdown.count(start_marker) != 1 or markdown.count(end_marker) != 1:
        raise ValueError(f"{guide_name}: expected exactly one start and end marker")
    start = markdown.index(start_marker) + len(start_marker)
    end = markdown.index(end_marker)
    if end <= start:
        raise ValueError(f"{guide_name}: end marker must follow start marker")
    fences = re.findall(r"```rust(?:,[^\n]*)?\n(.*?)\n```", markdown[start:end], re.DOTALL)
    if len(fences) != 1 or not fences[0].strip():
        raise ValueError(f"{guide_name}: expected exactly one non-empty Rust code block")
    example = fences[0]
    if any(required not in example for required in required_imports):
        raise ValueError(f"{guide_name}: managed example is missing required imports")
    return example


def extract_provider_example(markdown: str, guide_name: str) -> str:
    required = (
        "use qubit_magika::{MagikaMimeDetectorProvider, ManagedInferenceRuntime};",
        "MagikaMimeDetectorProvider::new().managed_runtime(Arc::clone(&runtime))",
    )
    return extract_example(markdown, guide_name, PROVIDER_START, PROVIDER_END, required)


def extract_dependencies(markdown: str, guide_name: str) -> tuple[str, dict[str, object]]:
    if markdown.count(DEPENDENCIES_START) != 1 or markdown.count(DEPENDENCIES_END) != 1:
        raise ValueError(f"{guide_name}: expected exactly one managed dependency block")
    start = markdown.index(DEPENDENCIES_START) + len(DEPENDENCIES_START)
    end = markdown.index(DEPENDENCIES_END)
    if end <= start:
        raise ValueError(f"{guide_name}: dependency end marker must follow start marker")
    marked = markdown[start:end]
    fences = re.findall(r"```toml\s*\n(.*?)\n```", marked, re.DOTALL)
    if len(fences) != 1:
        raise ValueError(f"{guide_name}: expected exactly one TOML dependency block")
    try:
        parsed = tomllib.loads(fences[0])
    except tomllib.TOMLDecodeError as error:
        raise ValueError(f"{guide_name}: invalid managed dependency TOML: {error}") from error
    dependencies = parsed.get("dependencies")
    if not isinstance(dependencies, dict):
        raise ValueError(f"{guide_name}: dependency block must declare [dependencies]")
    return fences[0], dependencies


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
    expected = {
        "qubit-magika": ".".join(package["version"].split("+", 1)[0].split(".")[:2])
    }
    dependencies = manifest.get("dependencies", {})
    for name in VERSIONED_DEPENDENCIES:
        version = dependency_version(dependencies.get(name))
        if version is None:
            raise ValueError(f"Cargo.toml has no versioned dependency {name}")
        expected[name] = version.removeprefix("=").removeprefix("^")
    return expected


def validate_dependencies(
    guide_name: str,
    dependencies: dict[str, object],
    examples: list[str],
    expected_versions: dict[str, str],
) -> None:
    used_crates: set[str] = set()
    for example in examples:
        used_crates.update(re.findall(r"\b(qubit_[a-z0-9_]+)::", example))
    crate_to_package = {name.replace("-", "_"): name for name in expected_versions}
    required = {crate_to_package[crate] for crate in used_crates if crate in crate_to_package}
    for package in sorted(required):
        if package not in dependencies:
            raise ValueError(f"{guide_name}: managed dependency block is missing {package}")
    for package, expected in expected_versions.items():
        if package not in dependencies:
            raise ValueError(f"{guide_name}: managed dependency block is missing {package}")
        actual = dependency_version(dependencies[package])
        if actual is None:
            raise ValueError(f"{guide_name}: {package} has no version in managed dependency block")
        if actual.removeprefix("^") != expected:
            raise ValueError(
                f"{guide_name}: {package} version {actual!r}; expected {expected!r}"
            )
    fs = dependencies["qubit-fs"]
    features = fs.get("features", []) if isinstance(fs, dict) else []
    if "AsyncFileSystem" in "\n".join(examples) and "async" not in features:
        raise ValueError(f"{guide_name}: qubit-fs is missing the async feature used by the examples")


def validate_current_settlement_names(guide_name: str, markdown: str) -> None:
    if re.search(r"\breleased\s*\(", markdown):
        raise ValueError(f"{guide_name}: use wait_settlement().await instead of released()")


def run_self_tests() -> None:
    guide = "English fixture"
    examples = ["use qubit_magika::MagikaMimeDetector;", "use qubit_operation::OperationManager;"]
    dependencies = {
        "qubit-magika": "0.18",
        "qubit-mime": "0.22",
        "qubit-operation": "0.3.2",
        "qubit-fs": {"version": "0.3", "default-features": False, "features": ["async"]},
    }
    block = """[dependencies]
qubit-magika = "0.18"
qubit-mime = "0.22"
qubit-operation = "0.3.2"
qubit-fs = { version = "0.4", default-features = false, features = ["async"] }
"""
    parsed = tomllib.loads(block)["dependencies"]
    expected = {"qubit-magika": "0.18", "qubit-mime": "0.22", "qubit-operation": "0.3.2", "qubit-fs": "0.4"}
    validate_dependencies(guide, parsed, examples, expected)
    registry_consumer = consumer_manifest(dependencies, Path("/project"), None)
    assert 'qubit-operation = "0.3.2"' in registry_consumer
    assert 'qubit-operation = {' not in registry_consumer
    assert "path =" not in registry_consumer
    assert "[patch." not in registry_consumer
    assert "[workspace]" in registry_consumer
    sibling_consumer = consumer_manifest(
        dependencies, Path("layout/rs-magika"), Path("layout/rs-operation")
    )
    assert (
        'qubit-operation = { version = "0.3.2", path = "layout/rs-operation" }'
        in sibling_consumer
    )
    validate_current_settlement_names(guide, "wait_settlement().await and all_released()")
    try:
        validate_current_settlement_names(guide, "released()")
    except ValueError:
        pass
    else:
        raise AssertionError("accepted obsolete released() method fixture")
    for label, mutated, expected_message in (
        ("English missing operation", {k: v for k, v in dependencies.items() if k != "qubit-operation"}, "missing qubit-operation"),
        ("Chinese missing operation", {k: v for k, v in dependencies.items() if k != "qubit-operation"}, "missing qubit-operation"),
        ("wrong Magika version", {**dependencies, "qubit-magika": "9.99"}, "qubit-magika version"),
        ("operation below manifest minimum", {**dependencies, "qubit-operation": "0.3"}, "qubit-operation version"),
    ):
        try:
            validate_dependencies(label, mutated, examples, expected)
        except ValueError as error:
            if expected_message not in str(error):
                raise AssertionError(f"{label}: unexpected validation error: {error}") from error
        else:
            raise AssertionError(f"accepted invalid dependency fixture: {label}")
    sample = f"{DEPENDENCIES_START}\n```toml\n{block}```\n{DEPENDENCIES_END}"
    _, parsed = extract_dependencies(sample, "self-test fixture")
    assert parsed == tomllib.loads(block)["dependencies"]
    for invalid in (sample.replace(DEPENDENCIES_START, ""), sample + "\n" + DEPENDENCIES_START):
        try:
            extract_dependencies(invalid, "self-test fixture")
        except ValueError:
            pass
        else:
            raise AssertionError("accepted invalid dependency marker fixture")


def toml_value(value: object) -> str:
    return json.dumps(value, ensure_ascii=False)


def consumer_manifest(
    dependencies: dict[str, object], project_root: Path, operation_path: Path | None
) -> str:
    entries: list[str] = []
    for package, value in dependencies.items():
        if package == "qubit-magika" and operation_path is not None:
            version = dependency_version(value)
            assert version is not None
            mapped: dict[str, object] = {"version": version}
            mapped["path"] = str(project_root)
            if isinstance(value, dict):
                mapped.update({k: v for k, v in value.items() if k != "path"})
            value = mapped
        elif package == "qubit-mime" and operation_path is not None:
            version = dependency_version(value)
            assert version is not None
            value = {"version": version, "path": str(project_root.parent / "rs-mime")}
        elif package == "qubit-fs" and operation_path is not None:
            version = dependency_version(value)
            assert version is not None
            mapped = {"version": version, "path": str(project_root.parent / "rs-fs")}
            if isinstance(value, dict):
                mapped.update({k: v for k, v in value.items() if k != "path"})
            value = mapped
        elif package == "qubit-operation" and operation_path is not None:
            version = dependency_version(value)
            assert version is not None
            mapped = {"version": version, "path": str(operation_path)}
            if isinstance(value, dict):
                mapped.update({k: v for k, v in value.items() if k != "path"})
            value = mapped
        if isinstance(value, dict):
            encoded = ", ".join(f"{key} = {toml_value(item)}" for key, item in value.items())
            entries.append(f"{package} = {{ {encoded} }}")
        else:
            entries.append(f"{package} = {toml_value(value)}")
    return """[package]
name = "managed-guide-example-check"
version = "0.0.0"
edition = "2024"

[workspace]

[dependencies]
""" + "\n".join(entries) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true", help="run checker validation checks")
    args = parser.parse_args()
    if args.self_test:
        run_self_tests()
        print("managed guide dependency checker self-tests passed")
        return 0

    project_root = Path(__file__).resolve().parent.parent
    project_manifest = tomllib.loads((project_root / "Cargo.toml").read_text(encoding="utf-8"))
    operation_dependency = project_manifest["dependencies"]["qubit-operation"]
    operation_path = None
    if isinstance(operation_dependency, dict) and isinstance(operation_dependency.get("path"), str):
        operation_path = (project_root / operation_dependency["path"]).resolve()
    target = project_root / "target"
    target.mkdir(exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix="managed-guide-check-", dir=target))
    try:
        cargo_env = os.environ.copy()
        cargo_env["CARGO_TARGET_DIR"] = str(target)
        for guide in GUIDES:
            guide_path = project_root / guide
            markdown = guide_path.read_text(encoding="utf-8")
            validate_current_settlement_names(guide, markdown)
            examples = [
                extract_example(markdown, guide),
                extract_provider_example(markdown, guide),
            ]
            _, dependencies = extract_dependencies(markdown, guide)
            validate_dependencies(guide, dependencies, examples, expected_versions(project_root))
            consumer = temp_dir / Path(guide).stem
            (consumer / "src").mkdir(parents=True)
            modules = [
                f"#[allow(dead_code)]\nmod guide_{index} {{\n{example}\n}}"
                for index, example in enumerate(examples)
            ]
            (consumer / "src/lib.rs").write_text("\n\n".join(modules) + "\n", encoding="utf-8")
            (consumer / "Cargo.toml").write_text(
                consumer_manifest(dependencies, project_root, operation_path), encoding="utf-8"
            )
            print(f"Compiling managed examples from {guide}", flush=True)
            result = subprocess.run(
                ["cargo", "check", "--manifest-path", str(consumer / "Cargo.toml")],
                cwd=project_root,
                env=cargo_env,
                check=False,
            )
            if result.returncode:
                return result.returncode
        return 0
    except (OSError, KeyError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"managed guide check failed: {error}", file=sys.stderr)
        return 1
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
