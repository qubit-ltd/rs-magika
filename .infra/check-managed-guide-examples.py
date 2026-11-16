#!/usr/bin/env python3
"""Compile the managed examples extracted directly from both user guides."""

from __future__ import annotations

import argparse
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
GUIDES = ("doc/user_guide.md", "doc/user_guide.zh_CN.md")


def extract_example(
    markdown: str,
    guide_name: str,
    start_marker: str = START,
    end_marker: str = END,
    required_imports: tuple[str, ...] = (
        "use qubit_magika::MagikaMimeDetector;",
        "use qubit_operation::{GroupOptions, ManagerOptions, OperationManager, OperationOptions, OperationOutcome, ShutdownMode};",
    ),
) -> str:
    if markdown.count(start_marker) != 1:
        raise ValueError(f"{guide_name}: expected exactly one start marker")
    if markdown.count(end_marker) != 1:
        raise ValueError(f"{guide_name}: expected exactly one end marker")
    start = markdown.index(start_marker) + len(start_marker)
    end = markdown.index(end_marker)
    if end <= start:
        raise ValueError(f"{guide_name}: end marker must follow start marker")
    marked = markdown[start:end]
    fences = re.findall(r"```rust(?:,[^\n]*)?\n(.*?)\n```", marked, re.DOTALL)
    if len(fences) != 1 or not fences[0].strip():
        raise ValueError(f"{guide_name}: expected exactly one non-empty Rust code block")
    example = fences[0]
    if any(required not in example for required in required_imports):
        raise ValueError(f"{guide_name}: managed example is missing required imports")
    return example


def extract_provider_example(markdown: str, guide_name: str) -> str:
    required_imports = (
        "use qubit_magika::{MagikaMimeDetectorProvider, ManagedInferenceRuntime};",
        "MagikaMimeDetectorProvider::new().managed_runtime(Arc::clone(&runtime))",
    )
    return extract_example(markdown, guide_name, PROVIDER_START, PROVIDER_END, required_imports)


def run_self_tests() -> None:
    valid = (
        f"{START}\n```rust\n"
        "use qubit_magika::MagikaMimeDetector;\n"
        "use qubit_operation::{GroupOptions, ManagerOptions, OperationManager, OperationOptions, OperationOutcome, ShutdownMode};\n"
        "```\n"
        f"{END}"
    )
    assert "GroupOptions" in extract_example(valid, "fixture")
    provider = (
        f"{PROVIDER_START}\n```rust\n"
        "use qubit_magika::{MagikaMimeDetectorProvider, ManagedInferenceRuntime};\n"
        "MagikaMimeDetectorProvider::new().managed_runtime(Arc::clone(&runtime))\n"
        "```\n"
        f"{PROVIDER_END}"
    )
    assert "managed_runtime" in extract_provider_example(provider, "provider fixture")
    for invalid, label in (
        (valid.replace(START, ""), "missing start marker"),
        (valid + "\n" + START, "repeated start marker"),
        (valid.replace("```rust\n", "```text\n"), "missing Rust block"),
    ):
        try:
            extract_example(invalid, "fixture")
        except ValueError:
            pass
        else:
            raise AssertionError(f"accepted invalid fixture: {label}")
    missing_import = valid.replace("GroupOptions", "SomeOtherType")
    try:
        extract_example(missing_import, "Chinese fixture")
    except ValueError:
        pass
    else:
        raise AssertionError("accepted guide fixture with missing GroupOptions import")


def toml_string(value: Path) -> str:
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true", help="run extractor validation checks")
    args = parser.parse_args()
    if args.self_test:
        run_self_tests()
        print("managed guide extractor self-tests passed")
        return 0

    project_root = Path(__file__).resolve().parent.parent
    project_manifest = tomllib.loads((project_root / "Cargo.toml").read_text(encoding="utf-8"))
    operation_dependency = project_manifest["dependencies"]["qubit-operation"]
    operation_path = (project_root / operation_dependency["path"]).resolve()
    operation_version = operation_dependency["version"]
    file_system_dependency = project_manifest["dependencies"]["qubit-fs"]
    file_system_version = file_system_dependency["version"]
    file_system_features = file_system_dependency.get("features", [])
    mime_dependency = project_manifest["dependencies"]["qubit-mime"]
    mime_version = mime_dependency if isinstance(mime_dependency, str) else mime_dependency["version"]
    examples = [
        extract_example((project_root / guide).read_text(encoding="utf-8"), guide)
        for guide in GUIDES
    ]
    provider_examples = [
        extract_provider_example((project_root / guide).read_text(encoding="utf-8"), guide)
        for guide in GUIDES
    ]
    target = project_root / "target"
    target.mkdir(exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix="managed-guide-check-", dir=target))
    try:
        consumer = temp_dir / "consumer"
        (consumer / "src").mkdir(parents=True)
        modules = [
            f"#[allow(dead_code)]\nmod guide_{index} {{\n{example}\n}}"
            for index, example in enumerate(examples + provider_examples)
        ]
        (consumer / "src/lib.rs").write_text("\n\n".join(modules) + "\n", encoding="utf-8")
        manifest = f"""[package]
name = "managed-guide-example-check"
version = "0.0.0"
edition = "2024"

[dependencies]
qubit-magika = {{ path = {toml_string(project_root)} }}
qubit-operation = {{ path = {toml_string(operation_path)}, version = {operation_version!r} }}
qubit-fs = {{ version = {file_system_version!r}, default-features = false, features = {file_system_features!r} }}
qubit-mime = {{ version = {mime_version!r} }}
"""
        (consumer / "Cargo.toml").write_text(manifest, encoding="utf-8")
        print(f"Compiling guide examples extracted from {', '.join(GUIDES)}")
        cargo_env = os.environ.copy()
        cargo_env["CARGO_TARGET_DIR"] = str(project_root / "target")
        return subprocess.run(
            ["cargo", "check", "--manifest-path", str(consumer / "Cargo.toml")],
            cwd=project_root,
            env=cargo_env,
            check=False,
        ).returncode
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
