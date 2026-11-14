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
from pathlib import Path


START = "<!-- managed-guide-example-start -->"
END = "<!-- managed-guide-example-end -->"
GUIDES = ("doc/user_guide.md", "doc/user_guide.zh_CN.md")


def extract_example(markdown: str, guide_name: str) -> str:
    if markdown.count(START) != 1:
        raise ValueError(f"{guide_name}: expected exactly one start marker")
    if markdown.count(END) != 1:
        raise ValueError(f"{guide_name}: expected exactly one end marker")
    start = markdown.index(START) + len(START)
    end = markdown.index(END)
    if end <= start:
        raise ValueError(f"{guide_name}: end marker must follow start marker")
    marked = markdown[start:end]
    fences = re.findall(r"```rust(?:,[^\n]*)?\n(.*?)\n```", marked, re.DOTALL)
    if len(fences) != 1 or not fences[0].strip():
        raise ValueError(f"{guide_name}: expected exactly one non-empty Rust code block")
    example = fences[0]
    required_imports = (
        "use qubit_magika::{MagikaMimeDetector, ManagedInferenceRuntime};",
        "use qubit_operation::supervisor::{OperationSupervisor, ServiceGroup, ShutdownMode};",
    )
    if any(required not in example for required in required_imports):
        raise ValueError(f"{guide_name}: managed example is missing required imports")
    return example


def run_self_tests() -> None:
    valid = (
        f"{START}\n```rust\n"
        "use qubit_magika::{MagikaMimeDetector, ManagedInferenceRuntime};\n"
        "use qubit_operation::supervisor::{OperationSupervisor, ServiceGroup, ShutdownMode};\n"
        "```\n"
        f"{END}"
    )
    assert "ServiceGroup" in extract_example(valid, "fixture")
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
    missing_import = valid.replace("ServiceGroup", "SomeOtherType")
    try:
        extract_example(missing_import, "Chinese fixture")
    except ValueError:
        pass
    else:
        raise AssertionError("accepted guide fixture with missing ServiceGroup import")


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
    examples = [
        extract_example((project_root / guide).read_text(encoding="utf-8"), guide)
        for guide in GUIDES
    ]
    target = project_root / "target"
    target.mkdir(exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix="managed-guide-check-", dir=target))
    try:
        consumer = temp_dir / "consumer"
        (consumer / "src").mkdir(parents=True)
        modules = [f"#[allow(dead_code)]\nmod guide_{index} {{\n{example}\n}}" for index, example in enumerate(examples)]
        (consumer / "src/lib.rs").write_text("\n\n".join(modules) + "\n", encoding="utf-8")
        manifest = f"""[package]
name = "managed-guide-example-check"
version = "0.0.0"
edition = "2024"

[dependencies]
qubit-magika = {{ path = {toml_string(project_root)} }}
qubit-fs = {{ path = {toml_string(project_root.parent / 'rs-fs')}, version = "^0.3", default-features = false, features = ["async"] }}
qubit-mime = {{ path = {toml_string(project_root.parent / 'rs-mime')}, version = "^0.21" }}
qubit-operation = {{ path = {toml_string(project_root.parent / 'rs-operation')}, version = "^0.1" }}

[patch.crates-io]
qubit-fs = {{ path = {toml_string(project_root.parent / 'rs-fs')} }}
qubit-command = {{ path = {toml_string(project_root.parent / 'rs-command')} }}
qubit-config = {{ path = {toml_string(project_root.parent / 'rs-config')} }}
qubit-local-files = {{ path = {toml_string(project_root.parent / 'rs-local-files')} }}
qubit-redact = {{ path = {toml_string(project_root.parent / 'rs-redact')} }}
qubit-json = {{ path = {toml_string(project_root.parent / 'rs-json')} }}
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
