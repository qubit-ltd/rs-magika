#!/usr/bin/env python3
"""Compile managed examples using only each guide's declared dependencies."""

from __future__ import annotations

import argparse
import json
import math
import os
import posixpath
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path, PureWindowsPath

from _local_package_identities import (
    run_self_tests as run_local_package_identity_self_tests,
    validate_local_package_identities,
)
from _dynamic_loader_feature_policy import (
    dynamic_ort_feature_violations,
    ort_link_graph_violations,
    run_self_tests as run_ort_graph_self_tests,
)



START = "<!-- managed-guide-example-start -->"
END = "<!-- managed-guide-example-end -->"
PROVIDER_START = "<!-- provider-runtime-example-start -->"
PROVIDER_END = "<!-- provider-runtime-example-end -->"
DEPENDENCIES_START = "<!-- managed-dependencies-start -->"
DEPENDENCIES_END = "<!-- managed-dependencies-end -->"
APPLICATION_START = "<!-- fixed-source-application-start -->"
APPLICATION_END = "<!-- fixed-source-application-end -->"
DYNAMIC_START = "<!-- dynamic-loader-application-start -->"
DYNAMIC_END = "<!-- dynamic-loader-application-end -->"
DYNAMIC_EXAMPLE_START = "<!-- dynamic-loader-rust-example-start -->"
DYNAMIC_EXAMPLE_END = "<!-- dynamic-loader-rust-example-end -->"
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


def extract_dynamic_example(markdown: str, guide_name: str) -> str:
    example = extract_example(
        markdown, guide_name, DYNAMIC_EXAMPLE_START, DYNAMIC_EXAMPLE_END, ()
    )
    for anchor in ("ort::init_from", "MagikaMimeDetector::new"):
        if anchor not in example:
            raise ValueError(f"{guide_name}: dynamic example is missing {anchor}")
    return example


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


def extract_manifest(markdown: str, guide_name: str, start_marker: str, end_marker: str) -> dict[str, object]:
    if markdown.count(start_marker) != 1 or markdown.count(end_marker) != 1:
        raise ValueError(f"{guide_name}: expected exactly one manifest marker pair")
    start = markdown.index(start_marker) + len(start_marker)
    end = markdown.index(end_marker)
    if end <= start:
        raise ValueError(f"{guide_name}: manifest end marker must follow start marker")
    fences = re.findall(r"```toml\s*\n(.*?)\n```", markdown[start:end], re.DOTALL)
    if len(fences) != 1:
        raise ValueError(f"{guide_name}: expected exactly one TOML manifest")
    try:
        manifest = tomllib.loads(fences[0])
    except tomllib.TOMLDecodeError as error:
        raise ValueError(f"{guide_name}: invalid TOML manifest: {error}") from error
    dependencies = manifest.get("dependencies")
    if not isinstance(dependencies, dict):
        raise ValueError(f"{guide_name}: manifest must declare [dependencies]")
    return manifest


def validate_and_rebase_manifest(
    manifest: dict[str, object], guide_name: str, project_root: Path, dynamic: bool = False
) -> dict[str, object]:
    unsupported = set(manifest) - {"package", "workspace", "dependencies", "patch"}
    if unsupported:
        name = sorted(unsupported)[0]
        raise ValueError(f"{guide_name}: cannot preserve unsupported root table/key {name!r}")
    for table in manifest:
        if not isinstance(manifest[table], dict):
            raise ValueError(f"{guide_name}: cannot preserve non-table value at root key {table!r}")
    for name, value in manifest.items():
        _validate_toml_value(value, guide_name, name)
    patch_table = manifest.get("patch", {})
    if not isinstance(patch_table, dict):
        raise ValueError(f"{guide_name}: cannot preserve non-table value at patch")
    for registry, packages in patch_table.items():
        if not isinstance(packages, dict):
            raise ValueError(f"{guide_name}: cannot preserve non-table value at patch.{registry}")
    dependencies = manifest["dependencies"]
    assert isinstance(dependencies, dict)
    virtual_siblings = {
        "qubit-magika": Path("/layout/rs-magika"),
        "qubit-mime": Path("/layout/rs-mime"),
        "qubit-fs": Path("/layout/rs-fs"),
        "qubit-operation": Path("/layout/rs-operation"),
    }
    actual_siblings = {
        "qubit-magika": project_root,
        "qubit-mime": project_root.parent / "rs-mime",
        "qubit-fs": project_root.parent / "rs-fs",
        "qubit-operation": project_root.parent / "rs-operation",
    }
    rebased = json.loads(json.dumps(manifest))
    new_dependencies = rebased["dependencies"]
    for name in actual_siblings:
        entry = dependencies.get(name)
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            raise ValueError(f"{guide_name}: {name} must use a fixed sibling path")
        original = _guide_relative_path(entry["path"], guide_name, name)
        expected_relative = "../rs-" + name.removeprefix("qubit-")
        if original != expected_relative:
            raise ValueError(f"{guide_name}: {name} path {entry['path']!r} does not resolve to expected sibling {virtual_siblings[name]}")
        sibling = actual_siblings[name]
        new_dependencies[name]["path"] = str(sibling.resolve())
    patch = manifest.get("patch", {})
    operation_patch = patch.get("crates-io", {}).get("qubit-operation") if isinstance(patch, dict) else None
    if not isinstance(operation_patch, dict) or not isinstance(operation_patch.get("path"), str):
        raise ValueError(f"{guide_name}: operation patch must use a fixed sibling path")
    if _guide_relative_path(operation_patch["path"], guide_name, "patch.crates-io.qubit-operation") != "../rs-operation":
        raise ValueError(f"{guide_name}: operation patch path does not resolve to expected sibling")
    rebased.setdefault("patch", {}).setdefault("crates-io", {})["qubit-operation"]["path"] = str(actual_siblings["qubit-operation"].resolve())
    if dynamic:
        magika = new_dependencies["qubit-magika"]
        ort = new_dependencies.get("ort")
        if not isinstance(magika, dict) or magika.get("default-features") is not False:
            raise ValueError(f"{guide_name}: dynamic manifest must disable Magika default features")
        features = ort.get("features", []) if isinstance(ort, dict) else []
        violations = dynamic_ort_feature_violations(features)
        if violations:
            raise ValueError(f"{guide_name}: dynamic manifest ORT features {sorted(features)}: {'; '.join(violations)}")
        if "bundled-onnxruntime" in magika.get("features", []):
            raise ValueError(f"{guide_name}: dynamic manifest Magika features {magika.get('features', [])}: must not enable bundled-onnxruntime")
    if tomllib.loads(manifest_text(rebased, add_package=False, add_workspace=False)) != rebased:
        raise ValueError(f"{guide_name}: TOML serialization changed the rebased manifest structure")
    return rebased


def _guide_relative_path(value: str, guide_name: str, key: str) -> str:
    windows_path = PureWindowsPath(value)
    if Path(value).is_absolute() or windows_path.is_absolute():
        raise ValueError(f"{guide_name}: {key} must use a relative guide path, got {value!r}")
    normalized = windows_path.as_posix() if "\\" in value else value
    return posixpath.normpath(normalized)


def _validate_toml_value(value: object, guide_name: str, key: str) -> None:
    if isinstance(value, bool) or isinstance(value, (str, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{guide_name}: cannot preserve non-finite TOML value at {key}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_toml_value(item, guide_name, f"{key}[{index}]")
        return
    if isinstance(value, dict):
        for child, item in value.items():
            _validate_toml_value(item, guide_name, f"{key}.{child}")
        return
    raise ValueError(f"{guide_name}: cannot preserve unsupported TOML value at {key}")


def manifest_text(
    manifest: dict[str, object], *, add_package: bool, add_workspace: bool
) -> str:
    unsupported = set(manifest) - {"package", "workspace", "dependencies", "patch"}
    if unsupported:
        raise ValueError(f"cannot serialize unsupported root table/key {sorted(unsupported)[0]!r}")
    lines: list[str] = []
    package = manifest.get("package")
    if isinstance(package, dict):
        lines.append("[package]")
        lines.extend(f"{toml_key(key)} = {toml_value(value)}" for key, value in package.items())
        lines.append("")
    elif add_package:
        lines.extend(("[package]", 'name = "managed-guide-graph"', 'version = "0.0.0"', 'edition = "2024"', ""))
    workspace = manifest.get("workspace")
    if isinstance(workspace, dict):
        lines.append("[workspace]")
        lines.extend(f"{toml_key(key)} = {toml_value(value)}" for key, value in workspace.items())
        lines.append("")
    elif add_workspace:
        lines.extend(("[workspace]", ""))
    for section in ("dependencies",):
        values = manifest.get(section, {})
        lines.append(f"[{section}]")
        for name, value in values.items():
            lines.append(f"{toml_key(name)} = {toml_value(value)}")
        lines.append("")
    patch = manifest.get("patch")
    if isinstance(patch, dict):
        for section, values in patch.items():
            lines.append(f"[patch.{toml_key(section)}]")
            for name, value in values.items():
                lines.append(f"{toml_key(name)} = {toml_value(value)}")
            lines.append("")
    return "\n".join(lines)


def validate_feature_graph(metadata: dict[str, object], variant: str, guide: str) -> None:
    mode = "bundled" if variant in ("default", "inventory") else "dynamic" if variant == "dynamic" else None
    if mode is not None:
        link_errors = ort_link_graph_violations(metadata, mode)
        if link_errors:
            raise ValueError(f"{guide}: {variant} variant actual features: {'; '.join(link_errors)}")
    packages = metadata.get("packages", [])
    nodes = metadata.get("resolve", {}).get("nodes", [])
    by_name: dict[str, list[tuple[str, set[str]]]] = {}
    for package in packages:
        if not isinstance(package, dict):
            continue
        package_id = package.get("id")
        package_nodes = [node for node in nodes if isinstance(node, dict) and node.get("id") == package_id]
        if len(package_nodes) != 1:
            continue
        by_name.setdefault(str(package.get("name")), []).append(
            (str(package_id), set(package_nodes[0].get("features", [])))
        )

    def one(name: str) -> set[str]:
        matches = by_name.get(name, [])
        if len(matches) != 1:
            identities = [identity for identity, _ in matches]
            raise ValueError(f"{guide}: expected one resolved {name} package identity; found {identities}")
        return matches[0][1]

    magika = one("qubit-magika")
    ort = one("ort")
    def require(feature: str, actual: set[str], package: str) -> None:
        if feature not in actual:
            raise ValueError(f"{guide}: {variant} variant {package} actual features {sorted(actual)}: missing {feature}")

    def exclude(feature: str, actual: set[str], package: str) -> None:
        if feature in actual:
            raise ValueError(f"{guide}: {variant} variant {package} actual features {sorted(actual)}: forbidden {feature}")

    if variant in ("default", "inventory"):
        require("bundled-onnxruntime", magika, "qubit-magika")
        require("download-binaries", ort, "ort")
        exclude("load-dynamic", ort, "ort")
    if variant == "default":
        exclude("inventory", magika, "qubit-magika")
    if variant == "inventory":
        require("inventory", magika, "qubit-magika")
    if variant == "dynamic":
        policy_errors = dynamic_ort_feature_violations(ort)
        if policy_errors:
            raise ValueError(f"{guide}: dynamic variant ort actual features {sorted(ort)}: {'; '.join(policy_errors)}")
        exclude("bundled-onnxruntime", magika, "qubit-magika")


def inventory_dependency(markdown: str, guide_name: str) -> dict[str, object]:
    if markdown.count(APPLICATION_END) != 1:
        raise ValueError(f"{guide_name}: expected exactly one fixed-source manifest end marker")
    section_start = markdown.index(APPLICATION_END) + len(APPLICATION_END)
    next_heading = re.search(r"(?m)^#{1,6}\s+", markdown[section_start:])
    section_end = section_start + next_heading.start() if next_heading else len(markdown)
    fixed_source_section = markdown[section_start:section_end]
    declarations = [
        code.strip()
        for code in re.findall(r"`([^`]+)`", fixed_source_section)
        if re.match(r"qubit-magika\s*=\s*\{", code.strip())
    ]
    if len(declarations) != 1:
        raise ValueError(f"{guide_name}: expected exactly one inventory dependency declaration")
    try:
        dependency = tomllib.loads("[dependencies]\n" + declarations[0])["dependencies"]["qubit-magika"]
    except tomllib.TOMLDecodeError as error:
        raise ValueError(f"{guide_name}: invalid fixed-source inventory dependency TOML: {error}") from error
    if not isinstance(dependency, dict):
        raise ValueError(f"{guide_name}: inventory dependency must be an inline table")
    if "inventory" not in dependency.get("features", []):
        raise ValueError(f"{guide_name}: inventory dependency does not enable inventory")
    if dependency.get("path") != "../rs-magika":
        raise ValueError(f"{guide_name}: inventory dependency must explicitly use path '../rs-magika'")
    return dependency


def inventory_manifest(
    manifest: dict[str, object], markdown: str, guide_name: str
) -> dict[str, object]:
    merged = json.loads(json.dumps(manifest))
    dependencies = merged["dependencies"]
    declaration = inventory_dependency(markdown, guide_name)
    dependencies["qubit-magika"] = declaration
    return merged


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
    run_local_package_identity_self_tests()
    run_ort_graph_self_tests()

    def rooted_fixture(graph: dict[str, object], variant: str) -> dict[str, object]:
        rooted = json.loads(json.dumps(graph))
        root_id = "path+guide-test#0.0.0"
        sys_id = "registry+ort-sys#2.0.0"
        rooted["packages"].append({"id": root_id, "name": "guide-test"})
        rooted["packages"].append({"id": sys_id, "name": "ort-sys"})
        nodes = rooted["resolve"]["nodes"]
        rooted["resolve"]["root"] = root_id
        for node in nodes:
            if next(package for package in rooted["packages"] if package["id"] == node["id"])["name"] == "ort":
                node.setdefault("deps", []).append({"name": "ort_sys", "pkg": sys_id, "dep_kinds": [{"kind": None, "target": None}]})
        rooted["resolve"]["nodes"].append({"id": root_id, "features": [], "deps": [
            {"name": package["name"].replace("-", "_"), "pkg": package["id"], "dep_kinds": [{"kind": None, "target": None}]}
            for package in rooted["packages"] if package["id"] != root_id
        ]})
        sys_features = ["download-binaries"] if variant in ("default", "inventory") else ["disable-linking"]
        rooted["resolve"]["nodes"].append({"id": sys_id, "features": sys_features, "deps": []})
        return rooted

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
    sibling_manifest = tomllib.loads(sibling_consumer)
    assert sibling_manifest["patch"]["crates-io"]["qubit-operation"]["path"] == "layout/rs-operation"
    mime_dependency = {"version": "0.22", "default-features": False, "features": ["inventory"]}
    mime_consumer = tomllib.loads(consumer_manifest(
        {"qubit-mime": mime_dependency}, Path("layout/rs-magika"), Path("layout/rs-operation")
    ))["dependencies"]["qubit-mime"]
    assert {key: value for key, value in mime_consumer.items() if key != "path"} == mime_dependency
    assert mime_consumer["path"] == "layout/rs-mime"
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

    application = """[workspace]
[package]
name = "app"
version = "0.1.0"
edition = "2024"
metadata = { "display name" = "Application" }
[dependencies]
qubit-magika = { version = "0.18", path = "../rs-magika" }
qubit-mime = { version = "0.22", path = "../rs-mime", default-features = false, features = ["inventory"] }
qubit-fs = { version = "0.4", path = "../rs-fs", default-features = false, features = ["async"] }
qubit-operation = { version = "0.3.2", path = "../rs-operation" }
[patch.crates-io]
qubit-operation = { path = "../rs-operation" }
"""
    marked = f"{APPLICATION_START}\n```toml\n{application}```\n{APPLICATION_END}"
    parsed_application = extract_manifest(marked, guide, APPLICATION_START, APPLICATION_END)
    fixture_root = Path("/layout/rs-magika")
    expected_fixture_paths = {
        "qubit-magika": fixture_root.resolve(),
        "qubit-mime": (fixture_root.parent / "rs-mime").resolve(),
        "qubit-fs": (fixture_root.parent / "rs-fs").resolve(),
        "qubit-operation": (fixture_root.parent / "rs-operation").resolve(),
    }
    rebased = validate_and_rebase_manifest(parsed_application, guide, fixture_root)
    serialized_rebased = tomllib.loads(manifest_text(rebased, add_package=False, add_workspace=False))
    for name, expected_path in expected_fixture_paths.items():
        assert Path(serialized_rebased["dependencies"][name]["path"]).resolve() == expected_path
    assert Path(serialized_rebased["patch"]["crates-io"]["qubit-operation"]["path"]).resolve() == expected_fixture_paths["qubit-operation"]
    assert serialized_rebased["dependencies"]["qubit-mime"]["version"] == "0.22"
    assert serialized_rebased["dependencies"]["qubit-mime"]["default-features"] is False
    assert serialized_rebased["dependencies"]["qubit-mime"]["features"] == ["inventory"]
    assert serialized_rebased["dependencies"]["qubit-operation"]["version"] == "0.3.2"
    windows_relative = json.loads(json.dumps(parsed_application))
    windows_relative["dependencies"]["qubit-magika"]["path"] = r"..\rs-magika"
    windows_relative["dependencies"]["qubit-mime"]["path"] = r"..\rs-mime"
    windows_relative["dependencies"]["qubit-fs"]["path"] = r"..\rs-fs"
    windows_relative["dependencies"]["qubit-operation"]["path"] = r"..\rs-operation"
    windows_relative["patch"]["crates-io"]["qubit-operation"]["path"] = r"..\rs-operation"
    windows_rebased = validate_and_rebase_manifest(windows_relative, guide, fixture_root)
    windows_roundtrip = tomllib.loads(manifest_text(windows_rebased, add_package=False, add_workspace=False))
    assert windows_roundtrip == windows_rebased
    for name, expected_path in expected_fixture_paths.items():
        assert Path(windows_roundtrip["dependencies"][name]["path"]).resolve() == expected_path
    assert windows_roundtrip["dependencies"]["qubit-mime"]["features"] == ["inventory"]
    assert windows_roundtrip["dependencies"]["qubit-mime"]["default-features"] is False
    assert Path(windows_roundtrip["patch"]["crates-io"]["qubit-operation"]["path"]).resolve() == expected_fixture_paths["qubit-operation"]
    for windows_absolute in (r"C:\apps\rs-magika", r"\\server\share\rs-magika"):
        try:
            _guide_relative_path(windows_absolute, guide, "Windows path self-test")
        except ValueError as error:
            assert "relative guide path" in str(error)
        else:
            raise AssertionError(f"accepted absolute Windows path: {windows_absolute}")
    serialized = tomllib.loads(
        manifest_text(parsed_application, add_package=False, add_workspace=False)
    )
    assert serialized == parsed_application
    def inventory_chapter(declaration: str) -> str:
        return (
            f"Text before the replacement. {APPLICATION_END}\n"
            f"{declaration}\n\n## Following section\n"
            '`qubit-magika = { version = "0.18", features = ["inventory"] }`\n'
        )

    shell_fixtures = (
        {"dependencies": {}, "package": {"name": "existing-package", "version": "1.0.0"}},
        {"dependencies": {}, "workspace": {"members": ["member"]}},
        {"dependencies": {}, "package": {"name": "existing-package"}, "workspace": {}},
        {"dependencies": {}},
    )
    for fixture in shell_fixtures:
        add_package = "package" not in fixture
        add_workspace = "workspace" not in fixture
        expected = json.loads(json.dumps(fixture))
        if add_package:
            expected["package"] = {
                "name": "managed-guide-graph", "version": "0.0.0", "edition": "2024"
            }
        if add_workspace:
            expected["workspace"] = {}
        generated = manifest_text(
            fixture, add_package=add_package, add_workspace=add_workspace
        )
        assert tomllib.loads(generated) == expected
    inventory_line = '`qubit-magika = { version = "0.18", path = "../rs-magika", features = ["inventory"] }`'
    inventory_source = inventory_manifest(
        parsed_application, inventory_chapter(inventory_line), guide
    )
    inventory_rebased = validate_and_rebase_manifest(inventory_source, guide, Path("/layout/rs-magika"))
    assert inventory_rebased["dependencies"]["qubit-magika"]["features"] == ["inventory"]
    try:
        invalid_inventory = inventory_manifest(
            parsed_application,
            inventory_chapter(
                inventory_line.replace('path = "../rs-magika"', 'path = "../wrong-magika"')
            ),
            guide,
        )
        validate_and_rebase_manifest(invalid_inventory, guide, Path("/layout/rs-magika"))
    except ValueError:
        pass
    else:
        raise AssertionError("accepted an inventory dependency with an incorrect source path")
    dynamic_manifest = json.loads(json.dumps(parsed_application))
    dynamic_manifest["dependencies"]["qubit-magika"]["default-features"] = False
    dynamic_manifest["dependencies"]["ort"] = {"version": ">=2.0.0-rc.12,<2.1", "default-features": False, "features": ["std", "ndarray", "load-dynamic", "api-24"]}
    validate_and_rebase_manifest(dynamic_manifest, guide, Path("/layout/rs-magika"), dynamic=True)
    dynamic_example = f"{DYNAMIC_EXAMPLE_START}\n```rust,no_run\nfn create_detector() {{ let _ = ort::init_from(\"runtime\"); let _ = MagikaMimeDetector::new(); }}\n```\n{DYNAMIC_EXAMPLE_END}"
    assert "ort::init_from" in extract_dynamic_example(dynamic_example, guide)
    for invalid_dynamic in (
        dynamic_example.replace(DYNAMIC_EXAMPLE_START, ""),
        dynamic_example + "\n" + DYNAMIC_EXAMPLE_START,
    ):
        try:
            extract_dynamic_example(invalid_dynamic, guide)
        except ValueError:
            pass
        else:
            raise AssertionError("accepted missing or duplicate dynamic example marker")
    for invalid_text, expected_error in (
        (application.replace('metadata = { "display name" = "Application" }', 'metadata = { value = inf }'), "non-finite TOML value at package.metadata.value"),
        (application.replace('[patch.crates-io]\nqubit-operation = { path = "../rs-operation" }', '[patch]\ncrates-io = "not-a-table"'), "non-table value at patch.crates-io"),
    ):
        try:
            validate_and_rebase_manifest(tomllib.loads(invalid_text), guide, Path("/layout/rs-magika"))
        except ValueError as error:
            assert expected_error in str(error), str(error)
        else:
            raise AssertionError(f"accepted unrepresentable TOML fixture: {expected_error}")
    for table in (
        "[target.'cfg(unix)'.dependencies]\nort = { features = [\"download-binaries\"] }\n",
        "[replace]\n\"qubit-magika:0.18.0\" = { path = \"../rs-magika\" }\n",
    ):
        try:
            validate_and_rebase_manifest(tomllib.loads(application + table), guide, Path("/layout/rs-magika"))
        except ValueError as error:
            assert "unsupported root table/key" in str(error)
        else:
            raise AssertionError(f"accepted unsupported manifest table: {table}")
    for key in ("qubit-magika", "patch.crates-io.qubit-operation"):
        invalid_absolute = json.loads(json.dumps(parsed_application))
        target = (
            invalid_absolute["dependencies"]["qubit-magika"]
            if key == "qubit-magika"
            else invalid_absolute["patch"]["crates-io"]["qubit-operation"]
        )
        target["path"] = "/layout/rs-magika" if key == "qubit-magika" else "/layout/rs-operation"
        try:
            validate_and_rebase_manifest(invalid_absolute, guide, Path("/layout/rs-magika"))
        except ValueError as error:
            assert "relative guide path" in str(error)
        else:
            raise AssertionError(f"accepted absolute guide path in {key}")
    inventory_base = json.loads(json.dumps(parsed_application))
    inventory_base["dependencies"]["qubit-magika"]["default-features"] = False
    inventory_base["dependencies"]["qubit-magika"]["optional"] = True
    inventory_missing_fields = inventory_manifest(
        inventory_base,
        inventory_chapter(
            '`qubit-magika = { version = "0.18", path = "../rs-magika", features = ["inventory"] }`'
        ),
        guide,
    )
    assert inventory_missing_fields["dependencies"]["qubit-magika"] == {
        "version": "0.18", "path": "../rs-magika", "features": ["inventory"]
    }
    assert inventory_missing_fields["dependencies"]["qubit-mime"] == parsed_application["dependencies"]["qubit-mime"]
    assert inventory_missing_fields["patch"] == parsed_application["patch"]
    for forbidden in ("download-binaries", "copy-dylibs", "download-binaries-check", "tls-rustls", "tls-rustls-no-provider"):
        dynamic_bad = json.loads(json.dumps(dynamic_manifest))
        dynamic_bad["dependencies"]["ort"]["features"].append(forbidden)
        try:
            validate_and_rebase_manifest(dynamic_bad, guide, Path("/layout/rs-magika"), dynamic=True)
        except ValueError as error:
            assert forbidden in str(error), str(error)
            assert guide in str(error) and "dynamic" in str(error)
        else:
            raise AssertionError(f"accepted dynamic manifest with forbidden ORT feature {forbidden}")
    for invalid in (
        marked.replace(APPLICATION_START, ""),
        marked + "\n" + APPLICATION_START,
        marked.replace('name = "app"', 'name = "unterminated'),
        marked.replace(
            'qubit-operation = { version = "0.3.2", path = "../rs-operation" }',
            'qubit-operation = { version = "0.3.2", path = "../wrong-operation" }',
        ),
        marked.replace(
            '[patch.crates-io]\nqubit-operation = { path = "../rs-operation" }',
            '[patch.crates-io]\nqubit-operation = { path = "../wrong-operation" }',
        ),
    ):
        try:
            parsed = extract_manifest(invalid, guide, APPLICATION_START, APPLICATION_END)
            validate_and_rebase_manifest(parsed, guide, Path("/layout/rs-magika"))
        except ValueError:
            continue
        raise AssertionError("accepted malformed or incorrectly relocated application TOML")
    for variant, magika_features, ort_features in (
        ("default", ["bundled-onnxruntime"], ["download-binaries"]),
        ("inventory", ["bundled-onnxruntime", "inventory"], ["download-binaries"]),
        ("dynamic", [], ["load-dynamic"]),
    ):
        graph = {
            "packages": [
                {"id": "path+magika#0.18.0", "name": "qubit-magika"},
                {"id": "registry+ort#2.0.0", "name": "ort"},
            ],
            "resolve": {"nodes": [
                {"id": "path+magika#0.18.0", "features": magika_features},
                {"id": "registry+ort#2.0.0", "features": ort_features},
            ]},
        }
        validate_feature_graph(rooted_fixture(graph, variant), variant, guide)
        if variant == "dynamic":
            try:
                validate_feature_graph(graph, variant, guide)
            except ValueError as error:
                assert "no root package" in str(error)
            else:
                raise AssertionError("accepted feature graph without a Cargo resolve root")
        if variant in ("default", "inventory"):
            unbundled = json.loads(json.dumps(graph))
            unbundled["resolve"]["nodes"][0]["features"] = (
                ["inventory"] if variant == "inventory" else []
            )
            try:
                validate_feature_graph(rooted_fixture(unbundled, variant), variant, guide)
            except ValueError as error:
                assert guide in str(error) and variant in str(error)
                assert "actual features" in str(error) and "missing bundled-onnxruntime" in str(error)
            else:
                raise AssertionError(f"accepted {variant} graph without bundled runtime")
    for variant, magika_features, ort_features, expected_message in (
        ("default", ["bundled-onnxruntime", "inventory"], ["download-binaries"], "forbidden inventory"),
        ("default", ["bundled-onnxruntime"], ["download-binaries", "load-dynamic"], "forbidden load-dynamic"),
        ("inventory", ["bundled-onnxruntime", "inventory"], ["download-binaries", "load-dynamic"], "forbidden load-dynamic"),
    ):
        graph = {
            "packages": [
                {"id": "path+magika#0.18.0", "name": "qubit-magika"},
                {"id": "registry+ort#2.0.0", "name": "ort"},
            ],
            "resolve": {"nodes": [
                {"id": "path+magika#0.18.0", "features": magika_features},
                {"id": "registry+ort#2.0.0", "features": ort_features},
            ]},
        }
        try:
            validate_feature_graph(rooted_fixture(graph, variant), variant, guide)
        except ValueError as error:
            assert guide in str(error) and variant in str(error) and expected_message in str(error), str(error)
            assert "actual features" in str(error), str(error)
        else:
            raise AssertionError(f"accepted invalid {variant} graph: {magika_features}, {ort_features}")
    inventory_without_feature = json.loads(json.dumps({
        "packages": [
            {"id": "path+magika#0.18.0", "name": "qubit-magika"},
            {"id": "registry+ort#2.0.0", "name": "ort"},
        ],
        "resolve": {"nodes": [
            {"id": "path+magika#0.18.0", "features": ["bundled-onnxruntime"]},
            {"id": "registry+ort#2.0.0", "features": ["download-binaries"]},
        ]},
    }))
    try:
        validate_feature_graph(rooted_fixture(inventory_without_feature, "inventory"), "inventory", guide)
    except ValueError as error:
        assert guide in str(error) and "inventory" in str(error)
        assert "actual features" in str(error) and "missing inventory" in str(error)
    else:
        raise AssertionError("accepted inventory graph without the inventory feature")
    for forbidden in ("download-binaries", "copy-dylibs", "download-binaries-check", "tls-rustls", "tls-rustls-no-provider"):
        dynamic_forbidden = {
            "packages": [
                {"id": "path+magika#0.18.0", "name": "qubit-magika"},
                {"id": "registry+ort#2.0.0", "name": "ort"},
            ],
            "resolve": {"nodes": [
                {"id": "path+magika#0.18.0", "features": []},
                {"id": "registry+ort#2.0.0", "features": ["load-dynamic", forbidden]},
            ]},
        }
        try:
            validate_feature_graph(rooted_fixture(dynamic_forbidden, "dynamic"), "dynamic", guide)
        except ValueError as error:
            assert guide in str(error) and "dynamic" in str(error)
            assert forbidden in str(error) and "actual features" in str(error)
        else:
            raise AssertionError(f"accepted dynamic graph with forbidden ORT feature {forbidden}")
    duplicate_graph = {
        "packages": [
            {"id": "path+magika#0.18.0", "name": "qubit-magika"},
            {"id": "registry+magika#0.18.0", "name": "qubit-magika"},
            {"id": "registry+ort#2.0.0", "name": "ort"},
        ],
        "resolve": {"nodes": [
            {"id": "path+magika#0.18.0", "features": ["bundled-onnxruntime"]},
            {"id": "registry+magika#0.18.0", "features": []},
            {"id": "registry+ort#2.0.0", "features": ["download-binaries"]},
        ]},
    }
    try:
        validate_feature_graph(rooted_fixture(duplicate_graph, "default"), "default", guide)
    except ValueError as error:
        assert "path+magika#0.18.0" in str(error)
        assert "registry+magika#0.18.0" in str(error)
    else:
        raise AssertionError("accepted duplicate qubit-magika package identities")
    for public_guide in ("README.md", "README.zh_CN.md"):
        normal_dependency = inventory_dependency(
            inventory_chapter(
                '`qubit-magika = { version = "0.18", path = "../rs-magika", features = ["inventory"] }`'
            ),
            public_guide,
        )
        assert normal_dependency["path"] == "../rs-magika"
        assert normal_dependency["features"] == ["inventory"]
    for declaration, expected_message in (
        ('`qubit-magika = { version = "0.18", features = ["inventory"] }`', "must explicitly use path"),
        ('`qubit-magika = { version = "0.18", path = "../rs-magika", features = [] }`', "does not enable inventory"),
    ):
        try:
            inventory_dependency(inventory_chapter(declaration), guide)
        except ValueError as error:
            assert expected_message in str(error), str(error)
        else:
            raise AssertionError(f"accepted invalid inventory declaration: {declaration}")
    whitespace_inventory = inventory_dependency(inventory_chapter(
        '`qubit-magika = { version = "0.18", path = "../rs-magika", features = ["inventory"], note = "two  spaces and \\\\ slash" }`'
    ), guide)
    assert whitespace_inventory["note"] == "two  spaces and \\ slash"


def toml_value(value: object) -> str:
    if isinstance(value, dict):
        return "{ " + ", ".join(f"{toml_key(key)} = {toml_value(item)}" for key, item in value.items()) + " }"
    if isinstance(value, list):
        return "[" + ", ".join(toml_value(item) for item in value) + "]"
    return json.dumps(value, ensure_ascii=False)


def toml_key(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_-]+", value):
        return value
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
            if isinstance(value, dict):
                value = dict(value)
            else:
                version = dependency_version(value)
                assert version is not None
                value = {"version": version}
            value["path"] = str(project_root.parent / "rs-mime")
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
""" + "\n".join(entries) + (
        "\n\n[patch.crates-io]\nqubit-operation = { path = "
        + toml_value(str(operation_path))
        + " }\n"
        if operation_path is not None
        else "\n"
    )


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
            dynamic_example = extract_dynamic_example(markdown, guide)
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
            metadata_result = subprocess.run(
                ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(consumer / "Cargo.toml")],
                cwd=project_root, env=cargo_env, capture_output=True, text=True, check=False,
            )
            if metadata_result.returncode:
                print(metadata_result.stderr, file=sys.stderr, end="")
                return metadata_result.returncode if metadata_result.returncode > 0 else 1
            if operation_path is not None:
                validate_local_package_identities(
                    json.loads(metadata_result.stdout),
                    {name: project_root if name == "qubit-magika" else project_root.parent / f"rs-{name.removeprefix('qubit-')}" for name in ("qubit-magika", "qubit-mime", "qubit-fs", "qubit-operation")},
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
            application_manifest = extract_manifest(markdown, guide, APPLICATION_START, APPLICATION_END)
            application = validate_and_rebase_manifest(application_manifest, guide, project_root)
            dynamic = validate_and_rebase_manifest(
                extract_manifest(markdown, guide, DYNAMIC_START, DYNAMIC_END),
                guide, project_root, dynamic=True,
            )
            inventory = validate_and_rebase_manifest(
                inventory_manifest(application_manifest, markdown, guide), guide, project_root
            )
            for variant, selected in (("default", application), ("inventory", inventory), ("dynamic", dynamic)):
                graph_dir = temp_dir / f"{Path(guide).stem}-{variant}"
                graph_dir.mkdir()
                (graph_dir / "src").mkdir()
                lib_source = (
                    f"#[allow(dead_code)]\nmod dynamic_guide {{\n{dynamic_example}\n}}\n"
                    if variant == "dynamic"
                    else "// metadata-only guide consumer\n"
                )
                (graph_dir / "src" / "lib.rs").write_text(lib_source, encoding="utf-8")
                graph_manifest = graph_dir / "Cargo.toml"
                add_package = "package" not in selected
                add_workspace = "workspace" not in selected
                serialized_manifest = manifest_text(
                    selected, add_package=add_package, add_workspace=add_workspace
                )
                expected_manifest = json.loads(json.dumps(selected))
                if add_package:
                    expected_manifest["package"] = {
                        "name": "managed-guide-graph",
                        "version": "0.0.0",
                        "edition": "2024",
                    }
                if add_workspace:
                    expected_manifest["workspace"] = {}
                if tomllib.loads(serialized_manifest) != expected_manifest:
                    raise ValueError(f"{guide}: generated {variant} Cargo shell changed manifest semantics")
                graph_manifest.write_text(serialized_manifest, encoding="utf-8")
                metadata_result = subprocess.run(
                    ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(graph_manifest)],
                    cwd=project_root, env=cargo_env, capture_output=True, text=True, check=False,
                )
                if metadata_result.returncode:
                    print(f"{guide} {variant} Cargo graph failed:", file=sys.stderr)
                    print(metadata_result.stderr, file=sys.stderr, end="")
                    return metadata_result.returncode if metadata_result.returncode > 0 else 1
                metadata = json.loads(metadata_result.stdout)
                expected_paths = {
                    name: project_root if name == "qubit-magika" else project_root.parent / f"rs-{name.removeprefix('qubit-')}"
                    for name in ("qubit-magika", "qubit-mime", "qubit-fs", "qubit-operation")
                }
                validate_local_package_identities(metadata, expected_paths)
                validate_feature_graph(metadata, variant, guide)
                print(f"Validated {variant} Cargo graph from {guide}", flush=True)
                if variant == "dynamic":
                    print(f"Compiling dynamic Rust example from {guide}", flush=True)
                    result = subprocess.run(
                        ["cargo", "check", "--manifest-path", str(graph_manifest)],
                        cwd=project_root, env=cargo_env, check=False,
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
