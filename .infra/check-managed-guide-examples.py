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

from _local_package_identities import (
    run_self_tests as run_local_package_identity_self_tests,
    validate_local_package_identities,
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
        original = (Path("/layout/app") / entry["path"]).resolve()
        if original != virtual_siblings[name].resolve():
            raise ValueError(f"{guide_name}: {name} path {entry['path']!r} does not resolve to expected sibling {virtual_siblings[name]}")
        sibling = actual_siblings[name]
        new_dependencies[name]["path"] = str(sibling.resolve())
    patch = manifest.get("patch", {})
    operation_patch = patch.get("crates-io", {}).get("qubit-operation") if isinstance(patch, dict) else None
    if not isinstance(operation_patch, dict) or not isinstance(operation_patch.get("path"), str):
        raise ValueError(f"{guide_name}: operation patch must use a fixed sibling path")
    if (Path("/layout/app") / operation_patch["path"]).resolve() != virtual_siblings["qubit-operation"].resolve():
        raise ValueError(f"{guide_name}: operation patch path does not resolve to expected sibling")
    rebased.setdefault("patch", {}).setdefault("crates-io", {})["qubit-operation"]["path"] = str(actual_siblings["qubit-operation"].resolve())
    if dynamic:
        magika = new_dependencies["qubit-magika"]
        ort = new_dependencies.get("ort")
        if not isinstance(magika, dict) or magika.get("default-features") is not False:
            raise ValueError(f"{guide_name}: dynamic manifest must disable Magika default features")
        features = ort.get("features", []) if isinstance(ort, dict) else []
        if "load-dynamic" not in features:
            raise ValueError(f"{guide_name}: dynamic manifest must enable ort/load-dynamic")
    return rebased


def manifest_text(manifest: dict[str, object], add_package: bool) -> str:
    lines: list[str] = []
    package = manifest.get("package")
    if isinstance(package, dict):
        lines.append("[package]")
        lines.extend(f"{key} = {toml_value(value)}" for key, value in package.items())
        lines.append("")
    elif add_package:
        lines.extend(("[package]", 'name = "managed-guide-graph"', 'version = "0.0.0"', 'edition = "2024"', ""))
    workspace = manifest.get("workspace")
    if isinstance(workspace, dict):
        lines.append("[workspace]")
        lines.extend(f"{key} = {toml_value(value)}" for key, value in workspace.items())
        lines.append("")
    elif add_package:
        lines.extend(("[workspace]", ""))
    for section in ("dependencies",):
        values = manifest.get(section, {})
        lines.append(f"[{section}]")
        for name, value in values.items():
            lines.append(f"{name} = {toml_value(value)}")
        lines.append("")
    patch = manifest.get("patch")
    if isinstance(patch, dict):
        for section, values in patch.items():
            lines.append(f"[patch.{section}]")
            for name, value in values.items():
                lines.append(f"{name} = {toml_value(value)}")
            lines.append("")
    return "\n".join(lines)


def validate_feature_graph(metadata: dict[str, object], variant: str, guide: str) -> None:
    packages = metadata.get("packages", [])
    nodes = metadata.get("resolve", {}).get("nodes", [])
    features = {
        package.get("name"): set(node.get("features", []))
        for package in packages if isinstance(package, dict)
        for node in nodes if isinstance(node, dict) and node.get("id") == package.get("id")
    }
    magika = features.get("qubit-magika", set())
    ort = features.get("ort", set())
    if variant == "inventory" and "inventory" not in magika:
        raise ValueError(f"{guide}: inventory variant did not enable qubit-magika/inventory")
    if variant == "dynamic":
        if "load-dynamic" not in ort:
            raise ValueError(f"{guide}: dynamic variant did not enable ort/load-dynamic")
        if "bundled-onnxruntime" in magika or "download-binaries" in ort:
            raise ValueError(f"{guide}: dynamic variant enables a bundled runtime download")


def inventory_dependency(markdown: str, guide_name: str) -> dict[str, object]:
    matches = re.findall(r"`(qubit-magika\s*=\s*\{[^`]+path\s*=\s*\"\.\./rs-magika\"[^`]+features\s*=\s*\[[^]]+\][^`]*\})`", markdown)
    if len(matches) != 1:
        raise ValueError(f"{guide_name}: expected exactly one inventory dependency declaration")
    try:
        dependency = tomllib.loads("[dependencies]\n" + matches[0])["dependencies"]["qubit-magika"]
    except tomllib.TOMLDecodeError as error:
        raise ValueError(f"{guide_name}: invalid inventory dependency TOML: {error}") from error
    if not isinstance(dependency, dict) or "inventory" not in dependency.get("features", []):
        raise ValueError(f"{guide_name}: inventory dependency does not enable inventory")
    return dependency


def inventory_manifest(
    manifest: dict[str, object], markdown: str, guide_name: str
) -> dict[str, object]:
    merged = json.loads(json.dumps(manifest))
    dependencies = merged["dependencies"]
    original = dependencies["qubit-magika"]
    if not isinstance(original, dict):
        raise ValueError(f"{guide_name}: fixed-source Magika dependency must be a table")
    dependency = dict(original)
    dependency.update(inventory_dependency(markdown, guide_name))
    dependencies["qubit-magika"] = dependency
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
[dependencies]
qubit-magika = { version = "0.18", path = "../rs-magika" }
qubit-mime = { version = "0.22", path = "../rs-mime" }
qubit-fs = { version = "0.4", path = "../rs-fs", default-features = false, features = ["async"] }
qubit-operation = { version = "0.3.2", path = "../rs-operation" }
[patch.crates-io]
qubit-operation = { path = "../rs-operation" }
"""
    marked = f"{APPLICATION_START}\n```toml\n{application}```\n{APPLICATION_END}"
    parsed_application = extract_manifest(marked, guide, APPLICATION_START, APPLICATION_END)
    rebased = validate_and_rebase_manifest(parsed_application, guide, Path("/layout/rs-magika"))
    assert rebased["dependencies"]["qubit-magika"]["path"] == "/layout/rs-magika"
    assert rebased["dependencies"]["qubit-operation"]["path"] == "/layout/rs-operation"
    assert rebased["patch"]["crates-io"]["qubit-operation"]["path"] == "/layout/rs-operation"
    serialized = tomllib.loads(manifest_text(parsed_application, add_package=False))
    assert serialized["package"] == parsed_application["package"]
    inventory_line = '`qubit-magika = { version = "0.18", path = "../rs-magika", features = ["inventory"] }`'
    inventory_source = inventory_manifest(parsed_application, inventory_line, guide)
    inventory_rebased = validate_and_rebase_manifest(inventory_source, guide, Path("/layout/rs-magika"))
    assert inventory_rebased["dependencies"]["qubit-magika"]["features"] == ["inventory"]
    try:
        invalid_inventory = inventory_manifest(
            parsed_application,
            inventory_line.replace('path = "../rs-magika"', 'path = "../wrong-magika"'),
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


def toml_value(value: object) -> str:
    if isinstance(value, dict):
        return "{ " + ", ".join(f"{key} = {toml_value(item)}" for key, item in value.items()) + " }"
    if isinstance(value, list):
        return "[" + ", ".join(toml_value(item) for item in value) + "]"
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
                (graph_dir / "src" / "lib.rs").write_text("// metadata-only guide consumer\n", encoding="utf-8")
                graph_manifest = graph_dir / "Cargo.toml"
                graph_manifest.write_text(manifest_text(selected, add_package="package" not in selected), encoding="utf-8")
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
        return 0
    except (OSError, KeyError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"managed guide check failed: {error}", file=sys.stderr)
        return 1
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
