"""Validate the resolved local identities used by fixed-source consumers."""

from __future__ import annotations

import json
from pathlib import Path


LOCAL_PACKAGES = ("qubit-magika", "qubit-mime", "qubit-fs", "qubit-operation")


def validate_local_package_identities(
    metadata: dict[str, object], expected_paths: dict[str, Path]
) -> None:
    packages = metadata.get("packages")
    resolved = metadata.get("resolve")
    nodes = resolved.get("nodes") if isinstance(resolved, dict) else None
    root_id = resolved.get("root") if isinstance(resolved, dict) else None
    if not isinstance(packages, list) or not isinstance(nodes, list) or not isinstance(root_id, str):
        raise ValueError("cargo metadata did not return a resolved application graph")
    package_by_id = {p.get("id"): p for p in packages if isinstance(p, dict)}
    node_by_id = {n.get("id"): n for n in nodes if isinstance(n, dict)}
    reachable: set[str] = set()
    pending = [root_id]
    while pending:
        package_id = pending.pop()
        if package_id in reachable:
            continue
        reachable.add(package_id)
        node = node_by_id.get(package_id)
        if node is not None:
            pending.extend(
                dep.get("pkg")
                for dep in node.get("deps", [])
                if isinstance(dep, dict) and isinstance(dep.get("pkg"), str)
            )
    for name, expected_path in expected_paths.items():
        actual = [
            package_by_id[package_id]
            for package_id in reachable
            if package_id in package_by_id and package_by_id[package_id].get("name") == name
        ]
        if len(actual) != 1:
            details = "; ".join(
                f"id={package.get('id')!r}, manifest={package.get('manifest_path')!r}"
                for package in actual
            )
            raise ValueError(f"expected one resolved {name} package, found {len(actual)}: {details}")
        package = actual[0]
        manifest_path = Path(str(package.get("manifest_path", ""))).resolve()
        if package.get("source") is not None or manifest_path != (expected_path / "Cargo.toml").resolve():
            raise ValueError(
                f"{name} resolved to id={package.get('id')!r}, source={package.get('source')!r}, "
                f"manifest={manifest_path}; expected {expected_path.resolve()}"
            )


def run_self_tests() -> None:
    root_id = "path+file:///tmp/app#app@0.1.0"
    packages = [{"id": root_id, "name": "app", "source": None, "manifest_path": "/tmp/app/Cargo.toml"}]
    nodes = [{"id": root_id, "deps": []}]
    paths: dict[str, Path] = {}
    for name in LOCAL_PACKAGES:
        checkout = Path("/layout/rs-magika") if name == "qubit-magika" else Path(f"/layout/rs-{name.removeprefix('qubit-')}")
        package_id = f"path+file://{checkout}#{name}@0.1.0"
        paths[name] = checkout
        packages.append({"id": package_id, "name": name, "source": None, "manifest_path": str(checkout / "Cargo.toml")})
        nodes[0]["deps"].append({"pkg": package_id})
        nodes.append({"id": package_id, "deps": []})
    metadata = {"packages": packages, "resolve": {"root": root_id, "nodes": nodes}}
    validate_local_package_identities(metadata, paths)

    duplicate = json.loads(json.dumps(metadata))
    duplicate_id = "registry+https://github.com/rust-lang/crates.io-index#qubit-operation@0.3.2"
    duplicate["packages"].append({
        "id": duplicate_id,
        "name": "qubit-operation",
        "source": "registry+https://github.com/rust-lang/crates.io-index",
        "manifest_path": "/cargo/registry/qubit-operation-0.3.2/Cargo.toml",
    })
    duplicate["resolve"]["nodes"][0]["deps"].append({"pkg": duplicate_id})
    duplicate["resolve"]["nodes"].append({"id": duplicate_id, "deps": []})
    try:
        validate_local_package_identities(duplicate, paths)
    except ValueError as error:
        assert "qubit-operation" in str(error) and "manifest" in str(error)
    else:
        raise AssertionError("accepted a second resolved operation package identity")
