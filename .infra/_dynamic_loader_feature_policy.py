"""Shared ORT feature policy for Cargo metadata dependency graphs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ORT_REQUIRED = "load-dynamic"
ORT_FORBIDDEN = (
    "download-binaries",
    "copy-dylibs",
    "download-binaries-check",
    "alternative-backend",
    "tls-rustls",
    "tls-rustls-no-provider",
    "tls-native",
    "tls-native-vendored",
)
SYS_FORBIDDEN_DYNAMIC = (
    "download-binaries",
    "copy-dylibs",
    "tls-rustls",
    "tls-rustls-no-provider",
    "tls-native",
    "tls-native-vendored",
)


def parse_metadata(raw: str) -> dict[str, Any]:
    """Parse cargo metadata JSON, reporting malformed input as a useful error."""
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError(f"invalid cargo metadata JSON: {error}") from error
    if not isinstance(value, dict) or not isinstance(value.get("packages"), list):
        raise ValueError("cargo metadata has no packages array")
    resolve = value.get("resolve")
    if not isinstance(resolve, dict) or not isinstance(resolve.get("nodes"), list):
        raise ValueError("cargo metadata has no resolved dependency nodes")
    return value


def _reachable_nodes(metadata: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], set[str]]:
    packages = {str(p["id"]): p for p in metadata["packages"] if isinstance(p, dict) and "id" in p}
    nodes = {str(n["id"]): n for n in metadata["resolve"]["nodes"] if isinstance(n, dict) and "id" in n}
    root = metadata.get("resolve", {}).get("root")
    if root is None:
        raise ValueError("cargo metadata resolve graph has no root package")
    reachable: set[str] = set()
    pending = [str(root)]
    while pending:
        package_id = pending.pop()
        if package_id in reachable:
            continue
        reachable.add(package_id)
        node = nodes.get(package_id)
        if node:
            pending.extend(str(dep.get("pkg")) for dep in node.get("deps", []) if isinstance(dep, dict))
    return ({key: value for key, value in packages.items() if key in reachable},
            {key for key in reachable if key in nodes})


def _one(metadata: dict[str, Any], name: str) -> tuple[str, set[str]]:
    packages, node_ids = _reachable_nodes(metadata)
    matches = [(package_id, packages[package_id])
               for package_id in packages
               if packages[package_id].get("name") == name and package_id in node_ids]
    if len(matches) != 1:
        identities = [package_id for package_id, _ in matches]
        raise ValueError(f"expected one reachable {name} package identity; found {identities}")
    package_id, _ = matches[0]
    node = next(n for n in metadata["resolve"]["nodes"] if n.get("id") == package_id)
    return package_id, set(node.get("features", []))


def ort_link_graph_violations(metadata: dict[str, Any], mode: str) -> list[str]:
    """Validate features on the reachable ort package and its actual ort-sys edge."""
    errors: list[str] = []
    try:
        ort_id, ort_features = _one(metadata, "ort")
        packages, _ = _reachable_nodes(metadata)
        ort_node = next(n for n in metadata["resolve"]["nodes"] if n.get("id") == ort_id)
        # Cargo metadata uses deps[].name as the library alias and packages[] as the package identity.
        sys_edges = [str(dep.get("pkg")) for dep in ort_node.get("deps", [])
                     if isinstance(dep, dict) and dep.get("name") in ("ort_sys", "ort-sys")]
        sys_matches = [package_id for package_id in sys_edges
                       if package_id in packages and packages[package_id].get("name") == "ort-sys"]
        if len(sys_matches) != 1:
            raise ValueError(f"ort package {ort_id} must point to one reachable ort-sys package; found {sys_matches}")
        sys_id = sys_matches[0]
        sys_node = next(n for n in metadata["resolve"]["nodes"] if n.get("id") == sys_id)
        sys_features = set(sys_node.get("features", []))
    except (ValueError, StopIteration) as error:
        return [f"{mode} Cargo graph: {error}"]

    def require(package: str, package_id: str, features: set[str], feature: str) -> None:
        if feature not in features:
            errors.append(f"{mode} {package} {package_id} features {sorted(features)}: missing {feature}")

    def reject(package: str, package_id: str, features: set[str], forbidden: tuple[str, ...]) -> None:
        for feature in forbidden:
            if feature in features:
                errors.append(f"{mode} {package} {package_id} features {sorted(features)}: forbidden {feature}")

    if mode == "bundled":
        require("ort", ort_id, ort_features, "download-binaries")
        require("ort-sys", sys_id, sys_features, "download-binaries")
        reject("ort-sys", sys_id, sys_features, ("disable-linking",))
        reject("ort", ort_id, ort_features, ("alternative-backend",))
    elif mode == "dynamic":
        require("ort", ort_id, ort_features, ORT_REQUIRED)
        reject("ort", ort_id, ort_features, ORT_FORBIDDEN)
        require("ort-sys", sys_id, sys_features, "disable-linking")
        reject("ort-sys", sys_id, sys_features, SYS_FORBIDDEN_DYNAMIC)
    else:
        errors.append(f"unknown ORT link mode {mode!r}")
    return errors


def check_dynamic_metadata(metadata: dict[str, Any], expect_inventory: str | None = None) -> list[str]:
    errors = ort_link_graph_violations(metadata, "dynamic")
    try:
        magika_id, magika_features = _one(metadata, "qubit-magika")
    except ValueError as error:
        errors.append(f"dynamic Cargo graph: {error}")
    else:
        if "bundled-onnxruntime" in magika_features:
            errors.append(
                f"dynamic qubit-magika {magika_id} features {sorted(magika_features)}: "
                "forbidden bundled-onnxruntime"
            )
        if expect_inventory is not None:
            if expect_inventory not in ("enabled", "disabled"):
                errors.append(f"invalid expected inventory state {expect_inventory!r}")
            elif ("inventory" in magika_features) != (expect_inventory == "enabled"):
                expectation = "enabled" if expect_inventory == "enabled" else "disabled"
                errors.append(
                    f"dynamic qubit-magika {magika_id} features {sorted(magika_features)}: "
                    f"expected inventory {expectation}"
                )
        root_id = metadata.get("resolve", {}).get("root")
        root_node = next((node for node in metadata["resolve"]["nodes"] if node.get("id") == root_id), None)
        normal_magika_edges = [
            dep for dep in (root_node or {}).get("deps", [])
            if isinstance(dep, dict) and str(dep.get("pkg")) == magika_id
            and any(isinstance(kind, dict) and "kind" in kind and kind["kind"] is None
                    for kind in dep.get("dep_kinds", []))
        ]
        if len(normal_magika_edges) != 1:
            errors.append(
                f"dynamic root {root_id} must have one normal dependency edge to qubit-magika "
                f"{magika_id}; found {len(normal_magika_edges)}"
            )
    return errors


def check_candidate_sources(metadata: dict[str, Any], expectations: list[str]) -> list[str]:
    """Require one reachable package identity at each expected local candidate path."""
    errors: list[str] = []
    for expectation in expectations:
        try:
            name, expected_path = expectation.split("=", 1)
            if not name or not expected_path:
                raise ValueError
            expected_manifest = (
                (Path(expected_path) / "Cargo.toml").resolve()
                if Path(expected_path).is_dir()
                else Path(expected_path).resolve()
            )
            packages, node_ids = _reachable_nodes(metadata)
            matches = [
                package
                for package_id, package in packages.items()
                if package.get("name") == name and package_id in node_ids
            ]
            if len(matches) != 1:
                raise ValueError(f"expected one reachable {name} package identity; found {len(matches)}")
            actual_manifest = Path(str(matches[0].get("manifest_path", ""))).resolve()
            if actual_manifest != expected_manifest:
                raise ValueError(
                    f"{name} package candidate is {actual_manifest}; expected {expected_manifest}"
                )
        except (ValueError, OSError) as error:
            errors.append(f"candidate source check {expectation!r}: {error or 'expected NAME=MANIFEST_PATH'}")
    return errors


def dynamic_ort_feature_violations(features: set[str] | list[str] | tuple[str, ...]) -> list[str]:
    enabled = set(features)
    errors = []
    if ORT_REQUIRED not in enabled:
        errors.append(f"missing required ort feature {ORT_REQUIRED}")
    errors.extend(f"forbidden ort feature enabled: {feature}" for feature in ORT_FORBIDDEN if feature in enabled)
    return errors


def run_self_tests() -> None:
    def graph(ort_features: list[str], sys_features: list[str]) -> dict[str, Any]:
        ids = {
            "app": "path+app#0.1.0",
            "magika": "path+qubit-magika#0.18",
            "ort": "registry+ort#2",
            "sys": "registry+ort-sys#2",
        }
        names = {"app": "qubit-app", "magika": "qubit-magika", "ort": "ort", "sys": "ort-sys"}
        packages = [{"id": ids[key], "name": names[key]} for key in ids]
        nodes = [
            {
                "id": ids["app"],
                "features": [],
                "deps": [
                    {"name": "ort", "pkg": ids["ort"], "dep_kinds": [{"kind": None, "target": None}]},
                    {
                        "name": "qubit_magika",
                        "pkg": ids["magika"],
                        "dep_kinds": [{"kind": None, "target": None}],
                    },
                ],
            },
            {"id": ids["magika"], "features": [], "deps": []},
            {
                "id": ids["ort"],
                "features": ort_features,
                "deps": [{"name": "ort_sys", "pkg": ids["sys"], "dep_kinds": [{"kind": None, "target": None}]}],
            },
            {"id": ids["sys"], "features": sys_features, "deps": []},
        ]
        return {"packages": packages, "resolve": {"root": ids["app"], "nodes": nodes}}

    dynamic = graph(["load-dynamic"], ["disable-linking"])
    assert not check_dynamic_metadata(dynamic, "disabled")
    inventory_graph = graph(["load-dynamic"], ["disable-linking"])
    next(node for node in inventory_graph["resolve"]["nodes"] if node["id"] == "path+qubit-magika#0.18")["features"].append("inventory")
    assert not check_dynamic_metadata(inventory_graph, "enabled")
    assert check_dynamic_metadata(dynamic, "enabled")
    assert check_dynamic_metadata(inventory_graph, "disabled")
    assert not ort_link_graph_violations(graph(["download-binaries"], ["download-binaries"]), "bundled")
    alternative_backend = graph(["load-dynamic", "alternative-backend"], ["disable-linking"])
    errors = check_dynamic_metadata(alternative_backend)
    assert errors and "forbidden alternative-backend" in " ".join(errors)
    for invalid in (
        graph(["load-dynamic"], ["disable-linking", "download-binaries"]),
        graph(["load-dynamic"], ["disable-linking", "tls-rustls"]),
    ):
        errors = check_dynamic_metadata(invalid)
        assert errors and "ort-sys" in " ".join(errors) and "features" in " ".join(errors)
    assert "alternative-backend" in " ".join(ort_link_graph_violations(graph(["download-binaries", "alternative-backend"], ["download-binaries"]), "bundled"))
    missing = graph(["load-dynamic"], ["disable-linking"])
    missing["packages"] = [p for p in missing["packages"] if p["name"] != "ort-sys"]
    errors = check_dynamic_metadata(missing)
    assert errors and "ort-sys" in " ".join(errors)
    ambiguous = graph(["load-dynamic"], ["disable-linking"])
    ambiguous["packages"].append({"id": "registry+ort#duplicate", "name": "ort"})
    ambiguous["resolve"]["nodes"].append({"id": "registry+ort#duplicate", "features": ["load-dynamic"], "deps": []})
    ambiguous["resolve"]["nodes"][0]["deps"].append({
        "name": "ort", "pkg": "registry+ort#duplicate", "dep_kinds": [{"kind": None, "target": None}]
    })
    errors = check_dynamic_metadata(ambiguous)
    assert errors and "expected one reachable ort package identity" in " ".join(errors)
    bundled_magika = graph(["load-dynamic"], ["disable-linking"])
    magika_node = next(
        node for node in bundled_magika["resolve"]["nodes"] if node["id"] == "path+qubit-magika#0.18"
    )
    magika_node["features"].append("bundled-onnxruntime")
    errors = check_dynamic_metadata(bundled_magika)
    assert errors and "bundled-onnxruntime" in " ".join(errors)
    missing_magika = graph(["load-dynamic"], ["disable-linking"])
    missing_magika["resolve"]["nodes"][0]["deps"] = [
        dep for dep in missing_magika["resolve"]["nodes"][0]["deps"] if dep["pkg"] != "path+qubit-magika#0.18"
    ]
    errors = check_dynamic_metadata(missing_magika)
    assert errors and "expected one reachable qubit-magika package identity" in " ".join(errors)
    candidate = graph(["load-dynamic"], ["disable-linking"])
    for package in candidate["packages"]:
        package["manifest_path"] = f"/candidate/{package['name']}/Cargo.toml"
    assert not check_candidate_sources(candidate, ["qubit-magika=/candidate/qubit-magika/Cargo.toml"])
    assert "candidate is" in " ".join(check_candidate_sources(
        candidate, ["qubit-magika=/wrong/qubit-magika/Cargo.toml"]
    ))
    assert "expected one reachable" in " ".join(
        check_candidate_sources(
            candidate,
            [
                "qubit-magika=/candidate/qubit-magika/Cargo.toml",
                "qubit-mime=/candidate/qubit-mime/Cargo.toml",
            ],
        )
    )
    for kind in ("dev", "build"):
        wrong_kind = graph(["load-dynamic"], ["disable-linking"])
        wrong_kind["resolve"]["nodes"][0]["deps"][1]["dep_kinds"] = [{"kind": kind, "target": None}]
        errors = check_dynamic_metadata(wrong_kind)
        assert errors and "one normal dependency edge" in " ".join(errors)
    missing_kind = graph(["load-dynamic"], ["disable-linking"])
    missing_kind["resolve"]["nodes"][0]["deps"][1]["dep_kinds"] = [{"target": None}]
    errors = check_dynamic_metadata(missing_kind)
    assert errors and "one normal dependency edge" in " ".join(errors)
