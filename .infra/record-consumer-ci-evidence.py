#!/usr/bin/env python3
"""Write a small provenance record for bundled documentation consumer locks."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEPENDENCY_TABLE = PROJECT_ROOT / ".infra/ci/local-path-dependencies.tsv"


def command_output(*command: str) -> str:
    result = subprocess.run(command, cwd=PROJECT_ROOT, capture_output=True, text=True, check=False)
    if result.returncode:
        return f"unavailable (exit {result.returncode}): {result.stderr.strip()}"
    return result.stdout.strip()


def run_directories(parent: Path) -> list[Path]:
    if not parent.is_dir():
        return []
    return sorted(path for path in parent.iterdir() if path.is_dir())


def validate_run_directory(script: str, check_exit: int, runs: list[Path], errors: list[str]) -> Path | None:
    if check_exit == 0 and not runs:
        errors.append(f"successful {script} check produced no lock run directory")
    if len(runs) > 1:
        errors.append(f"expected one {script} lock run directory, found {len(runs)}")
    return runs[0] if len(runs) == 1 else None


def validate_provenance(candidate: str, fields: dict[str, str], errors: list[str]) -> None:
    if not re.fullmatch(r"[0-9a-f]{40,64}", candidate):
        errors.append(f"candidate SHA is unavailable or malformed: {candidate}")
    for label, value in fields.items():
        if value.startswith("unavailable"):
            errors.append(f"required provenance field {label} is unavailable: {value}")


def expected_locks(script: str) -> set[Path]:
    if script == "readme":
        configs = ("default", "inventory")
    else:
        configs = ("managed", "default", "inventory", "dynamic")
    return {
        Path(script) / language / config / "Cargo.lock"
        for language in ("en", "zh")
        for config in configs
    }


def dependency_revisions() -> tuple[list[str], list[str]]:
    lines = DEPENDENCY_TABLE.read_text(encoding="utf-8").splitlines()
    entries: list[tuple[str, str, str]] = []
    for line in lines:
        if not line or line.startswith("#"):
            continue
        relative, repository, expected = line.split("\t")
        entries.append((relative, repository, expected))

    actual_lines: list[str] = []
    errors: list[str] = []
    if len(entries) != 7:
        errors.append(f"expected 7 pinned dependency rows, found {len(entries)}")
    for relative, repository, expected in entries:
        checkout = (PROJECT_ROOT / relative).resolve()
        actual = command_output("git", "-C", str(checkout), "rev-parse", "HEAD")
        actual_lines.append(f"{relative}\t{repository}\texpected={expected}\tactual={actual}")
        if actual != expected:
            errors.append(f"{relative}: expected {expected}, found {actual}")
    return actual_lines, errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--readme-exit", type=int, required=True)
    parser.add_argument("--guide-exit", type=int, required=True)
    args = parser.parse_args()

    target = PROJECT_ROOT / "target"
    readme_runs = run_directories(target / "readme-example-check/lock-records")
    guide_runs = run_directories(target / "managed-guide-lock-records")
    errors: list[str] = []
    dependencies, dependency_errors = dependency_revisions()
    errors.extend(dependency_errors)

    reports: list[str] = []
    run_info = (
        ("readme", args.readme_exit, readme_runs),
        ("managed-guide", args.guide_exit, guide_runs),
    )
    for script, check_exit, runs in run_info:
        run = validate_run_directory(script, check_exit, runs, errors)
        reports.append(f"{script}_check_exit: {check_exit}")
        reports.append(f"{script}_lock_directory: {run.relative_to(target) if run else 'missing'}")
        if run:
            missing = sorted(
                str(path)
                for path in expected_locks(script)
                if not (run / path).is_file() or (run / path).stat().st_size == 0
            )
            if check_exit == 0 and missing:
                errors.append(f"successful {script} check has missing or empty locks: {', '.join(missing)}")
            for path in missing:
                reports.append(f"missing_{script}_lock: {path}")

    candidate = command_output("git", "rev-parse", "HEAD")
    cargo = command_output("cargo", "+1.94.0", "--version")
    rustc = command_output("rustc", "+1.94.0", "--version")
    rust_host = command_output("rustc", "+1.94.0", "-vV")
    host_field = next((line for line in rust_host.splitlines() if line.startswith("host: ")), "unavailable")
    platform = command_output("uname", "-srm")
    validate_provenance(
        candidate,
        {"rustc +1.94.0": rustc, "cargo +1.94.0": cargo, "Rust host": host_field, "platform": platform},
        errors,
    )
    complete = args.readme_exit == 0 and args.guide_exit == 0 and not errors
    evidence_dir = target / "consumer-ci-evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    readme_dir = readme_runs[0].relative_to(target) if len(readme_runs) == 1 else Path("<missing>")
    guide_dir = guide_runs[0].relative_to(target) if len(guide_runs) == 1 else Path("<missing>")
    report = [
        f"status: {'complete' if complete else 'incomplete'}",
        f"candidate_sha: {candidate}",
        *reports,
        f"rustc: {rustc}",
        f"cargo: {cargo}",
        f"rust_host: {host_field}",
        f"platform: {platform}",
        "fixed_dependencies:",
        *dependencies,
        "replay_commands_from_project_root_after_extracting_artifact_into_project_root:",
        f"python3 -B .infra/check-readme-examples.py --run-example --replay-locks {readme_dir}",
        f"python3 -B .infra/check-managed-guide-examples.py --replay-locks {guide_dir}",
    ]
    if errors:
        report.extend(("evidence_errors:", *(f"- {error}" for error in errors)))
    (evidence_dir / "source.txt").write_text("\n".join(report) + "\n", encoding="utf-8")

    artifact_paths = ["sources/rs-magika/target/consumer-ci-evidence/source.txt"]
    if len(readme_runs) == 1:
        artifact_paths.append(f"sources/rs-magika/target/{readme_runs[0].relative_to(target)}")
    if len(guide_runs) == 1:
        artifact_paths.append(f"sources/rs-magika/target/{guide_runs[0].relative_to(target)}")
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with Path(github_output).open("a", encoding="utf-8") as output:
            output.write("evidence_present=true\n")
            output.write("artifact_paths<<CONSUMER_EVIDENCE_PATHS\n")
            output.write("\n".join(artifact_paths) + "\n")
            output.write("CONSUMER_EVIDENCE_PATHS\n")

    if errors:
        for error in errors:
            print(f"consumer evidence: {error}", file=sys.stderr)
        return 1
    if not complete:
        print("consumer checks failed; writing an incomplete lock record", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
