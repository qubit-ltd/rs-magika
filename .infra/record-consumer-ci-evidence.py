#!/usr/bin/env python3
"""Write a small provenance record for bundled documentation consumer locks."""

from __future__ import annotations

import argparse
import contextlib
import io
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch


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


def observe_locks(script: str, runs: list[Path]) -> tuple[Path | None, list[Path], list[Path]]:
    """Observe a run directory and its lock completeness exactly once."""
    run = runs[0] if len(runs) == 1 else None
    present: list[Path] = []
    missing = sorted(expected_locks(script))
    if run is not None:
        present = [path for path in missing if (run / path).is_file() and (run / path).stat().st_size > 0]
        missing = [path for path in missing if path not in present]
    return run, present, missing


def replay_command(script: str, run: Path | None, missing: list[Path], target: Path) -> str | None:
    if run is None or missing:
        return None
    replay_dir = run.relative_to(target)
    if script == "readme":
        command = ["python3", "-B", ".infra/check-readme-examples.py", "--run-example", "--replay-locks", str(replay_dir)]
    else:
        command = ["python3", "-B", ".infra/check-managed-guide-examples.py", "--replay-locks", str(replay_dir)]
    return shlex.join(command)


def self_test() -> None:
    from tempfile import TemporaryDirectory

    with TemporaryDirectory(prefix="consumer-evidence-self-test-") as temporary:
        root = Path(temporary)
        project = root / "project"
        project.mkdir()
        dependency_table = project / "dependencies.tsv"
        rows = [f"dep-{index}\tqubit-dep-{index}\t{'a' * 40}" for index in range(7)]
        dependency_table.write_text("\n".join(rows) + "\n", encoding="utf-8")
        def fake_command_output(*command: str) -> str:
            if len(command) >= 3 and command[:2] == ("git", "-C"):
                return "a" * 40 if Path(command[2]).name.startswith("dep-") else "unavailable (exit 1): missing fixture"
            if command[:3] == ("git", "rev-parse", "HEAD"):
                return "b" * 40
            if command[:3] == ("cargo", "+1.94.0", "--version"):
                return "cargo 1.94.0"
            if command[:3] == ("rustc", "+1.94.0", "--version"):
                return "rustc 1.94.0"
            if command[:3] == ("rustc", "+1.94.0", "-vV"):
                return "host: x86_64-unknown-linux-gnu"
            return "Linux x86_64"

        def fill_locks(script: str, run: Path, empty: Path | None = None) -> None:
            for lock in expected_locks(script):
                destination = run / lock
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text("" if lock == empty else "lock", encoding="utf-8")

        def collect(project_root: Path, readme_exit: int, guide_exit: int) -> tuple[int, str, str, str]:
            output_file = root / "github-output.txt"
            output_file.write_text("", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()
            with patch(__name__ + ".PROJECT_ROOT", project_root), patch(
                __name__ + ".DEPENDENCY_TABLE", dependency_table
            ), patch(__name__ + ".command_output", fake_command_output), patch.dict(
                os.environ, {"GITHUB_OUTPUT": str(output_file)}
            ), patch.object(sys, "argv", ["record-consumer-ci-evidence.py", "--readme-exit", str(readme_exit), "--guide-exit", str(guide_exit)]):
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    code = main()
            report = (project_root / "target/consumer-ci-evidence/source.txt").read_text(encoding="utf-8")
            upload = output_file.read_text(encoding="utf-8")
            return code, report, upload, stderr.getvalue()

        def fixture(name: str) -> Path:
            project_root = root / name
            project_root.mkdir()
            return project_root

        # Complete output uses target as the archive root and shell-quotes paths.
        project_root = fixture("complete")
        target = project_root / "target"
        readme_run = target / "readme-example-check/lock-records/run 'quoted"
        guide_run = target / "managed-guide-lock-records/guide-run"
        fill_locks("readme", readme_run)
        fill_locks("managed-guide", guide_run)
        code, report, upload, _ = collect(project_root, 0, 0)
        assert code == 0 and "status: complete" in report, (code, report)
        assert "quoted" in report
        for command in (line for line in report.splitlines() if line.startswith("python3 -B ")):
            arguments = shlex.split(command)
            replay_path = Path(arguments[arguments.index("--replay-locks") + 1])
            assert (project_root / "target" / replay_path).is_dir()
            assert all((project_root / "target" / replay_path / lock).stat().st_size > 0 for lock in expected_locks("readme" if "check-readme" in command else "managed-guide"))
        assert "sources/rs-magika/target/consumer-ci-evidence/source.txt" in upload
        assert "sources/rs-magika/target/readme-example-check/lock-records/run 'quoted" in upload
        assert "sources/rs-magika/target/managed-guide-lock-records/guide-run" in upload

        # No locks produces diagnostics and uploads only the source report.
        project_root = fixture("no-locks")
        code, report, upload, _ = collect(project_root, 7, 9)
        assert code == 0 and "status: incomplete" in report
        assert "replay command omitted; lock directory missing" in report and "<missing>" not in report
        assert "sources/rs-magika/target/consumer-ci-evidence/source.txt" in upload
        assert "lock-records" not in upload and "managed-guide-lock-records" not in upload

        # One checker can be incomplete while the other retains its replay command.
        project_root = fixture("one-sided")
        good_guide = project_root / "target/managed-guide-lock-records/guide-run"
        fill_locks("managed-guide", good_guide)
        bad_readme = project_root / "target/readme-example-check/lock-records/readme-run"
        fill_locks("readme", bad_readme, sorted(expected_locks("readme"))[0])
        code, report, upload, _ = collect(project_root, 0, 0)
        assert code == 1 and "status: incomplete" in report
        readme_line = next(line for line in report.splitlines() if line.startswith("readme: replay command omitted"))
        guide_line = next(line for line in report.splitlines() if line.startswith("python3 -B .infra/check-managed-guide-examples.py"))
        assert "replay command omitted; missing or empty locks" in readme_line
        assert "python3 -B .infra/check-managed-guide-examples.py" in guide_line
        assert "missing_readme_lock:" in report and "managed-guide-lock-records/guide-run" in upload

        # Multiple directories suppress only the ambiguous checker command.
        project_root = fixture("ambiguous")
        for name in ("run-a", "run-b"):
            run = project_root / f"target/readme-example-check/lock-records/{name}"
            fill_locks("readme", run)
        guide = project_root / "target/managed-guide-lock-records/guide-run"
        fill_locks("managed-guide", guide)
        code, report, _, _ = collect(project_root, 0, 0)
        assert code == 1 and "multiple lock directories found" in report
        commands = [line for line in report.splitlines() if line.startswith("python3 -B ")]
        assert len(commands) == 1 and "check-managed-guide-examples.py" in commands[0]

        # A failed checker remains replayable if it saved all locks; the report stays incomplete.
        project_root = fixture("failed-but-locked")
        readme = project_root / "target/readme-example-check/lock-records/readme-run"
        guide = project_root / "target/managed-guide-lock-records/guide-run"
        fill_locks("readme", readme)
        fill_locks("managed-guide", guide)
        code, report, _, _ = collect(project_root, 7, 9)
        assert code == 0 and "status: incomplete" in report
        assert "python3 -B .infra/check-readme-examples.py --run-example" in report
        assert "python3 -B .infra/check-managed-guide-examples.py" in report

        # Successful checks missing even an empty lock remain a hard error.
        project_root = fixture("success-missing-lock")
        readme = project_root / "target/readme-example-check/lock-records/readme-run"
        fill_locks("readme", readme, sorted(expected_locks("readme"))[0])
        code, report, _, stderr = collect(project_root, 0, 9)
        assert code == 1 and "status: incomplete" in report
        assert "successful readme check has missing or empty locks" in stderr
        assert "replay command omitted; missing or empty locks" in report


def replay_diagnostic(script: str, runs: list[Path], run: Path | None, missing: list[Path], target: Path) -> str:
    label = "readme" if script == "readme" else "managed-guide"
    if len(runs) > 1:
        return f"{label}: replay command omitted; multiple lock directories found"
    if run is None:
        return f"{label}: replay command omitted; lock directory missing"
    if missing:
        details = ", ".join(str(path) for path in missing)
        return f"{label}: replay command omitted; missing or empty locks: {details}"
    return replay_command(script, run, missing, target) or ""


def artifact_paths(readme_runs: list[Path], guide_runs: list[Path], target: Path) -> list[str]:
    paths = ["sources/rs-magika/target/consumer-ci-evidence/source.txt"]
    if len(readme_runs) == 1:
        paths.append(f"sources/rs-magika/target/{readme_runs[0].relative_to(target)}")
    if len(guide_runs) == 1:
        paths.append(f"sources/rs-magika/target/{guide_runs[0].relative_to(target)}")
    return paths


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
    parser.add_argument("--self-test", action="store_true", help="run isolated artifact-layout checks")
    parser.add_argument("--readme-exit", type=int)
    parser.add_argument("--guide-exit", type=int)
    args = parser.parse_args()

    if args.self_test:
        self_test()
        print("consumer evidence layout self-tests passed")
        return 0
    if args.readme_exit is None or args.guide_exit is None:
        parser.error("--readme-exit and --guide-exit are required unless --self-test is used")

    target = PROJECT_ROOT / "target"
    readme_runs = run_directories(target / "readme-example-check/lock-records")
    guide_runs = run_directories(target / "managed-guide-lock-records")
    errors: list[str] = []
    dependencies, dependency_errors = dependency_revisions()
    errors.extend(dependency_errors)

    reports: list[str] = []
    observations: dict[str, tuple[Path | None, list[Path], list[Path]]] = {}
    run_info = (
        ("readme", args.readme_exit, readme_runs),
        ("managed-guide", args.guide_exit, guide_runs),
    )
    for script, check_exit, runs in run_info:
        run, present, missing = observe_locks(script, runs)
        observations[script] = (run, present, missing)
        validate_run_directory(script, check_exit, runs, errors)
        reports.append(f"{script}_check_exit: {check_exit}")
        reports.append(f"{script}_lock_directory: {run.relative_to(target) if run else 'missing'}")
        if run and check_exit == 0 and missing:
            errors.append(f"successful {script} check has missing or empty locks: {', '.join(map(str, missing))}")
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
    readme_run, _, readme_missing = observations["readme"]
    guide_run, _, guide_missing = observations["managed-guide"]
    readme_replay = replay_diagnostic("readme", readme_runs, readme_run, readme_missing, target)
    guide_replay = replay_diagnostic("managed-guide", guide_runs, guide_run, guide_missing, target)
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
        readme_replay,
        guide_replay,
    ]
    if errors:
        report.extend(("evidence_errors:", *(f"- {error}" for error in errors)))
    (evidence_dir / "source.txt").write_text("\n".join(report) + "\n", encoding="utf-8")

    upload_paths = artifact_paths(readme_runs, guide_runs, target)
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with Path(github_output).open("a", encoding="utf-8") as output:
            output.write("evidence_present=true\n")
            output.write("artifact_paths<<CONSUMER_EVIDENCE_PATHS\n")
            output.write("\n".join(upload_paths) + "\n")
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
