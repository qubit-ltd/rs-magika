"""Private Cargo.lock helpers shared by the fixed-source example checkers."""

from __future__ import annotations

from pathlib import Path


def lock_path(directory: Path, script: str, language: str, config: str) -> Path:
    return directory / script / language / config / "Cargo.lock"


def prepare_lock(
    manifest: Path,
    replay_dir: Path | None,
    record_dir: Path | None,
    script: str,
    language: str,
    config: str,
) -> tuple[list[str], Path | None]:
    destination = (
        lock_path(replay_dir or record_dir, script, language, config)
        if replay_dir is not None or record_dir is not None
        else None
    )
    if replay_dir is not None:
        assert destination is not None
        if not destination.is_file():
            raise ValueError(f"missing replay lock for {language}/{config}: {destination}")
        (manifest.parent / "Cargo.lock").write_bytes(destination.read_bytes())
        return ["--locked"], None
    return [], destination


def finish_lock(manifest: Path, destination: Path | None) -> None:
    if destination is not None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((manifest.parent / "Cargo.lock").read_bytes())
