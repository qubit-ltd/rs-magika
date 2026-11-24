"""Shared ORT feature policy for dynamic-loader checks."""

from __future__ import annotations


REQUIRED = "load-dynamic"
FORBIDDEN = (
    "download-binaries",
    "copy-dylibs",
    "download-binaries-check",
    "tls-rustls",
    "tls-rustls-no-provider",
)


def violations(features: set[str] | list[str] | tuple[str, ...]) -> list[str]:
    """Return missing or forbidden ORT features for dynamic mode."""
    enabled = set(features)
    errors: list[str] = []
    if REQUIRED not in enabled:
        errors.append(f"missing required ort feature {REQUIRED}")
    errors.extend(f"forbidden ort feature enabled: {feature}" for feature in FORBIDDEN if feature in enabled)
    return errors
