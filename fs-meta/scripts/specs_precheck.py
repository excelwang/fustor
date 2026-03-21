#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from pathlib import Path


CANONICAL_FRONT_MATTER = re.compile(r"^---\nversion:\s+[0-9]+\.[0-9]+\.[0-9]+\n---\n\n# ", re.MULTILINE)
RFC2119 = re.compile(r"\b(MUST|SHOULD|SHALL|MAY|MUST NOT|SHOULD NOT|SHALL NOT)\b")


def fail(msg: str) -> None:
    print(f"ERROR: {msg}")


def main() -> int:
    specs_root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("specs")
    errors: list[str] = []

    formal_specs = [
        specs_root / "L0-GLOSSARY.md",
        specs_root / "L0-VISION.md",
        specs_root / "L1-CONTRACTS.md",
        specs_root / "L2-ARCHITECTURE.md",
        specs_root / "L3-RUNTIME" / "API_HTTP.md",
        specs_root / "L3-RUNTIME" / "WORKFLOWS.md",
        specs_root / "L3-RUNTIME" / "OBSERVATION_CUTOVER.md",
        specs_root / "L3-RUNTIME" / "WORKER_RUNTIME_SUPPORT.md",
    ]

    for path in formal_specs:
        text = path.read_text(encoding="utf-8")
        if not CANONICAL_FRONT_MATTER.search(text):
            errors.append(f"{path.name}: non-canonical front matter")

    glossary = (specs_root / "L0-GLOSSARY.md").read_text(encoding="utf-8")
    if RFC2119.search(glossary):
        errors.append("L0-GLOSSARY.md: glossary must not contain RFC2119 normative language")
    for banned_term in [
        "Sync-Refresh Update",
        "Write-Significant Update",
        "Metadata Mode Axis",
        "Stability Evaluation Axis",
        "PIT Query Pagination Axis",
        "`pit_id`",
        "`group_page_size`",
        "`entry_page_size`",
    ]:
        if banned_term in glossary:
            errors.append(f"L0-GLOSSARY.md: glossary still contains implementation-rule term `{banned_term}`")

    vision = (specs_root / "L0-VISION.md").read_text(encoding="utf-8")
    worker_artifact_suffixes = ("source", "sink", "scan", "facade")
    for banned_term in [
        "capanix-app-sdk",
        "capanix-runtime-api",
        "capanix-kernel-api",
        "runtime-support",
        "statecell",
        "pit_id",
        "group_page_size",
        "entry_page_size",
        "path_b64",
        "`embedded | external`",
        "Cargo.toml",
    ]:
        if banned_term in vision:
            errors.append(f"L0-VISION.md: implementation-facing term `{banned_term}` must stay below L0")
    for suffix in worker_artifact_suffixes:
        artifact_term = "worker-" + suffix
        if artifact_term in vision:
            errors.append(
                f"L0-VISION.md: implementation-facing term `{artifact_term}` must stay below L0"
            )

    l2 = (specs_root / "L2-ARCHITECTURE.md").read_text(encoding="utf-8")
    for banned_section in [
        "## ARCHITECTURE.REPOSITORY_TOPOLOGY",
        "## ARCHITECTURE.CRATE_OWNERSHIP",
        "## ARCHITECTURE.DEPENDENCY_RULES",
    ]:
        if banned_section in l2:
            errors.append(f"L2-ARCHITECTURE.md: governance section `{banned_section}` must live outside formal specs")

    if errors:
        for err in errors:
            fail(err)
        return 1

    print("OK: repo-local specs precheck passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
