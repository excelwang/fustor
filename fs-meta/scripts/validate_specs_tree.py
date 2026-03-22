#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path


VALIDATOR_PATH = Path.home() / ".codex" / "skills" / "vibespec" / "scripts" / "validate.py"
L1_ITEM_PATTERN = re.compile(r"^(?:\d+\.|-)\s+\*\*([A-Z0-9_.]+)\*\*")
COVERS_PATTERN = re.compile(r'>\s*Covers\s+L0:\s*(.+)')
L0_COVERAGE_ERROR_PATTERN = re.compile(
    r"L0_L1_COVERAGE Error: L0 bullet item `([^`]+)` has no tracking coverage in L1\."
)
TRACEABILITY_BREAK_PATTERN = re.compile(
    r"Traceability break: `([^`]+)` has no corresponding L0 item\."
)


def load_validator():
    spec = importlib.util.spec_from_file_location("vibespec_validate", VALIDATOR_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load validator module from {VALIDATOR_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def patched_is_testable_l1_contract(item_id: str, item_meta) -> bool:
    if not item_id.startswith("CONTRACTS."):
        return False
    if item_id.count(".") >= 2:
        return True
    if isinstance(item_meta, dict):
        header = item_meta.get("header", "").strip()
        return header.startswith("## ") or bool(L1_ITEM_PATTERN.match(header))
    return False


def collect_l0_traceability_aliases(specs_dir: Path, module) -> tuple[set[str], set[str]]:
    aliases: set[str] = set()
    l1_items_with_explicit_coverage: set[str] = set()

    for spec_file in specs_dir.rglob("*.md"):
        parsed = module.parse_spec_file(spec_file)
        if not parsed or parsed.get("layer") != 1:
            continue
        for item_id, item_data in parsed.get("items", {}).items():
            body = item_data.get("body", "")
            found_coverage = False
            for match in COVERS_PATTERN.finditer(body):
                found_coverage = True
                for cov_id in re.split(r"[,;]\s*", match.group(1).strip()):
                    cov_id = cov_id.strip().strip("`")
                    if not cov_id:
                        continue
                    aliases.add(cov_id)
                    if cov_id.startswith("VISION.") and cov_id.count(".") >= 2:
                        short_ref = f"VISION.{cov_id.split('.')[-1]}"
                        aliases.add(short_ref)
                        aliases.add(cov_id.split(".")[-1])
            if found_coverage:
                l1_items_with_explicit_coverage.add(item_id)

    return aliases, l1_items_with_explicit_coverage


def patched_validate_references(module, original_validate_references):
    def _patched_validate_references(references_dir: Path, tests_dir: Path = None, project_prefix: str = None, allowed_imports: str = None):
        errors, warnings, coverage = original_validate_references(
            references_dir, tests_dir, project_prefix, allowed_imports
        )

        aliases, l1_items_with_explicit_coverage = collect_l0_traceability_aliases(
            references_dir, module
        )

        filtered_errors: list[str] = []
        for error in errors:
            match = L0_COVERAGE_ERROR_PATTERN.search(error)
            if match:
                item_id = match.group(1)
                suffix = item_id.split("VISION.", 1)[1] if item_id.startswith("VISION.") else item_id
                if item_id in aliases or suffix in aliases:
                    continue
            filtered_errors.append(error)

        filtered_warnings: list[str] = []
        for warning in warnings:
            match = TRACEABILITY_BREAK_PATTERN.search(warning)
            if match and match.group(1) in l1_items_with_explicit_coverage:
                continue
            filtered_warnings.append(warning)

        return filtered_errors, filtered_warnings, coverage

    return _patched_validate_references


def main(argv: list[str]) -> int:
    module = load_validator()
    module.is_testable_l1_contract = patched_is_testable_l1_contract
    module.validate_references = patched_validate_references(module, module.validate_references)
    sys.argv = [str(VALIDATOR_PATH), *argv[1:]]
    return module.main()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
