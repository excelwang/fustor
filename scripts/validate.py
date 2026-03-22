#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    root = Path(__file__).resolve().parents[1]
    if len(argv) > 1 and argv[1] not in {"specs", "specs/"}:
        print(
            "fustor routes gate spec validation through fs-meta/scripts/validate_specs.sh; "
            "expected target `specs/`.",
            file=sys.stderr,
        )
        return 2
    command = ["/bin/bash", str(root / "fs-meta" / "scripts" / "validate_specs.sh")]
    return subprocess.run(command, cwd=root, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
