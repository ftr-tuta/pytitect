#!/usr/bin/env python3
"""Install a built wheel in clean environments for each supported extras boundary."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

COMBINATIONS = {
    "": "import pytitect",
    "django": "import pytitect.django",
    "drf": "import pytitect.drf",
    "contracts": "import pytitect.contracts.spectacular",
    "canonical-json": "from pytitect.security import canonical_json; canonical_json({'ok': True})",
    "dpop": "from pytitect.security import DPoPVerifier",
    "signed-http": "from pytitect.security import HttpMessageSignaturesBackend",
    "security": "import pytitect.security",
    "drf,contracts": "import pytitect.drf, pytitect.contracts.spectacular",
}


def run(command: list[str]) -> None:
    subprocess.run(command, check=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=Path)
    args = parser.parse_args()
    wheel = args.wheel.resolve(strict=True)
    for extra, adapter_import in COMBINATIONS.items():
        with tempfile.TemporaryDirectory(prefix="pytitect-smoke-") as directory:
            environment = Path(directory)
            run([sys.executable, "-m", "venv", str(environment)])
            python = environment / "bin" / "python"
            target = f"{wheel}[{extra}]" if extra else str(wheel)
            install = [str(python), "-m", "pip", "install", target]
            if not extra:
                install.insert(4, "--no-deps")
            run(install)
            run(
                [
                    str(python),
                    "-I",
                    "-c",
                    (
                        "import pytitect; assert pytitect.__version__ == '0.9.0a1'; "
                        f"{adapter_import}"
                    ),
                ]
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
