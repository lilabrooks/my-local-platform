#!/usr/bin/env python3
"""Run a command with selected values resolved by Docker Compose's dotenv parser."""

from __future__ import annotations

import os
from pathlib import Path
import re
import subprocess
import sys
from typing import NoReturn


def fail(message: str) -> NoReturn:
    raise SystemExit(message)


def main() -> None:
    try:
        separator = sys.argv.index("--")
    except ValueError:
        fail("usage: with-compose-env.py [--chdir DIR] KEY... -- COMMAND...")

    arguments = sys.argv[1:separator]
    command = sys.argv[separator + 1 :]
    if not command:
        fail("with-compose-env.py: COMMAND is required")

    chdir = "."
    if arguments[:1] == ["--chdir"]:
        if len(arguments) < 3:
            fail("with-compose-env.py: --chdir needs a directory and at least one KEY")
        chdir = arguments[1]
        arguments = arguments[2:]

    if not arguments:
        fail("with-compose-env.py: at least one KEY is required")
    invalid = [key for key in arguments if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key)]
    if invalid:
        fail(f"with-compose-env.py: invalid environment key: {invalid[0]}")

    root = Path(__file__).resolve().parent.parent
    env_file = Path(os.environ.get("MLP_ENV_FILE", ".env"))
    if not env_file.is_absolute():
        env_file = root / env_file
    if not env_file.is_file():
        fail(f"with-compose-env.py: dotenv file not found: {env_file}")

    compose = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(env_file),
            "-f",
            str(root / "local/docker-compose.yml"),
            "config",
            "--environment",
        ],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if compose.returncode != 0:
        detail = compose.stderr.strip()
        suffix = f": {detail}" if detail else ""
        fail(f"with-compose-env.py: Docker Compose could not read {env_file}{suffix}")

    resolved: dict[str, str] = {}
    for line in compose.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator and key in arguments:
            resolved[key] = value

    child_env = os.environ.copy()
    for key in arguments:
        child_env.pop(key, None)
    child_env.update(resolved)

    workdir = Path(chdir)
    if not workdir.is_absolute():
        workdir = root / workdir
    os.chdir(workdir)
    os.execvpe(command[0], command, child_env)


if __name__ == "__main__":
    main()
