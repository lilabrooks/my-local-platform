"""Manage just the disposable Compose project for the database MCP example."""

import argparse
import hashlib
import fcntl
import os
import secrets
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "local/tmp/db-mcp"
PROJECT = "mlp-db-mcp-" + hashlib.sha256(str(ROOT).encode()).hexdigest()[:10]
CREDS = ("pg-owner", "pg-reader", "my-owner", "my-reader", "mg-owner", "mg-reader")


def create_secret(path):
    # Publish only a complete value, even if two sessions start the demo together.
    with tempfile.NamedTemporaryFile(dir=STATE, mode="w", delete=False) as stream:
        stream.write(secrets.token_hex(24))
    try:
        os.link(stream.name, path)
    except FileExistsError:
        pass
    finally:
        Path(stream.name).unlink()


def run(action):
    env = os.environ.copy()
    for name in CREDS:
        path = STATE / name
        if action == "up" and not path.exists():
            create_secret(path)
        # Compose interpolates secrets even when only removing its own project.
        env["METER_" + name.replace("-", "_").upper()] = path.read_text() if path.exists() else "unused"
    compose = ["docker", "compose", "-p", PROJECT, "-f", str(ROOT / "local/docker-compose.db-mcp.yml")]
    args = ["up", "-d", "--wait", "--wait-timeout", "240"] if action == "up" else ["down", "--volumes"]
    subprocess.run([*compose, *args], env=env, check=True)
    if action == "down":
        for name in CREDS:
            (STATE / name).unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["up", "down"])
    action = parser.parse_args().action
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (STATE / ".lifecycle").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        run(action)


if __name__ == "__main__":
    main()
