"""Operator-owned data exposure rules. Requests refer only to names in this file."""

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z_0-9]{0,62}\Z")
TARGET_NAME = re.compile(r"[a-z][a-z0-9-]{0,62}\Z")
ROWS = 50
BYTES = 32 * 1024
TIMEOUT = 15


class Refused(ValueError):
    """An error with a fixed, safe-to-display message."""


@dataclass(frozen=True)
class Target:
    name: str
    engine: str
    environment: str
    host: str
    port: int
    database: str
    schema: str
    username: str
    password_file: Path
    tls: str
    ca_file: str | None
    replica_set: str | None
    read_preference: str
    auth_database: str
    objects: dict[str, tuple[str, ...]]

    def fields(self, name: str) -> tuple[str, ...]:
        if name not in self.objects:
            raise Refused("Object is outside the catalog.")
        return self.objects[name]

    def password(self) -> str:
        return self.password_file.read_text(encoding="utf-8").rstrip("\r\n")


def load(path: Path) -> dict[str, Target]:
    with path.open("rb") as stream:
        data = tomllib.load(stream)
    catalog = {}
    for entry in data["targets"]:
        name = entry["name"]
        engine = entry["engine"]
        env = entry["environment"]
        if not TARGET_NAME.fullmatch(name) or name in catalog:
            raise ValueError("Target names must be unique lowercase identifiers.")
        if engine not in {"postgres", "mysql", "mongodb"} or env not in {"local", "dev", "test", "qa", "staging"}:
            raise ValueError("Unsupported engine or non-production environment.")
        objects = entry["objects"]
        if not isinstance(objects, dict) or not objects:
            raise ValueError("Every target needs an explicit object catalog.")
        for obj, fields in objects.items():
            if not isinstance(fields, list) or not fields or len(fields) != len(set(fields)):
                raise ValueError("Every object needs a list of unique fields.")
            if not all(IDENTIFIER.fullmatch(s) for s in (obj, *fields)):
                raise ValueError("Use simple identifiers for objects and fields.")
        database = entry["database"]
        schema = entry.get("schema", "public" if engine == "postgres" else database)
        if not all(IDENTIFIER.fullmatch(s) for s in (database, schema)):
            raise ValueError("Invalid database or schema identifier.")
        if engine == "mysql" and schema != database:
            raise ValueError("A MySQL schema must match its database.")
        tls = entry.get("tls", "verify")
        if tls not in {"verify", "local"}:
            raise ValueError("Unknown TLS mode.")
        if tls == "local" and (env != "local" or entry["host"] not in {"127.0.0.1", "::1"}):
            raise ValueError("Unencrypted access requires a literal loopback address and a local target.")
        replica_set = entry.get("replica_set")
        read_preference = entry.get("read_preference", "secondary" if replica_set else "primary")
        if read_preference not in {"primary", "secondary", "secondaryPreferred"}:
            raise ValueError("Invalid read preference.")
        ca = entry.get("ca_file")
        catalog[name] = Target(name, engine, env, entry["host"], int(entry["port"]), database, schema,
                               entry["username"], path.parent / entry["password_file"], tls,
                               str(path.parent / ca) if ca else None, replica_set, read_preference,
                               entry.get("auth_database", database), {k: tuple(v) for k, v in objects.items()})
    return catalog
