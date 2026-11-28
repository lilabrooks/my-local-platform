import json
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock, patch

import anyio
from mcp import Client
from mcp.client.stdio import StdioServerParameters, stdio_client

from platform_db_mcp import adapters
from platform_db_mcp.catalog import load

DEMO = Path(__file__).resolve().parents[1] / "demo.toml"


class DiscoveryTests(unittest.TestCase):
    def test_empty_catalog_and_unknown_target_over_stdio(self):
        with tempfile.TemporaryDirectory() as scratch, tempfile.TemporaryFile(mode="w+") as logs:
            config = Path(scratch) / "empty.toml"
            config.write_text("targets = []\n")
            params = StdioServerParameters(command=str(Path(sys.executable).parent / "mlp-db-mcp"),
                                           args=["--catalog", str(config)])

            async def check():
                async with Client(stdio_client(params, errlog=logs)) as c:
                    targets = await c.call_tool("list_targets", {})
                    self.assertEqual(json.loads(targets.content[0].text)["targets"], [])
                    unknown = await c.call_tool("list_databases", {"target": "untrusted-input-sentinel"})
                    self.assertEqual(json.loads(unknown.content[0].text)["code"], "refused")
            anyio.run(check)
            logs.seek(0)
            self.assertNotIn("untrusted-input-sentinel", logs.read())

    def test_mysql_schema_cannot_redirect_queries_to_another_database(self):
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "catalog.toml"
            path.write_text(DEMO.read_text().replace('engine = "mysql"', 'engine = "mysql"\nschema = "other"'))
            with self.assertRaisesRegex(ValueError, "MySQL schema"):
                load(path)

    def test_mongo_explain_respects_the_database_read_preference(self):
        target = load(DEMO)["demo-mongo"]
        client = MagicMock()
        db = client.__enter__.return_value.__getitem__.return_value
        db.command.return_value = {"queryPlanner": {"winningPlan": {"stage": "COLLSCAN"},
                                                    "parsedQuery": {"stage": "private filter value"}}}
        with patch.object(adapters, "mongo_client", return_value=client):
            operators = adapters.mongo_result(target, "test-id", "readings", [], 1, explain=True)
        self.assertEqual(operators, {"operators": ["COLLSCAN"]})
        self.assertIs(db.command.call_args.kwargs["read_preference"], db.read_preference)

    def test_docdb_connection_options_keep_tls_and_placement_explicit(self):
        with tempfile.TemporaryDirectory() as scratch:
            password = Path(scratch) / "reader"
            password.write_text("fixture")
            target = replace(load(DEMO)["demo-mongo"], tls="verify", ca_file="/example/ca.pem",
                             password_file=password, auth_database="admin", replica_set="rs0",
                             read_preference="primary")
            with patch.object(adapters, "MongoClient") as constructor:
                adapters.mongo_client(target)
            options = constructor.call_args.kwargs
            self.assertTrue(options["tls"])
            self.assertEqual(options["tlsCAFile"], "/example/ca.pem")
            self.assertEqual(options["authSource"], "admin")
            self.assertEqual(options["replicaSet"], "rs0")
            self.assertEqual(options["readPreference"], "primary")
            self.assertFalse(options["retryWrites"])
