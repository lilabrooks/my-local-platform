import json
import unittest
from pathlib import Path

from platform_db_mcp.catalog import BYTES, Refused, load
from platform_db_mcp.mongo_read import aggregation, match, projection
from platform_db_mcp.server import fit
from platform_db_mcp.sql_read import compile_select

TARGETS = load(Path(__file__).resolve().parents[1] / "demo.toml")


class ContractTests(unittest.TestCase):
    def test_compiled_sql_binds_values_on_both_engines(self):
        for target in (TARGETS["demo-mysql"], TARGETS["demo-postgres"]):
            statement = compile_select(target, "SELECT station FROM readings WHERE state = ? ORDER BY station",
                                       ["charging"], 3)
            self.assertEqual(statement.values, ["charging"])
            self.assertNotIn("charging", statement.text)
            self.assertTrue(statement.text.endswith("LIMIT 4"))

    def test_only_catalog_queries_compile(self):
        for target in (TARGETS["demo-mysql"], TARGETS["demo-postgres"]):
            for sql in ("DELETE FROM readings", "SELECT * FROM meter_events", "SELECT service_note FROM readings",
                        "SELECT * FROM readings; SELECT 1", "SELECT station FROM readings FOR UPDATE",
                        "SELECT * FROM readings JOIN meter_events ON 1=1", "SELECT unknown_fn(watts) FROM readings"):
                with self.subTest(target=target.name, sql=sql), self.assertRaises(Refused):
                    compile_select(target, sql, [], 2)

    def test_mongo_field_scope_survives_grouping(self):
        fields = ("watts", "station")
        with self.assertRaises(Refused):
            match({"service_note": "x"}, fields)
        with self.assertRaises(Refused):
            projection(fields, {"service_note": 1})
        with self.assertRaises(Refused):
            aggregation([{"$group": {"_id": "$station", "n": {"$sum": 1}}}, {"$sort": {"watts": 1}}], fields)
        with self.assertRaises(Refused):
            aggregation([{"$out": "archive"}], fields)

    def test_entire_utf8_reply_fits(self):
        doc = json.loads(fit({"ok": True, "rows": [["界" * 12000]]}, 50))
        self.assertTrue(doc["truncated"])
        self.assertEqual(doc["row_count"], 0)
        text = fit({"ok": True, "rows": [["界" * 3000] for _ in range(12)]}, 50)
        self.assertLessEqual(len(text.encode()), BYTES)
        self.assertGreater(json.loads(text)["row_count"], 0)

    def test_result_counts_and_mongo_projection(self):
        self.assertEqual(json.loads(fit({"rows": [[1], [2], [3]]}, 2))["rows"], [[1], [2]])
        self.assertEqual(projection(("watts",)), {"_id": 0, "watts": 1})
