"""Exercise the installed MCP entry point and each synthetic database account."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import anyio
import psycopg
import pymysql
from mcp import Client
from mcp.client.stdio import StdioServerParameters, stdio_client
from pymongo.errors import OperationFailure

from demo import PROJECT
from platform_db_mcp import adapters
from platform_db_mcp.catalog import BYTES, load

SERVICE = Path(__file__).resolve().parents[1]
CATALOG = SERVICE / "demo.toml"
DB = "meter_demo"
VIEWS = load(CATALOG)
TOOLS = {"list_targets", "list_databases", "list_tables", "describe_table", "whoami",
         "run_sql", "explain_query", "mongo_find", "mongo_count", "mongo_distinct", "mongo_aggregate"}


class DemoTests(unittest.TestCase):
    def test_tools_over_stdio(self):
        anyio.run(self.exercise_stdio)

    async def exercise_stdio(self):
        ids = set()
        with tempfile.TemporaryFile(mode="w+") as err:
            server = StdioServerParameters(command=str(Path(sys.executable).parent / "mlp-db-mcp"),
                                           args=["--catalog", str(CATALOG)])
            async with Client(stdio_client(server, errlog=err)) as client:
                self.assertEqual({t.name for t in (await client.list_tools()).tools}, TOOLS)

                async def read(tool, arguments=None, *, ok=True):
                    response = await client.call_tool(tool, arguments or {})
                    self.assertFalse(response.is_error, tool)
                    payload = response.content[0].text
                    self.assertLessEqual(len(payload.encode()), BYTES)
                    data = json.loads(payload)
                    self.assertEqual(data["ok"], ok, (tool, data))
                    self.assertNotIn(data["request_id"], ids)
                    ids.add(data["request_id"])
                    return data

                targets = (await read("list_targets"))["targets"]
                self.assertEqual({t["target"] for t in targets}, set(VIEWS))
                self.assertEqual((await read("whoami"))["transport"], "stdio")
                for target in VIEWS:
                    args = {"target": target, "database": DB}
                    self.assertEqual((await read("list_databases", {"target": target}))["databases"], [DB])
                    self.assertEqual((await read("list_tables", args))["objects"], ["readings"])
                    self.assertEqual((await read("describe_table", {**args, "table": "readings"}))["fields"],
                                     ["sequence_no", "station", "state", "watts"])
                    await read("describe_table", {**args, "table": "meter_events"}, ok=False)
                    if target == "demo-mongo":
                        continue
                    query = "SELECT state, COUNT(*) AS total FROM readings WHERE state = ? GROUP BY state"
                    result = await read("run_sql", {**args, "sql": query, "params": ["ready"]})
                    self.assertEqual(result["rows"], [["ready", 32]])
                    result = await read("run_sql", {**args, "sql": "SELECT sequence_no FROM readings ORDER BY sequence_no"})
                    self.assertEqual(result["row_count"], 50)
                    self.assertEqual(result["rows"][0], [1])
                    self.assertEqual(result["rows"][-1], [50])
                    self.assertTrue(result["truncated"])
                    plan = await read("explain_query", {**args, "sql": "SELECT watts FROM readings"},
                                      ok=target == "demo-postgres")
                    if target == "demo-postgres":
                        self.assertTrue(plan["operators"])
                    else:
                        self.assertEqual(plan["code"], "refused")
                    await read("run_sql", {**args, "sql": "UPDATE readings SET watts=0"}, ok=False)
                    await read("run_sql", {**args, "sql": "SELECT * FROM meter_events"}, ok=False)
                    print(target, "returned 32 ready readings and capped 64 rows at 50")
                args = {"target": "demo-mongo", "database": DB, "collection": "readings"}
                page = await read("mongo_find", {**args, "projection": {"sequence_no": 1}, "sort": {"sequence_no": 1}})
                self.assertEqual(page["row_count"], 50)
                self.assertEqual(page["rows"][-1], {"sequence_no": 50})
                self.assertTrue(page["truncated"])
                result = await read("mongo_find", {**args, "filter": {"sequence_no": 1}, "projection": {"watts": 1}})
                self.assertEqual(result["rows"], [{"watts": 10}])
                self.assertEqual((await read("mongo_count", {**args, "filter": {"state": "ready"}}))["count"], 32)
                distinct = await read("mongo_distinct", {**args, "field": "state"})
                self.assertEqual(distinct["rows"], [{"value": "charging"}, {"value": "ready"}])
                pipeline = [{"$group": {"_id": "$state", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
                self.assertEqual((await read("mongo_aggregate", {**args, "pipeline": pipeline}))["rows"],
                                 [{"_id": "charging", "n": 32}, {"_id": "ready", "n": 32}])
                plan = await read("explain_query", {**args, "pipeline": []})
                self.assertTrue(plan["operators"])
                await read("mongo_aggregate", {**args, "pipeline": [{"$out": "archive"}]}, ok=False)
                await read("mongo_find", {**args, "collection": "meter_events"}, ok=False)
                print("demo-mongo returned the same counts for charging and ready readings")
            err.seek(0)
            records = [json.loads(line) for line in err if line.startswith('{"request_id":')]
            self.assertEqual(len(records), len(ids))
            self.assertEqual({r["request_id"] for r in records}, ids)
            allowed = {"request_id", "operation", "caller", "outcome", "target", "rows", "bytes", "elapsed_ms"}
            self.assertTrue(all(set(r) <= allowed for r in records))
            print(len(ids), "MCP requests matched metadata-only audit records")

    def test_native_reader_grants(self):
        # Confirm excluded objects exist with a separate fixture-owner identity.
        confirmations = {
            "postgres": ["sh", "-c", 'psql -U postgres -d meter_demo -Atqc "select count(*) from meter_events"'],
            "mysql": ["sh", "-c", 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -NB -e "select count(*) from meter_demo.meter_events; select count(*) from meter_private.calibration"'],
            "mongo": ["sh", "-c", 'mongosh --quiet meter_demo -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin --eval "print(db.meter_events.countDocuments({}))"'],
        }
        for name, command in confirmations.items():
            output = subprocess.check_output(["docker", "exec", PROJECT + "-" + name + "-1", *command], text=True)
            self.assertEqual(output.strip().splitlines(), ["64", "1"] if name == "mysql" else ["64"])
        target = VIEWS["demo-postgres"]
        with psycopg.connect(host=target.host, port=target.port, dbname=DB, user=target.username,
                             password=target.password(), autocommit=True) as conn:
            self.assertFalse(conn.execute("SELECT has_database_privilege(current_database(), 'TEMP')").fetchone()[0])
            for query in ("SELECT * FROM meter_events", "UPDATE meter_events SET watts=0 WHERE sequence_no=-1"):
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    conn.execute(query)
        target = VIEWS["demo-mysql"]
        with pymysql.connect(host=target.host, port=target.port, database=DB, user=target.username,
                             password=target.password()) as conn, conn.cursor() as cur:
            for query in ("SELECT * FROM meter_events", "SELECT * FROM meter_private.calibration",
                          "UPDATE meter_events SET watts=0 WHERE sequence_no=-1"):
                with self.assertRaises(pymysql.MySQLError) as caught:
                    cur.execute(query)
                self.assertIn(caught.exception.args[0], (1044, 1142, 1143))
        with adapters.mongo_client(VIEWS["demo-mongo"]) as client:
            base = client[DB]["meter_events"]
            with self.assertRaises(OperationFailure) as denied:
                base.find_one({})
            self.assertEqual(denied.exception.code, 13)
            with self.assertRaises(OperationFailure) as denied:
                base.update_one({"sequence_no": -1}, {"$set": {"watts": 0}})
            self.assertEqual(denied.exception.code, 13)
        print("Native database readers refused base reads and writes on all three engines")


if __name__ == "__main__":
    unittest.main()
