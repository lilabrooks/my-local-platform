"""Native driver calls. Only this module handles database credentials."""

import json
import re
import ssl
from contextlib import closing, contextmanager
from datetime import date, datetime
from decimal import Decimal
from math import isfinite

import psycopg
import pymysql
from bson import ObjectId
from pymongo import MongoClient

from .catalog import TIMEOUT, Refused
from .sql_read import Statement


def scalar_value(value):
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if isfinite(value) else None
    if isinstance(value, (Decimal, ObjectId)):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    # Only scalar fields belong in this API. An approved field may change type.
    return "[non-scalar]"


@contextmanager
def sql_cursor(target, request_id):
    if target.engine == "postgres":
        options = {"sslmode": "disable"} if target.tls == "local" else {
            "sslmode": "verify-full", "sslrootcert": target.ca_file or "system"}
        with closing(psycopg.connect(host=target.host, port=target.port, dbname=target.database,
                                     user=target.username, password=target.password(), connect_timeout=5,
                                     application_name="meter-mcp/" + request_id, **options)) as conn:
            conn.read_only = True
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {TIMEOUT * 1000}")
                yield cur
    else:
        tls = {"ssl_disabled": True} if target.tls == "local" else {
            "ssl": ssl.create_default_context(cafile=target.ca_file)}
        with closing(pymysql.connect(host=target.host, port=target.port, database=target.database,
                                     user=target.username, password=target.password(), connect_timeout=5,
                                     read_timeout=TIMEOUT + 2, write_timeout=5,
                                     program_name="meter-mcp/" + request_id, **tls)) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET SESSION max_execution_time = {TIMEOUT * 1000}")
                cur.execute("START TRANSACTION READ ONLY")
                yield cur


def plan_nodes(plan, engine):
    # Visit only plan-tree edges. A predicate may contain a user field named "stage".
    name_key, edges = {
        "postgres": ("Node Type", {"Plan", "Plans"}),
        "mysql": ("access_type", {"query_block", "table", "nested_loop", "ordering_operation",
                                  "grouping_operation", "duplicates_removal", "materialized_from_subquery"}),
        "mongodb": ("stage", {"queryPlanner", "winningPlan", "queryPlan", "inputStage", "inputStages",
                              "outerStage", "innerStage", "stages", "$cursor", "shards"}),
    }[engine]
    names = []

    def visit(node):
        if isinstance(node, list):
            for item in node:
                visit(item)
        elif isinstance(node, dict):
            name = node.get(name_key)
            if isinstance(name, str) and re.fullmatch(r"[a-zA-Z_ ]{1,48}", name) and name not in names:
                names.append(name)
            for key in edges.intersection(node):
                visit(node[key])

    visit(plan)
    return {"operators": names}


def sql_result(target, request_id, statement: Statement, explain=False):
    prefix = ""
    if explain:
        prefix = "EXPLAIN (FORMAT JSON) " if target.engine == "postgres" else "EXPLAIN FORMAT=JSON "
    with sql_cursor(target, request_id) as cur:
        try:
            cur.execute(prefix + statement.text, statement.values or None)
        except pymysql.OperationalError as exc:
            if explain and exc.args[:1] == (1345,):
                raise Refused("MySQL withheld the execution plan for this view.") from None
            raise
        if explain:
            plan = cur.fetchone()[0]
            return plan_nodes(json.loads(plan) if isinstance(plan, str) else plan, target.engine)
        return {"columns": [col[0] for col in cur.description],
                "rows": [[scalar_value(v) for v in row] for row in cur.fetchall()]}


def mongo_client(target):
    options = {"replicaSet": target.replica_set} if target.replica_set else {}
    if target.ca_file:
        options["tlsCAFile"] = target.ca_file
    return MongoClient(host=target.host, port=target.port, username=target.username, password=target.password(),
                       authSource=target.auth_database, tls=target.tls == "verify", timeoutMS=TIMEOUT * 1000,
                       readPreference=target.read_preference, retryWrites=False,
                       serverSelectionTimeoutMS=5000, **options)


def mongo_result(target, request_id, collection, stages, cap, explain=False):
    pipeline = [*stages, {"$limit": cap + 1}]
    with mongo_client(target) as client:
        db = client[target.database]
        if explain:
            plan = db.command("explain", {"aggregate": collection, "pipeline": pipeline, "cursor": {}},
                              verbosity="queryPlanner", comment=request_id, maxTimeMS=TIMEOUT * 1000,
                              read_preference=db.read_preference)
            return plan_nodes(plan, target.engine)
        with db[collection].aggregate(pipeline, comment=request_id, maxTimeMS=TIMEOUT * 1000,
                                      allowDiskUse=False, batchSize=cap + 1) as cursor:
            return {"rows": [{key: scalar_value(value) for key, value in row.items()} for row in cursor]}
