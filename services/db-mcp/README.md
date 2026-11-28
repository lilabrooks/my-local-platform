# Database MCP

Query the same synthetic charging-station data through PostgreSQL, MySQL
and self-hosted MongoDB. Each engine exposes a `readings` view with four
fields. The underlying meter events also contain a maintenance note that
the reader cannot access.

## Run the showcase

From the repository root on macOS or Linux, use Go 1.27 and Docker with
Compose. The first run downloads the pinned database images and Go modules.

```sh
make db-mcp-test
make db-mcp-demo
```

The demo builds `bin/mlp-db-mcp`, starts three dedicated containers, and
launches the binary as an MCP subprocess. All three engines
should report 32 ready readings out of 64. The final lines report the
database permission checks and the number of requests matched to audit records.

The fixtures publish only on loopback, on ports 58432, 58306 and 58017.
One checkout can use those ports at a time. Each checkout owns its Compose
project and volumes. Generated owner and reader credentials live under
the ignored `local/tmp/db-mcp/` directory. The server reads only the reader
files named in its catalog.

Use `make db-mcp-up` to start just the fixtures. When finished, run
`make db-mcp-down` to delete this checkout's fixture containers, data
volumes and generated passwords. When upgrading the fixture database
majors, run it before `make db-mcp-demo` to seed fresh synthetic data.

## Connect an MCP client

Run `make db-mcp-build` to build the executable. Use absolute paths for the
binary and catalog in your client's stdio configuration. The example uses
the `mcpServers` configuration accepted by Claude Code.

```json
{
  "mcpServers": {
    "platform-data": {
      "command": "/absolute/path/to/my-local-platform/bin/mlp-db-mcp",
      "args": ["--catalog", "/absolute/path/to/my-local-platform/services/db-mcp/demo.toml"]
    }
  }
}
```

Ask the client to list its database targets, then count readings whose state is
`ready` on each engine. Each should return 32. Several readings can come
from the same station.

| Tool | Supported operation |
|---|---|
| `whoami`, `list_targets`, `list_databases` | Report the local user label and the configured non-production targets and databases. |
| `list_tables`, `describe_table` | Discover the objects and fields the operator has exposed in the catalog. |
| `run_sql` | Select fields from one approved view, filter with comparisons and `AND` or `OR`, count, group, order and limit. Bind values with `?` and a `params` array. |
| `explain_query` | Return plan operator names for a permitted SQL query or Mongo aggregation. The MySQL fixture's view-only login receives a refusal for its execution plan. |
| `mongo_find`, `mongo_count`, `mongo_distinct` | Read using approved fields, scalar comparisons, inclusion projections and sort keys. |
| `mongo_aggregate` | Compose `$match`, `$sort`, `$limit` and `$group` with count accumulators over approved fields. |

For example, call `run_sql` with these arguments, then call `mongo_count`
on `demo-mongo` with the same database, collection `readings`, and filter
`{"state":"ready"}`.

```json
{
  "target": "demo-postgres",
  "database": "meter_demo",
  "sql": "SELECT COUNT(*) AS total FROM readings WHERE state = ?",
  "params": ["ready"]
}
```

## How access works

The catalog maps a target to one database, a database identity, and approved
objects and fields. The SQL compiler emits a new parameterized statement
from its supported grammar. It quotes catalog identifiers for the selected
engine and binds values through pgx or the Go MySQL driver. Use `?`
parameters for input values, including strings with backslashes. The SQL
drivers open a read-only transaction on the connection that runs the query.
MongoDB uses a native role with `find` on the view. Every tool invocation
runs as the configured database identity for that target.

Tool argument names are exact. For example, `mongo_count` with `filters`
returns `refused` before querying the database. SQL comparisons accept
integers and decimals either inline or as numeric parameters. PostgreSQL
folds unquoted identifiers to lowercase and preserves the case of quoted
names. MySQL matches column names and ordering aliases case-insensitively.
Use the configured spelling for MySQL database and view names.

For a MySQL comparison, the reader fetches the types of the approved
predicate columns with a zero-row query on the same view and transaction.
Pass JSON numbers for numeric fields and strings for text fields. For
example, both `station = 0` and `watts = '10'` return `refused`.
MySQL also refuses unsupported comparison types and exact numeric inputs
that its `DECIMAL` type would round or clip.

SQL refuses a null comparison parameter. For MongoDB, a filter such as
`{"state": null}` matches both null and missing fields, and
`{"state": {"$ne": null}}` matches existing, non-null values.
MongoDB filter values written as JSON integers use BSON int64. Fractional
and scientific notation use BSON doubles. The server compares the input
with the shortest decimal spelling of that double. It accepts `0.1` and
`0.1000`, and refuses `640.00000000000000000001`, which rounds to `640`.
[MongoDB null comparisons](https://www.mongodb.com/docs/manual/tutorial/query-for-null-fields/)

Put data suitable for the client into the approved view. Selected scalar
values can enter the model's context as returned. The application audit on
stderr records a request ID, the local user label, operation, outcome, elapsed
time and response bytes. It includes the target and object once resolved
against the catalog. On a successful call, the row count describes the
result prepared for the client. A `mongo_count` of zero is one result.
Query text, parameter values and returned rows stay out of the audit.
Client transcripts and the database's own logging have their own retention
settings.

MCP clients can check `isError` on the tool result. It is false on success
and true for a tool execution failure. The JSON text also carries `ok`, the
request ID and, on failure, the public outcome code. Transport and JSON-RPC
errors follow their own protocol paths.
[MCP tool error handling](https://modelcontextprotocol.io/specification/2025-11-25/server/tools#error-handling)

A malformed tool argument or catalog refusal returns `refused`. A native
permission denial on the configured reader returns `denied`. Recognized database
type and grouping errors return `invalid_query`. Deadline errors return
`timeout`, including PostgreSQL's server-side query cancellation signal.
A client cancellation is recorded as
`canceled`. Other database errors return `unavailable`. Driver messages can
contain query values, so the server supplies its own messages.

The server applies a 50-row limit and a 32 KiB compact UTF-8 limit to its
logical tool response, including metadata. It removes whole rows to fit and
reports truncation. Drivers may receive larger values before the response
is sized. Tool arguments are limited to 64 KiB and SQL text to 8,000 bytes.
Query timeouts are 15 seconds. The server marks database values
as untrusted content.

For your own development database, supply a separate TOML catalog following
[the example](demo.toml). Create and verify a read-only database login with
grants on the exact view or collection, and put its password in a protected
file. Relative paths resolve against the catalog. TLS defaults to certificate
and hostname verification. Set `ca_file` for a private CA. Cleartext fixture
connections require a local target and a literal loopback address. MongoDB
pins these connections to the configured member. Replica discovery uses TLS.
MongoDB accepts `auth_database` and an optional `replica_set`. Configuring
a replica set defaults to secondary reads. Set `read_preference` to
`primary` or `secondaryPreferred` only when that placement is intended.
The demonstration exercises a standalone MongoDB server.

### When the audit destination stops reading

The stdio server reserves an audit slot before it runs a tool. The pilot
router reserves one before it invokes the executor, and the executor reserves
its own slot before it requests the database password. Each process can hold
32 reservations and has one writer. When the slots are full or a known write
failure is awaiting recovery, callers receive `unavailable` with their request
ID before that process starts the work. This applies to `whoami` as well as
database reads.

A read already in flight can finish. Its result is released only after the
audit writer acknowledges the complete line. A write error or a wait of
250 ms withholds the result. The client receives `isError: true`,
`unavailable` and the original request ID. When a blocked write resumes,
the writer records the final outcome with `audit_state: "response_withheld"`.
After a write error, the next request triggers a retry of that record. A
delayed write can put an earlier `ok` for the same ID in the log. Use the
terminal record to interpret that pair. If a writer accepts the whole line
and also reports an error, a retry can repeat the same correction.

Requests refused before admission get their receipt in the caller's response.
A process that exits while its writer is blocked can lose pending records.
The 250 ms allowance bounds the audit wait for each process. Database
execution, network transit and client scheduling add to the total call time.

## Validation

On 2026-11-28, `make db-mcp-test` passed with Go 1.27.4 and the race detector
on Apple silicon. The tests cover catalog and query refusals, ordered MongoDB
sorts, numeric precision, the UTF-8 response limit and credential retention
after a failed fixture teardown. Each driver's TLS configuration verified a
certificate and rejected a hostname mismatch against a local HTTPS test server.

`make db-mcp-demo` exercised the compiled Go binary against PostgreSQL 18.7,
MySQL 8.4.13 and MongoDB 8.0.38. All 11 tools ran. Each engine counted 32 ready
readings, counted 63 readings above 10.5 watts and capped a 64-row result at
50. All 69 calls matched audit records, including their resolved target or
its absence. A second catalog deliberately named an existing object outside
each reader's grants. All three requests returned and logged `denied`
without exposing driver messages. Separate owner queries confirmed the
excluded objects existed.
All three native readers were denied a base-data read and a write. MySQL
also denied the reader access to an existing table in another database.
The SQL drivers masked a PostgreSQL composite and MySQL geometry value
while preserving exact decimals and dates.
Both SQL engines rejected a zero-row update through the MCP transaction even
with a fixture owner login. Each stopped a slow query on its 15-second server
timer. Canceling a separate query returned promptly on both clients. The
fixture observer saw the PostgreSQL statement disappear in about 0.1 seconds.
The MySQL statement continued until about 15 seconds after cancellation.
The PostgreSQL driver refused to fall back to plaintext when the fixture declined TLS.
The MongoDB driver sent a server-side deadline for both aggregate and explain.
An authenticated query over verified TLS to each actual deployment remains
a pilot prerequisite.

At revision `a1d2743` a separate client passed a 32-request rehearsal against
the Go binary. The tests now check each of the 11 published input schemas
against the arguments its handler consumes.

The November 28 upgrade rehearsal repeated all nine integration tests on
PostgreSQL 19.0, MySQL 26.10.0 and MongoDB 9.0.8. The 69 routine and boundary
calls and three native permission denials all matched their audit records.

### Audit recovery and tool errors

On November 28, 2026, the Go 1.27.4 race suite (`make db-mcp-test`) and
both local rehearsals passed for implementation `490d0be` on Apple silicon.
The stdio SDK and signed-client tests checked the error flag, public code,
request ID and audit outcome for a successful read, a catalog refusal, a
native database denial and a transient failure. The signed MySQL rehearsal
also distinguished a denied observer from an entitled reader using the wrong CA.

In `make db-mcp-demo`, the compiled server withheld a MySQL read in 259 ms
with stderr full. A subsequent `whoami` returned `unavailable` in 0.53 ms.
After the pipe drained, the test matched the correction to the original ID
and the same query returned 32. Closing stdin with stderr still full exited
in 1.9 ms. The race tests also decoded the entire audit stream after partial
writes and confirmed that a known outage prevented another executor call or
password lookup. These timings describe this local run.

## Personal AWS pilot and adoption

**Status.** The Go client, IAM router and private MySQL executor have passed
a local two-role rehearsal. Live AWS identity, permissions, routing and
teardown still need a personal-account run.

Use `make db-mcp-pilot-test` to repeat the signed client and verified-TLS MySQL
check on a random loopback port. The entitled role counted 32 ready readings.
The observer discovered zero targets and received a denial on the same query
before the executor obtained a credential. A different CA caused the entitled
reader's connection to fail. The local `--catalog` showcase still uses the
user label of the machine that launches the process.

The [personal pilot runbook](pilot/README.md) covers the cheap identity stage,
the separately approved hourly MySQL session, cost assumptions and cleanup.

The intended work deployment has AWS-hosted PostgreSQL, MySQL and
self-hosted MongoDB in different environments. Place an executor within
each approved AWS network boundary. The adapters use native endpoints.
Qualify the SQL service or self-managed topology that supplies each endpoint.
Map federated engineer identities to environment and dataset grants.
Start with two engineers who have different entitlements. Verify
discovery, direct requests using a known target name, revocation and audit
attribution in the actual environment. Database owners choose the views
and fields permitted in model prompts. Qualify TLS trust, private DNS,
credential rotation and MongoDB member discovery and failover against the
company's own deployment before expanding access.

Record a live result for each engine and hosting configuration before
claiming that configuration on AWS. A single-account MySQL result leaves
cross-account trust, the PostgreSQL path and the MongoDB topology open.

### Aurora MySQL and Amazon DocumentDB

**Assessment.** Both are candidates for read-only access from the same AWS
environment. Give each its own catalog target, database identity and dataset
grant. A shared private executor is an option when both database owners accept
that trust boundary. Check each caller's target access separately.

**Aurora MySQL.** Use the MySQL adapter with verified TLS and a login granted
`SELECT` on one approved view. The reader endpoint routes to the primary
when the cluster has no replicas, so the login must also deny writes and
base-table reads there. Qualify the exact Aurora release by reading known
rows, exercising the execution timeout and checking the native grants. A
view-only account may be denied the execution plan.
[Aurora reader endpoints](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Reader.html)

**DocumentDB.** Use the MongoDB adapter with a verified AWS CA, password
authentication against `admin`, and the cluster endpoint with replica set
`rs0` for an instance-based cluster. The driver disables retryable writes.
Select the read preference deliberately. A one-instance pilot needs primary
reads; `secondary` requires a reachable replica. These settings are checked
locally, and the service behavior still needs a live run.
[Connections](https://docs.aws.amazon.com/documentdb/latest/devguide/connect-to-replica-set.html)
and [database users](https://docs.aws.amazon.com/documentdb/latest/devguide/security.managing-users.html)

DocumentDB 8.0 with planner v3 supports views. Test a role with `find` on
one view, including direct denial on its known underlying collection. On
instance-based 4.0 or 5.0, have the owner publish a separate sanitized
collection and grant reads only there. The API matrix lists neither
`createView` nor `createRole` for elastic clusters. That deployment needs
a separate access design before claiming the same object-level isolation.
[Version and planner compatibility](https://docs.aws.amazon.com/documentdb/latest/devguide/compatibility.html),
[supported APIs](https://docs.aws.amazon.com/documentdb/latest/devguide/mongo-apis.html),
and [role scope](https://docs.aws.amazon.com/documentdb/latest/devguide/role_based_access_control.html)

For each target, verify exact known results, denied reads of existing excluded
data, a denied native write to a disposable fixture, timeouts and response
limits. On DocumentDB run the actual aggregation-backed find, count and
distinct tools, including their command comments and explain behavior.
Verify that a caller admitted only to Aurora cannot discover or directly
query DocumentDB. Repeat in the other direction. Record engine and topology
with the result. Neither managed service has been exercised by the local run.
