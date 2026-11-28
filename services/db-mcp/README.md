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
volumes and generated passwords.

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

Put data suitable for the client into the approved view. Selected scalar
values can enter the model's context as returned. The application audit on
stderr records a request ID, the local user label, operation, resolved
target, outcome, elapsed time, bytes and row count. Client transcripts
and the database's own logging have their own retention settings.

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

## Validation

On 2026-11-27 and 2026-11-28, `make db-mcp-test` passed with Go 1.27.4 and
the race detector on Apple silicon. The tests cover the catalog, allowed and refused queries,
ordered MongoDB sorts, numeric precision, the UTF-8 response limit,
certificate and hostname verification against a local TLS server, and
credential retention after a failed fixture teardown.

`make db-mcp-demo` exercised the compiled Go binary against PostgreSQL 18.7,
MySQL 8.4.13 and MongoDB 8.0.38. All 11 tools ran. Each engine counted 32 ready
readings and capped a 64-row result at 50. All 35 requests matched individual
audit records. Separate owner queries confirmed the excluded objects existed.
All three native readers were denied a base-data read and a write. MySQL
also denied the reader access to an existing table in another database.
The SQL drivers masked a PostgreSQL composite and MySQL geometry value
while preserving exact decimals and dates.
The MongoDB driver sent a server-side deadline for both aggregate and explain.

The previous Python client also ran its 32-request rehearsal against the
Go binary. All 11 tool names, descriptions and input schemas matched those
advertised by the previous server.

## Personal AWS pilot and adoption

**Status.** The local stdio path has passed the rehearsal above. AWS deployment
and multi-user authorization are planned. The local user label provides
diagnostics on the machine that launches the server.

The first AWS question is whether two authenticated callers get different
access to one private synthetic view. Add a signed client transport and
a router that takes caller identity from AWS, authorizes discovery and
queries against the same target policy, and dispatches to a private
executor. Only that executor should obtain the database reader credential.
Prove caller identity and denial first. Then run the pinned MySQL image on
a short-lived private EC2 instance to check the known result, TLS, native
grants and metadata-only audit correlation. Include image delivery, secret
access and the private CA in the network plan. Capture a second caller's
denial on the same target.
The three-engine compatibility proof can run locally.

Before staging, record the candidate revision, image and driver pins, secret
and CA delivery, private network access, and who owns initialization and
cleanup. Price the selected region and check remaining room in the shared
$5 monthly allowance. Staging and the hourly run have separate owner
approvals. Set a spend cap, a destroy deadline and a cleanup reserve, and
verify the pilot's resources in both state and service inventories. Relay's
M4 runner and receipts cover its own deployment.
[Application validation](../../docs/application-validation.md)

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
