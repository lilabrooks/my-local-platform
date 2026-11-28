//go:build integration && unix

package dbmcp

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lilabrooks/my-local-platform/db-mcp/internal/fixture"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func with(base, additional document) document {
	args := maps.Clone(base)
	maps.Copy(args, additional)
	return args
}
func checkJSON(t *testing.T, got any, want string) {
	t.Helper()
	data, err := json.Marshal(got)
	if err != nil || string(data) != want {
		t.Fatalf("got %s, want %s (%v)", data, want, err)
	}
}
func TestDemoStdio(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	c := connectTestClient(t, exec.Command(filepath.Join(root, "bin", "mlp-db-mcp"), "--catalog", filepath.Join(root, "services", "db-mcp", "demo.toml")))
	targets := c.call(t, "list_targets", document{}, true)["targets"].([]any)
	cat := demoCatalog(t)
	if len(targets) != len(cat.Targets) {
		t.Fatal(targets)
	}
	checkJSON(t, c.call(t, "whoami", document{}, true)["transport"], `"stdio"`)
	for _, target := range cat.Targets {
		args := document{"target": target.Name, "database": target.Database}
		checkJSON(t, c.call(t, "list_databases", document{"target": target.Name}, true)["databases"], `["meter_demo"]`)
		checkJSON(t, c.call(t, "list_tables", args, true)["objects"], `["readings"]`)
		checkJSON(t, c.call(t, "describe_table", with(args, document{"table": "readings"}), true)["fields"], `["sequence_no","station","state","watts"]`)
		c.call(t, "describe_table", with(args, document{"table": "meter_events"}), false)
		if target.Engine == "mongodb" {
			continue
		}
		callSQL := func(query string, params []any, ok bool) document {
			return c.call(t, "run_sql", with(args, document{"sql": query, "params": params}), ok)
		}
		counts := callSQL("SELECT state, COUNT(*) AS total FROM readings WHERE state=? GROUP BY state", []any{"ready"}, true)
		checkJSON(t, counts["rows"], `[["ready",32]]`)
		page := callSQL("SELECT sequence_no FROM readings ORDER BY sequence_no", nil, true)
		seq := page["rows"].([]any)
		if len(seq) != RowLimit || page["row_count"] != float64(RowLimit) || page["truncated"] != true {
			t.Fatal(page)
		}
		checkJSON(t, seq[0], `[1]`)
		checkJSON(t, seq[len(seq)-1], `[50]`)
		numeric := callSQL("SELECT watts, watts FROM readings WHERE sequence_no=?", []any{3}, true)
		checkJSON(t, numeric["columns"], `["watts","watts"]`)
		checkJSON(t, numeric["rows"], `[[30,30]]`)
		plan := c.call(t, "explain_query", with(args, document{"sql": "SELECT watts FROM readings"}), target.Engine == "postgres")
		if target.Engine == "postgres" && len(plan["operators"].([]any)) == 0 {
			t.Fatal(plan)
		}
		callSQL("UPDATE readings SET watts=0", nil, false)
		callSQL("SELECT * FROM meter_events", nil, false)
		t.Log(target.Name, "returned 32 ready readings and capped the 64-row view at 50")
	}
	args := document{"target": "demo-mongo", "database": "meter_demo", "collection": "readings"}
	page := c.call(t, "mongo_find", with(args, document{"projection": document{"sequence_no": 1}, "sort": document{"sequence_no": 1}}), true)
	seq := page["rows"].([]any)
	if len(seq) != RowLimit || page["truncated"] != true {
		t.Fatal(page)
	}
	checkJSON(t, seq[49], `{"sequence_no":50}`)
	query := document{"filter": document{"sequence_no": 1}, "projection": document{"watts": 1}}
	checkJSON(t, c.call(t, "mongo_find", with(args, query), true)["rows"], `[{"watts":10}]`)
	// Alphabetizing these keys would return 1 before 2.
	order := document{"sort": json.RawMessage(`{"state":-1,"sequence_no":1}`), "limit": 2, "projection": document{"sequence_no": 1}}
	checkJSON(t, c.call(t, "mongo_find", with(args, order), true)["rows"], `[{"sequence_no":2},{"sequence_no":4}]`)
	checkJSON(t, c.call(t, "mongo_count", with(args, document{"filter": document{"state": "ready"}}), true)["count"], "32")
	checkJSON(t, c.call(t, "mongo_distinct", with(args, document{"field": "state"}), true)["rows"], `[{"value":"charging"},{"value":"ready"}]`)
	stages := json.RawMessage(`[{"$group":{"_id":"$state","n":{"$sum":1}}},{"$sort":{"_id":1}}]`)
	checkJSON(t, c.call(t, "mongo_aggregate", with(args, document{"pipeline": stages}), true)["rows"], `[{"_id":"charging","n":32},{"_id":"ready","n":32}]`)
	if p := c.call(t, "explain_query", with(args, document{"pipeline": []any{}}), true); len(p["operators"].([]any)) == 0 {
		t.Fatal(p)
	}
	c.call(t, "mongo_aggregate", with(args, document{"pipeline": json.RawMessage(`[{"$out":"archive"}]`)}), false)
	c.call(t, "mongo_find", with(args, document{"collection": "meter_events"}), false)
	c.checkAudit(t)
}

func TestDemoNativeGrants(t *testing.T) {
	project, err := fixture.Project("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct{ service, query, want string }{
		{"postgres", `psql -U postgres -d meter_demo -Atqc "select count(*) from meter_events"`, "64"},
		{"mysql", `MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -NB -e "select count(*) from meter_demo.meter_events; select count(*) from meter_private.calibration"`, "64\n1"},
		{"mongo", `mongosh --quiet meter_demo -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin --eval "print(db.meter_events.countDocuments({}))"`, "64"},
	} {
		out, err := exec.CommandContext(t.Context(), "docker", "exec", project+"-"+check.service+"-1", "sh", "-c", check.query).Output()
		if err != nil || strings.TrimSpace(string(out)) != check.want {
			t.Fatalf("fixture owner check %s: %q (%v)", check.service, out, err)
		}
	}
	cat := demoCatalog(t)
	for _, name := range []string{"demo-postgres", "demo-mysql"} {
		target := cat.byName[name]
		db, err := openSQL(target, "native-proof")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		queries := []string{"SELECT * FROM meter_events", "UPDATE meter_events SET watts=0 WHERE sequence_no=-1", "UPDATE readings SET watts=0 WHERE sequence_no=-1"}
		if target.Engine == "mysql" {
			queries = append(queries, "SELECT * FROM meter_private.calibration")
		} else {
			var temp bool
			if err := db.QueryRowContext(t.Context(), "SELECT has_database_privilege(current_database(), 'TEMP')").Scan(&temp); err != nil || temp {
				t.Fatal("unexpected TEMP privilege", err)
			}
		}
		for _, q := range queries {
			_, err := db.ExecContext(t.Context(), q)
			var pg *pgconn.PgError
			var my *mysql.MySQLError
			if errors.As(err, &pg) && pg.Code == "42501" {
				continue
			}
			if errors.As(err, &my) && slices.Contains([]uint16{1044, 1142, 1143}, my.Number) {
				continue
			}
			t.Fatalf("%s did not return a native permission denial: %v", target.Name, err)
		}
	}
	opts, err := mongoOptions(cat.byName["demo-mongo"])
	if err != nil {
		t.Fatal(err)
	}
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Disconnect(t.Context()) }()
	base := client.Database("meter_demo").Collection("meter_events")
	readErr := base.FindOne(t.Context(), bson.D{}).Err()
	_, writeErr := base.UpdateOne(t.Context(), stage("sequence_no", -1), stage("$set", stage("watts", 0)))
	for _, err := range []error{readErr, writeErr} {
		var native mongo.ServerError
		if !errors.As(err, &native) || !native.HasErrorCode(13) {
			t.Fatal("expected native MongoDB permission denial", err)
		}
	}
	t.Log("all three native readers denied reads and writes on existing base data")
}

func TestDemoSQLScalarTypes(t *testing.T) {
	cat := demoCatalog(t)
	for _, test := range []struct{ target, query string }{
		{"demo-postgres", "SELECT ROW('internal', 42), CAST(1.234567890123456789 AS NUMERIC(30,18)), CAST('2026-11-27' AS DATE) FROM readings LIMIT 1"},
		{"demo-mysql", "SELECT ST_GeomFromText('POINT(1 2)'), CAST(1.234567890123456789 AS DECIMAL(30,18)), CAST('2026-11-27' AS DATE) FROM readings LIMIT 1"},
	} {
		result, err := readSQL(t.Context(), cat.byName[test.target], "scalar-proof", statement{text: test.query}, false)
		if err != nil {
			t.Fatal(test.target, err)
		}
		checkJSON(t, result["rows"], `[["[non-scalar]","1.234567890123456789","2026-11-27"]]`)
	}
}

func TestDemoMongoServerDeadline(t *testing.T) {
	opts, err := mongoOptions(demoCatalog(t).byName["demo-mongo"])
	if err != nil {
		t.Fatal(err)
	}
	type observedCommand struct {
		name     string
		deadline int64
	}
	captured := make(chan observedCommand, 2)
	opts.SetMonitor(&event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "aggregate" || e.CommandName == "explain" {
			duration, _ := e.Command.Lookup("maxTimeMS").AsInt64OK()
			captured <- observedCommand{e.CommandName, duration}
		}
	}})
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Disconnect(t.Context()) }()
	for _, explain := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(t.Context(), queryTimeout)
		_, err := mongoCommand(ctx, client.Database("meter_demo"), opts.ReadPreference, "timeout-proof", "readings", mongo.Pipeline{}, 1, explain)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		select {
		case cmd := <-captured:
			if cmd.deadline < 1 || cmd.deadline > queryTimeout.Milliseconds() {
				t.Fatalf("%s server maxTimeMS=%d", cmd.name, cmd.deadline)
			}
		case <-time.After(time.Second):
			t.Fatal("command monitor did not see query")
		}
	}
}
