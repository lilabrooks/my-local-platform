//go:build integration && unix

package dbmcp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"maps"
	"os"
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
	targets := c.call(t, "", "list_targets", document{}, true)["targets"].([]any)
	cat := demoCatalog(t)
	if len(targets) != len(cat.Targets) {
		t.Fatal(targets)
	}
	checkJSON(t, c.call(t, "", "whoami", document{}, true)["transport"], `"stdio"`)
	c.call(t, "", "list_tables", document{"target": "demo-mysql", "database": "unlisted"}, false)
	var knownObjects, excludedObjects []string
	for _, target := range cat.Targets {
		args := document{"target": target.Name, "database": target.Database}
		checkJSON(t, c.call(t, target.Name, "list_databases", document{"target": target.Name}, true)["databases"], `["meter_demo"]`)
		checkJSON(t, c.call(t, target.Name, "list_tables", args, true)["objects"], `["readings"]`)
		checkJSON(t, c.call(t, target.Name, "describe_table", with(args, document{"table": "readings"}), true)["fields"], `["sequence_no","station","state","watts"]`)
		c.call(t, target.Name, "describe_table", with(args, document{"table": "meter_events"}), false)
		if target.Engine == "mongodb" {
			continue
		}
		callSQL := func(query string, params []any, ok bool) document {
			return c.call(t, target.Name, "run_sql", with(args, document{"sql": query, "params": params}), ok)
		}
		counts := callSQL("SELECT state, COUNT(*) AS total FROM readings WHERE state=? GROUP BY state", []any{"ready"}, true)
		checkJSON(t, counts["rows"], `[["ready",32]]`)
		knownObjects = append(knownObjects, counts["request_id"].(string))
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
		t.Run(target.Engine+" decimal comparisons", func(t *testing.T) {
			for _, input := range []document{
				{"sql": "SELECT COUNT(*) FROM readings WHERE watts > 10.5"},
				{"sql": "SELECT COUNT(*) FROM readings WHERE watts > ?", "params": []any{json.Number("1.05e1")}},
			} {
				count := c.call(t, target.Name, "run_sql", with(args, input), true)
				checkJSON(t, count["rows"], `[[63]]`)
			}
			exact := c.call(t, target.Name, "run_sql", with(args, document{"sql": "SELECT COUNT(*) FROM readings WHERE watts < ?", "params": []any{json.Number("10.00000000000000000001")}}), true)
			checkJSON(t, exact["rows"], `[[1]]`)
			signed := c.call(t, target.Name, "run_sql", with(args, document{"sql": "SELECT COUNT(*) FROM readings WHERE watts > -10.5"}), true)
			checkJSON(t, signed["rows"], `[[64]]`)
		})
		badGroup := with(args, document{"sql": "SELECT state, COUNT(*) FROM readings"})
		c.callOutcome(t, target.Name, "run_sql", badGroup, "invalid_query")
		c.call(t, "", "run_sql", with(args, document{"sql": "SELECT watts FROM readings", "maxRows": 1}), false)
		callSQL("SELECT state FROM readings WHERE state=?", []any{nil}, false)
		if target.Engine == "postgres" {
			badValue := with(args, document{"sql": "SELECT watts FROM readings WHERE watts=?", "params": []any{"sentinel-private-value"}})
			failed := c.callOutcome(t, target.Name, "run_sql", badValue, "invalid_query")
			if strings.Contains(failed["message"].(string), "sentinel-private-value") {
				t.Fatal("driver value appeared in response")
			}
		} else {
			for _, number := range []string{"0", "0.0"} {
				callSQL("SELECT COUNT(*) FROM readings WHERE station = "+number, nil, false)
				callSQL("SELECT COUNT(*) FROM readings WHERE station = ?", []any{json.Number(number)}, false)
			}
			for _, value := range []string{"abc", "10"} {
				callSQL("SELECT watts FROM readings WHERE watts > ?", []any{value}, false)
			}
			callSQL("SELECT watts FROM readings WHERE watts > '10'", nil, false)
			badPlan := c.call(t, target.Name, "explain_query", with(args, document{"sql": "SELECT watts FROM readings WHERE station = 0"}), false)
			checkJSON(t, badPlan["message"], `"Use a value compatible with the MySQL column type."`)
			res := callSQL("SELECT STATE AS Sample, COUNT(*) AS Total FROM readings WHERE STATE = ? GROUP BY StAtE ORDER BY sAMPLE", []any{"ready"}, true)
			checkJSON(t, res["rows"], `[["ready",32]]`)
		}
		plan := c.call(t, target.Name, "explain_query", with(args, document{"sql": "SELECT watts FROM readings"}), target.Engine == "postgres")
		if target.Engine == "postgres" && len(plan["operators"].([]any)) == 0 {
			t.Fatal(plan)
		}
		c.call(t, target.Name, "explain_query", with(args, document{"sql": "SELECT watts FROM readings", "pipeline": []any{}}), false)
		callSQL("UPDATE readings SET watts=0", nil, false)
		excluded := callSQL("SELECT * FROM meter_events", nil, false)
		excludedObjects = append(excludedObjects, excluded["request_id"].(string))
		t.Log(target.Name, "returned 32 ready readings and capped the 64-row view at 50")
	}
	const mongoTarget = "demo-mongo"
	args := document{"target": mongoTarget, "database": "meter_demo", "collection": "readings"}
	page := c.call(t, mongoTarget, "mongo_find", with(args, document{"projection": document{"sequence_no": 1}, "sort": document{"sequence_no": 1}}), true)
	seq := page["rows"].([]any)
	if len(seq) != RowLimit || page["truncated"] != true {
		t.Fatal(page)
	}
	checkJSON(t, seq[49], `{"sequence_no":50}`)
	query := document{"filter": document{"sequence_no": 1}, "projection": document{"watts": 1}}
	checkJSON(t, c.call(t, mongoTarget, "mongo_find", with(args, query), true)["rows"], `[{"watts":10}]`)
	t.Run("misspelled optional filter", func(t *testing.T) {
		c.call(t, "", "mongo_count", with(args, document{"filters": document{"state": "ready"}}), false)
	})
	c.call(t, "", "mongo_aggregate", with(args, document{"pipeline": []any{}, "filter": document{"state": "ready"}}), false)
	rounded := c.call(t, mongoTarget, "mongo_count", with(args, document{"filter": document{"watts": document{"$gte": json.Number("640.00000000000000000001")}}}), false)
	checkJSON(t, rounded["message"], `"The number would change at BSON double precision."`)
	ordinary := c.call(t, mongoTarget, "mongo_count", with(args, document{"filter": document{"watts": document{"$gt": 0.1}}}), true)
	checkJSON(t, ordinary["count"], "64")
	checkJSON(t, c.call(t, mongoTarget, "mongo_count", with(args, document{"filter": document{"watts": document{"$gt": 10.5}}}), true)["count"], "63")
	// Alphabetizing these keys would return 1 before 2.
	order := document{"sort": json.RawMessage(`{"state":-1,"sequence_no":1}`), "limit": 2, "projection": document{"sequence_no": 1}}
	checkJSON(t, c.call(t, mongoTarget, "mongo_find", with(args, order), true)["rows"], `[{"sequence_no":2},{"sequence_no":4}]`)
	count := c.call(t, mongoTarget, "mongo_count", with(args, document{"filter": document{"state": "ready"}}), true)
	checkJSON(t, count["count"], "32")
	zero := c.call(t, mongoTarget, "mongo_count", with(args, document{"filter": document{"watts": -1}}), true)
	checkJSON(t, zero["count"], "0")
	knownObjects = append(knownObjects, count["request_id"].(string), zero["request_id"].(string))
	checkJSON(t, c.call(t, mongoTarget, "mongo_distinct", with(args, document{"field": "state"}), true)["rows"], `[{"value":"charging"},{"value":"ready"}]`)
	stages := json.RawMessage(`[{"$group":{"_id":"$state","n":{"$sum":1}}},{"$sort":{"_id":1}}]`)
	checkJSON(t, c.call(t, mongoTarget, "mongo_aggregate", with(args, document{"pipeline": stages}), true)["rows"], `[{"_id":"charging","n":32},{"_id":"ready","n":32}]`)
	if p := c.call(t, mongoTarget, "explain_query", with(args, document{"pipeline": []any{}}), true); len(p["operators"].([]any)) == 0 {
		t.Fatal(p)
	}
	c.call(t, mongoTarget, "explain_query", with(args, document{"params": []any{"unused"}}), false)
	c.call(t, mongoTarget, "mongo_aggregate", with(args, document{"pipeline": json.RawMessage(`[{"$out":"archive"}]`)}), false)
	excluded := c.call(t, mongoTarget, "mongo_find", with(args, document{"collection": "meter_events"}), false)
	excludedObjects = append(excludedObjects, excluded["request_id"].(string))
	records := c.checkAudit(t)
	for _, id := range knownObjects {
		rec := records[id]
		if rec.Object != "readings" || rec.Rows == nil || *rec.Rows != 1 {
			t.Fatalf("expected one result for the approved view: %+v", rec)
		}
	}
	for _, id := range excludedObjects {
		if rec := records[id]; rec.Object != "" || rec.Rows != nil {
			t.Fatalf("recorded an unapproved object or result: %+v", rec)
		}
	}
	log, err := os.ReadFile(c.logs.Name())
	if err != nil || strings.Contains(string(log), "sentinel-private-value") || strings.Contains(string(log), "meter_events") {
		t.Fatal("audit redaction failed", err)
	}
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

func TestDemoDeniedOverStdio(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	template, err := os.ReadFile(filepath.Join(root, "services", "db-mcp", "demo.toml"))
	if err != nil {
		t.Fatal(err)
	}
	// Model a catalog/grant mismatch on the existing base data, keeping the
	// native readers unchanged. Passwords still belong to this checkout.
	catalog := strings.ReplaceAll(string(template), "../../local/tmp/db-mcp/", filepath.Join(root, "local", "tmp", "db-mcp")+"/")
	catalog = strings.ReplaceAll(catalog, "[targets.objects]", "[targets.objects]\nmeter_events = [\"watts\"]")
	path := filepath.Join(t.TempDir(), "misconfigured.toml")
	if err := os.WriteFile(path, []byte(catalog), 0600); err != nil {
		t.Fatal(err)
	}
	c := connectTestClient(t, exec.Command(filepath.Join(root, "bin", "mlp-db-mcp"), "--catalog", path))
	for _, target := range []string{"demo-postgres", "demo-mysql", "demo-mongo"} {
		args := document{"target": target, "database": "meter_demo"}
		var result document
		if target == "demo-mongo" {
			result = c.callOutcome(t, target, "mongo_count", with(args, document{"collection": "meter_events", "filter": document{"watts": 3731731}}), "denied")
		} else {
			result = c.callOutcome(t, target, "run_sql", with(args, document{"sql": "SELECT watts FROM meter_events WHERE watts=?", "params": []any{3731731}}), "denied")
		}
		checkJSON(t, result["message"], `"The configured database reader lacks permission for this operation."`)
	}
	for _, rec := range c.checkAudit(t) {
		if rec.Object != "meter_events" || rec.Rows != nil {
			t.Fatalf("expected a denied operation on the catalogued object: %+v", rec)
		}
	}
	log, err := os.ReadFile(c.logs.Name())
	if err != nil || strings.Contains(string(log), "3731731") || strings.Contains(string(log), "meter_reader") {
		t.Fatal("native denial was not redacted", err)
	}
}

func TestDemoPostgresRequiresTLS(t *testing.T) {
	pg := *demoCatalog(t).byName["demo-postgres"]
	query := statement{text: "SELECT COUNT(*) FROM readings"}
	control, err := readSQL(t.Context(), &pg, "local-tls-control", query, false)
	if err != nil {
		t.Fatal(err)
	}
	checkJSON(t, control["rows"], `[[64]]`)
	pg.TLS = "verify"
	_, err = readSQL(t.Context(), &pg, "local-tls-required", query, false)
	if err == nil || !strings.Contains(err.Error(), "server refused TLS connection") {
		t.Fatalf("expected the verified driver to stop at the plaintext fixture: %v", err)
	}
}

func TestDemoSQLTransactionGuards(t *testing.T) {
	cat := demoCatalog(t)
	for _, name := range []string{"demo-postgres", "demo-mysql"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			target := *cat.byName[name]
			owner, passwordFile, slow := "postgres", "pg-owner", "SELECT pg_sleep(30)"
			if target.Engine == "mysql" {
				owner, passwordFile, slow = "root", "my-owner", "SELECT watts FROM readings WHERE SLEEP(1)=0"
			}
			// A privileged fixture login isolates the transaction guard from the
			// reader grants. The control UPDATE matches zero rows on both engines.
			target.Username = owner
			target.PasswordFile = filepath.Join(filepath.Dir(target.PasswordFile), passwordFile)
			const update = "UPDATE meter_events SET watts=watts WHERE sequence_no=-1"
			db, err := openSQL(&target, "guard-control")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			res, err := db.ExecContext(t.Context(), update)
			if err != nil {
				t.Fatal("fixture owner could not run the control", err)
			}
			if n, err := res.RowsAffected(); err != nil || n != 0 {
				t.Fatal("expected zero affected rows", n, err)
			}
			_, err = readSQL(t.Context(), &target, "transaction-proof", statement{text: update}, false)
			var pg *pgconn.PgError
			var my *mysql.MySQLError
			if !(errors.As(err, &pg) && pg.Code == "25006" || errors.As(err, &my) && my.Number == 1792) {
				t.Fatalf("missing native read-only transaction rejection: %v", err)
			}
			// Keep the client deadline later than the configured database timer.
			ctx, cancel := context.WithTimeout(t.Context(), queryTimeout+10*time.Second)
			defer cancel()
			_, err = readSQL(ctx, cat.byName[name], "deadline-proof", statement{text: slow}, false)
			if !(errors.As(err, &pg) && pg.Code == "57014" || errors.As(err, &my) && my.Number == 3024) || ctx.Err() != nil {
				t.Fatalf("expected the database to stop the query first: %v (%v)", err, ctx.Err())
			}
			if code, _ := publicError(ctx, err); code != "timeout" {
				t.Fatalf("native timeout was reported as %s", code)
			}
			checkSQLCancellation(t, db, cat.byName[name], slow)
		})
	}
}

func checkSQLCancellation(t *testing.T, observer *sql.DB, target *Target, slow string) {
	t.Helper()
	slow += " /* fixture cancellation check */"
	lookup := "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = 'meter-mcp/cancel-proof' AND state = 'active'"
	var params []any
	if target.Engine == "mysql" {
		lookup = "SELECT COUNT(*) FROM information_schema.processlist WHERE id <> CONNECTION_ID() AND info = ?"
		params = []any{slow}
	}
	waitFor := func(want int, bound time.Duration) {
		t.Helper()
		deadline, cancel := context.WithTimeout(t.Context(), bound)
		defer cancel()
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			var active int
			if err := observer.QueryRowContext(deadline, lookup, params...).Scan(&active); err != nil {
				t.Fatal("observe fixture query", err)
			}
			if active == want {
				return
			}
			select {
			case <-deadline.Done():
				t.Fatalf("expected %d active fixture queries within %s; saw %d", want, bound, active)
			case <-tick.C:
			}
		}
	}
	client, stop := context.WithCancel(t.Context())
	defer stop()
	done := make(chan error, 1)
	go func() {
		_, err := readSQL(client, target, "cancel-proof", statement{text: slow}, false)
		done <- err
	}()
	waitFor(1, 5*time.Second)
	start := time.Now()
	stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("unexpected client cancellation result", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client did not return after cancellation")
	}
	// The server can stop immediately or on its native timer. Allow for
	// polling and CI scheduling after the 15-second database limit.
	waitFor(0, time.Until(start.Add(queryTimeout+2*time.Second)))
	t.Logf("canceled client returned; server query gone after %s", time.Since(start).Round(time.Millisecond))
}

func TestDemoMongoNullFilters(t *testing.T) {
	target := demoCatalog(t).byName["demo-mongo"]
	for _, tc := range []struct {
		field          any
		equal, unequal int
	}{
		{0, 1, 0},                      // Exclude the field (missing).
		{stage("$literal", nil), 1, 0}, // Explicit null.
		{1, 0, 1},                      // Keep the existing non-null state.
	} {
		for _, op := range []struct {
			name string
			want int
		}{{"$eq", tc.equal}, {"$ne", tc.unequal}} {
			filter, err := json.Marshal(document{"state": document{op.name: nil}})
			if err != nil {
				t.Fatal(err)
			}
			count, err := mongoPipeline("mongo_count", []string{"state"}, arguments{Filter: filter})
			if err != nil {
				t.Fatal(err)
			}
			// Shape one existing fixture row in memory on the server.
			one := mongo.Pipeline{stage("$limit", 1), stage("$project", stage("state", tc.field))}
			result, err := readMongo(t.Context(), target, "null-proof", "readings", append(one, count...), 1, false)
			if err != nil {
				t.Fatal(err)
			}
			rows := result["rows"].([]any)
			if op.want == 0 {
				checkJSON(t, rows, "[]")
			} else {
				checkJSON(t, rows, `[{"total":1}]`)
			}
		}
	}
}

func TestDemoMongoDoubleFilter(t *testing.T) {
	target := demoCatalog(t).byName["demo-mongo"]
	count, err := mongoPipeline("mongo_count", []string{"watts"}, arguments{Filter: json.RawMessage(`{"watts":0.1}`)})
	if err != nil {
		t.Fatal(err)
	}
	// A Decimal128 0.1 would not equal this native BSON double.
	one := mongo.Pipeline{stage("$limit", 1), stage("$project", stage("watts", stage("$literal", 0.1)))}
	result, err := readMongo(t.Context(), target, "double-proof", "readings", append(one, count...), 1, false)
	if err != nil {
		t.Fatal(err)
	}
	checkJSON(t, result["rows"], `[{"total":1}]`)
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
