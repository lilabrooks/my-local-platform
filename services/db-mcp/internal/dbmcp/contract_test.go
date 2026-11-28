package dbmcp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/jackc/pgx/v5/pgconn"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func demoCatalog(t *testing.T) *Catalog {
	t.Helper()
	cat, err := LoadCatalog("../../demo.toml")
	if err != nil {
		t.Fatal(err)
	}
	return cat
}
func TestCatalogBoundaries(t *testing.T) {
	original, err := os.ReadFile("../../demo.toml")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, from, to string }{
		{"remote cleartext", "127.0.0.1", "db.example.invalid"},
		{"nonlocal cleartext", `environment = "local"`, `environment = "dev"`},
		{"production", `environment = "local"`, `environment = "production"`},
		{"redirected mysql schema", `engine = "mysql"`, "engine = \"mysql\"\nschema = \"private\""},
		{"duplicate target", "demo-mongo", "demo-mysql"},
		{"repeated field", `"watts"]`, `"state"]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "catalog.toml")
			if err := os.WriteFile(path, []byte(strings.ReplaceAll(string(original), test.from, test.to)), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadCatalog(path); err == nil {
				t.Fatal("catalog accepted")
			}
		})
	}
}
func TestCompileSQL(t *testing.T) {
	cat := demoCatalog(t)
	for _, engine := range []string{"postgres", "mysql"} {
		t.Run(engine, func(t *testing.T) {
			target := cat.byName["demo-"+engine]
			sql := "SELECT state,COUNT(*) AS total FROM readings WHERE (state = ? OR station = ?) AND watts >= 10 GROUP BY state ORDER BY total DESC LIMIT 1000;"
			query, err := compileSQL(target, sql, []any{"ready", "river"}, 3)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(query.args, []any{"ready", "river", int64(10)}) || strings.Contains(query.text, "ready") || !strings.HasSuffix(query.text, " LIMIT 4") {
				t.Fatal(query)
			}
			want := `("state" = $1 OR "station" = $2) AND "watts" >= $3`
			if engine == "mysql" {
				want = "(\x60state\x60 = ? OR \x60station\x60 = ?) AND \x60watts\x60 >= ?"
			}
			if !strings.Contains(query.text, want) {
				t.Fatal(query.text)
			}
			for _, bad := range []string{"DELETE FROM readings", "UPDATE readings SET watts=0", "SELECT * FROM meter_events",
				"SELECT service_note FROM readings", "SELECT * FROM readings; SELECT 1", "SELECT station FROM readings FOR UPDATE",
				"SELECT * FROM readings JOIN meter_events ON 1=1", "SELECT unknown_fn(watts) FROM readings", "SELECT * FROM other.readings",
				"SELECT * FROM readings WHERE watts IN (SELECT watts FROM meter_events)", "SELECT * FROM readings LIMIT 1 OFFSET 20",
				"SELECT * FROM readings WHERE " + strings.Repeat("(", 21) + "watts=1" + strings.Repeat(")", 21)} {
				if _, err := compileSQL(target, bad, nil, 50); err == nil {
					t.Errorf("accepted %q", bad)
				}
			}
			if _, err := compileSQL(target, "SELECT * FROM readings WHERE state=?", nil, 1); err == nil {
				t.Fatal("missing bind accepted")
			}
			if _, err := compileSQL(target, "SELECT * FROM readings", []any{"extra"}, 1); err == nil {
				t.Fatal("extra bind accepted")
			}
			if q, err := compileSQL(target, "SELECT * FROM readings LIMIT 0", nil, 3); err != nil || !strings.HasSuffix(q.text, " LIMIT 0") {
				t.Fatal(q, err)
			}
			if q, err := compileSQL(target, "SELECT * FROM readings LIMIT 18446744073709551615", nil, 3); err != nil || !strings.HasSuffix(q.text, " LIMIT 4") {
				t.Fatal("full-width limit was not capped", q, err)
			}
			if _, err := compileSQL(target, "SELECT * FROM readings LIMIT 18446744073709551616", nil, 3); err == nil {
				t.Fatal("accepted overflowing limit")
			}
			if _, err := compileSQL(target, "SELECT state FROM readings WHERE state=?", []any{nil}, 1); err == nil {
				t.Fatal("accepted NULL comparison")
			}
		})
	}
}
func TestDialectIdentifiersAndValues(t *testing.T) {
	cat := demoCatalog(t)
	for _, target := range []*Target{cat.byName["demo-postgres"], cat.byName["demo-mysql"]} {
		if _, err := compileSQL(target, "-- approved summary\nSELECT count(*) AS count FROM readings /* meters */ ORDER BY count", nil, 2); err != nil {
			t.Fatal(err)
		}
		if _, err := compileSQL(target, "SELECT /*! hint */ * FROM readings", nil, 2); err == nil {
			t.Fatal("accepted executable comment")
		}
	}
	s, err := compileSQL(cat.byName["demo-postgres"], `SELECT "watts" FROM "public"."readings" WHERE state = 'it''s ready' ORDER BY watts NULLS FIRST`, nil, 2)
	if err != nil || !slices.EqualFunc(s.args, []any{"it's ready"}, reflect.DeepEqual) || !strings.Contains(s.text, "ASC NULLS FIRST") {
		t.Fatal(s, err)
	}
	if _, err := compileSQL(cat.byName["demo-mysql"], "SELECT \x60watts\x60 FROM \x60readings\x60", nil, 2); err != nil {
		t.Fatal(err)
	}
	pg := cat.byName["demo-postgres"]
	if s, err := compileSQL(pg, "SELECT WATTS AS W FROM PUBLIC.READINGS ORDER BY W", nil, 2); err != nil || s.object != "readings" || !strings.Contains(s.text, `"watts" AS "w"`) {
		t.Fatal(s, err)
	}
	if _, err := compileSQL(pg, `SELECT watts FROM "READINGS"`, nil, 2); err == nil {
		t.Fatal("quoted object lost case sensitivity")
	}
}

func TestArgumentsHonorExactSchemaKeys(t *testing.T) {
	var defs map[string]definition
	if err := json.Unmarshal(toolContract, &defs); err != nil {
		t.Fatal(err)
	}
	// Every advertised property must have an explicit decoder field, and every
	// decoder field must be reachable through at least one tool.
	advertised := map[string]bool{}
	for name, def := range defs {
		var spec struct {
			Properties           map[string]json.RawMessage
			AdditionalProperties *bool
		}
		if err := json.Unmarshal(def.InputSchema, &spec); err != nil || spec.AdditionalProperties == nil || *spec.AdditionalProperties {
			t.Fatalf("%s must advertise a closed input object: %v", name, err)
		}
		for key := range spec.Properties {
			advertised[key] = true
		}
	}
	decoded := map[string]bool{}
	for _, field := range reflect.VisibleFields(reflect.TypeFor[arguments]()) {
		decoded[field.Tag.Get("json")] = true
	}
	if !reflect.DeepEqual(advertised, decoded) {
		t.Fatalf("schema keys %v, decoder fields %v", advertised, decoded)
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(defs["list_databases"].InputSchema, &schema); err != nil {
		t.Fatal(err)
	}
	shape, err := schema.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeArguments(json.RawMessage(`{"target":"demo-mysql"}`), shape)
	if err != nil || got.Target != "demo-mysql" {
		t.Fatal(got, err)
	}
	if _, err := decodeArguments(json.RawMessage(`{"target":"demo-mysql","target":"other"}`), shape); err == nil {
		t.Fatal("accepted duplicate target")
	}
	if _, err := decodeArguments(json.RawMessage(`{"target":"demo-mysql","Target":"other"}`), shape); err == nil {
		t.Fatal("accepted a case-mismatched argument")
	}
}

func TestOrderedMongoAndProjection(t *testing.T) {
	fields := []string{"watts", "station", "sequence_no"}
	args := arguments{Filter: json.RawMessage(`{"sequence_no":{"$gte":9007199254740993}}`), Sort: json.RawMessage(`{"watts":-1,"station":1}`)}
	p, err := mongoPipeline("mongo_find", fields, args)
	if err != nil {
		t.Fatal(err)
	}
	order := p[1][0].Value.(bson.D)
	if len(order) != 2 || order[0].Key != "watts" || order[1].Key != "station" {
		t.Fatal(order)
	}
	f := p[0][0].Value.(bson.D)[0].Value.(bson.D)
	if f[0].Value != int64(9007199254740993) {
		t.Fatal("lost integer precision", f)
	}
	for _, raw := range []string{`{"watts":1,"watts":2}`, `{"sequence_no":[1,2]}`, `{"station":{"$regex":"x"}}`, `{"service_note":"x"}`} {
		if _, err := mongoPipeline("mongo_find", fields, arguments{Filter: json.RawMessage(raw)}); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, raw := range []string{`{"station":true}`, `{"service_note":1}`, `{}`} {
		if _, err := mongoPipeline("mongo_find", fields, arguments{Projection: json.RawMessage(raw)}); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	var stages []json.RawMessage
	for _, raw := range []string{`[{"$out":"other"}]`, `[{"$group":{"_id":"$station","n":{"$sum":true}}}]`,
		`[{"$group":{"_id":"$station","n":{"$sum":1}}},{"$sort":{"watts":1}}]`} {
		if err := json.Unmarshal([]byte(raw), &stages); err != nil {
			t.Fatal(err)
		}
		if _, err := aggregate(fields, stages); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}
func TestWholeResponseBudget(t *testing.T) {
	for _, test := range []struct {
		input     []any
		cap, want int
	}{
		{[]any{strings.Repeat("界", 12000)}, 50, 0},
		{[]any{strings.Repeat("界", 4000), strings.Repeat("界", 4000), strings.Repeat("界", 4000)}, 50, 2},
		{[]any{1, 2, 3}, 2, 2},
	} {
		out, n, err := fit(document{"ok": true, "request_id": "id", "rows": test.input}, test.cap)
		if err != nil || n != test.want || len(out) > ByteLimit || !bytes.Contains(out, []byte(`"truncated":true`)) {
			t.Fatal(n, len(out), err)
		}
	}
	if _, _, err := fit(document{"targets": strings.Repeat("a", ByteLimit)}, 50); err == nil {
		t.Fatal("oversized metadata accepted")
	}
}

func TestMySQLDecimalScale(t *testing.T) {
	for _, tt := range []struct {
		number string
		scale  int64 // -1 means the comparison would lose precision.
	}{
		{"10.500", 1}, {"1.05e1", 1}, {"-1e-30", 30}, {"100e-32", 30},
		{"1e64", 0}, {"1e65", -1}, {"1e-31", -1}, {"1e2147483648", -1},
		{strings.Repeat("9", 35) + "." + strings.Repeat("1", 30), 30},
		{strings.Repeat("9", 36) + "." + strings.Repeat("1", 30), -1},
	} {
		got, err := mysqlDecimalScale(tt.number)
		if (err != nil) != (tt.scale == -1) || err == nil && got != tt.scale {
			t.Errorf("%s: scale=%d, err=%v", tt.number, got, err)
		}
	}
}

func TestSafeErrorCodes(t *testing.T) {
	const secret = "sentinel-private-value"
	for _, tt := range []struct {
		err  error
		want string
	}{
		{&pgconn.PgError{Code: "22P02", Message: secret}, "invalid_query"},
		{&pgconn.PgError{Code: "42501", Message: secret}, "unavailable"},
		{&mysql.MySQLError{Number: 1140, Message: secret}, "invalid_query"},
		{&mysql.MySQLError{Number: 1045, Message: secret}, "unavailable"},
		{mongo.CommandError{Code: 14, Message: secret}, "invalid_query"},
		{mongo.CommandError{Code: 50, Message: secret}, "timeout"},
		{mongo.CommandError{Code: 13, Message: secret}, "unavailable"},
		{context.DeadlineExceeded, "timeout"}, {context.Canceled, "canceled"},
		{errors.New(secret), "unavailable"},
	} {
		code, message := publicError(t.Context(), fmt.Errorf("driver %s: %w", secret, tt.err))
		if code != tt.want || strings.Contains(message, secret) {
			t.Errorf("expected safe %s, got %s %q", tt.want, code, message)
		}
	}
}
func TestPlanContainsOnlyOperators(t *testing.T) {
	plan := bson.D{{Key: "queryPlanner", Value: bson.D{
		{Key: "winningPlan", Value: stage("stage", "COLLSCAN")},
		{Key: "parsedQuery", Value: stage("stage", "secret predicate")}}}}
	if got := planNodes(plan, "mongodb"); !slices.Equal(got, []string{"COLLSCAN"}) {
		t.Fatal(got)
	}
}
func TestDriverTLSAndPlacement(t *testing.T) {
	server := httptest.NewTLSServer(nil)
	defer server.Close()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	pass := filepath.Join(dir, "pass")
	cert := server.Certificate()
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pass, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	target := *demoCatalog(t).byName["demo-mongo"]
	target.PasswordFile = pass
	local, err := mongoOptions(&target)
	if err != nil || local.Direct == nil || !*local.Direct || local.TLSConfig != nil {
		t.Fatal("cleartext fixture must stay on its loopback seed", err)
	}
	target.TLS, target.CAFile, target.PasswordFile = "verify", ca, pass
	target.Host, target.ReplicaSet, target.ReadPreference, target.AuthDatabase = "example.com", "rs0", "secondary", "admin"
	pg, err := postgresConfig(&target, "fixture", "id")
	if err != nil {
		t.Fatal(err)
	}
	my, err := mysqlConfig(&target, "fixture", "id")
	if err != nil {
		t.Fatal(err)
	}
	mg, err := mongoOptions(&target)
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range []*tls.Config{pg.TLSConfig, my.TLS, mg.TLSConfig} {
		if config == nil || config.InsecureSkipVerify || config.RootCAs == nil || config.MinVersion < tls.VersionTLS12 {
			t.Fatal("missing TLS verification")
		}
		dialConfig := config.Clone()
		if dialConfig.ServerName == "" {
			dialConfig.ServerName = target.Host
		}
		conn, err := (&tls.Dialer{Config: dialConfig}).DialContext(t.Context(), "tcp", server.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		dialConfig.ServerName = "wrong.invalid"
		if conn, err := (&tls.Dialer{Config: dialConfig}).DialContext(t.Context(), "tcp", server.Listener.Addr().String()); err == nil {
			_ = conn.Close()
			t.Fatal("accepted wrong hostname")
		}
	}
	if len(pg.Fallbacks) != 0 || my.AllowFallbackToPlaintext || mg.RetryWrites == nil || *mg.RetryWrites || mg.ReadPreference.Mode() != readpref.SecondaryMode || *mg.ReplicaSet != "rs0" || mg.Auth.AuthSource != "admin" {
		t.Fatal("unexpected driver configuration")
	}
}
func TestScalarResults(t *testing.T) {
	date := time.Date(2026, 11, 27, 1, 2, 3, 0, time.UTC)
	if got := mongoScalar(bson.NewDateTimeFromTime(date.Add(10 * time.Millisecond))); got != "2026-11-27T01:02:03.010000" {
		t.Fatal(got)
	}
	if got := mongoScalar(bson.NewDecimal128(0, 1)); got != nonScalar {
		t.Fatal(got)
	}
	for _, tt := range []struct {
		value    any
		kind     string
		expected any
	}{
		{[]byte("1.023456789012345678"), "DECIMAL", "1.023456789012345678"},
		{"(visible,private)", "36701", nonScalar}, {"(visible,private)", "RECORD", nonScalar},
		{[]byte{1, 2}, "GEOMETRY", nonScalar},
		{[]byte{0, 1}, "BYTEA", nonScalar}, {date, "DATE", "2026-11-27"}, {date, "DATETIME", "2026-11-27T01:02:03"},
		{[]byte(`{"nested":1}`), "JSON", nonScalar}, {[]byte("true"), "JSON", true}, {math.NaN(), "FLOAT", nil},
	} {
		if got := sqlScalar(tt.value, tt.kind); !reflect.DeepEqual(got, tt.expected) {
			t.Fatal(tt.kind, got)
		}
	}
}
func FuzzCompileSQL(f *testing.F) {
	for _, q := range []string{"SELECT station FROM readings", "SELECT * FROM readings WHERE (state='ready')", "", "SELECT COUNT(*) FROM readings"} {
		f.Add(q)
	}
	t := &Target{Engine: "postgres", Schema: "public", Objects: map[string][]string{"readings": {"station", "state"}}}
	f.Fuzz(func(test *testing.T, q string) {
		s, err := compileSQL(t, q, nil, 3)
		if err == nil && (!strings.HasPrefix(s.text, "SELECT ") || !strings.Contains(s.text, ` FROM "public"."readings"`)) {
			test.Fatal(s)
		}
	})
}
