package dbmcp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type document = map[string]any

func postgresConfig(t *Target, password, rid string) (*pgx.ConnConfig, error) {
	endpoint := url.URL{Scheme: "postgres", User: url.User(t.Username), Host: net.JoinHostPort(t.Host, strconv.Itoa(t.Port)), Path: "/" + t.Database, RawQuery: "sslmode=disable"}
	cfg, err := pgx.ParseConfig(endpoint.String())
	if err != nil {
		return nil, err
	}
	cfg.Password = password
	cfg.ConnectTimeout = 5 * time.Second
	cfg.RuntimeParams = map[string]string{"application_name": "meter-mcp/" + rid}
	cfg.TLSConfig, err = t.tlsConfig()
	if err != nil {
		return nil, err
	}
	if cfg.TLSConfig != nil {
		cfg.TLSConfig.ServerName = t.Host
	}
	cfg.Fallbacks = nil
	return cfg, nil
}

func mysqlConfig(t *Target, password, rid string) (*mysql.Config, error) {
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.DBName = t.Username, password, t.Database
	cfg.Net, cfg.Addr = "tcp", net.JoinHostPort(t.Host, strconv.Itoa(t.Port))
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 5*time.Second, queryTimeout+2*time.Second, 5*time.Second
	cfg.ParseTime = true
	cfg.Logger = &mysql.NopLogger{}
	cfg.ConnectionAttributes = "program_name:meter-mcp/" + rid
	tlsConfig, err := t.tlsConfig()
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		tlsConfig.ServerName = t.Host
	}
	cfg.TLS = tlsConfig
	return cfg, nil
}

func openSQL(t *Target, rid string) (*sql.DB, error) {
	pass, err := t.password()
	if err != nil {
		return nil, err
	}
	if t.Engine == "postgres" {
		cfg, err := postgresConfig(t, pass, rid)
		if err != nil {
			return nil, err
		}
		return stdlib.OpenDB(*cfg), nil
	}
	cfg, err := mysqlConfig(t, pass, rid)
	if err != nil {
		return nil, err
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

func readSQL(ctx context.Context, t *Target, rid string, stmt statement, explain bool) (document, error) {
	db, err := openSQL(t, rid)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if t.Engine == "mysql" {
		_, err = conn.ExecContext(ctx, "SET SESSION max_execution_time = "+strconv.FormatInt(queryTimeout.Milliseconds(), 10))
		if err != nil {
			return nil, err
		}
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if t.Engine == "postgres" {
		_, err = tx.ExecContext(ctx, "SET LOCAL statement_timeout = "+strconv.FormatInt(queryTimeout.Milliseconds(), 10))
		if err != nil {
			return nil, err
		}
	} else if stmt.mysqlTypeQuery != "" {
		if err = checkMySQLComparisons(ctx, tx, stmt); err != nil {
			return nil, err
		}
	}
	if explain {
		prefix := "EXPLAIN FORMAT=JSON "
		if t.Engine == "postgres" {
			prefix = "EXPLAIN (FORMAT JSON) "
		}
		var raw []byte
		err = tx.QueryRowContext(ctx, prefix+stmt.text, stmt.args...).Scan(&raw)
		var denied *mysql.MySQLError
		if errors.As(err, &denied) && denied.Number == 1345 {
			return nil, refusal("MySQL withheld the execution plan for this view.")
		}
		if err != nil {
			return nil, err
		}
		var tree any
		if err = json.Unmarshal(raw, &tree); err != nil {
			return nil, err
		}
		return document{"operators": planNodes(tree, t.Engine)}, nil
	}
	rows, err := tx.QueryContext(ctx, stmt.text, stmt.args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		for i, v := range values {
			values[i] = sqlScalar(v, types[i].DatabaseTypeName())
		}
		result = append(result, values)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return document{"columns": columns, "rows": result}, nil
}

const nonScalar = "[non-scalar]"

// Resolve only approved predicate columns, using the same reader, view and
// transaction as the result query. No additional metadata grants are needed.
func checkMySQLComparisons(ctx context.Context, tx *sql.Tx, stmt statement) error {
	rows, err := tx.QueryContext(ctx, stmt.mysqlTypeQuery)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	const incompatible = refusal("Use a value compatible with the MySQL column type.")
	if len(columns) != len(stmt.mysqlNumeric) {
		return incompatible
	}
	for i, column := range columns {
		var numeric bool
		switch strings.TrimPrefix(column.DatabaseTypeName(), "UNSIGNED ") {
		case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "DECIMAL", "FLOAT", "DOUBLE", "BIT", "YEAR":
			numeric = true
		case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM", "SET",
			"BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB", "DATE", "TIME", "DATETIME", "TIMESTAMP":
			numeric = false
		default:
			return incompatible
		}
		if numeric != stmt.mysqlNumeric[i] {
			return incompatible
		}
	}
	return rows.Close()
}

func isoTimestamp(t time.Time, withZone bool) string {
	layout := "2006-01-02T15:04:05"
	if t.Nanosecond()/1000 != 0 {
		layout += ".000000"
	}
	if withZone {
		layout += "-07:00"
	}
	return t.Format(layout)
}

func sqlScalar(value any, dbType string) any {
	if value == nil {
		return nil
	}
	switch strings.TrimPrefix(dbType, "UNSIGNED ") {
	case "BOOL", "INT2", "INT4", "INT8", "OID", "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT",
		"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "NUMERIC", "DECIMAL", "YEAR",
		"TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "NAME", "CHAR", "BPCHAR", "VARCHAR", "ENUM", "UUID",
		"DATE", "DATETIME", "TIMESTAMP", "TIMESTAMPTZ":
		// Known scalar types can use either a native Go value or the SQL text encoding.
	case "JSON", "JSONB":
		var raw []byte
		switch v := value.(type) {
		case []byte:
			raw = v
		case string:
			raw = []byte(v)
		default:
			return nonScalar
		}
		var parsed any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&parsed); err != nil {
			return nonScalar
		}
		switch parsed.(type) {
		case nil, bool, json.Number, string:
			return parsed
		default:
			return nonScalar
		}
	default:
		// Drivers also return opaque composites, arrays and geometry as text or bytes.
		return nonScalar
	}
	switch v := value.(type) {
	case time.Time:
		switch dbType {
		case "DATE":
			return v.Format(time.DateOnly)
		case "DATETIME", "TIMESTAMP":
			return isoTimestamp(v, false)
		default:
			return isoTimestamp(v, true)
		}
	case []byte:
		return string(v)
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil
		}
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil
		}
	}
	return value
}
