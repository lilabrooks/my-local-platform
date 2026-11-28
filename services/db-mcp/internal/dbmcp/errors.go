package dbmcp

import (
	"context"
	"errors"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Driver messages can include SQL, field values, hostnames and credentials.
// Only application refusals and these fixed messages may cross the MCP boundary.
func publicError(ctx context.Context, err error) (code, message string) {
	var denied refusal
	if errors.As(err, &denied) {
		return "refused", denied.Error()
	}
	const canceled = "The database operation was canceled."
	const timeout = "The database operation timed out or was interrupted."
	const invalid = "The database rejected the query. Check field types and grouping."
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return "canceled", canceled
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || mongo.IsTimeout(err) {
		return "timeout", timeout
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		if strings.HasPrefix(pg.Code, "22") { // Data exception.
			return "invalid_query", invalid
		}
		switch pg.Code {
		case "42601", "42702", "42703", "42803", "42804", "42883", "42P10", "42P18":
			return "invalid_query", invalid
		case "57014": // Includes a server-side statement timeout.
			return "timeout", timeout
		}
	}
	var my *mysql.MySQLError
	if errors.As(err, &my) {
		switch my.Number {
		case 1052, 1054, 1055, 1064, 1111, 1140, 1264, 1292, 1366, 1690:
			return "invalid_query", invalid
		case 3024:
			return "timeout", timeout
		case 1317:
			return "canceled", canceled
		}
	}
	var mg mongo.ServerError
	if errors.As(err, &mg) && (mg.HasErrorCode(2) || mg.HasErrorCode(9) || mg.HasErrorCode(14)) {
		return "invalid_query", invalid
	}
	return "unavailable", "The database operation could not complete."
}
