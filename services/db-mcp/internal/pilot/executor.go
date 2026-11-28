package pilot

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/lilabrooks/my-local-platform/db-mcp/internal/dbmcp"
)

type Executor struct {
	Tools                             *dbmcp.Tools
	Account, ReaderRole, Host, CAFile string
	Port                              int
	Password                          func(context.Context) (string, error)
	Audit                             *dbmcp.AuditSink
}

func (e *Executor) Handle(ctx context.Context, call Invocation) (json.RawMessage, error) {
	if !requestIDPattern.MatchString(call.RequestID) || !accountPattern.MatchString(e.Account) || !rolePattern.MatchString(e.ReaderRole) ||
		call.RoleID != e.ReaderRole || roleSession(e.Account, call.Caller) == "" {
		return nil, errors.New("invalid router invocation")
	}
	target, err := e.Tools.Target(call.Tool, call.Arguments)
	if err != nil || target != TargetName {
		return nil, errors.New("invalid executor target or tool")
	}
	slot := e.Audit.Reserve()
	if slot == nil {
		return dbmcp.AuditUnavailable(call.RequestID), nil
	}
	defer slot.Release()
	password, err := e.Password(ctx)
	if err != nil {
		return nil, errors.New("reader credential unavailable")
	}
	catalog, err := dbmcp.NewMySQLReader(TargetName, e.Host, e.Port, e.CAFile, password)
	if err != nil {
		return nil, errors.New("invalid reader configuration")
	}
	return e.Tools.Execute(ctx, catalog, slot, call.Caller, call.RequestID, call.Tool, call.Arguments)
}
