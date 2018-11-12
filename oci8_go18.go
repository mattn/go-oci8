package oci8

import (
	"database/sql"
	"database/sql/driver"

	"context"
)

func toNamedValue(nv driver.NamedValue) namedValue {
	mv := namedValue(nv)
	return mv
}

// QueryContext implement QueryerContext.
func (conn *OCI8Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	list := make([]namedValue, len(args))
	for i, nv := range args {
		list[i] = toNamedValue(nv)
	}
	return conn.query(ctx, query, list)
}

// ExecContext implement ExecerContext.
func (conn *OCI8Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	list := make([]namedValue, len(args))
	for i, nv := range args {
		list[i] = toNamedValue(nv)
	}
	return conn.exec(ctx, query, list)
}

// QueryContext implement QueryerContext.
func (stmt *OCI8Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	list := make([]namedValue, len(args))
	for i, nv := range args {
		list[i] = toNamedValue(nv)
	}
	return stmt.query(ctx, list, false)
}

// ExecContext implement ExecerContext.
func (stmt *OCI8Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	list := make([]namedValue, len(args))
	for i, nv := range args {
		list[i] = toNamedValue(nv)
	}
	return stmt.exec(ctx, list)
}

// CheckNamedValue checks the named value
func (conn *OCI8Conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	default:
		return driver.ErrSkip
	case sql.Out:
		return nil
	}
}
