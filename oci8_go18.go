// +build go1.8

package oci8

import (
	"database/sql"
	"database/sql/driver"

	"context"
)

// Ping implement Pinger.
func (c *OCI8Conn) Ping(ctx context.Context) error {
	return c.ping(ctx)
}

func toNamedValue(nv driver.NamedValue) namedValue {
	mv := namedValue(nv)
	if out, ok := mv.Value.(sql.Out); ok {
		mv.Value = outValue{Dest: out.Dest, In: out.In}
	}
	return mv
}

// QueryContext implement QueryerContext.
func (c *OCI8Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	list := make([]namedValue, len(args))
	for i, nv := range args {
		list[i] = namedValue(nv)
	}
	return c.query(ctx, query, list)
}

// ExecContext implement ExecerContext.
func (c *OCI8Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	list := make([]namedValue, len(args))
	for i, nv := range args {
		list[i] = namedValue(nv)
	}
	return c.exec(ctx, query, list)
}

// PrepareContext implement ConnPrepareContext.
func (c *OCI8Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.prepare(ctx, query)
}

// BeginTx implement ConnBeginTx.
func (c *OCI8Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.begin(ctx)
}

// QueryContext implement QueryerContext.
func (s *OCI8Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	list := make([]namedValue, len(args))
	for i, nv := range args {
		list[i] = namedValue(nv)
	}
	return s.query(ctx, list)
}

// ExecContext implement ExecerContext.
func (s *OCI8Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	list := make([]namedValue, len(args))
	for i, nv := range args {
		list[i] = namedValue(nv)
	}
	return s.exec(ctx, list)
}

func (c *OCI8Conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	default:
		return driver.ErrSkip
	case sql.Out:
		return nil
	}
}
