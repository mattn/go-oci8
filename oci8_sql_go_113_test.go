// +build go1.13

package oci8

import (
	"context"
	"testing"
)

// TestStatementCaching tests to ensure statement caching is working
func TestStatementCaching(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	t.Parallel()

	var err error

	db := testGetDB("?stmt_cache_size=10")
	if db == nil {
		t.Fatal("db is null")
	}

	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal("db close error:", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	conn, err := db.Conn(ctx)
	cancel()
	// we need to get access to the raw connection so we can access the different fields on the oci8.Stmt
	var rawConn *Conn
	// NOTE that conn.Raw() is only available with Go >= 1.13
	_ = conn.Raw(func(driverConn interface{}) error {
		rawConn = driverConn.(*Conn)
		return nil
	})

	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := rawConn.PrepareContext(ctx, "select ?, ?, ? from dual")
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	rawStmt := stmt.(*Stmt)
	if rawStmt.cacheKey != "select ?, ?, ? from dual" {
		err := stmt.Close()
		if err != nil {
			t.Fatal("stmt close error:", err)
		}
		t.Fatalf("cacheKey not equal: expected %s, but got %s", "select ?, ?, ? from dual", rawStmt.cacheKey)
	}

	// closing the statement should put the statement into the cache
	err = stmt.Close()
	if err != nil {
		t.Fatal("stmt close error:", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err = rawConn.PrepareContext(ctx, "select ?, ?, ? from dual")
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	rawStmt = stmt.(*Stmt)
	if rawStmt.cacheKey != "select ?, ?, ? from dual" {
		err := stmt.Close()
		if err != nil {
			t.Fatal("stmt close error:", err)
		}
		t.Fatalf("cacheKey not equal: expected %s, but got %s", "select ?, ?, ? from dual", rawStmt.cacheKey)
	}
	err = stmt.Close()
	if err != nil {
		t.Fatal("stmt close error:", err)
	}
}
