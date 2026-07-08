// +build go1.10

package oci8

import (
	"context"
	"database/sql"
	"testing"
)

// TestConnector tests that a connection from sql.OpenDB with NewConnector works
func TestConnector(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	var openString string
	if len(TestUsername) > 0 {
		if len(TestPassword) > 0 {
			openString = TestUsername + "/" + TestPassword + "@"
		} else {
			openString = TestUsername + "@"
		}
	}
	openString += TestHostValid

	db := sql.OpenDB(NewConnector(openString))
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	defer cancel()

	err := db.PingContext(ctx)
	if err != nil {
		t.Fatal("ping error:", err)
	}

	var one int64
	err = db.QueryRowContext(ctx, "select 1 from dual").Scan(&one)
	if err != nil {
		t.Fatal("select error:", err)
	}
	if one != 1 {
		t.Fatal("select expected: 1, received:", one)
	}
}
