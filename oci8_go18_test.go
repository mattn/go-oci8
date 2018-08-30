// +build go1.8

package oci8

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"
)

func TestNamedParam(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	r := sqlstest(TestDB, t, "select :foo||:bar as message from dual", sql.Named("foo", "hello"), sql.Named("bar", "world"))
	if "helloworld" != r["MESSAGE"].(string) {
		t.Fatal("message should be: helloworld", r)
	}
}

func TestTimeout(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	for i := 0; i < 2000; i++ {
		TestDB.Exec("insert into foo(c3) values(:1)", i)
	}

	var wg sync.WaitGroup
	wg.Add(10)

	f := func() {
		defer wg.Done()

		stmt, err := TestDB.Prepare(`select * from foo order by c3`)
		if err != nil {
			t.Fatal(err)
		}
		ctx, _ := context.WithTimeout(context.Background(), 200*time.Millisecond)
		rows, err := stmt.QueryContext(ctx)
		if err != nil && err.Error() != "ORA-01013: user requested cancel of current operation\n" {
			t.Fatal(err)
		}
		if err != nil && rows != nil {
			defer rows.Close()
		}
		err = ctx.Err()
		if err != nil && err != context.DeadlineExceeded {
			t.Fatal(err)
		}
	}

	for j := 0; j < 10; j++ {
		go f()
	}

	wg.Wait()
}
