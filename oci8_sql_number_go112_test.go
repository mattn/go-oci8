// +build go1.12

package oci8

import (
	"context"
	"database/sql"
	"testing"
)

// TestDestructiveNumberCursor checks select cursor
func TestDestructiveNumberCursor(t *testing.T) {
	if TestDisableDatabase || TestDisableDestructive {
		t.SkipNow()
	}

	tableName := "number_cursor_" + TestTimeString
	err := testExec(t, "create table "+tableName+" ( A INTEGER )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	testDestructiveNumberCursorInsert(t, tableName)

	testDestructiveNumberCursorSelect(t, tableName)

	testDestructiveNumberCursorSelectWithClose(t, tableName)

	testDestructiveNumberCursorFunction(t, tableName)
}

func testDestructiveNumberCursorInsert(t *testing.T, tableName string) {
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, "insert into "+tableName+" ( A ) values (:1)")
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	defer func() {
		err = stmt.Close()
		if err != nil {
			if t.Failed() {
				t.Logf("stmt close error: %v", err)
			} else {
				t.Fatal("stmt close error:", err)
			}
		}
	}()

	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	_, err = stmt.ExecContext(ctx, 1)
	cancel()
	if err != nil {
		t.Fatal("exec error:", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	_, err = stmt.ExecContext(ctx, 2)
	cancel()
	if err != nil {
		stmt.Close()
		t.Fatal("exec error:", err)
	}
}

func testDestructiveNumberCursorSelect(t *testing.T, tableName string) {
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, "select 1.5, cursor(select A from "+tableName+" order by A) from dual")
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	defer func() {
		err = stmt.Close()
		if err != nil {
			if t.Failed() {
				t.Logf("stmt close err: %v", err)
			} else {
				t.Fatal("stmt close error:", err)
			}
		}
	}()

	var rows *sql.Rows
	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	rows, err = stmt.QueryContext(ctx)
	if err != nil {
		cancel()
		t.Fatal("query error:", err)
	}

	defer func() {
		cancel()
		err = rows.Close()
		if err != nil {
			if t.Failed() {
				t.Logf("rows close error: %v", err)
			} else {
				t.Fatal("rows close error:", err)
			}
		}
	}()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	var float float64
	var subRows *sql.Rows
	err = rows.Scan(&float, &subRows)
	if err != nil {
		t.Fatal("scan error:", err)
	}

	if float != 1.5 {
		t.Fatal("float != 1.5")
	}

	if subRows == nil {
		t.Fatal("subRows is nil")
	}

	if !subRows.Next() {
		t.Fatal("expected row")
	}

	var aInt int64
	err = subRows.Scan(&aInt)
	if err != nil {
		t.Fatal("scan error:", err)
	}

	if aInt != 1 {
		t.Fatal("aInt != 1")
	}

	if !subRows.Next() {
		t.Fatal("expected row")
	}

	err = subRows.Scan(&aInt)
	if err != nil {
		t.Fatal("scan error:", err)
	}

	if aInt != 2 {
		t.Fatal("aInt != 2")
	}

	if subRows.Next() {
		t.Fatal("unexpected row")
	}

	if rows.Next() {
		t.Fatal("unexpected row")
	}
}

func testDestructiveNumberCursorSelectWithClose(t *testing.T, tableName string) {
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, "select 1.5, cursor(select A from "+tableName+" order by A) from dual")
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	defer func() {
		err = stmt.Close()
		if err != nil {
			if t.Failed() {
				t.Logf("stmt close err: %v", err)
			} else {
				t.Fatal("stmt close error:", err)
			}
		}
	}()

	var rows *sql.Rows
	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	rows, err = stmt.QueryContext(ctx)
	if err != nil {
		cancel()
		t.Fatal("query error:", err)
	}

	defer func() {
		cancel()
		err = rows.Close()
		if err != nil {
			if t.Failed() {
				t.Logf("rows close error: %v", err)
			} else {
				t.Fatal("rows close error:", err)
			}
		}
	}()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	var float float64
	var subRows *sql.Rows
	err = rows.Scan(&float, &subRows)
	if err != nil {
		t.Fatal("scan error:", err)
	}

	if float != 1.5 {
		t.Fatal("float != 1.5")
	}

	if subRows == nil {
		t.Fatal("subRows is nil")
	}

	if !subRows.Next() {
		t.Fatal("expected row")
	}

	var aInt int64
	err = subRows.Scan(&aInt)
	if err != nil {
		t.Fatal("scan error:", err)
	}

	if aInt != 1 {
		t.Fatal("aInt != 1")
	}

	if !subRows.Next() {
		t.Fatal("expected row")
	}

	err = subRows.Scan(&aInt)
	if err != nil {
		t.Fatal("scan error:", err)
	}

	if aInt != 2 {
		t.Fatal("aInt != 2")
	}

	if subRows.Next() {
		t.Fatal("unexpected row")
	}

	err = subRows.Close()
	if err != nil {
		t.Fatal("subRows close error:", err)
	}

	if rows.Next() {
		t.Fatal("unexpected row")
	}
}

func testDestructiveNumberCursorFunction(t *testing.T, tableName string) {
	functionName := "F_" + tableName
	err := testExec(t, `create or replace function `+functionName+` return SYS_REFCURSOR
is
	l_cursor SYS_REFCURSOR;
begin
	open l_cursor for select A from `+tableName+` order by A;
	return l_cursor;
end `+functionName+`;`, nil)
	if err != nil {
		t.Fatal("create function error:", err)
	}

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
		stmt, err := TestDB.PrepareContext(ctx, "drop function "+functionName)
		cancel()
		if err != nil {
			t.Fatal("prepare error:", err)
		}

		ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
		_, err = stmt.ExecContext(ctx)
		cancel()
		if err != nil {
			stmt.Close()
			t.Fatal("exec error:", err)
		}

		err = stmt.Close()
		if err != nil {
			t.Fatal("stmt close error:", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, "select 1.5, "+functionName+"() from dual")
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	defer func() {
		err = stmt.Close()
		if err != nil {
			if t.Failed() {
				t.Logf("stmt close err: %v", err)
			} else {
				t.Fatal("stmt close error:", err)
			}
		}
	}()

	var rows *sql.Rows
	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	rows, err = stmt.QueryContext(ctx)
	if err != nil {
		cancel()
		t.Fatal("query error:", err)
	}

	defer func() {
		cancel()
		err = rows.Close()
		if err != nil {
			if t.Failed() {
				t.Logf("rows close error: %v", err)
			} else {
				t.Fatal("rows close error:", err)
			}
		}
	}()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	var float float64
	var subRows *sql.Rows
	err = rows.Scan(&float, &subRows)
	if err != nil {
		t.Fatal("scan error:", err)
	}

	if float != 1.5 {
		t.Fatal("float != 1.5")
	}

	if subRows == nil {
		t.Fatal("subRows is nil")
	}

	if !subRows.Next() {
		t.Fatal("expected row")
	}

	var aInt int64
	err = subRows.Scan(&aInt)
	if err != nil {
		t.Fatal("scan error:", err)
	}

	if aInt != 1 {
		t.Fatal("aInt != 1")
	}

	if !subRows.Next() {
		t.Fatal("expected row")
	}

	err = subRows.Scan(&aInt)
	if err != nil {
		t.Fatal("scan error:", err)
	}

	if aInt != 2 {
		t.Fatal("aInt != 2")
	}

	if subRows.Next() {
		t.Fatal("unexpected row")
	}

	if rows.Next() {
		t.Fatal("unexpected row")
	}
}
