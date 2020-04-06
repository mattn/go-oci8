// +build go1.12

package oci8_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/mattn/go-oci8"
)

func Example_sqlCursor() {
	// Example shows how to do a cursor select

	// For testing, check if database tests are disabled
	if oci8.TestDisableDatabase {
		fmt.Println(3)
		return
	}

	oci8.Driver.Logger = log.New(os.Stderr, "oci8 ", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile)

	var openString string
	// [username/[password]@]host[:port][/service_name][?param1=value1&...&paramN=valueN]
	if len(oci8.TestUsername) > 0 {
		if len(oci8.TestPassword) > 0 {
			openString = oci8.TestUsername + "/" + oci8.TestPassword + "@"
		} else {
			openString = oci8.TestUsername + "@"
		}
	}
	openString += oci8.TestHostValid

	// A normal simple Open to localhost would look like:
	// db, err := sql.Open("oci8", "127.0.0.1")
	// For testing, need to use additional variables
	db, err := sql.Open("oci8", openString)
	if err != nil {
		fmt.Printf("Open error is not nil: %v", err)
		return
	}
	if db == nil {
		fmt.Println("db is nil")
		return
	}

	// defer close database
	defer func() {
		err = db.Close()
		if err != nil {
			fmt.Println("Close error is not nil:", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	err = db.PingContext(ctx)
	cancel()
	if err != nil {
		fmt.Println("PingContext error is not nil:", err)
		return
	}

	var rows *sql.Rows
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	rows, err = db.QueryContext(ctx, "select 1, cursor(select 2 from dual union select 3 from dual) from dual")
	if err != nil {
		fmt.Println("QueryContext error is not nil:", err)
		return
	}

	// defer close rows
	defer func() {
		err = rows.Close()
		if err != nil {
			fmt.Println("Close error is not nil:", err)
		}
	}()

	if !rows.Next() {
		fmt.Println("no Next rows")
		return
	}

	var aInt int64
	var subRows *sql.Rows
	err = rows.Scan(&aInt, &subRows)
	if err != nil {
		fmt.Println("Scan error is not nil:", err)
		return
	}

	if aInt != 1 {
		fmt.Println("aInt != 1")
		return
	}
	if subRows == nil {
		fmt.Println("subRows is nil")
		return
	}

	if !subRows.Next() {
		fmt.Println("no Next subRows")
		return
	}

	err = subRows.Scan(&aInt)
	if err != nil {
		fmt.Println("Scan error is not nil:", err)
		return
	}

	if aInt != 2 {
		fmt.Println("aInt != 2")
		return
	}

	if !subRows.Next() {
		fmt.Println("no Next subRows")
		return
	}

	err = subRows.Scan(&aInt)
	if err != nil {
		fmt.Println("Scan error is not nil:", err)
		return
	}

	if aInt != 3 {
		fmt.Println("aInt != 3")
		return
	}

	if subRows.Next() {
		fmt.Println("has Next rows")
		return
	}

	err = subRows.Err()
	if err != nil {
		fmt.Println("Err error is not nil:", err)
		return
	}

	if rows.Next() {
		fmt.Println("has Next rows")
		return
	}

	err = rows.Err()
	if err != nil {
		fmt.Println("Err error is not nil:", err)
		return
	}

	fmt.Println(aInt)

	// output: 3
}

func Example_sqlCursorFunction() {
	// Example shows how to do a cursor select from function

	// For testing, check if database tests are disabled
	if oci8.TestDisableDatabase || oci8.TestDisableDestructive {
		fmt.Println(3)
		return
	}

	oci8.Driver.Logger = log.New(os.Stderr, "oci8 ", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile)

	var openString string
	// [username/[password]@]host[:port][/service_name][?param1=value1&...&paramN=valueN]
	if len(oci8.TestUsername) > 0 {
		if len(oci8.TestPassword) > 0 {
			openString = oci8.TestUsername + "/" + oci8.TestPassword + "@"
		} else {
			openString = oci8.TestUsername + "@"
		}
	}
	openString += oci8.TestHostValid

	// A normal simple Open to localhost would look like:
	// db, err := sql.Open("oci8", "127.0.0.1")
	// For testing, need to use additional variables
	db, err := sql.Open("oci8", openString)
	if err != nil {
		fmt.Printf("Open error is not nil: %v", err)
		return
	}
	if db == nil {
		fmt.Println("db is nil")
		return
	}

	// defer close database
	defer func() {
		err = db.Close()
		if err != nil {
			fmt.Println("Close error is not nil:", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	err = db.PingContext(ctx)
	cancel()
	if err != nil {
		fmt.Println("PingContext error is not nil:", err)
		return
	}

	// create function
	functionName := "E_F_CURSOR_" + oci8.TestTimeString
	query := `create or replace function ` + functionName + ` return SYS_REFCURSOR
	is
		l_cursor SYS_REFCURSOR;
	begin
		open l_cursor for select 2 from dual union select 3 from dual;
		return l_cursor;
	end ` + functionName + `;`
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	_, err = db.ExecContext(ctx, query)
	cancel()
	if err != nil {
		fmt.Println("ExecContext error is not nil:", err)
		return
	}

	var rows *sql.Rows
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	rows, err = db.QueryContext(ctx, "select 1, "+functionName+"() from dual")
	if err != nil {
		fmt.Println("QueryContext error is not nil:", err)
		return
	}

	// defer close rows
	defer func() {
		err = rows.Close()
		if err != nil {
			fmt.Println("Close error is not nil:", err)
		}
	}()

	if !rows.Next() {
		fmt.Println("no Next rows")
		return
	}

	var aInt int64
	var subRows *sql.Rows
	err = rows.Scan(&aInt, &subRows)
	if err != nil {
		fmt.Println("Scan error is not nil:", err)
		return
	}

	if aInt != 1 {
		fmt.Println("aInt != 1")
		return
	}
	if subRows == nil {
		fmt.Println("subRows is nil")
		return
	}

	if !subRows.Next() {
		fmt.Println("no Next subRows")
		return
	}

	err = subRows.Scan(&aInt)
	if err != nil {
		fmt.Println("Scan error is not nil:", err)
		return
	}

	if aInt != 2 {
		fmt.Println("aInt != 2")
		return
	}

	if !subRows.Next() {
		fmt.Println("no Next subRows")
		return
	}

	err = subRows.Scan(&aInt)
	if err != nil {
		fmt.Println("Scan error is not nil:", err)
		return
	}

	if aInt != 3 {
		fmt.Println("aInt != 3")
		return
	}

	if subRows.Next() {
		fmt.Println("has Next rows")
		return
	}

	err = subRows.Err()
	if err != nil {
		fmt.Println("Err error is not nil:", err)
		return
	}

	if rows.Next() {
		fmt.Println("has Next rows")
		return
	}

	err = rows.Err()
	if err != nil {
		fmt.Println("Err error is not nil:", err)
		return
	}

	// drop function
	query = "drop function " + functionName
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	_, err = db.ExecContext(ctx, query)
	cancel()
	if err != nil {
		fmt.Println("ExecContext error is not nil:", err)
		return
	}

	fmt.Println(aInt)

	// output: 3
}
