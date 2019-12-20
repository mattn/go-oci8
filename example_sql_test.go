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

func Example_sqlSelect() {
	// Example shows how to do a basic select

	// For testing, check if database tests are disabled
	if oci8.TestDisableDatabase {
		fmt.Println(1)
		return
	}

	oci8.OCI8Driver.Logger = log.New(os.Stderr, "oci8 ", log.Ldate|log.Ltime|log.LUTC|log.Llongfile)

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
	rows, err = db.QueryContext(ctx, "select 1 from dual")
	if err != nil {
		fmt.Println("QueryContext error is not nil:", err)
		return
	}
	if !rows.Next() {
		fmt.Println("no Next rows")
		return
	}

	dest := make([]interface{}, 1)
	destPointer := make([]interface{}, 1)
	destPointer[0] = &dest[0]
	err = rows.Scan(destPointer...)
	if err != nil {
		fmt.Println("Scan error is not nil:", err)
		return
	}

	if len(dest) != 1 {
		fmt.Println("len dest != 1")
		return
	}
	data, ok := dest[0].(float64)
	if !ok {
		fmt.Println("dest type not float64")
		return
	}
	if data != 1 {
		fmt.Println("data not equal to 1")
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
	err = rows.Close()
	if err != nil {
		fmt.Println("Close error is not nil:", err)
		return
	}

	fmt.Println(data)

	// output: 1
}

func Example_sqlFunction() {
	// Example shows how to do a function call with binds

	// For testing, check if database tests are disabled
	if oci8.TestDisableDatabase {
		fmt.Println(3)
		return
	}

	oci8.OCI8Driver.Logger = log.New(os.Stderr, "oci8 ", log.Ldate|log.Ltime|log.LUTC|log.Llongfile)

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

	number := int64(2)
	query := `
declare
	function ADD_ONE(p_number INTEGER) return INTEGER as
	begin
		return p_number + 1;
	end ADD_ONE;
begin
	:num1 := ADD_ONE(:num1);
end;`

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	_, err = db.ExecContext(ctx, query, sql.Out{Dest: &number, In: true})
	cancel()
	if err != nil {
		fmt.Println("ExecContext error is not nil:", err)
		return
	}

	if number != 3 {
		fmt.Println("number != 3")
		return
	}

	fmt.Println(number)

	// output: 3
}

func Example_sqlInsert() {
	// Example shows how to do a single insert

	// For testing, check if database tests are disabled
	if oci8.TestDisableDatabase || oci8.TestDisableDestructive {
		fmt.Println(1)
		return
	}

	oci8.OCI8Driver.Logger = log.New(os.Stderr, "oci8 ", log.Ldate|log.Ltime|log.LUTC|log.Llongfile)

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

	// create table
	tableName := "E_INSERT_" + oci8.TestTimeString
	query := "create table " + tableName + " ( A INTEGER )"
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	_, err = db.ExecContext(ctx, query)
	cancel()
	if err != nil {
		fmt.Println("ExecContext error is not nil:", err)
		return
	}

	// insert row
	var result sql.Result
	query = "insert into " + tableName + " ( A ) values (:1)"
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	result, err = db.ExecContext(ctx, query, 1)
	cancel()
	if err != nil {
		fmt.Println("ExecContext error is not nil:", err)
		return
	}

	// can see number of RowsAffected if wanted
	var rowsAffected int64
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		fmt.Println("RowsAffected error is not nil:", err)
		return
	}

	// drop table
	query = "drop table " + tableName
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	_, err = db.ExecContext(ctx, query)
	cancel()
	if err != nil {
		fmt.Println("ExecContext error is not nil:", err)
		return
	}

	fmt.Println(rowsAffected)

	// output: 1
}

func Example_sqlManyInserts() {
	// Example shows how to do a many inserts

	// For testing, check if database tests are disabled
	if oci8.TestDisableDatabase || oci8.TestDisableDestructive {
		fmt.Println(3)
		return
	}

	oci8.OCI8Driver.Logger = log.New(os.Stderr, "oci8 ", log.Ldate|log.Ltime|log.LUTC|log.Llongfile)

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

	// create table
	tableName := "E_MANY_INSERT_" + oci8.TestTimeString
	query := "create table " + tableName + " ( A INTEGER )"
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	_, err = db.ExecContext(ctx, query)
	cancel()
	if err != nil {
		fmt.Println("ExecContext error is not nil:", err)
		return
	}

	// prepare insert query statement
	var stmt *sql.Stmt
	query = "insert into " + tableName + " ( A ) values (:1)"
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	stmt, err = db.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		fmt.Println("PrepareContext error is not nil:", err)
		return
	}

	// insert 3 rows
	for i := 0; i < 3; i++ {
		ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
		_, err = stmt.ExecContext(ctx, i)
		cancel()
		if err != nil {
			stmt.Close()
			fmt.Println("ExecContext error is not nil:", err)
			return
		}
	}

	// close insert query statement
	err = stmt.Close()
	if err != nil {
		fmt.Println("Close error is not nil:", err)
		return
	}

	// select count/number of rows
	var rows *sql.Rows
	query = "select count(1) from " + tableName
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	rows, err = db.QueryContext(ctx, query)
	if err != nil {
		fmt.Println("QueryContext error is not nil:", err)
		return
	}
	if !rows.Next() {
		fmt.Println("no Next rows")
		return
	}

	var count int64
	err = rows.Scan(&count)
	if err != nil {
		fmt.Println("Scan error is not nil:", err)
		return
	}

	if count != 3 {
		fmt.Println("count not equal to 3")
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
	err = rows.Close()
	if err != nil {
		fmt.Println("Close error is not nil:", err)
		return
	}

	// drop table
	query = "drop table " + tableName
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	_, err = db.ExecContext(ctx, query)
	cancel()
	if err != nil {
		fmt.Println("ExecContext error is not nil:", err)
		return
	}

	fmt.Println(count)

	// output: 3
}

func Example_sqlRowid() {
	// Example shows a few ways to get rowid

	// For testing, check if database tests are disabled
	if oci8.TestDisableDatabase || oci8.TestDisableDestructive {
		fmt.Println("done")
		return
	}

	oci8.OCI8Driver.Logger = log.New(os.Stderr, "oci8 ", log.Ldate|log.Ltime|log.LUTC|log.Llongfile)

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

	// create table
	tableName := "E_ROWID_" + oci8.TestTimeString
	query := "create table " + tableName + " ( A INTEGER )"
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	_, err = db.ExecContext(ctx, query)
	cancel()
	if err != nil {
		fmt.Println("ExecContext error is not nil:", err)
		return
	}

	// insert row and get rowid from returning
	var rowid1 string // rowid will be put into here
	var result sql.Result
	query = "insert into " + tableName + " ( A ) values (:1) returning rowid into :rowid1"
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	result, err = db.ExecContext(ctx, query, 1, sql.Named("rowid1", sql.Out{Dest: &rowid1}))
	cancel()
	if err != nil {
		fmt.Println("ExecContext error is not nil:", err)
		return
	}

	// get rowid from LastInsertId
	var rowid2 string // rowid will be put into here
	var id int64
	id, err = result.LastInsertId()
	if err != nil {
		fmt.Println("LastInsertId error is not nil:", err)
		return
	}
	rowid2 = oci8.GetLastInsertId(id)

	// select rowid
	var rowid3 string // rowid will be put into here
	var rows *sql.Rows
	query = "select rowid from " + tableName
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	rows, err = db.QueryContext(ctx, query)
	if err != nil {
		fmt.Println("QueryContext error is not nil:", err)
		return
	}
	if !rows.Next() {
		fmt.Println("no Next rows")
		return
	}

	err = rows.Scan(&rowid3)
	if err != nil {
		fmt.Println("Scan error is not nil:", err)
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
	err = rows.Close()
	if err != nil {
		fmt.Println("Close error is not nil:", err)
		return
	}

	// drop table
	query = "drop table " + tableName
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	_, err = db.ExecContext(ctx, query)
	cancel()
	if err != nil {
		fmt.Println("ExecContext error is not nil:", err)
		return
	}

	if len(rowid1) != len(rowid2) || len(rowid2) != len(rowid3) {
		fmt.Println("rowid len is not equal", rowid1, rowid2, rowid3)
		return
	}

	fmt.Println("done")

	// output: done
}
