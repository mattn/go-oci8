package oci8

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

// testGetDB connects to the test database and returns the database connection
func testGetDB() *sql.DB {
	os.Setenv("NLS_LANG", "American_America.AL32UTF8")

	var openString string
	// [username/[password]@]host[:port][/instance_name][?param1=value1&...&paramN=valueN]
	if len(TestUsername) > 0 {
		if len(TestPassword) > 0 {
			openString = TestUsername + "/" + TestPassword + "@"
		} else {
			openString = TestUsername + "@"
		}
	}
	openString += TestHostValid

	db, err := sql.Open("oci8", openString)
	if err != nil {
		fmt.Println("open error:", err)
		return nil
	}
	if db == nil {
		fmt.Println("db is nil")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	err = db.PingContext(ctx)
	cancel()
	if err != nil {
		fmt.Println("ping error:", err)
		return nil
	}

	db.Exec("drop table foo")

	_, err = db.Exec(sql1)
	if err != nil {
		fmt.Println("sql1 error:", err)
		return nil
	}

	_, err = db.Exec("truncate table foo")
	if err != nil {
		fmt.Println("truncate error:", err)
		return nil
	}

	return db
}

// testGetRows runs a statment and returns the rows as [][]interface{}
func testGetRows(t *testing.T, stmt *sql.Stmt, args []interface{}) ([][]interface{}, error) {
	// get rows
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	defer cancel()
	var rows *sql.Rows
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("query error: %v", err)
	}

	// get column infomration
	var columns []string
	columns, err = rows.Columns()
	if err != nil {
		rows.Close()
		return nil, fmt.Errorf("columns error: %v", err)
	}

	// create values
	values := make([][]interface{}, 0, 1)

	// get values
	pRowInterface := make([]interface{}, len(columns))

	for rows.Next() {
		rowInterface := make([]interface{}, len(columns))
		for i := 0; i < len(rowInterface); i++ {
			pRowInterface[i] = &rowInterface[i]
		}

		err = rows.Err()
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("rows error: %v", err)
		}

		err = rows.Scan(pRowInterface...)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan error: %v", err)
		}

		values = append(values, rowInterface)
	}

	err = rows.Err()
	if err != nil {
		rows.Close()
		return nil, fmt.Errorf("rows error: %v", err)
	}

	err = rows.Close()
	if err != nil {
		return nil, fmt.Errorf("close error: %v", err)
	}

	// return values
	return values, nil
}

// testExec runs an exec query and returns error
func testExec(t *testing.T, query string, args []interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		return fmt.Errorf("prepare error: %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	_, err = stmt.ExecContext(ctx, args...)
	cancel()
	if err != nil {
		stmt.Close()
		return fmt.Errorf("exec error: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		return fmt.Errorf("stmt close error: %v", err)
	}

	return nil
}

// testExecRows runs exec query for each arg row and returns error
func testExecRows(t *testing.T, query string, args [][]interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		return fmt.Errorf("prepare error: %v", err)
	}

	for i := 0; i < len(args); i++ {
		ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
		_, err = stmt.ExecContext(ctx, args[i]...)
		cancel()
		if err != nil {
			stmt.Close()
			return fmt.Errorf("exec - row %v - error: %v", i, err)
		}
	}

	err = stmt.Close()
	if err != nil {
		return fmt.Errorf("stmt close error: %v", err)
	}

	return nil
}

// testRunQueryResults runs a slice of testQueryResults tests
func testRunQueryResults(t *testing.T, queryResults []testQueryResults) {
	for _, queryResult := range queryResults {

		if len(queryResult.args) != len(queryResult.results) {
			t.Errorf("args len %v and results len %v do not match - query: %v",
				len(queryResult.args), len(queryResult.results), queryResult.query)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
		stmt, err := TestDB.PrepareContext(ctx, queryResult.query)
		cancel()
		if err != nil {
			t.Errorf("prepare error: %v - query: %v", err, queryResult.query)
			continue
		}

		testRunQueryResult(t, queryResult, stmt)

		err = stmt.Close()
		if err != nil {
			t.Errorf("close error: %v - query: %v", err, queryResult.query)
		}

	}
}

// testRunQueryResult runs a single testQueryResults test
func testRunQueryResult(t *testing.T, queryResult testQueryResults, stmt *sql.Stmt) {
	for i := 0; i < len(queryResult.args); i++ {
		result, err := testGetRows(t, stmt, queryResult.args[i])
		if err != nil {
			t.Errorf("get rows error: %v - query: %v", err, queryResult.query)
			continue
		}
		if result == nil && queryResult.results[i] != nil {
			t.Errorf("result is nil - query: %v", queryResult.query)
			continue
		}
		if len(result) != len(queryResult.results[i]) {
			t.Errorf("result rows len %v not equal to expected len %v - query: %v",
				len(result), len(queryResult.results[i]), queryResult.query)
			continue
		}

		for j := 0; j < len(result); j++ {
			if len(result[j]) != len(queryResult.results[i][j]) {
				t.Errorf("result columns len %v not equal to expected len %v - query: %v",
					len(result[j]), len(queryResult.results[i][j]), queryResult.query)
				continue
			}

			for k := 0; k < len(result[j]); k++ {
				bad := false
				type1 := reflect.TypeOf(result[j][k])
				type2 := reflect.TypeOf(queryResult.results[i][j][k])
				switch {
				case type1 == nil || type2 == nil:
					if type1 != type2 {
						bad = true
					}
				case type1 == TestTypeTime || type2 == TestTypeTime:
					if type1 != type2 {
						bad = true
						break
					}
					time1 := result[j][k].(time.Time)
					time2 := queryResult.results[i][j][k].(time.Time)
					if !time1.Equal(time2) {
						bad = true
					}
				case type1.Kind() == reflect.Slice || type2.Kind() == reflect.Slice:
					if !reflect.DeepEqual(result[j][k], queryResult.results[i][j][k]) {
						bad = true
					}
				default:
					if result[j][k] != queryResult.results[i][j][k] {
						bad = true
					}
				}
				if bad {
					t.Errorf("result - %v row %v, %v - received: %T, %v  - expected: %T, %v - query: %v", i, j, k,
						result[j][k], result[j][k], queryResult.results[i][j][k], queryResult.results[i][j][k], queryResult.query)
				}
			}

		}

	}
}

// TestConnect checks basic invalid connection
func TestConnect(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	OCI8Driver.Logger = log.New(os.Stderr, "oci8 ", log.Ldate|log.Ltime|log.LUTC|log.Llongfile)

	// invalid
	db, err := sql.Open("oci8", TestHostInvalid)
	if err != nil {
		t.Fatal("open error:", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = db.PingContext(ctx)
	cancel()
	if err == nil || err != driver.ErrBadConn {
		t.Fatalf("ping error - received: %v - expected: %v", err, driver.ErrBadConn)
	}

	err = db.Close()
	if err != nil {
		t.Fatal("close error:", err)
	}

	// wrong username
	db, err = sql.Open("oci8", "dFQXYoApiU2YbquMQnfPyqxR2kAoeuWngDvtTpl3@"+TestHostValid)
	if err != nil {
		t.Fatal("open error:", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	err = db.PingContext(ctx)
	cancel()
	if err == nil || err != driver.ErrBadConn {
		t.Fatalf("ping error - received: %v - expected: %v", err, driver.ErrBadConn)
	}

	err = db.Close()
	if err != nil {
		t.Fatal("close error:", err)
	}

	OCI8Driver.Logger = log.New(ioutil.Discard, "", 0)
}

// TestSelectParallel checks parallel select from dual
func TestSelectParallel(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, "select :1 from dual")
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(100)

	for i := 0; i < 100; i++ {
		go func(num int) {
			defer waitGroup.Done()
			var result [][]interface{}
			result, err = testGetRows(t, stmt, []interface{}{num})
			if err != nil {
				t.Fatal("get rows error:", err)
			}
			if result == nil {
				t.Fatal("result is nil")
			}
			if len(result) < 1 {
				t.Fatal("len result less than 1")
			}
			if len(result[0]) < 1 {
				t.Fatal("len result[0] less than 1")
			}
			data, ok := result[0][0].(int64)
			if !ok {
				t.Fatal("result not int64")
			}
			if data != int64(num) {
				t.Fatal("result not equal to:", num)
			}
		}(i)
	}

	waitGroup.Wait()

	err = stmt.Close()
	if err != nil {
		t.Fatal("stmt close error:", err)
	}
}

// TestContextTimeoutBreak checks that ExecContext timeout works
func TestContextTimeoutBreak(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	// exec
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, "begin SYS.DBMS_LOCK.SLEEP(1); end;")
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, err = stmt.ExecContext(ctx)
	cancel()
	expected := "ORA-01013"
	if err == nil || len(err.Error()) < len(expected) || err.Error()[:len(expected)] != expected {
		t.Fatalf("stmt exec - expected: %v - received: %v", expected, err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatal("stmt close error:", err)
	}

	// query
	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err = TestDB.PrepareContext(ctx, "select SLEEP_SECONDS(1) from dual")
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, err = stmt.QueryContext(ctx)
	cancel()
	if err == nil || len(err.Error()) < len(expected) || err.Error()[:len(expected)] != expected {
		t.Fatalf("stmt query - expected: %v - received: %v", expected, err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatal("stmt close error:", err)
	}
}
