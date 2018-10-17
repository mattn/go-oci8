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

// testRunExecResults runs exec queries for each arg row and checks results
func testRunExecResults(t *testing.T, execResults testExecResults) {
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, execResults.query)
	cancel()
	if err != nil {
		t.Errorf("prepare error: %v - query: %v", err, execResults.query)
		return
	}

	for _, execResult := range execResults.execResults {
		testRunExecResult(t, execResult, execResults.query, stmt)
	}
}

// testRunExecResult runs exec query for each arg row and checks results
func testRunExecResult(t *testing.T, execResult testExecResult, query string, stmt *sql.Stmt) {
	var rv reflect.Value
	results := make(map[string]interface{}, len(execResult.args))

	// change args to namedArgs
	namedArgs := make([]interface{}, 0, len(execResult.args))
	for key, value := range execResult.args {
		// make pointer
		rv = reflect.ValueOf(value.Dest)
		out := reflect.New(rv.Type())
		if !out.Elem().CanSet() {
			t.Fatalf("unable to set pointer: %v - query: %v", key, query)
			return
		}
		out.Elem().Set(rv)
		results[key] = out.Interface()

		namedArgs = append(namedArgs, sql.Named(key, sql.Out{Dest: out.Interface(), In: value.In}))
	}

	// exec query with namedArgs
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	_, err := stmt.ExecContext(ctx, namedArgs...)
	if err != nil {
		t.Errorf("exec error: %v - query: %v - args: %v", err, query, execResult.args)
		return
	}
	cancel()

	// check results
	for key, value := range execResult.results {
		// check if have result
		result, ok := results[key]
		if !ok {
			t.Errorf("result not found: %v - query: %v", key, query)
			continue
		}

		// get result from result pointer
		rv = reflect.ValueOf(result)
		rv = reflect.Indirect(rv)
		result = rv.Interface()

		// check if value matches result
		if result != value {
			t.Errorf("arg: %v - received: %T, %v - expected: %T, %v - query: %v",
				key, result, result, value, value, query)
		}
	}
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
			if len(result) != 1 {
				t.Fatal("len result not equal to 1")
			}
			if len(result[0]) != 1 {
				t.Fatal("len result[0] not equal to 1")
			}
			data, ok := result[0][0].(float64)
			if !ok {
				t.Fatal("result not float64")
			}
			if data != float64(num) {
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

// TestDestructiveTransaction tests a transaction
func TestDestructiveTransaction(t *testing.T) {
	if TestDisableDatabase || TestDisableDestructive {
		t.SkipNow()
	}

	err := testExec(t, "create table TRANSACTION_"+TestTimeString+
		" ( A INT, B INT, C INT )", nil)
	if err != nil {
		t.Error("create table error:", err)
		return
	}

	defer func() {
		err = testExec(t, "drop table TRANSACTION_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into TRANSACTION_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{1, 2, 3},
			[]interface{}{4, 5, 6},
			[]interface{}{6, 7, 8},
		})
	if err != nil {
		t.Error("insert error:", err)
		return
	}

	// TODO: How should context work? Probably should have more context create and cancel.

	var tx1 *sql.Tx
	var tx2 *sql.Tx
	ctx, cancel := context.WithTimeout(context.Background(), 2*TestContextTimeout)
	defer cancel()
	tx1, err = TestDB.BeginTx(ctx, nil)
	if err != nil {
		t.Error("begin tx error:", err)
		return
	}
	tx2, err = TestDB.BeginTx(ctx, nil)
	if err != nil {
		t.Error("begin tx error:", err)
		return
	}

	queryResults := []testQueryResults{
		testQueryResults{
			query: "select A, B, C from TRANSACTION_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{int64(1), int64(2), int64(3)},
					[]interface{}{int64(4), int64(5), int64(6)},
					[]interface{}{int64(6), int64(7), int64(8)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	_, err = tx1.ExecContext(ctx, "update TRANSACTION_"+TestTimeString+" set B = :1 where A = :2", []interface{}{22, 1}...)
	if err != nil {
		t.Error("exec error:", err)
		return
	}
	_, err = tx2.ExecContext(ctx, "update TRANSACTION_"+TestTimeString+" set B = :1 where A = :2", []interface{}{55, 4}...)
	if err != nil {
		t.Error("exec error:", err)
		return
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from TRANSACTION_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{int64(1), int64(2), int64(3)},
					[]interface{}{int64(4), int64(5), int64(6)},
					[]interface{}{int64(6), int64(7), int64(8)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// tx1 with rows A = 1
	var stmt *sql.Stmt
	stmt, err = tx1.PrepareContext(ctx, "select A, B, C from TRANSACTION_"+TestTimeString+" where A = :1")
	if err != nil {
		t.Error("prepare error:", err)
		return
	}
	var result [][]interface{}
	result, err = testGetRows(t, stmt, []interface{}{1})
	if result == nil {
		t.Error("result is nil")
		return
	}
	if len(result) != 1 {
		t.Error("len result not equal to 1")
		return
	}
	if len(result[0]) != 3 {
		t.Error("len result[0] not equal to 3")
		return
	}
	data, ok := result[0][0].(int64)
	if !ok {
		t.Error("result not int64")
		return
	}
	expected := int64(1)
	if data != expected {
		t.Error("result not equal to:", expected)
		return
	}
	data, ok = result[0][1].(int64)
	if !ok {
		t.Error("result not int64")
		return
	}
	expected = int64(22)
	if data != expected {
		t.Error("result not equal to:", expected)
		return
	}
	data, ok = result[0][2].(int64)
	if !ok {
		t.Error("result not int64")
		return
	}
	expected = int64(3)
	if data != expected {
		t.Error("result not equal to:", expected)
		return
	}

	// tx1 with rows A = 4
	result, err = testGetRows(t, stmt, []interface{}{4})
	if result == nil {
		t.Error("result is nil")
		return
	}
	if len(result) != 1 {
		t.Error("len result not equal to 1")
		return
	}
	if len(result[0]) != 3 {
		t.Error("len result[0] not equal to 3")
		return
	}
	data, ok = result[0][0].(int64)
	if !ok {
		t.Error("result not int64")
		return
	}
	expected = int64(4)
	if data != expected {
		t.Error("result not equal to:", expected)
		return
	}
	data, ok = result[0][1].(int64)
	if !ok {
		t.Error("result not int64")
		return
	}
	expected = int64(5)
	if data != expected {
		t.Error("result not equal to:", expected)
		return
	}
	data, ok = result[0][2].(int64)
	if !ok {
		t.Error("result not int64")
		return
	}
	expected = int64(6)
	if data != expected {
		t.Error("result not equal to:", expected)
		return
	}

	// tx2 with rows A = 1
	stmt, err = tx2.PrepareContext(ctx, "select A, B, C from TRANSACTION_"+TestTimeString+" where A = :1")
	if err != nil {
		t.Error("prepare error:", err)
		return
	}
	result, err = testGetRows(t, stmt, []interface{}{1})
	if result == nil {
		t.Error("result is nil")
		return
	}
	if len(result) != 1 {
		t.Error("len result not equal to 1")
		return
	}
	if len(result[0]) != 3 {
		t.Error("len result[0] not equal to 3")
		return
	}
	data, ok = result[0][0].(int64)
	if !ok {
		t.Error("result not int64")
		return
	}
	expected = int64(1)
	if data != expected {
		t.Error("result not equal to:", expected)
		return
	}
	data, ok = result[0][1].(int64)
	if !ok {
		t.Error("result not int64")
		return
	}
	expected = int64(2)
	if data != expected {
		t.Error("result not equal to:", expected)
		return
	}
	data, ok = result[0][2].(int64)
	if !ok {
		t.Error("result not int64")
		return
	}
	expected = int64(3)
	if data != expected {
		t.Error("result not equal to:", expected)
		return
	}

	// tx2 with rows A = 4
	result, err = testGetRows(t, stmt, []interface{}{4})
	if result == nil {
		t.Error("result is nil")
		return
	}
	if len(result) != 1 {
		t.Error("len result not equal to 1")
		return
	}
	if len(result[0]) != 3 {
		t.Error("len result[0] not equal to 3")
		return
	}
	data, ok = result[0][0].(int64)
	if !ok {
		t.Error("result not int64")
		return
	}
	expected = int64(4)
	if data != expected {
		t.Error("result not equal to:", expected)
		return
	}
	data, ok = result[0][1].(int64)
	if !ok {
		t.Error("result not int64")
		return
	}
	expected = int64(55)
	if data != expected {
		t.Error("result not equal to:", expected)
		return
	}
	data, ok = result[0][2].(int64)
	if !ok {
		t.Error("result not int64")
		return
	}
	expected = int64(6)
	if data != expected {
		t.Error("result not equal to:", expected)
		return
	}

	err = tx1.Commit()
	if err != nil {
		t.Error("commit err:", err)
		return
	}
	err = tx2.Commit()
	if err != nil {
		t.Error("commit err:", err)
		return
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from TRANSACTION_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{int64(1), int64(22), int64(3)},
					[]interface{}{int64(4), int64(55), int64(6)},
					[]interface{}{int64(6), int64(7), int64(8)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)
}
