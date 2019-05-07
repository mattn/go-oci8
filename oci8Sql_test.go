package oci8

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// testGetDB connects to the test database and returns the database connection
func testGetDB(params string) *sql.DB {
	OCI8Driver.Logger = log.New(os.Stderr, "oci8 ", log.Ldate|log.Ltime|log.LUTC|log.Llongfile)

	var openString string
	// [username/[password]@]host[:port][/instance_name][?param1=value1&...&paramN=valueN]
	if len(TestUsername) > 0 {
		if len(TestPassword) > 0 {
			openString = TestUsername + "/" + TestPassword + "@"
		} else {
			openString = TestUsername + "@"
		}
	}
	openString += TestHostValid + params

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

func testDropTable(t *testing.T, tableName string) {
	err := testExec(t, "drop table "+tableName, nil)
	if err != nil {
		t.Errorf("drop table %v error: %v", tableName, err)
	}
}

func testExecQuery(t *testing.T, query string, args []interface{}) {
	err := testExec(t, query, args)
	if err != nil {
		t.Errorf("query %v error: %v", query, err)
	}
}

// testGetRows runs a statement and returns the rows as [][]interface{}
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

// testRunExecResults runs testRunExecResult for each execResults
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

	err = stmt.Close()
	if err != nil {
		t.Errorf("close error: %v - query: %v", err, execResults.query)
	}
}

// testRunExecResult runs exec query for execResult and tests result
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
	cancel()
	if err != nil {
		t.Errorf("exec error: %v - query: %v - args: %v", err, query, execResult.args)
		return
	}

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
		if !reflect.DeepEqual(result, value) {
			t.Errorf("arg: %v - received: %T, %v - expected: %T, %v - query: %v",
				key, result, result, value, value, query)
		}
	}
}

// testRunQueryResults runs testRunQueryResult for each queryResults
func testRunQueryResults(t *testing.T, queryResults testQueryResults) {
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, queryResults.query)
	cancel()
	if err != nil {
		t.Errorf("prepare error: %v - query: %v", err, queryResults.query)
		return
	}

	for _, queryResult := range queryResults.queryResults {
		testRunQueryResult(t, queryResult, queryResults.query, stmt)
	}

	err = stmt.Close()
	if err != nil {
		t.Errorf("close error: %v - query: %v", err, queryResults.query)
	}
}

// testRunQueryResult runs a single testQueryResults test
func testRunQueryResult(t *testing.T, queryResult testQueryResult, query string, stmt *sql.Stmt) {
	result, err := testGetRows(t, stmt, queryResult.args)
	if err != nil {
		t.Errorf("get rows error: %v - query: %v", err, query)
		return
	}
	if result == nil && queryResult.results != nil {
		t.Errorf("result is nil - query: %v", query)
		return
	}
	if len(result) != len(queryResult.results) {
		t.Errorf("result rows len %v not equal to results len %v - query: %v",
			len(result), len(queryResult.results), query)
		return
	}

	for i := 0; i < len(result); i++ {
		if len(result[i]) != len(queryResult.results[i]) {
			t.Errorf("result columns len %v not equal to results len %v - query: %v",
				len(result[i]), len(queryResult.results[i]), query)
			continue
		}

		for j := 0; j < len(result[i]); j++ {
			bad := false
			type1 := reflect.TypeOf(result[i][j])
			type2 := reflect.TypeOf(queryResult.results[i][j])
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
				time1 := result[i][j].(time.Time)
				time2 := queryResult.results[i][j].(time.Time)
				if !time1.Equal(time2) {
					bad = true
				}
			case type1.Kind() == reflect.Slice || type2.Kind() == reflect.Slice:
				if !reflect.DeepEqual(result[i][j], queryResult.results[i][j]) {
					bad = true
				}
			default:
				if result[i][j] != queryResult.results[i][j] {
					bad = true
				}
			}
			if bad {
				t.Errorf("result - row %v, %v - received: %T, %v - expected: %T, %v - query: %v",
					i, j, result[i][j], result[i][j], queryResult.results[i][j], queryResult.results[i][j], query)
			}
		}

	}

}

// TestConnect checks basic invalid connection
func TestConnect(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

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
	if err == nil || len(err.Error()) < 4 || err.Error()[0:4] != "ORA-" {
		t.Fatalf("ping error - received: %v - expected ORA- error", err)
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
	if err == nil || len(err.Error()) < 4 || err.Error()[0:4] != "ORA-" {
		t.Fatalf("ping error - received: %v - expected ORA- error", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal("close error:", err)
	}
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
		t.Fatal("create table error:", err)
	}

	defer testExecQuery(t, "drop table TRANSACTION_"+TestTimeString, nil)

	err = testExecRows(t, "insert into TRANSACTION_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			{1, 2, 3},
			{4, 5, 6},
			{6, 7, 8},
		})
	if err != nil {
		t.Fatal("insert error:", err)
	}

	// TODO: How should context work? Probably should have more context create and cancel.

	var tx1 *sql.Tx
	var tx2 *sql.Tx
	ctx, cancel := context.WithTimeout(context.Background(), 2*TestContextTimeout)
	defer cancel()
	tx1, err = TestDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal("begin tx error:", err)
	}
	tx2, err = TestDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal("begin tx error:", err)
	}

	queryResults := testQueryResults{
		query: "select A, B, C from TRANSACTION_" + TestTimeString + " order by A",
		queryResults: []testQueryResult{
			{
				results: [][]interface{}{
					{int64(1), int64(2), int64(3)},
					{int64(4), int64(5), int64(6)},
					{int64(6), int64(7), int64(8)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	var result sql.Result
	result, err = tx1.ExecContext(ctx, "update TRANSACTION_"+TestTimeString+" set B = :1 where A = :2", []interface{}{22, 1}...)
	if err != nil {
		t.Fatal("exec error:", err)
	}

	var count int64
	count, err = result.RowsAffected()
	if err != nil {
		t.Fatal("rows affected error:", err)
	}
	if count != 1 {
		t.Fatalf("rows affected %v not equal to 1", count)
	}

	result, err = tx2.ExecContext(ctx, "update TRANSACTION_"+TestTimeString+" set B = :1 where A = :2", []interface{}{55, 4}...)
	if err != nil {
		t.Fatal("exec error:", err)
	}

	count, err = result.RowsAffected()
	if err != nil {
		t.Fatal("rows affected error:", err)
	}
	if count != 1 {
		t.Fatalf("rows affected %v not equal to 1", count)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from TRANSACTION_" + TestTimeString + " order by A",
		queryResults: []testQueryResult{
			{
				results: [][]interface{}{
					{int64(1), int64(2), int64(3)},
					{int64(4), int64(5), int64(6)},
					{int64(6), int64(7), int64(8)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// tx1 with rows A = 1
	var stmt *sql.Stmt
	stmt, err = tx1.PrepareContext(ctx, "select A, B, C from TRANSACTION_"+TestTimeString+" where A = :1")
	if err != nil {
		t.Fatal("prepare error:", err)
	}
	var rows [][]interface{}
	rows, err = testGetRows(t, stmt, []interface{}{1})
	if err != nil {
		t.Fatal("get rows error:", err)
	}
	if result == nil {
		t.Fatal("rows is nil")
	}
	if len(rows) != 1 {
		t.Fatal("len rows not equal to 1")
	}
	if len(rows[0]) != 3 {
		t.Fatal("len rows[0] not equal to 3")
	}
	data, ok := rows[0][0].(int64)
	if !ok {
		t.Fatal("rows not int64")
	}
	expected := int64(1)
	if data != expected {
		t.Fatal("rows not equal to:", expected)
	}
	data, ok = rows[0][1].(int64)
	if !ok {
		t.Fatal("rows not int64")
	}
	expected = int64(22)
	if data != expected {
		t.Fatal("rows not equal to:", expected)
	}
	data, ok = rows[0][2].(int64)
	if !ok {
		t.Fatal("rows not int64")
	}
	expected = int64(3)
	if data != expected {
		t.Fatal("rows not equal to:", expected)
	}

	// tx1 with rows A = 4
	rows, err = testGetRows(t, stmt, []interface{}{4})
	if err != nil {
		t.Fatal("get rows error:", err)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}
	if len(rows) != 1 {
		t.Fatal("len rows not equal to 1")
	}
	if len(rows[0]) != 3 {
		t.Fatal("len rows[0] not equal to 3")
	}
	data, ok = rows[0][0].(int64)
	if !ok {
		t.Fatal("rows not int64")
	}
	expected = int64(4)
	if data != expected {
		t.Fatal("rows not equal to:", expected)
	}
	data, ok = rows[0][1].(int64)
	if !ok {
		t.Fatal("rows not int64")
	}
	expected = int64(5)
	if data != expected {
		t.Fatal("rows not equal to:", expected)
	}
	data, ok = rows[0][2].(int64)
	if !ok {
		t.Fatal("rows not int64")
	}
	expected = int64(6)
	if data != expected {
		t.Fatal("rows not equal to:", expected)
	}

	// tx2 with rows A = 1
	stmt, err = tx2.PrepareContext(ctx, "select A, B, C from TRANSACTION_"+TestTimeString+" where A = :1")
	if err != nil {
		t.Fatal("prepare error:", err)
	}
	rows, err = testGetRows(t, stmt, []interface{}{1})
	if err != nil {
		t.Fatal("get rows error:", err)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}
	if len(rows) != 1 {
		t.Fatal("len rows not equal to 1")
	}
	if len(rows[0]) != 3 {
		t.Fatal("len rows[0] not equal to 3")
	}
	data, ok = rows[0][0].(int64)
	if !ok {
		t.Fatal("rows not int64")
	}
	expected = int64(1)
	if data != expected {
		t.Fatal("rows not equal to:", expected)
	}
	data, ok = rows[0][1].(int64)
	if !ok {
		t.Fatal("rows not int64")
	}
	expected = int64(2)
	if data != expected {
		t.Fatal("rows not equal to:", expected)
	}
	data, ok = rows[0][2].(int64)
	if !ok {
		t.Fatal("rows not int64")
	}
	expected = int64(3)
	if data != expected {
		t.Fatal("rows not equal to:", expected)
	}

	// tx2 with rows A = 4
	rows, err = testGetRows(t, stmt, []interface{}{4})
	if err != nil {
		t.Fatal("get rows error:", err)
	}
	if result == nil {
		t.Fatal("rows is nil")
	}
	if len(rows) != 1 {
		t.Fatal("len rows not equal to 1")
	}
	if len(rows[0]) != 3 {
		t.Fatal("len rows[0] not equal to 3")
	}
	data, ok = rows[0][0].(int64)
	if !ok {
		t.Fatal("rows not int64")
	}
	expected = int64(4)
	if data != expected {
		t.Fatal("rows not equal to:", expected)
	}
	data, ok = rows[0][1].(int64)
	if !ok {
		t.Fatal("rows not int64")
	}
	expected = int64(55)
	if data != expected {
		t.Fatal("rows not equal to:", expected)
	}
	data, ok = rows[0][2].(int64)
	if !ok {
		t.Fatal("rows not int64")
	}
	expected = int64(6)
	if data != expected {
		t.Fatal("rows not equal to:", expected)
	}

	err = tx1.Commit()
	if err != nil {
		t.Fatal("commit err:", err)
	}
	err = tx2.Commit()
	if err != nil {
		t.Fatal("commit err:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from TRANSACTION_" + TestTimeString + " order by A",
		queryResults: []testQueryResult{
			{
				results: [][]interface{}{
					{int64(1), int64(22), int64(3)},
					{int64(4), int64(55), int64(6)},
					{int64(6), int64(7), int64(8)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)
}

// TestSelectDualNull checks nulls
func TestSelectDualNull(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	queryResults := testQueryResults{
		query: "select null from dual",
		queryResults: []testQueryResult{{
			results: [][]interface{}{{nil}}}}}
	testRunQueryResults(t, queryResults)
}

func TestInsertRowid(t *testing.T) {
	if TestDisableDatabase || TestDisableDestructive {
		t.SkipNow()
	}

	// INSERT_ROWID
	tableName := "INSERT_ROWID_" + TestTimeString
	query := "create table " + tableName + " ( A INTEGER )"

	// create table
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, query)
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

	// drop table
	defer func() {
		query = "drop table " + tableName
		ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
		stmt, err = TestDB.PrepareContext(ctx, query)
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

	// insert into table
	query = "insert into " + tableName + " ( A ) values (:1) returning rowid into :rowid2"
	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err = TestDB.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	rowids := make([]string, 3)
	var result sql.Result
	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	result, err = stmt.ExecContext(ctx, 1, sql.Named("rowid2", sql.Out{Dest: &rowids[0]}))
	cancel()
	if err != nil {
		stmt.Close()
		t.Fatal("exec error:", err)
	}

	var id int64
	id, err = result.LastInsertId()
	if err != nil {
		stmt.Close()
		t.Fatal("exec error:", err)
	}

	rowids[1] = GetLastInsertId(id)

	err = stmt.Close()
	if err != nil {
		t.Fatal("stmt close error", err)
	}

	// get select rowid
	query = "select rowid from " + tableName
	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err = TestDB.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	var rows *sql.Rows
	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	defer cancel()
	rows, err = stmt.QueryContext(ctx)
	if err != nil {
		t.Fatal("query error:", err)
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			t.Fatal("row close error:", err)
		}
	}()

	if !rows.Next() {
		t.Fatal("expected row")
	}
	err = rows.Scan(&rowids[2])
	if err != nil {
		t.Fatal("scan error:", err)
	}

	if rows.Next() {
		t.Fatal("more than one row")
	}

	err = rows.Err()
	if err != nil {
		t.Fatal("rows error:", err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatal("stmt close error", err)
	}

	// select rowids
	query = "select A from " + tableName + " where rowid = :1"
	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err = TestDB.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	var data int64
	for _, rowid := range rowids {
		ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
		defer cancel()
		rows, err = stmt.QueryContext(ctx, rowid)
		if err != nil {
			t.Fatal("query error:", err)
		}

		defer func() {
			err = rows.Close()
			if err != nil {
				t.Fatal("row close error:", err)
			}
		}()

		if !rows.Next() {
			t.Fatal("expected row")
		}
		err = rows.Scan(&data)
		if err != nil {
			t.Fatal("scan error:", err)
		}

		if data != 1 {
			t.Fatal("row not equal to 1")
		}

		if rows.Next() {
			t.Fatal("more than one row")
		}

		err = rows.Err()
		if err != nil {
			t.Fatal("rows error:", err)
		}
	}

	err = stmt.Close()
	if err != nil {
		t.Fatal("stmt close error", err)
	}

}

// TestNullBool tests NullBool
func TestNullBool(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	query := `
declare
	function GET_BOOL(p_bool1 NUMERIC) return NUMERIC as
	begin
		if p_bool1 is null then
			return 1;
		end if;
		return 0;
	end GET_BOOL;
begin
	:bool1 := GET_BOOL(:bool1);
end;`

	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	var nullBool1 sql.NullBool

	nullBool1.Bool = false
	nullBool1.Valid = false

	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	_, err = stmt.ExecContext(ctx, sql.Named("bool1", sql.Out{Dest: &nullBool1, In: true}))
	cancel()
	if err != nil {
		t.Fatal("exec error:", err)
	}
	if !nullBool1.Valid {
		t.Fatal("nullBool1 not Valid")
	}
	if !nullBool1.Bool {
		t.Fatal("nullBool1 is false")
	}

	nullBool1.Bool = true
	nullBool1.Valid = true

	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	_, err = stmt.ExecContext(ctx, sql.Named("bool1", sql.Out{Dest: &nullBool1, In: true}))
	cancel()
	if err != nil {
		t.Fatal("exec error:", err)
	}
	if !nullBool1.Valid {
		t.Fatal("nullBool1 not Valid")
	}
	if nullBool1.Bool {
		t.Fatal("nullBool1 is true")
	}

	query = `
declare
	function GET_BOOL(p_bool1 NUMERIC) return NUMERIC as
	begin
		return null;
	end GET_BOOL;
begin
	:bool1 := GET_BOOL(:bool1);
end;`

	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err = TestDB.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	nullBool1.Bool = true
	nullBool1.Valid = true

	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	_, err = stmt.ExecContext(ctx, sql.Named("bool1", sql.Out{Dest: &nullBool1, In: true}))
	cancel()
	if err != nil {
		t.Fatal("exec error:", err)
	}
	if nullBool1.Valid {
		t.Fatal("nullBool1 is Valid")
	}
	if nullBool1.Bool {
		t.Fatal("nullBool1 is true")
	}
}

func BenchmarkSimpleInsert(b *testing.B) {
	if TestDisableDatabase || TestDisableDestructive {
		b.SkipNow()
	}

	b.StopTimer()

	// SIMPLE_INSERT
	tableName := "SIMPLE_INSERT_" + TestTimeString
	query := "create table " + tableName + " ( A INTEGER )"

	// create table
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		b.Fatal("prepare error:", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	_, err = stmt.ExecContext(ctx)
	cancel()
	if err != nil {
		stmt.Close()
		b.Fatal("exec error:", err)
	}

	err = stmt.Close()
	if err != nil {
		b.Fatal("stmt close error:", err)
	}

	// drop table
	defer func() {
		query = "drop table " + tableName
		ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
		stmt, err = TestDB.PrepareContext(ctx, query)
		cancel()
		if err != nil {
			b.Fatal("prepare error:", err)
		}

		ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
		_, err = stmt.ExecContext(ctx)
		cancel()
		if err != nil {
			stmt.Close()
			b.Fatal("exec error:", err)
		}

		err = stmt.Close()
		if err != nil {
			b.Fatal("stmt close error:", err)
		}
	}()

	// insert into table
	query = "insert into " + tableName + " ( A ) values (:1)"
	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err = TestDB.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		b.Fatal("prepare error:", err)
	}

	b.ResetTimer()
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
		_, err = stmt.ExecContext(ctx, n)
		cancel()
		if err != nil {
			stmt.Close()
			b.Fatal("exec error:", err)
		}
	}

	err = stmt.Close()
	if err != nil {
		b.Fatal("stmt close error", err)
	}
}

func benchmarkSelectSetup(b *testing.B) {
	fmt.Println("benchmark select setup start")

	benchmarkSelectTableName = "BM_SELECT_" + TestTimeString

	// create table
	tableName := benchmarkSelectTableName
	query := "create table " + tableName +
		"( A INTEGER, B INTEGER, C INTEGER, D INTEGER, E VARCHAR2(255), F VARCHAR2(255), G VARCHAR2(255), H VARCHAR2(255) )"
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		b.Fatal("prepare error:", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	_, err = stmt.ExecContext(ctx)
	cancel()
	if err != nil {
		stmt.Close()
		b.Fatal("exec error:", err)
	}

	// enable drop table in TestMain
	benchmarkSelectTableCreated = true

	err = stmt.Close()
	if err != nil {
		b.Fatal("stmt close error:", err)
	}

	// insert into table
	query = "insert into " + tableName + ` ( A, B, C, D, E, F, G, H )
select :1, :2, :3, :4, :5, :6, :7, :8 from dual
union all select :9, :10, :11, :12, :13, :14, :15, :16 from dual
union all select :17, :18, :19, :20, :21, :22, :23, :24 from dual
union all select :25, :26, :27, :28, :29, :30, :31, :32 from dual
union all select :33, :34, :35, :36, :37, :38, :39, :40 from dual
union all select :41, :42, :43, :44, :45, :46, :47, :48 from dual
union all select :49, :50, :51, :52, :53, :54, :55, :56 from dual
union all select :57, :58, :59, :60, :61, :62, :63, :64 from dual
union all select :65, :66, :67, :68, :69, :70, :71, :72 from dual
union all select :73, :74, :75, :76, :77, :78, :79, :80 from dual`
	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err = TestDB.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		b.Fatal("prepare error:", err)
	}

	insertString := strings.Repeat("a", 255)
	for i := 0; i < 5000; i += 10 {
		ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
		_, err = stmt.ExecContext(ctx,
			i, i+20000, i+40000, i+60000, insertString, insertString, insertString, insertString,
			i+1, i+20001, i+40001, i+60001, insertString, insertString, insertString, insertString,
			i+2, i+20002, i+40002, i+60002, insertString, insertString, insertString, insertString,
			i+3, i+20003, i+40003, i+60003, insertString, insertString, insertString, insertString,
			i+4, i+20004, i+40004, i+60004, insertString, insertString, insertString, insertString,
			i+5, i+20005, i+40005, i+60005, insertString, insertString, insertString, insertString,
			i+6, i+20006, i+40006, i+60006, insertString, insertString, insertString, insertString,
			i+7, i+20007, i+40007, i+60007, insertString, insertString, insertString, insertString,
			i+8, i+20008, i+40008, i+60008, insertString, insertString, insertString, insertString,
			i+9, i+20009, i+40009, i+60009, insertString, insertString, insertString, insertString)
		cancel()
		if err != nil {
			stmt.Close()
			b.Fatal("exec error:", err)
		}
	}

	err = stmt.Close()
	if err != nil {
		b.Fatal("stmt close error", err)
	}

	// select from table to warm up database cache
	query = "select A, B, C, D, E, F, G, H from " + tableName
	ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err = TestDB.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		b.Fatal("prepare error:", err)
	}

	defer func() {
		err = stmt.Close()
		if err != nil {
			b.Fatal("stmt close error", err)
		}
	}()

	var rows *sql.Rows
	ctx, cancel = context.WithTimeout(context.Background(), 20*TestContextTimeout)
	defer cancel()
	rows, err = stmt.QueryContext(ctx)
	if err != nil {
		b.Fatal("exec error:", err)
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			b.Fatal("row close error:", err)
		}
	}()

	var data1 int64
	var data2 int64
	var data3 int64
	var data4 int64
	var data5 string
	var data6 string
	var data7 string
	var data8 string
	var count int64
	for rows.Next() {
		err = rows.Scan(&data1, &data2, &data3, &data4, &data5, &data6, &data7, &data8)
		if err != nil {
			b.Fatal("scan error:", err)
		}
		count++
	}
	fmt.Printf("select data is %v bytes\n", (count*4*8)+(count*4*255))

	err = rows.Err()
	if err != nil {
		b.Fatal("err error:", err)
	}

	b.ResetTimer()

	fmt.Println("benchmark select setup end")
}

func benchmarkPrefetchSelect(b *testing.B, prefetchRows int64, prefetchMemory int64, n *int) {
	benchmarkSelectTableOnce.Do(func() { benchmarkSelectSetup(b) })

	var err error

	db := testGetDB("?prefetch_rows=" + strconv.FormatInt(prefetchRows, 10) + "&prefetch_memory=" + strconv.FormatInt(prefetchMemory, 10))
	if db == nil {
		b.Fatal("db is null")
	}

	defer func() {
		err = db.Close()
		if err != nil {
			b.Fatal("db close error:", err)
		}
	}()

	b.StartTimer()

	var stmt *sql.Stmt
	tableName := benchmarkSelectTableName
	query := "select A, B, C, D, E, F, G, H from " + tableName
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err = db.PrepareContext(ctx, query)
	cancel()
	if err != nil {
		b.Fatal("prepare error:", err)
	}

	defer func() {
		err = stmt.Close()
		if err != nil {
			b.Fatal("stmt close error", err)
		}
	}()

	var rows *sql.Rows
	ctx, cancel = context.WithTimeout(context.Background(), 20*TestContextTimeout)
	defer cancel()
	rows, err = stmt.QueryContext(ctx)
	if err != nil {
		b.Fatal("exec error:", err)
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			b.Fatal("row close error:", err)
		}
	}()

	var data1 int64
	var data2 int64
	var data3 int64
	var data4 int64
	var data5 string
	var data6 string
	var data7 string
	var data8 string
	for ; rows.Next() && *n < b.N; *n++ {
		err = rows.Scan(&data1, &data2, &data3, &data4, &data5, &data6, &data7, &data8)
		if err != nil {
			b.Fatal("scan error:", err)
		}
	}

	b.StopTimer()

	err = rows.Err()
	if err != nil {
		b.Fatal("err error:", err)
	}
}

func BenchmarkPrefetchR0M32768(b *testing.B) {
	b.StopTimer()

	if TestDisableDatabase || TestDisableDestructive {
		b.SkipNow()
	}

	for n := 0; n < b.N; {
		benchmarkPrefetchSelect(b, 0, 32768, &n)
	}
}

func BenchmarkPrefetchR0M16384(b *testing.B) {
	b.StopTimer()

	if TestDisableDatabase || TestDisableDestructive {
		b.SkipNow()
	}

	for n := 0; n < b.N; {
		benchmarkPrefetchSelect(b, 0, 16384, &n)
	}
}

func BenchmarkPrefetchR0M8192(b *testing.B) {
	b.StopTimer()

	if TestDisableDatabase || TestDisableDestructive {
		b.SkipNow()
	}

	for n := 0; n < b.N; {
		benchmarkPrefetchSelect(b, 0, 8192, &n)
	}
}

func BenchmarkPrefetchR0M4096(b *testing.B) {
	b.StopTimer()

	if TestDisableDatabase || TestDisableDestructive {
		b.SkipNow()
	}

	for n := 0; n < b.N; {
		benchmarkPrefetchSelect(b, 0, 4096, &n)
	}
}

func BenchmarkPrefetchR1000M0(b *testing.B) {
	b.StopTimer()

	if TestDisableDatabase || TestDisableDestructive {
		b.SkipNow()
	}

	for n := 0; n < b.N; {
		benchmarkPrefetchSelect(b, 1000, 0, &n)
	}
}
