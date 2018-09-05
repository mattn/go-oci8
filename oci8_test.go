package oci8

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

// to run database tests
// go test -v github.com/mattn/go-oci8 -args -disableDatabase=false -hostValid type_hostname_here -username type_username_here -password "type_password_here"
// note minimum Go version for testing is 1.8

var (
	TestDisableDatabase    bool
	TestHostValid          string
	TestHostInvalid        string
	TestUsername           string
	TestPassword           string
	TestDatabase           string
	TestDisableDestructive bool

	TestTimeString string

	TestDB *sql.DB

	TestTypeTime      = reflect.TypeOf(time.Time{})
	TestTypeByteSlice = reflect.TypeOf([]byte{})

	testString1    string
	testByteSlice1 []byte

	testTimeLocUTC *time.Location
	testTimeLocGMT *time.Location
	testTimeLocEST *time.Location
	testTimeLocMST *time.Location
	testTimeLocNZ  *time.Location
)

// testQueryResults is for testing a query
type testQueryResults struct {
	query   string
	args    [][]interface{}
	results [][][]interface{}
}

// TestMain sets up testing
func TestMain(m *testing.M) {
	code := setupForTesting()
	if code != 0 {
		os.Exit(code)
	}
	code = m.Run()

	if !TestDisableDatabase {
		err := TestDB.Close()
		if err != nil {
			fmt.Println("close error:", err)
			os.Exit(2)
		}
	}

	os.Exit(code)
}

// setupForTesting sets up flags and connects to test database
func setupForTesting() int {
	flag.BoolVar(&TestDisableDatabase, "disableDatabase", true, "set to true to disable the Oracle tests")
	flag.StringVar(&TestHostValid, "hostValid", "127.0.0.1", "a host where a Oracle database is running")
	flag.StringVar(&TestHostInvalid, "hostInvalid", "169.254.200.200", "a host where a Oracle database is not running")
	flag.StringVar(&TestUsername, "username", "", "the username for the Oracle database")
	flag.StringVar(&TestPassword, "password", "", "the password for the Oracle database")
	flag.BoolVar(&TestDisableDestructive, "disableDestructive", false, "set to true to disable the destructive Oracle tests")

	flag.Parse()

	if !TestDisableDatabase {
		TestDB = testGetDB()
		if TestDB == nil {
			return 4
		}
	}

	TestTimeString = time.Now().UTC().Format("20060102150405")

	var i int
	var buffer bytes.Buffer
	for i = 0; i < 1000; i++ {
		buffer.WriteRune(rune(i))
	}
	testString1 = buffer.String()

	testByteSlice1 = make([]byte, 2000)
	for i = 0; i < 2000; i++ {
		testByteSlice1[i] = byte(i)
	}

	testTimeLocUTC, _ = time.LoadLocation("UTC")
	testTimeLocGMT, _ = time.LoadLocation("GMT")
	testTimeLocEST, _ = time.LoadLocation("EST")
	testTimeLocMST, _ = time.LoadLocation("MST")
	testTimeLocNZ, _ = time.LoadLocation("NZ")

	return 0
}

// TestParseDSN tests parsing the DSN
func TestParseDSN(t *testing.T) {
	var (
		pacific *time.Location
		err     error
	)

	if pacific, err = time.LoadLocation("America/Los_Angeles"); err != nil {
		panic(err)
	}
	var dsnTests = []struct {
		dsnString   string
		expectedDSN *DSN
	}{
		{"oracle://xxmc:xxmc@107.20.30.169:1521/ORCL?loc=America%2FLos_Angeles", &DSN{Username: "xxmc", Password: "xxmc", Connect: "107.20.30.169:1521/ORCL", prefetch_rows: 10, Location: pacific}},
		{"xxmc/xxmc@107.20.30.169:1521/ORCL?loc=America%2FLos_Angeles", &DSN{Username: "xxmc", Password: "xxmc", Connect: "107.20.30.169:1521/ORCL", prefetch_rows: 10, Location: pacific}},
		{"sys/syspwd@107.20.30.169:1521/ORCL?loc=America%2FLos_Angeles&as=sysdba", &DSN{Username: "sys", Password: "syspwd", Connect: "107.20.30.169:1521/ORCL", prefetch_rows: 10, Location: pacific, operationMode: 0x00000002}}, // with operationMode: 0x00000002 = C.OCI_SYDBA
		{"xxmc/xxmc@107.20.30.169:1521/ORCL", &DSN{Username: "xxmc", Password: "xxmc", Connect: "107.20.30.169:1521/ORCL", prefetch_rows: 10, Location: time.Local}},
		{"xxmc/xxmc@107.20.30.169/ORCL", &DSN{Username: "xxmc", Password: "xxmc", Connect: "107.20.30.169/ORCL", prefetch_rows: 10, Location: time.Local}},
	}

	for _, tt := range dsnTests {
		actualDSN, err := ParseDSN(tt.dsnString)

		if err != nil {
			t.Errorf("ParseDSN(%s) got error: %+v", tt.dsnString, err)
		}

		if !reflect.DeepEqual(actualDSN, tt.expectedDSN) {
			t.Errorf("ParseDSN(%s): expected %+v, actual %+v", tt.dsnString, tt.expectedDSN, actualDSN)
		}
	}
}

// TestIsBadConn tests bad connection error codes
func TestIsBadConn(t *testing.T) {
	var errorCode = "ORA-03114"
	if !isBadConnection(errorCode) {
		t.Errorf("TestIsBadConn: expected %+v, actual %+v", true, isBadConnection(errorCode))
	}
}

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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

// testRunQueryResults runs a slice of testQueryResults tests
func testRunQueryResults(t *testing.T, queryResults []testQueryResults) {
	for _, queryResult := range queryResults {

		if len(queryResult.args) != len(queryResult.results) {
			t.Errorf("args len %v and results len %v do not match - query: %v",
				len(queryResult.args), len(queryResult.results), queryResult.query)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
		if len(result) < 1 && queryResult.results[i] != nil {
			t.Errorf("result len less than 1 - query: %v", queryResult.query)
			continue
		}

		for j := 0; j < len(result); j++ {
			if len(result[j]) != len(queryResult.results[i][j]) {
				t.Errorf("result len %v not equal to expected len %v - query: %v",
					len(result), len(queryResult.results[i][j]), queryResult.query)
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

var sql1 = `create table foo(
	c1 varchar2(256),
	c2 nvarchar2(256),
	c3 number,
	c4 float,
	c6 date,
	c7 BINARY_FLOAT,
	c8 BINARY_DOUBLE,
	c9 TIMESTAMP,
	c10 TIMESTAMP WITH TIME ZONE,
	c11 TIMESTAMP WITH LOCAL TIME ZONE,
	c12 INTERVAL YEAR TO MONTH,
	c13 INTERVAL DAY TO SECOND,
	c14 RAW(80),
	c15 ROWID,
	c17 CHAR(15),
	c18 NCHAR(20),
	c19 CLOB,
	c21 BLOB,
	cend varchar2(12)
	)`

var sql12 = `insert( c1,c2,c3,c4,c6,c7,c8,c9,c10,c11,c12,c13,c14,c17,c18,c19,c20,c21,cend) into foo values(
:1,
:2,
:3,
:4,
:6,
:7,
:8,
:9,
:10,
:11,
NUMTOYMINTERVAL( :12, 'MONTH'),
NUMTODSINTERVAL( :13 / 1000000000, 'SECOND'),
:14,
:17,
:18,
:19,
:21,
'END'
)`
