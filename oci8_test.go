package oci8

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

// to run database tests
// go test -v github.com/mattn/go-oci8 -args -disableDatabase=false -hostValid type_hostname_here -username type_username_here -password "type_password_here"
// look at test.sh for Oracle user setup example

var (
	TestDisableDatabase      bool
	TestHostValid            string
	TestHostInvalid          string
	TestUsername             string
	TestPassword             string
	TestContextTimeoutString string
	TestContextTimeout       time.Duration
	TestDatabase             string
	TestDisableDestructive   bool

	TestTimeString string

	TestDB *sql.DB

	TestTypeTime      = reflect.TypeOf(time.Time{})
	TestTypeByteSlice = reflect.TypeOf([]byte{})

	testString1        string
	testByteSlice2000  []byte
	testByteSlice4000  []byte
	testByteSlice32767 []byte
	testByteSlice65535 []byte
	testByteSlice70000 []byte

	benchmarkSelectTableName    string
	benchmarkSelectTableOnce    sync.Once
	benchmarkSelectTableCreated bool
)

// testExecResults is for testing an exec query with many sets of args
type testExecResults struct {
	query       string
	execResults []testExecResult
}

// testExecResult is for testing an exec query with single set of args
type testExecResult struct {
	args    map[string]sql.Out
	results map[string]interface{}
}

// testQueryResults is for testing a query with many sets of args
type testQueryResults struct {
	query        string
	queryResults []testQueryResult
}

// testQueryResult is for testing a query with single set of args
type testQueryResult struct {
	args    []interface{}
	results [][]interface{}
}

// TestMain sets up testing
func TestMain(m *testing.M) {
	code := setupForTesting()
	if code != 0 {
		os.Exit(code)
	}
	code = m.Run()

	// drop benchmark select table
	if benchmarkSelectTableCreated {
		func() {
			tableName := benchmarkSelectTableName
			query := "drop table " + tableName
			ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
			stmt, err := TestDB.PrepareContext(ctx, query)
			cancel()
			if err != nil {
				fmt.Println("prepare error:", err)
				return
			}

			ctx, cancel = context.WithTimeout(context.Background(), TestContextTimeout)
			_, err = stmt.ExecContext(ctx)
			cancel()
			if err != nil {
				stmt.Close()
				fmt.Println("exec error:", err)
				return
			}

			err = stmt.Close()
			if err != nil {
				fmt.Println("stmt close error:", err)
				return
			}
		}()
	}

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
	flag.StringVar(&TestContextTimeoutString, "contextTimeout", "30s", "the context timeout for queries")
	flag.BoolVar(&TestDisableDestructive, "disableDestructive", false, "set to true to disable the destructive Oracle tests")

	flag.Parse()

	var err error
	TestContextTimeout, err = time.ParseDuration(TestContextTimeoutString)
	if err != nil {
		fmt.Println("parse context timeout error:", err)
		return 4
	}

	if !TestDisableDatabase {
		TestDB = testGetDB("")
		if TestDB == nil {
			return 6
		}
	}

	TestTimeString = time.Now().UTC().Format("20060102150405")

	var i int
	var buffer bytes.Buffer
	for i = 0; i < 1000; i++ {
		buffer.WriteRune(rune(i))
	}
	testString1 = buffer.String()

	testByteSlice2000 = make([]byte, 2000)
	for i = 0; i < 2000; i++ {
		testByteSlice2000[i] = byte(i)
	}
	testByteSlice4000 = make([]byte, 4000)
	for i = 0; i < 4000; i++ {
		testByteSlice4000[i] = byte(i)
	}
	testByteSlice32767 = make([]byte, 32767)
	for i = 0; i < 32767; i++ {
		testByteSlice32767[i] = byte(i)
	}
	testByteSlice65535 = make([]byte, 65535)
	for i = 0; i < 65535; i++ {
		testByteSlice65535[i] = byte(i)
	}
	testByteSlice70000 = make([]byte, 70000)
	for i = 0; i < 70000; i++ {
		testByteSlice70000[i] = byte(i)
	}

	return 0
}

// TestParseDSN tests parsing the DSN
func TestParseDSN(t *testing.T) {
	const prefetchRows = 0
	const prefetchMemory = 4096

	var dsnTests = []struct {
		dsnString   string
		expectedDSN *DSN
	}{
		{"oracle://xxmc:xxmc@107.20.30.169:1521/ORCL?loc=America%2FPhoenix", &DSN{Username: "xxmc", Password: "xxmc", Connect: "107.20.30.169:1521/ORCL", prefetchRows: prefetchRows, prefetchMemory: prefetchMemory, timeLocation: timeLocations[5]}},
		{"xxmc/xxmc@107.20.30.169:1521/ORCL?loc=America%2FPhoenix", &DSN{Username: "xxmc", Password: "xxmc", Connect: "107.20.30.169:1521/ORCL", prefetchRows: prefetchRows, prefetchMemory: prefetchMemory, timeLocation: timeLocations[5]}},
		{"sys/syspwd@107.20.30.169:1521/ORCL?loc=America%2FPhoenix&as=sysdba", &DSN{Username: "sys", Password: "syspwd", Connect: "107.20.30.169:1521/ORCL", prefetchRows: prefetchRows, prefetchMemory: prefetchMemory, timeLocation: timeLocations[5], operationMode: 0x00000002}}, // with operationMode: 0x00000002 = C.OCI_SYDBA
		{"xxmc/xxmc@107.20.30.169:1521/ORCL", &DSN{Username: "xxmc", Password: "xxmc", Connect: "107.20.30.169:1521/ORCL", prefetchRows: prefetchRows, prefetchMemory: prefetchMemory, timeLocation: time.UTC}},
		{"xxmc/xxmc@107.20.30.169/ORCL", &DSN{Username: "xxmc", Password: "xxmc", Connect: "107.20.30.169/ORCL", prefetchRows: prefetchRows, prefetchMemory: prefetchMemory, timeLocation: time.UTC}},
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
