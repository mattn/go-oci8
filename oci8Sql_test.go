package oci8

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestConnect checks basic invalid connection
func TestConnect(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	// Invalid
	db, err := sql.Open("oci8", TestHostInvalid+"/")
	if err != nil {
		t.Fatal("open error:", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = db.PingContext(ctx)
	cancel()
	expected := "ping failed"
	if err == nil || err.Error() != expected {
		t.Fatalf("ping error - received: %v - expected: %v", err, expected)
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

// TestContextTimeout checks that ExecContext timeout works
func TestContextTimeout(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	// exec
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
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

// TestSelectCast checks cast x from dual works for each SQL types
func TestSelectCast(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	// https://ss64.com/ora/syntax-datatypes.html

	queryResults := []testQueryResults{

		// VARCHAR2(1)
		testQueryResults{
			query: "select cast (:1 as VARCHAR2(1)) from dual",
			args: [][]interface{}{
				[]interface{}{""},
				[]interface{}{"a"},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a"}},
			},
		},

		// VARCHAR2(4000)
		testQueryResults{
			query: "select cast (:1 as VARCHAR2(4000)) from dual",
			args: [][]interface{}{
				[]interface{}{""},
				[]interface{}{"a"},
				[]interface{}{"abc    "},
				[]interface{}{strings.Repeat("a", 10)},
				[]interface{}{strings.Repeat("a", 100)},
				[]interface{}{strings.Repeat("a", 500)},
				[]interface{}{strings.Repeat("a", 1000)},
				[]interface{}{strings.Repeat("a", 1500)},
				[]interface{}{strings.Repeat("a", 2000)},
				[]interface{}{strings.Repeat("a", 3000)},
				[]interface{}{strings.Repeat("a", 4000)},
				[]interface{}{testString1},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a"}},
				[][]interface{}{[]interface{}{"abc    "}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 10)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 100)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 500)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 1000)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 1500)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 2000)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 3000)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 4000)}},
				[][]interface{}{[]interface{}{testString1}},
			},
		},

		// NVARCHAR2(1)
		testQueryResults{
			query: "select cast (:1 as NVARCHAR2(1)) from dual",
			args: [][]interface{}{
				[]interface{}{""},
				[]interface{}{"a"},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a"}},
			},
		},

		// NVARCHAR2(2000)
		testQueryResults{
			query: "select cast (:1 as NVARCHAR2(2000)) from dual",
			args: [][]interface{}{
				[]interface{}{""},
				[]interface{}{"a"},
				[]interface{}{"abc    "},
				[]interface{}{strings.Repeat("a", 10)},
				[]interface{}{strings.Repeat("a", 100)},
				[]interface{}{strings.Repeat("a", 500)},
				[]interface{}{strings.Repeat("a", 1000)},
				[]interface{}{strings.Repeat("a", 1500)},
				[]interface{}{strings.Repeat("a", 2000)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a"}},
				[][]interface{}{[]interface{}{"abc    "}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 10)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 100)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 500)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 1000)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 1500)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 2000)}},
			},
		},

		// CHAR(1)
		testQueryResults{
			query: "select cast (:1 as CHAR(1)) from dual",
			args: [][]interface{}{
				[]interface{}{""},
				[]interface{}{"a"},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a"}},
			},
		},

		// CHAR(2000)
		testQueryResults{
			query: "select cast (:1 as CHAR(2000)) from dual",
			args: [][]interface{}{
				[]interface{}{""},
				[]interface{}{"a"},
				[]interface{}{"abc    "},
				[]interface{}{strings.Repeat("a", 10)},
				[]interface{}{strings.Repeat("a", 100)},
				[]interface{}{strings.Repeat("a", 500)},
				[]interface{}{strings.Repeat("a", 1000)},
				[]interface{}{strings.Repeat("a", 1500)},
				[]interface{}{strings.Repeat("a", 2000)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a" + strings.Repeat(" ", 1999)}},
				[][]interface{}{[]interface{}{"abc" + strings.Repeat(" ", 1997)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 10) + strings.Repeat(" ", 1990)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 100) + strings.Repeat(" ", 1900)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 500) + strings.Repeat(" ", 1500)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 1000) + strings.Repeat(" ", 1000)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 1500) + strings.Repeat(" ", 500)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 2000)}},
			},
		},

		// NCHAR(1)
		testQueryResults{
			query: "select cast (:1 as NCHAR(1)) from dual",
			args: [][]interface{}{
				[]interface{}{""},
				[]interface{}{"a"},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a"}},
			},
		},

		// NCHAR(1000)
		testQueryResults{
			query: "select cast (:1 as NCHAR(1000)) from dual",
			args: [][]interface{}{
				[]interface{}{""},
				[]interface{}{"a"},
				[]interface{}{"abc    "},
				[]interface{}{strings.Repeat("a", 10)},
				[]interface{}{strings.Repeat("a", 100)},
				[]interface{}{strings.Repeat("a", 500)},
				[]interface{}{strings.Repeat("a", 1000)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a" + strings.Repeat(" ", 999)}},
				[][]interface{}{[]interface{}{"abc" + strings.Repeat(" ", 997)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 10) + strings.Repeat(" ", 990)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 100) + strings.Repeat(" ", 900)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 500) + strings.Repeat(" ", 500)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 1000)}},
			},
		},

		// NUMBER(38,10)
		testQueryResults{
			query: "select cast (:1 as NUMBER(38,10)) from dual",
			args: [][]interface{}{
				[]interface{}{float64(-99999999999999999999999999.9999999999)},
				[]interface{}{float64(-2147483648)},
				[]interface{}{float64(-123456792)},
				[]interface{}{float64(-1.9873046875)},
				[]interface{}{float64(-1)},
				[]interface{}{float64(-0.76171875)},
				[]interface{}{float64(0)},
				[]interface{}{float64(0.76171875)},
				[]interface{}{float64(1)},
				[]interface{}{float64(1.9873046875)},
				[]interface{}{float64(123456792)},
				[]interface{}{float64(2147483647)},
				[]interface{}{float64(99999999999999999999999999.9999999999)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{float64(-99999999999999999999999999.9999999999)}},
				[][]interface{}{[]interface{}{float64(-2147483648)}},
				[][]interface{}{[]interface{}{float64(-123456792)}},
				[][]interface{}{[]interface{}{float64(-1.9873046875)}},
				[][]interface{}{[]interface{}{float64(-1)}},
				[][]interface{}{[]interface{}{float64(-0.76171875)}},
				[][]interface{}{[]interface{}{float64(0)}},
				[][]interface{}{[]interface{}{float64(0.76171875)}},
				[][]interface{}{[]interface{}{float64(1)}},
				[][]interface{}{[]interface{}{float64(1.9873046875)}},
				[][]interface{}{[]interface{}{float64(123456792)}},
				[][]interface{}{[]interface{}{float64(2147483647)}},
				[][]interface{}{[]interface{}{float64(99999999999999999999999999.9999999999)}},
			},
		},

		// DEC(38,10)
		testQueryResults{
			query: "select cast (:1 as DEC(38,10)) from dual",
			args: [][]interface{}{
				[]interface{}{float64(-99999999999999999999999999.9999999999)},
				[]interface{}{float64(-2147483648)},
				[]interface{}{float64(-123456792)},
				[]interface{}{float64(-1.9873046875)},
				[]interface{}{float64(-1)},
				[]interface{}{float64(-0.76171875)},
				[]interface{}{float64(0)},
				[]interface{}{float64(0.76171875)},
				[]interface{}{float64(1)},
				[]interface{}{float64(1.9873046875)},
				[]interface{}{float64(123456792)},
				[]interface{}{float64(2147483647)},
				[]interface{}{float64(99999999999999999999999999.9999999999)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{float64(-99999999999999999999999999.9999999999)}},
				[][]interface{}{[]interface{}{float64(-2147483648)}},
				[][]interface{}{[]interface{}{float64(-123456792)}},
				[][]interface{}{[]interface{}{float64(-1.9873046875)}},
				[][]interface{}{[]interface{}{float64(-1)}},
				[][]interface{}{[]interface{}{float64(-0.76171875)}},
				[][]interface{}{[]interface{}{float64(0)}},
				[][]interface{}{[]interface{}{float64(0.76171875)}},
				[][]interface{}{[]interface{}{float64(1)}},
				[][]interface{}{[]interface{}{float64(1.9873046875)}},
				[][]interface{}{[]interface{}{float64(123456792)}},
				[][]interface{}{[]interface{}{float64(2147483647)}},
				[][]interface{}{[]interface{}{float64(99999999999999999999999999.9999999999)}},
			},
		},

		// DECIMAL(38,10)
		testQueryResults{
			query: "select cast (:1 as DECIMAL(38,10)) from dual",
			args: [][]interface{}{
				[]interface{}{float64(-99999999999999999999999999.9999999999)},
				[]interface{}{float64(-2147483648)},
				[]interface{}{float64(-123456792)},
				[]interface{}{float64(-1.9873046875)},
				[]interface{}{float64(-1)},
				[]interface{}{float64(-0.76171875)},
				[]interface{}{float64(0)},
				[]interface{}{float64(0.76171875)},
				[]interface{}{float64(1)},
				[]interface{}{float64(1.9873046875)},
				[]interface{}{float64(123456792)},
				[]interface{}{float64(2147483647)},
				[]interface{}{float64(99999999999999999999999999.9999999999)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{float64(-99999999999999999999999999.9999999999)}},
				[][]interface{}{[]interface{}{float64(-2147483648)}},
				[][]interface{}{[]interface{}{float64(-123456792)}},
				[][]interface{}{[]interface{}{float64(-1.9873046875)}},
				[][]interface{}{[]interface{}{float64(-1)}},
				[][]interface{}{[]interface{}{float64(-0.76171875)}},
				[][]interface{}{[]interface{}{float64(0)}},
				[][]interface{}{[]interface{}{float64(0.76171875)}},
				[][]interface{}{[]interface{}{float64(1)}},
				[][]interface{}{[]interface{}{float64(1.9873046875)}},
				[][]interface{}{[]interface{}{float64(123456792)}},
				[][]interface{}{[]interface{}{float64(2147483647)}},
				[][]interface{}{[]interface{}{float64(99999999999999999999999999.9999999999)}},
			},
		},

		// NUMERIC(38,10)
		testQueryResults{
			query: "select cast (:1 as NUMERIC(38,10)) from dual",
			args: [][]interface{}{
				[]interface{}{float64(-99999999999999999999999999.9999999999)},
				[]interface{}{float64(-2147483648)},
				[]interface{}{float64(-123456792)},
				[]interface{}{float64(-1.9873046875)},
				[]interface{}{float64(-1)},
				[]interface{}{float64(-0.76171875)},
				[]interface{}{float64(0)},
				[]interface{}{float64(0.76171875)},
				[]interface{}{float64(1)},
				[]interface{}{float64(1.9873046875)},
				[]interface{}{float64(123456792)},
				[]interface{}{float64(2147483647)},
				[]interface{}{float64(99999999999999999999999999.9999999999)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{float64(-99999999999999999999999999.9999999999)}},
				[][]interface{}{[]interface{}{float64(-2147483648)}},
				[][]interface{}{[]interface{}{float64(-123456792)}},
				[][]interface{}{[]interface{}{float64(-1.9873046875)}},
				[][]interface{}{[]interface{}{float64(-1)}},
				[][]interface{}{[]interface{}{float64(-0.76171875)}},
				[][]interface{}{[]interface{}{float64(0)}},
				[][]interface{}{[]interface{}{float64(0.76171875)}},
				[][]interface{}{[]interface{}{float64(1)}},
				[][]interface{}{[]interface{}{float64(1.9873046875)}},
				[][]interface{}{[]interface{}{float64(123456792)}},
				[][]interface{}{[]interface{}{float64(2147483647)}},
				[][]interface{}{[]interface{}{float64(99999999999999999999999999.9999999999)}},
			},
		},

		// FLOAT
		testQueryResults{
			query: "select cast (:1 as FLOAT) from dual",
			args: [][]interface{}{
				[]interface{}{float64(-288230381928101358902502915674136903680)},
				[]interface{}{float64(-2147483648)},
				[]interface{}{float64(-123456792)},
				[]interface{}{float64(-1.99999988079071044921875)},
				[]interface{}{float64(-1)},
				[]interface{}{float64(-0.00415134616196155548095703125)},
				[]interface{}{float64(0)},
				[]interface{}{float64(0.00415134616196155548095703125)},
				[]interface{}{float64(1)},
				[]interface{}{float64(1.99999988079071044921875)},
				[]interface{}{float64(123456792)},
				[]interface{}{float64(2147483647)},
				[]interface{}{float64(288230381928101358902502915674136903680)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{float64(-288230381928101358902502915674136903680)}},
				[][]interface{}{[]interface{}{float64(-2147483648)}},
				[][]interface{}{[]interface{}{float64(-123456792)}},
				[][]interface{}{[]interface{}{float64(-1.99999988079071044921875)}},
				[][]interface{}{[]interface{}{float64(-1)}},
				[][]interface{}{[]interface{}{float64(-0.00415134616196155548095703125)}},
				[][]interface{}{[]interface{}{float64(0)}},
				[][]interface{}{[]interface{}{float64(0.00415134616196155548095703125)}},
				[][]interface{}{[]interface{}{float64(1)}},
				[][]interface{}{[]interface{}{float64(1.99999988079071044921875)}},
				[][]interface{}{[]interface{}{float64(123456792)}},
				[][]interface{}{[]interface{}{float64(2147483647)}},
				[][]interface{}{[]interface{}{float64(288230381928101358902502915674136903680)}},
			},
		},

		// INTEGER
		testQueryResults{
			query: "select cast (:1 as INTEGER) from dual",
			args: [][]interface{}{
				[]interface{}{int64(-2147483648)},
				[]interface{}{int64(-1)},
				[]interface{}{int64(0)},
				[]interface{}{int64(1)},
				[]interface{}{int64(2147483647)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-2147483648)}},
				[][]interface{}{[]interface{}{int64(-1)}},
				[][]interface{}{[]interface{}{int64(0)}},
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(2147483647)}},
			},
		},

		// INT
		testQueryResults{
			query: "select cast (:1 as INT) from dual",
			args: [][]interface{}{
				[]interface{}{int64(-2147483648)},
				[]interface{}{int64(-1)},
				[]interface{}{int64(0)},
				[]interface{}{int64(1)},
				[]interface{}{int64(2147483647)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-2147483648)}},
				[][]interface{}{[]interface{}{int64(-1)}},
				[][]interface{}{[]interface{}{int64(0)}},
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(2147483647)}},
			},
		},

		// SMALLINT
		testQueryResults{
			query: "select cast (:1 as SMALLINT) from dual",
			args: [][]interface{}{
				[]interface{}{int64(-2147483648)},
				[]interface{}{int64(-1)},
				[]interface{}{int64(0)},
				[]interface{}{int64(1)},
				[]interface{}{int64(2147483647)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-2147483648)}},
				[][]interface{}{[]interface{}{int64(-1)}},
				[][]interface{}{[]interface{}{int64(0)}},
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(2147483647)}},
			},
		},

		// REAL
		testQueryResults{
			query: "select cast (:1 as REAL) from dual",
			args: [][]interface{}{
				[]interface{}{float64(-288230381928101358902502915674136903680)},
				[]interface{}{float64(-2147483648)},
				[]interface{}{float64(-123456792)},
				[]interface{}{float64(-1.99999988079071044921875)},
				[]interface{}{float64(-1)},
				[]interface{}{float64(-0.00415134616196155548095703125)},
				[]interface{}{float64(0)},
				[]interface{}{float64(0.00415134616196155548095703125)},
				[]interface{}{float64(1)},
				[]interface{}{float64(1.99999988079071044921875)},
				[]interface{}{float64(123456792)},
				[]interface{}{float64(2147483647)},
				[]interface{}{float64(288230381928101358902502915674136903680)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{float64(-288230381928101358902502915674136903680)}},
				[][]interface{}{[]interface{}{float64(-2147483648)}},
				[][]interface{}{[]interface{}{float64(-123456792)}},
				[][]interface{}{[]interface{}{float64(-1.99999988079071044921875)}},
				[][]interface{}{[]interface{}{float64(-1)}},
				[][]interface{}{[]interface{}{float64(-0.00415134616196155548095703125)}},
				[][]interface{}{[]interface{}{float64(0)}},
				[][]interface{}{[]interface{}{float64(0.00415134616196155548095703125)}},
				[][]interface{}{[]interface{}{float64(1)}},
				[][]interface{}{[]interface{}{float64(1.99999988079071044921875)}},
				[][]interface{}{[]interface{}{float64(123456792)}},
				[][]interface{}{[]interface{}{float64(2147483647)}},
				[][]interface{}{[]interface{}{float64(288230381928101358902502915674136903680)}},
			},
		},

		// BINARY_FLOAT
		testQueryResults{
			query: "select cast (:1 as BINARY_FLOAT) from dual",
			args: [][]interface{}{
				[]interface{}{float64(-288230381928101358902502915674136903680)},
				[]interface{}{float64(-2147483648)},
				[]interface{}{float64(-123456792)},
				[]interface{}{float64(-1.99999988079071044921875)},
				[]interface{}{float64(-1)},
				[]interface{}{float64(-0.00415134616196155548095703125)},
				[]interface{}{float64(0)},
				[]interface{}{float64(0.00415134616196155548095703125)},
				[]interface{}{float64(1)},
				[]interface{}{float64(1.99999988079071044921875)},
				[]interface{}{float64(123456792)},
				[]interface{}{float64(2147483648)},
				[]interface{}{float64(288230381928101358902502915674136903680)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{float64(-288230381928101358902502915674136903680)}},
				[][]interface{}{[]interface{}{float64(-2147483648)}},
				[][]interface{}{[]interface{}{float64(-123456792)}},
				[][]interface{}{[]interface{}{float64(-1.99999988079071044921875)}},
				[][]interface{}{[]interface{}{float64(-1)}},
				[][]interface{}{[]interface{}{float64(-0.00415134616196155548095703125)}},
				[][]interface{}{[]interface{}{float64(0)}},
				[][]interface{}{[]interface{}{float64(0.00415134616196155548095703125)}},
				[][]interface{}{[]interface{}{float64(1)}},
				[][]interface{}{[]interface{}{float64(1.99999988079071044921875)}},
				[][]interface{}{[]interface{}{float64(123456792)}},
				[][]interface{}{[]interface{}{float64(2147483648)}},
				[][]interface{}{[]interface{}{float64(288230381928101358902502915674136903680)}},
			},
		},

		// BINARY_DOUBLE
		testQueryResults{
			query: "select cast (:1 as BINARY_DOUBLE) from dual",
			args: [][]interface{}{
				[]interface{}{float64(-288230381928101358902502915674136903680)},
				[]interface{}{float64(-2147483648)},
				[]interface{}{float64(-123456792)},
				[]interface{}{float64(-1.99999988079071044921875)},
				[]interface{}{float64(-1)},
				[]interface{}{float64(-0.00415134616196155548095703125)},
				[]interface{}{float64(0)},
				[]interface{}{float64(0.00415134616196155548095703125)},
				[]interface{}{float64(1)},
				[]interface{}{float64(1.99999988079071044921875)},
				[]interface{}{float64(123456792)},
				[]interface{}{float64(2147483647)},
				[]interface{}{float64(288230381928101358902502915674136903680)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{float64(-288230381928101358902502915674136903680)}},
				[][]interface{}{[]interface{}{float64(-2147483648)}},
				[][]interface{}{[]interface{}{float64(-123456792)}},
				[][]interface{}{[]interface{}{float64(-1.99999988079071044921875)}},
				[][]interface{}{[]interface{}{float64(-1)}},
				[][]interface{}{[]interface{}{float64(-0.00415134616196155548095703125)}},
				[][]interface{}{[]interface{}{float64(0)}},
				[][]interface{}{[]interface{}{float64(0.00415134616196155548095703125)}},
				[][]interface{}{[]interface{}{float64(1)}},
				[][]interface{}{[]interface{}{float64(1.99999988079071044921875)}},
				[][]interface{}{[]interface{}{float64(123456792)}},
				[][]interface{}{[]interface{}{float64(2147483647)}},
				[][]interface{}{[]interface{}{float64(288230381928101358902502915674136903680)}},
			},
		},

		// TIMESTAMP(9) WITH TIME ZONE
		testQueryResults{
			query: "select cast (:1 as TIMESTAMP(9) WITH TIME ZONE) from dual",
			args: [][]interface{}{
				[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC)},
				[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
				[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT)},
				[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
				[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST)},
				[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
				[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
				[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)},
				[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT)},
				[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)},
				[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)},
				// TOFIX: ORA-08192: Flashback Table operation is not allowed on fixed tables
				// []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocNZ)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)}},
				// TOFIX: ORA-08192: Flashback Table operation is not allowed on fixed tables
				// [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocNZ)}},
			},
		},

		// RAW(2000)
		testQueryResults{
			query: "select cast (:1 as RAW(2000)) from dual",
			args: [][]interface{}{
				[]interface{}{[]byte{}},
				[]interface{}{[]byte{10}},
				[]interface{}{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
				[]interface{}{[]byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
				[]interface{}{[]byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}},
				[]interface{}{testByteSlice1},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{[]byte{10}}},
				[][]interface{}{[]interface{}{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}},
				[][]interface{}{[]interface{}{[]byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}}},
				[][]interface{}{[]interface{}{[]byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}}},
				[][]interface{}{[]interface{}{testByteSlice1}},
			},
		},
	}

	testRunQueryResults(t, queryResults)
}

// TestSelectGoTypes is select :1 from dual for each Go Type
func TestSelectGoTypes(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	// https://tour.golang.org/basics/11

	queryResults := []testQueryResults{
		// bool
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{true},
				[]interface{}{false},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(0)}},
			},
		},

		// string
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{""},
				[]interface{}{"a"},
				[]interface{}{"123"},
				[]interface{}{"1234.567"},
				[]interface{}{"abc      "},
				[]interface{}{"abcdefghijklmnopqrstuvwxyz"},
				[]interface{}{"a b c d e f g h i j k l m n o p q r s t u v w x y z "},
				[]interface{}{"a\nb\nc"},
				[]interface{}{testString1},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a"}},
				[][]interface{}{[]interface{}{"123"}},
				[][]interface{}{[]interface{}{"1234.567"}},
				[][]interface{}{[]interface{}{"abc      "}},
				[][]interface{}{[]interface{}{"abcdefghijklmnopqrstuvwxyz"}},
				[][]interface{}{[]interface{}{"a b c d e f g h i j k l m n o p q r s t u v w x y z "}},
				[][]interface{}{[]interface{}{"a\nb\nc"}},
				[][]interface{}{[]interface{}{testString1}},
			},
		},

		// int8: -128 to 127
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{int16(-128)},
				[]interface{}{int16(-1)},
				[]interface{}{int16(0)},
				[]interface{}{int16(1)},
				[]interface{}{int16(127)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-128)}},
				[][]interface{}{[]interface{}{int64(-1)}},
				[][]interface{}{[]interface{}{int64(0)}},
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(127)}},
			},
		},
		// int16: -32768 to 32767
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{int16(-32768)},
				[]interface{}{int16(-128)},
				[]interface{}{int16(-1)},
				[]interface{}{int16(0)},
				[]interface{}{int16(1)},
				[]interface{}{int16(127)},
				[]interface{}{int16(32767)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-32768)}},
				[][]interface{}{[]interface{}{int64(-128)}},
				[][]interface{}{[]interface{}{int64(-1)}},
				[][]interface{}{[]interface{}{int64(0)}},
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(127)}},
				[][]interface{}{[]interface{}{int64(32767)}},
			},
		},
		// int32: -2147483648 to 2147483647
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{int32(-2147483648)},
				[]interface{}{int32(-32768)},
				[]interface{}{int32(-128)},
				[]interface{}{int32(-1)},
				[]interface{}{int32(0)},
				[]interface{}{int32(1)},
				[]interface{}{int32(127)},
				[]interface{}{int32(32767)},
				[]interface{}{int32(2147483647)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-2147483648)}},
				[][]interface{}{[]interface{}{int64(-32768)}},
				[][]interface{}{[]interface{}{int64(-128)}},
				[][]interface{}{[]interface{}{int64(-1)}},
				[][]interface{}{[]interface{}{int64(0)}},
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(127)}},
				[][]interface{}{[]interface{}{int64(32767)}},
				[][]interface{}{[]interface{}{int64(2147483647)}},
			},
		},
		// int64: -9223372036854775808 to 9223372036854775807
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{int64(-9223372036854775808)},
				[]interface{}{int64(-2147483648)},
				[]interface{}{int64(-32768)},
				[]interface{}{int64(-128)},
				[]interface{}{int64(-1)},
				[]interface{}{int64(0)},
				[]interface{}{int64(1)},
				[]interface{}{int64(127)},
				[]interface{}{int64(32767)},
				[]interface{}{int64(2147483647)},
				[]interface{}{int64(9223372036854775807)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-9223372036854775808)}},
				[][]interface{}{[]interface{}{int64(-2147483648)}},
				[][]interface{}{[]interface{}{int64(-32768)}},
				[][]interface{}{[]interface{}{int64(-128)}},
				[][]interface{}{[]interface{}{int64(-1)}},
				[][]interface{}{[]interface{}{int64(0)}},
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(127)}},
				[][]interface{}{[]interface{}{int64(32767)}},
				[][]interface{}{[]interface{}{int64(2147483647)}},
				[][]interface{}{[]interface{}{int64(9223372036854775807)}},
			},
		},

		// uint8: 0 to 255
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{uint32(0)},
				[]interface{}{uint32(1)},
				[]interface{}{uint32(127)},
				[]interface{}{uint32(255)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(0)}},
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(127)}},
				[][]interface{}{[]interface{}{int64(255)}},
			},
		},
		// uint16: 0 to 65535
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{uint32(0)},
				[]interface{}{uint32(1)},
				[]interface{}{uint32(127)},
				[]interface{}{uint32(32767)},
				[]interface{}{uint32(65535)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(0)}},
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(127)}},
				[][]interface{}{[]interface{}{int64(32767)}},
				[][]interface{}{[]interface{}{int64(65535)}},
			},
		},
		// uint32: 0 to 4294967295
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{uint32(0)},
				[]interface{}{uint32(1)},
				[]interface{}{uint32(127)},
				[]interface{}{uint32(32767)},
				[]interface{}{uint32(2147483647)},
				[]interface{}{uint32(4294967295)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(0)}},
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(127)}},
				[][]interface{}{[]interface{}{int64(32767)}},
				[][]interface{}{[]interface{}{int64(2147483647)}},
				[][]interface{}{[]interface{}{int64(4294967295)}},
			},
		},
		// uint64: 0 to 18446744073709551615
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{uint64(0)},
				[]interface{}{uint64(1)},
				[]interface{}{uint64(127)},
				[]interface{}{uint64(32767)},
				[]interface{}{uint64(2147483647)},
				[]interface{}{uint64(9223372036854775807)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(0)}},
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(127)}},
				[][]interface{}{[]interface{}{int64(32767)}},
				[][]interface{}{[]interface{}{int64(2147483647)}},
				[][]interface{}{[]interface{}{int64(9223372036854775807)}},
			},
		},

		// byte
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{byte('a')},
				[]interface{}{byte('z')},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(97)}},
				[][]interface{}{[]interface{}{int64(122)}},
			},
		},

		// rune
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{'a'},
				[]interface{}{'z'},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(97)}},
				[][]interface{}{[]interface{}{int64(122)}},
			},
		},

		// float32
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{float32(-9223372036854775808)},
				[]interface{}{float32(-2147483648)},
				[]interface{}{float32(-32767.123046875)},
				[]interface{}{float32(-32767)},
				[]interface{}{float32(-128.1234588623046875)},
				[]interface{}{float32(-128)},
				[]interface{}{float32(-1.12345683574676513671875)},
				[]interface{}{float32(-1)},
				[]interface{}{float32(-0.12345679104328155517578125)},
				[]interface{}{float32(0)},
				[]interface{}{float32(0.12345679104328155517578125)},
				[]interface{}{float32(1)},
				[]interface{}{float32(1.12345683574676513671875)},
				[]interface{}{float32(128)},
				[]interface{}{float32(128.1234588623046875)},
				[]interface{}{float32(32767)},
				[]interface{}{float32(32767.123046875)},
				[]interface{}{float32(2147483648)},
				[]interface{}{float32(9223372036854775808)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{float64(-9223372036854775808)}},
				[][]interface{}{[]interface{}{float64(-2147483648)}},
				[][]interface{}{[]interface{}{float64(-32767.123046875)}},
				[][]interface{}{[]interface{}{float64(-32767)}},
				[][]interface{}{[]interface{}{float64(-128.1234588623046875)}},
				[][]interface{}{[]interface{}{float64(-128)}},
				[][]interface{}{[]interface{}{float64(-1.12345683574676513671875)}},
				[][]interface{}{[]interface{}{float64(-1)}},
				[][]interface{}{[]interface{}{float64(-0.12345679104328155517578125)}},
				[][]interface{}{[]interface{}{float64(0)}},
				[][]interface{}{[]interface{}{float64(0.12345679104328155517578125)}},
				[][]interface{}{[]interface{}{float64(1)}},
				[][]interface{}{[]interface{}{float64(1.12345683574676513671875)}},
				[][]interface{}{[]interface{}{float64(128)}},
				[][]interface{}{[]interface{}{float64(128.1234588623046875)}},
				[][]interface{}{[]interface{}{float64(32767)}},
				[][]interface{}{[]interface{}{float64(32767.123046875)}},
				[][]interface{}{[]interface{}{float64(2147483648)}},
				[][]interface{}{[]interface{}{float64(9223372036854775808)}},
			},
		},
		// float64
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{float64(-9223372036854775808)},
				[]interface{}{float64(-2147483648)},
				[]interface{}{float64(-32767.123046875)},
				[]interface{}{float64(-32767)},
				[]interface{}{float64(-128.1234588623046875)},
				[]interface{}{float64(-128)},
				[]interface{}{float64(-1.12345683574676513671875)},
				[]interface{}{float64(-1)},
				[]interface{}{float64(-0.12345679104328155517578125)},
				[]interface{}{float64(0)},
				[]interface{}{float64(0.12345679104328155517578125)},
				[]interface{}{float64(1)},
				[]interface{}{float64(1.12345683574676513671875)},
				[]interface{}{float64(128)},
				[]interface{}{float64(128.1234588623046875)},
				[]interface{}{float64(32767)},
				[]interface{}{float64(32767.123046875)},
				[]interface{}{float64(2147483648)},
				[]interface{}{float64(9223372036854775808)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{float64(-9223372036854775808)}},
				[][]interface{}{[]interface{}{float64(-2147483648)}},
				[][]interface{}{[]interface{}{float64(-32767.123046875)}},
				[][]interface{}{[]interface{}{float64(-32767)}},
				[][]interface{}{[]interface{}{float64(-128.1234588623046875)}},
				[][]interface{}{[]interface{}{float64(-128)}},
				[][]interface{}{[]interface{}{float64(-1.12345683574676513671875)}},
				[][]interface{}{[]interface{}{float64(-1)}},
				[][]interface{}{[]interface{}{float64(-0.12345679104328155517578125)}},
				[][]interface{}{[]interface{}{float64(0)}},
				[][]interface{}{[]interface{}{float64(0.12345679104328155517578125)}},
				[][]interface{}{[]interface{}{float64(1)}},
				[][]interface{}{[]interface{}{float64(1.12345683574676513671875)}},
				[][]interface{}{[]interface{}{float64(128)}},
				[][]interface{}{[]interface{}{float64(128.1234588623046875)}},
				[][]interface{}{[]interface{}{float64(32767)}},
				[][]interface{}{[]interface{}{float64(32767.123046875)}},
				[][]interface{}{[]interface{}{float64(2147483648)}},
				[][]interface{}{[]interface{}{float64(9223372036854775808)}},
			},
		},

		// time
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC)},
				[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
				[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT)},
				[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
				[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST)},
				[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
				[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
				[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)},
				[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT)},
				[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)},
				[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)},
				// TOFIX: ORA-08192: Flashback Table operation is not allowed on fixed tables
				// []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocNZ)},
				[]interface{}{time.Date(9998, 12, 31, 3, 4, 5, 123456789, time.UTC)},
				[]interface{}{time.Date(9998, 12, 31, 3, 4, 5, 123456789, testTimeLocUTC)},
				[]interface{}{time.Date(9998, 12, 31, 3, 4, 5, 123456789, testTimeLocGMT)},
				[]interface{}{time.Date(9998, 12, 31, 3, 4, 5, 123456789, testTimeLocEST)},
				[]interface{}{time.Date(9998, 12, 31, 3, 4, 5, 123456789, testTimeLocMST)},
				[]interface{}{time.Date(9998, 12, 31, 3, 4, 5, 123456789, testTimeLocNZ)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)}},
				// TOFIX: ORA-08192: Flashback Table operation is not allowed on fixed tables
				// [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocNZ)}},
				[][]interface{}{[]interface{}{time.Date(9998, 12, 31, 3, 4, 5, 123456789, time.UTC)}},
				[][]interface{}{[]interface{}{time.Date(9998, 12, 31, 3, 4, 5, 123456789, testTimeLocUTC)}},
				[][]interface{}{[]interface{}{time.Date(9998, 12, 31, 3, 4, 5, 123456789, testTimeLocGMT)}},
				[][]interface{}{[]interface{}{time.Date(9998, 12, 31, 3, 4, 5, 123456789, testTimeLocEST)}},
				[][]interface{}{[]interface{}{time.Date(9998, 12, 31, 3, 4, 5, 123456789, testTimeLocMST)}},
				[][]interface{}{[]interface{}{time.Date(9998, 12, 31, 3, 4, 5, 123456789, testTimeLocNZ)}},
			},
		},

		// []byte
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{[]byte{}},
				[]interface{}{[]byte{10}},
				[]interface{}{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
				[]interface{}{[]byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
				[]interface{}{[]byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}},
				[]interface{}{testByteSlice1},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{[]byte{10}}},
				[][]interface{}{[]interface{}{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}},
				[][]interface{}{[]interface{}{[]byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}}},
				[][]interface{}{[]interface{}{[]byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}}},
				[][]interface{}{[]interface{}{testByteSlice1}},
			},
		},
	}

	testRunQueryResults(t, queryResults)
}
