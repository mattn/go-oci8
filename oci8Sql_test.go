package oci8

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"
)

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

func TestSelectParallel(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	stmt, err := TestDB.PrepareContext(ctx, "select :1 from dual")
	if err != nil {
		t.Fatal("prepare error:", err)
	}
	cancel()

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

func TestSelectTypes(t *testing.T) {
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
				[]interface{}{strings.Repeat("a", 10)},
				[]interface{}{strings.Repeat("a", 100)},
				[]interface{}{strings.Repeat("a", 500)},
				[]interface{}{strings.Repeat("a", 1000)},
				[]interface{}{strings.Repeat("a", 1500)},
				[]interface{}{strings.Repeat("a", 2000)},
				[]interface{}{strings.Repeat("a", 3000)},
				[]interface{}{strings.Repeat("a", 4000)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a"}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 10)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 100)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 500)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 1000)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 1500)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 2000)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 3000)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 4000)}},
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
				[]interface{}{strings.Repeat("a", 10)},
				[]interface{}{strings.Repeat("a", 100)},
				[]interface{}{strings.Repeat("a", 500)},
				[]interface{}{strings.Repeat("a", 1000)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a" + strings.Repeat(" ", 999)}},
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
	}

	testRunQueryResults(t, queryResults)
}
