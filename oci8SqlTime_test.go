package oci8

import (
	"testing"
	"time"
)

// TestSelectDualNullTime checks nulls
func TestSelectDualNullTime(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	// TIMESTAMP(9)
	queryResults := testQueryResults{
		query:        "select cast (null as TIMESTAMP(9)) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// TIMESTAMP(9) WITH TIME ZONE
	queryResults = testQueryResults{
		query:        "select cast (null as TIMESTAMP(9) WITH TIME ZONE) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// TIMESTAMP(9) WITH LOCAL TIME ZONE
	queryResults = testQueryResults{
		query:        "select cast (null as TIMESTAMP(9) WITH LOCAL TIME ZONE) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

}

// TestSelectDualTime checks select dual for time types
func TestSelectDualTime(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	queryResults := testQueryResults{}

	// TOFIX: ORA-08192: Flashback Table operation is not allowed on fixed tables
	// []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocNZ)},

	queryResultTimeToLocal := []testQueryResult{
		testQueryResult{
			args:    []interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC)},
			results: [][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
			results: [][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT)},
			results: [][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
			results: [][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST)},
			results: [][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
			results: [][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
			results: [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)},
			results: [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT)},
			results: [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)},
			results: [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)},
			results: [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)}},
		},
	}

	queryResultTimeToTZ := []testQueryResult{
		testQueryResult{
			args:    []interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC)},
			results: [][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
			results: [][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT)},
			results: [][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
			results: [][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST)},
			results: [][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
			results: [][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
			results: [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)},
			results: [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT)},
			results: [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)},
			results: [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)}},
		},
		testQueryResult{
			args:    []interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)},
			results: [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)}},
		},
	}

	// https://ss64.com/ora/syntax-datatypes.html

	// TIMESTAMP(9)
	queryResults.query = "select cast (:1 as TIMESTAMP(9)) from dual"
	queryResults.queryResults = queryResultTimeToLocal
	testRunQueryResults(t, queryResults)

	// TIMESTAMP(9) WITH TIME ZONE
	queryResults.query = "select cast (:1 as TIMESTAMP(9) WITH TIME ZONE) from dual"
	queryResults.queryResults = queryResultTimeToTZ
	testRunQueryResults(t, queryResults)

	// TIMESTAMP(9) WITH LOCAL TIME ZONE
	queryResults.query = "select cast (:1 as TIMESTAMP(9) WITH LOCAL TIME ZONE) from dual"
	queryResults.queryResults = queryResultTimeToTZ
	testRunQueryResults(t, queryResults)

	// https://tour.golang.org/basics/11

	// Go
	queryResults.query = "select :1 from dual"
	queryResults.queryResults = queryResultTimeToTZ
	testRunQueryResults(t, queryResults)

	queryResultTimeYearToMonth := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int64(-2)},
			results: [][]interface{}{[]interface{}{int64(-24)}},
		},
		testQueryResult{
			args:    []interface{}{int64(-1)},
			results: [][]interface{}{[]interface{}{int64(-12)}},
		},
		testQueryResult{
			args:    []interface{}{int64(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{int64(1)},
			results: [][]interface{}{[]interface{}{int64(12)}},
		},
		testQueryResult{
			args:    []interface{}{int64(2)},
			results: [][]interface{}{[]interface{}{int64(24)}},
		},
		testQueryResult{
			args:    []interface{}{float64(-2.5)},
			results: [][]interface{}{[]interface{}{int64(-30)}},
		},
		testQueryResult{
			args:    []interface{}{float64(-1.25)},
			results: [][]interface{}{[]interface{}{int64(-15)}},
		},
		testQueryResult{
			args:    []interface{}{float64(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{float64(1.25)},
			results: [][]interface{}{[]interface{}{int64(15)}},
		},
		testQueryResult{
			args:    []interface{}{float64(2.5)},
			results: [][]interface{}{[]interface{}{int64(30)}},
		},
	}

	// INTERVAL DAY TO MONTH - YEAR
	queryResults.query = "select NUMTOYMINTERVAL(:1, 'YEAR') from dual"
	queryResults.queryResults = queryResultTimeYearToMonth
	testRunQueryResults(t, queryResults)

	queryResultTimeMonthToMonth := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int64(-2)},
			results: [][]interface{}{[]interface{}{int64(-2)}},
		},
		testQueryResult{
			args:    []interface{}{int64(-1)},
			results: [][]interface{}{[]interface{}{int64(-1)}},
		},
		testQueryResult{
			args:    []interface{}{int64(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{int64(1)},
			results: [][]interface{}{[]interface{}{int64(1)}},
		},
		testQueryResult{
			args:    []interface{}{int64(2)},
			results: [][]interface{}{[]interface{}{int64(2)}},
		},
		testQueryResult{
			args:    []interface{}{float64(-2.75)},
			results: [][]interface{}{[]interface{}{int64(-3)}},
		},
		testQueryResult{
			args:    []interface{}{float64(-1.25)},
			results: [][]interface{}{[]interface{}{int64(-1)}},
		},
		testQueryResult{
			args:    []interface{}{float64(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{float64(1.25)},
			results: [][]interface{}{[]interface{}{int64(1)}},
		},
		testQueryResult{
			args:    []interface{}{float64(2.75)},
			results: [][]interface{}{[]interface{}{int64(3)}},
		},
	}

	// INTERVAL DAY TO MONTH - MONTH
	queryResults.query = "select NUMTOYMINTERVAL(:1, 'MONTH') from dual"
	queryResults.queryResults = queryResultTimeMonthToMonth
	testRunQueryResults(t, queryResults)

	queryResultTimeDayToSecond := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int64(-2)},
			results: [][]interface{}{[]interface{}{int64(-172800000000000)}},
		},
		testQueryResult{
			args:    []interface{}{int64(-1)},
			results: [][]interface{}{[]interface{}{int64(-86400000000000)}},
		},
		testQueryResult{
			args:    []interface{}{int64(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{int64(1)},
			results: [][]interface{}{[]interface{}{int64(86400000000000)}},
		},
		testQueryResult{
			args:    []interface{}{int64(2)},
			results: [][]interface{}{[]interface{}{int64(172800000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(-2.5)},
			results: [][]interface{}{[]interface{}{int64(-216000000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(-1.25)},
			results: [][]interface{}{[]interface{}{int64(-108000000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{float64(1.25)},
			results: [][]interface{}{[]interface{}{int64(108000000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(2.5)},
			results: [][]interface{}{[]interface{}{int64(216000000000000)}},
		},
	}

	// INTERVAL DAY TO SECOND - DAY
	queryResults.query = "select NUMTODSINTERVAL(:1, 'DAY') from dual"
	queryResults.queryResults = queryResultTimeDayToSecond
	testRunQueryResults(t, queryResults)

	queryResultTimeHourToSecond := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int64(-2)},
			results: [][]interface{}{[]interface{}{int64(-7200000000000)}},
		},
		testQueryResult{
			args:    []interface{}{int64(-1)},
			results: [][]interface{}{[]interface{}{int64(-3600000000000)}},
		},
		testQueryResult{
			args:    []interface{}{int64(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{int64(1)},
			results: [][]interface{}{[]interface{}{int64(3600000000000)}},
		},
		testQueryResult{
			args:    []interface{}{int64(2)},
			results: [][]interface{}{[]interface{}{int64(7200000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(-2.5)},
			results: [][]interface{}{[]interface{}{int64(-9000000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(-1.25)},
			results: [][]interface{}{[]interface{}{int64(-4500000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{float64(1.25)},
			results: [][]interface{}{[]interface{}{int64(4500000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(2.5)},
			results: [][]interface{}{[]interface{}{int64(9000000000000)}},
		},
	}

	// INTERVAL DAY TO SECOND - HOUR
	queryResults.query = "select NUMTODSINTERVAL(:1, 'HOUR') from dual"
	queryResults.queryResults = queryResultTimeHourToSecond
	testRunQueryResults(t, queryResults)

	queryResultTimeMinuteToSecond := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int64(-2)},
			results: [][]interface{}{[]interface{}{int64(-120000000000)}},
		},
		testQueryResult{
			args:    []interface{}{int64(-1)},
			results: [][]interface{}{[]interface{}{int64(-60000000000)}},
		},
		testQueryResult{
			args:    []interface{}{int64(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{int64(1)},
			results: [][]interface{}{[]interface{}{int64(60000000000)}},
		},
		testQueryResult{
			args:    []interface{}{int64(2)},
			results: [][]interface{}{[]interface{}{int64(120000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(-2.5)},
			results: [][]interface{}{[]interface{}{int64(-150000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(-1.25)},
			results: [][]interface{}{[]interface{}{int64(-75000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{float64(1.25)},
			results: [][]interface{}{[]interface{}{int64(75000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(2.5)},
			results: [][]interface{}{[]interface{}{int64(150000000000)}},
		},
	}

	// INTERVAL DAY TO SECOND - MINUTE
	queryResults.query = "select NUMTODSINTERVAL(:1, 'MINUTE') from dual"
	queryResults.queryResults = queryResultTimeMinuteToSecond
	testRunQueryResults(t, queryResults)

	queryResultTimeSecondToSecond := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int64(-2)},
			results: [][]interface{}{[]interface{}{int64(-2000000000)}},
		},
		testQueryResult{
			args:    []interface{}{int64(-1)},
			results: [][]interface{}{[]interface{}{int64(-1000000000)}},
		},
		testQueryResult{
			args:    []interface{}{int64(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{int64(1)},
			results: [][]interface{}{[]interface{}{int64(1000000000)}},
		},
		testQueryResult{
			args:    []interface{}{int64(2)},
			results: [][]interface{}{[]interface{}{int64(2000000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(-2.5)},
			results: [][]interface{}{[]interface{}{int64(-2500000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(-1.25)},
			results: [][]interface{}{[]interface{}{int64(-1250000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{float64(1.25)},
			results: [][]interface{}{[]interface{}{int64(1250000000)}},
		},
		testQueryResult{
			args:    []interface{}{float64(2.5)},
			results: [][]interface{}{[]interface{}{int64(2500000000)}},
		},
	}

	// INTERVAL DAY TO SECOND - SECOND
	queryResults.query = "select NUMTODSINTERVAL(:1, 'SECOND') from dual"
	queryResults.queryResults = queryResultTimeSecondToSecond
	testRunQueryResults(t, queryResults)
}

// TestDestructiveTime checks insert, select, update, and delete of time types
func TestDestructiveTime(t *testing.T) {
	if TestDisableDatabase || TestDisableDestructive {
		t.SkipNow()
	}

	// https://ss64.com/ora/syntax-datatypes.html

	// TIMESTAMP(9)
	tableName := "TIMESTAMP_" + TestTimeString
	err := testExec(t, "create table "+tableName+" ( A int, B TIMESTAMP(9), C TIMESTAMP(9) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{1, time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
			[]interface{}{2, time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
			[]interface{}{3, time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
			[]interface{}{4, time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)},
			[]interface{}{5, time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)},
			[]interface{}{6, time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)},
			// TOFIX: testTimeLocNZ - ORA-08192: Flashback Table operation is not allowed on fixed tables
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults := testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(1), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)},
					[]interface{}{int64(2), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)},
					[]interface{}{int64(3), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)},
					[]interface{}{int64(4), time.Date(1, 1, 1, 0, 0, 0, 0, time.Local), time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)},
					[]interface{}{int64(5), time.Date(1, 1, 1, 0, 0, 0, 0, time.Local), time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)},
					[]interface{}{int64(6), time.Date(1, 1, 1, 0, 0, 0, 0, time.Local), time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{4},
			[]interface{}{5},
			[]interface{}{6},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(1), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)},
					[]interface{}{int64(2), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)},
					[]interface{}{int64(3), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// TIMESTAMP(9) WITH TIME ZONE
	tableName = "TIMESTAMPWTZ_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A int, B TIMESTAMP(9) WITH TIME ZONE, C TIMESTAMP(9) WITH TIME ZONE )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{1, time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
			[]interface{}{2, time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
			[]interface{}{3, time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
			[]interface{}{4, time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)},
			[]interface{}{5, time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)},
			[]interface{}{6, time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)},
			// TOFIX: testTimeLocNZ - ORA-08192: Flashback Table operation is not allowed on fixed tables
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(1), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
					[]interface{}{int64(2), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
					[]interface{}{int64(3), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
					[]interface{}{int64(4), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)},
					[]interface{}{int64(5), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)},
					[]interface{}{int64(6), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{4},
			[]interface{}{5},
			[]interface{}{6},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(1), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
					[]interface{}{int64(2), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
					[]interface{}{int64(3), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// TIMESTAMP(9) WITH LOCAL TIME ZONE
	tableName = "TIMESTAMPWLTZ_" + TestTimeString
	err = testExec(t, "create table "+tableName+
		" ( A int, B TIMESTAMP(9) WITH LOCAL TIME ZONE, C TIMESTAMP(9) WITH LOCAL TIME ZONE )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{1, time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
			[]interface{}{2, time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
			[]interface{}{3, time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
			[]interface{}{4, time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)},
			[]interface{}{5, time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)},
			[]interface{}{6, time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)},
			// TOFIX: testTimeLocNZ - ORA-08192: Flashback Table operation is not allowed on fixed tables
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(1), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
					[]interface{}{int64(2), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
					[]interface{}{int64(3), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
					[]interface{}{int64(4), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)},
					[]interface{}{int64(5), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)},
					[]interface{}{int64(6), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{4},
			[]interface{}{5},
			[]interface{}{6},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(1), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
					[]interface{}{int64(2), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
					[]interface{}{int64(3), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// INTERVAL YEAR TO MONTH
	tableName = "INTERVALYTM_" + TestTimeString
	err = testExec(t, "create table "+tableName+
		" ( A int, B INTERVAL YEAR TO MONTH, C INTERVAL YEAR TO MONTH )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, NUMTOYMINTERVAL(:2, 'YEAR'), NUMTOYMINTERVAL(:3, 'MONTH'))",
		[][]interface{}{
			[]interface{}{1, -2, -2},
			[]interface{}{2, -1, -1},
			[]interface{}{3, 1, 1},
			[]interface{}{4, 2, 2},
			[]interface{}{5, 1.25, 2.1},
			[]interface{}{6, 1.5, 2.9},
			[]interface{}{7, 2.75, 3},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(1), int64(-24), int64(-2)},
					[]interface{}{int64(2), int64(-12), int64(-1)},
					[]interface{}{int64(3), int64(12), int64(1)},
					[]interface{}{int64(4), int64(24), int64(2)},
					[]interface{}{int64(5), int64(15), int64(2)},
					[]interface{}{int64(6), int64(18), int64(3)},
					[]interface{}{int64(7), int64(33), int64(3)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{5},
			[]interface{}{6},
			[]interface{}{7},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(1), int64(-24), int64(-2)},
					[]interface{}{int64(2), int64(-12), int64(-1)},
					[]interface{}{int64(3), int64(12), int64(1)},
					[]interface{}{int64(4), int64(24), int64(2)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// INTERVAL DAY TO SECOND
	tableName = "INTERVALDTS_" + TestTimeString
	err = testExec(t, "create table "+tableName+
		" ( A int, B INTERVAL DAY TO SECOND, C INTERVAL DAY TO SECOND )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, NUMTODSINTERVAL(:2, 'DAY'), NUMTODSINTERVAL(:3, 'HOUR'))",
		[][]interface{}{
			[]interface{}{1, -2, -2},
			[]interface{}{2, -1, -1},
			[]interface{}{3, 1, 1},
			[]interface{}{4, 2, 2},
			[]interface{}{5, 1.25, 1.25},
			[]interface{}{6, 1.5, 1.5},
			[]interface{}{7, 2.75, 2.75},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(1), int64(-172800000000000), int64(-7200000000000)},
					[]interface{}{int64(2), int64(-86400000000000), int64(-3600000000000)},
					[]interface{}{int64(3), int64(86400000000000), int64(3600000000000)},
					[]interface{}{int64(4), int64(172800000000000), int64(7200000000000)},
					[]interface{}{int64(5), int64(108000000000000), int64(4500000000000)},
					[]interface{}{int64(6), int64(129600000000000), int64(5400000000000)},
					[]interface{}{int64(7), int64(237600000000000), int64(9900000000000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{5},
			[]interface{}{6},
			[]interface{}{7},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(1), int64(-172800000000000), int64(-7200000000000)},
					[]interface{}{int64(2), int64(-86400000000000), int64(-3600000000000)},
					[]interface{}{int64(3), int64(86400000000000), int64(3600000000000)},
					[]interface{}{int64(4), int64(172800000000000), int64(7200000000000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "truncate table "+tableName, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, NUMTODSINTERVAL(:2, 'MINUTE'), NUMTODSINTERVAL(:3, 'SECOND'))",
		[][]interface{}{
			[]interface{}{1, -2, -2},
			[]interface{}{2, -1, -1},
			[]interface{}{3, 1, 1},
			[]interface{}{4, 2, 2},
			[]interface{}{5, 1.25, 1.25},
			[]interface{}{6, 1.5, 1.5},
			[]interface{}{7, 2.75, 2.75},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(1), int64(-120000000000), int64(-2000000000)},
					[]interface{}{int64(2), int64(-60000000000), int64(-1000000000)},
					[]interface{}{int64(3), int64(60000000000), int64(1000000000)},
					[]interface{}{int64(4), int64(120000000000), int64(2000000000)},
					[]interface{}{int64(5), int64(75000000000), int64(1250000000)},
					[]interface{}{int64(6), int64(90000000000), int64(1500000000)},
					[]interface{}{int64(7), int64(165000000000), int64(2750000000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{5},
			[]interface{}{6},
			[]interface{}{7},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(1), int64(-120000000000), int64(-2000000000)},
					[]interface{}{int64(2), int64(-60000000000), int64(-1000000000)},
					[]interface{}{int64(3), int64(60000000000), int64(1000000000)},
					[]interface{}{int64(4), int64(120000000000), int64(2000000000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)
}
