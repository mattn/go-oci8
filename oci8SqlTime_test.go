package oci8

import (
	"testing"
	"time"
)

// TestSelectCastTime checks cast x from dual for time SQL types
func TestSelectCastTime(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	// https://ss64.com/ora/syntax-datatypes.html

	queryResults := []testQueryResults{

		// TIMESTAMP(9)
		testQueryResults{
			query: "select cast (:1 as TIMESTAMP(9)) from dual",
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
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)}},
				[][]interface{}{[]interface{}{time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)}},
				[][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)}},
				// TOFIX: ORA-08192: Flashback Table operation is not allowed on fixed tables
				// [][]interface{}{[]interface{}{time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)}},
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

		// TIMESTAMP(9) WITH LOCAL TIME ZONE
		testQueryResults{
			query: "select cast (:1 as TIMESTAMP(9) WITH LOCAL TIME ZONE) from dual",
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

		// INTERVAL DAY TO MONTH - YEAR
		testQueryResults{
			query: "select NUMTOYMINTERVAL(:1, 'YEAR') from dual",
			args: [][]interface{}{
				[]interface{}{-2},
				[]interface{}{-1},
				[]interface{}{1},
				[]interface{}{2},
				[]interface{}{float64(1.25)},
				[]interface{}{float64(1.5)},
				[]interface{}{float64(2.75)},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-24)}},
				[][]interface{}{[]interface{}{int64(-12)}},
				[][]interface{}{[]interface{}{int64(12)}},
				[][]interface{}{[]interface{}{int64(24)}},
				[][]interface{}{[]interface{}{int64(15)}},
				[][]interface{}{[]interface{}{int64(18)}},
				[][]interface{}{[]interface{}{int64(33)}},
			},
		},

		// INTERVAL DAY TO MONTH - MONTH
		testQueryResults{
			query: "select NUMTOYMINTERVAL(:1, 'MONTH') from dual",
			args: [][]interface{}{
				[]interface{}{-2},
				[]interface{}{-1},
				[]interface{}{1},
				[]interface{}{2},
				[]interface{}{2.1},
				[]interface{}{2.9},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-2)}},
				[][]interface{}{[]interface{}{int64(-1)}},
				[][]interface{}{[]interface{}{int64(1)}},
				[][]interface{}{[]interface{}{int64(2)}},
				[][]interface{}{[]interface{}{int64(2)}},
				[][]interface{}{[]interface{}{int64(3)}},
			},
		},

		// INTERVAL DAY TO SECOND - DAY
		testQueryResults{
			query: "select NUMTODSINTERVAL(:1, 'DAY') from dual",
			args: [][]interface{}{
				[]interface{}{-2},
				[]interface{}{-1},
				[]interface{}{1},
				[]interface{}{2},
				[]interface{}{1.25},
				[]interface{}{1.5},
				[]interface{}{2.75},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-172800000000000)}},
				[][]interface{}{[]interface{}{int64(-86400000000000)}},
				[][]interface{}{[]interface{}{int64(86400000000000)}},
				[][]interface{}{[]interface{}{int64(172800000000000)}},
				[][]interface{}{[]interface{}{int64(108000000000000)}},
				[][]interface{}{[]interface{}{int64(129600000000000)}},
				[][]interface{}{[]interface{}{int64(237600000000000)}},
			},
		},

		// INTERVAL DAY TO SECOND - HOUR
		testQueryResults{
			query: "select NUMTODSINTERVAL(:1, 'HOUR') from dual",
			args: [][]interface{}{
				[]interface{}{-2},
				[]interface{}{-1},
				[]interface{}{1},
				[]interface{}{2},
				[]interface{}{1.25},
				[]interface{}{1.5},
				[]interface{}{2.75},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-7200000000000)}},
				[][]interface{}{[]interface{}{int64(-3600000000000)}},
				[][]interface{}{[]interface{}{int64(3600000000000)}},
				[][]interface{}{[]interface{}{int64(7200000000000)}},
				[][]interface{}{[]interface{}{int64(4500000000000)}},
				[][]interface{}{[]interface{}{int64(5400000000000)}},
				[][]interface{}{[]interface{}{int64(9900000000000)}},
			},
		},

		// INTERVAL DAY TO SECOND - MINUTE
		testQueryResults{
			query: "select NUMTODSINTERVAL(:1, 'MINUTE') from dual",
			args: [][]interface{}{
				[]interface{}{-2},
				[]interface{}{-1},
				[]interface{}{1},
				[]interface{}{2},
				[]interface{}{1.25},
				[]interface{}{1.5},
				[]interface{}{2.75},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-120000000000)}},
				[][]interface{}{[]interface{}{int64(-60000000000)}},
				[][]interface{}{[]interface{}{int64(60000000000)}},
				[][]interface{}{[]interface{}{int64(120000000000)}},
				[][]interface{}{[]interface{}{int64(75000000000)}},
				[][]interface{}{[]interface{}{int64(90000000000)}},
				[][]interface{}{[]interface{}{int64(165000000000)}},
			},
		},

		// INTERVAL DAY TO SECOND - SECOND
		testQueryResults{
			query: "select NUMTODSINTERVAL(:1, 'SECOND') from dual",
			args: [][]interface{}{
				[]interface{}{-2},
				[]interface{}{-1},
				[]interface{}{1},
				[]interface{}{2},
				[]interface{}{1.25},
				[]interface{}{1.5},
				[]interface{}{2.75},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{int64(-2000000000)}},
				[][]interface{}{[]interface{}{int64(-1000000000)}},
				[][]interface{}{[]interface{}{int64(1000000000)}},
				[][]interface{}{[]interface{}{int64(2000000000)}},
				[][]interface{}{[]interface{}{int64(1250000000)}},
				[][]interface{}{[]interface{}{int64(1500000000)}},
				[][]interface{}{[]interface{}{int64(2750000000)}},
			},
		},
	}

	testRunQueryResults(t, queryResults)
}

// TestSelectGoTypesTime is select :1 from dual for time Go Type
func TestSelectGoTypesTime(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	// https://tour.golang.org/basics/11

	queryResults := []testQueryResults{

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
	}

	testRunQueryResults(t, queryResults)
}

// TestDestructiveTime checks insert, select, update, and delete of time types
func TestDestructiveTime(t *testing.T) {
	if TestDisableDatabase || TestDisableDestructive {
		t.SkipNow()
	}

	// https://ss64.com/ora/syntax-datatypes.html

	// TIMESTAMP(9)
	err := testExec(t, "create table TIMESTAMP_"+TestTimeString+
		" ( A int, B TIMESTAMP(9), C TIMESTAMP(9) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table TIMESTAMP_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into TIMESTAMP_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
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

	queryResults := []testQueryResults{
		testQueryResults{
			query: "select A, B, C from TIMESTAMP_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{int64(1), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)},
					[]interface{}{int64(2), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)},
					[]interface{}{int64(3), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)},
					[]interface{}{int64(4), time.Date(1, 1, 1, 0, 0, 0, 0, time.Local), time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)},
					[]interface{}{int64(5), time.Date(1, 1, 1, 0, 0, 0, 0, time.Local), time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)},
					[]interface{}{int64(6), time.Date(1, 1, 1, 0, 0, 0, 0, time.Local), time.Date(1, 1, 1, 0, 0, 0, 0, time.Local)},
					// TOFIX: testTimeLocNZ - ORA-08192: Flashback Table operation is not allowed on fixed tables
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from TIMESTAMP_"+TestTimeString+" where A = :1",
		[][]interface{}{
			[]interface{}{4},
			[]interface{}{5},
			[]interface{}{6},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from TIMESTAMP_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{int64(1), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)},
					[]interface{}{int64(2), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)},
					[]interface{}{int64(3), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.Local)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// TIMESTAMP(9) WITH TIME ZONE
	err = testExec(t, "create table TIMESTAMPWTZ_"+TestTimeString+
		" ( A int, B TIMESTAMP(9) WITH TIME ZONE, C TIMESTAMP(9) WITH TIME ZONE )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table TIMESTAMPWTZ_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into TIMESTAMPWTZ_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
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

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from TIMESTAMPWTZ_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{int64(1), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
					[]interface{}{int64(2), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
					[]interface{}{int64(3), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
					[]interface{}{int64(4), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)},
					[]interface{}{int64(5), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)},
					[]interface{}{int64(6), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)},
					// TOFIX: testTimeLocNZ - ORA-08192: Flashback Table operation is not allowed on fixed tables
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from TIMESTAMPWTZ_"+TestTimeString+" where A = :1",
		[][]interface{}{
			[]interface{}{4},
			[]interface{}{5},
			[]interface{}{6},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from TIMESTAMPWTZ_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{int64(1), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
					[]interface{}{int64(2), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
					[]interface{}{int64(3), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// TIMESTAMP(9) WITH LOCAL TIME ZONE
	err = testExec(t, "create table TIMESTAMPWLTZ_"+TestTimeString+
		" ( A int, B TIMESTAMP(9) WITH LOCAL TIME ZONE, C TIMESTAMP(9) WITH LOCAL TIME ZONE )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table TIMESTAMPWLTZ_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into TIMESTAMPWLTZ_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
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

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from TIMESTAMPWLTZ_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{int64(1), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
					[]interface{}{int64(2), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
					[]interface{}{int64(3), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
					[]interface{}{int64(4), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocUTC)},
					[]interface{}{int64(5), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocGMT), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocEST)},
					[]interface{}{int64(6), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST), time.Date(1, 1, 1, 0, 0, 0, 0, testTimeLocMST)},
					// TOFIX: testTimeLocNZ - ORA-08192: Flashback Table operation is not allowed on fixed tables
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from TIMESTAMPWLTZ_"+TestTimeString+" where A = :1",
		[][]interface{}{
			[]interface{}{4},
			[]interface{}{5},
			[]interface{}{6},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from TIMESTAMPWLTZ_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{int64(1), time.Date(2006, 1, 2, 3, 4, 5, 123456789, time.UTC), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocUTC)},
					[]interface{}{int64(2), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocGMT), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocEST)},
					[]interface{}{int64(3), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocMST), time.Date(2006, 1, 2, 3, 4, 5, 123456789, testTimeLocNZ)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// INTERVAL YEAR TO MONTH
	err = testExec(t, "create table INTERVALYTM_"+TestTimeString+
		" ( A int, B INTERVAL YEAR TO MONTH, C INTERVAL YEAR TO MONTH )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table INTERVALYTM_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into INTERVALYTM_"+TestTimeString+" ( A, B, C ) values (:1, NUMTOYMINTERVAL(:2, 'YEAR'), NUMTOYMINTERVAL(:3, 'MONTH'))",
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

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from INTERVALYTM_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
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

	err = testExecRows(t, "delete from INTERVALYTM_"+TestTimeString+" where A = :1",
		[][]interface{}{
			[]interface{}{5},
			[]interface{}{6},
			[]interface{}{7},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from INTERVALYTM_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
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
	err = testExec(t, "create table INTERVALDTS_"+TestTimeString+
		" ( A int, B INTERVAL DAY TO SECOND, C INTERVAL DAY TO SECOND )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table INTERVALDTS_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into INTERVALDTS_"+TestTimeString+" ( A, B, C ) values (:1, NUMTODSINTERVAL(:2, 'DAY'), NUMTODSINTERVAL(:3, 'HOUR'))",
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

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from INTERVALDTS_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
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

	err = testExecRows(t, "delete from INTERVALDTS_"+TestTimeString+" where A = :1",
		[][]interface{}{
			[]interface{}{5},
			[]interface{}{6},
			[]interface{}{7},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from INTERVALDTS_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{int64(1), int64(-172800000000000), int64(-7200000000000)},
					[]interface{}{int64(2), int64(-86400000000000), int64(-3600000000000)},
					[]interface{}{int64(3), int64(86400000000000), int64(3600000000000)},
					[]interface{}{int64(4), int64(172800000000000), int64(7200000000000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "truncate table INTERVALDTS_"+TestTimeString, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	err = testExecRows(t, "insert into INTERVALDTS_"+TestTimeString+" ( A, B, C ) values (:1, NUMTODSINTERVAL(:2, 'MINUTE'), NUMTODSINTERVAL(:3, 'SECOND'))",
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

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from INTERVALDTS_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
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

	err = testExecRows(t, "delete from INTERVALDTS_"+TestTimeString+" where A = :1",
		[][]interface{}{
			[]interface{}{5},
			[]interface{}{6},
			[]interface{}{7},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from INTERVALDTS_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
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
