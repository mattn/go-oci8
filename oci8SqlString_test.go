package oci8

import (
	"context"
	"strings"
	"testing"
)

// TestSelectCastString checks cast x from dual for string SQL types
func TestSelectCastString(t *testing.T) {
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

		// CLOB
		testQueryResults{
			query: "select TO_CLOB(:1) from dual",
			args: [][]interface{}{
				[]interface{}{""},
				[]interface{}{"a"},
				[]interface{}{"abc    "},
				[]interface{}{strings.Repeat("a", 100)},
				[]interface{}{strings.Repeat("a", 500)},
				[]interface{}{strings.Repeat("a", 1000)},
				[]interface{}{strings.Repeat("a", 2000)},
				[]interface{}{strings.Repeat("a", 4000)},
				[]interface{}{testString1},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a"}},
				[][]interface{}{[]interface{}{"abc    "}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 100)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 500)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 1000)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 2000)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 4000)}},
				[][]interface{}{[]interface{}{testString1}},
			},
		},

		// NCLOB
		testQueryResults{
			query: "select TO_NCLOB(:1) from dual",
			args: [][]interface{}{
				[]interface{}{""},
				[]interface{}{"a"},
				[]interface{}{"abc    "},
				[]interface{}{strings.Repeat("a", 100)},
				[]interface{}{strings.Repeat("a", 500)},
				[]interface{}{strings.Repeat("a", 1000)},
				[]interface{}{strings.Repeat("a", 2000)},
				[]interface{}{strings.Repeat("a", 4000)},
				[]interface{}{testString1},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{nil}},
				[][]interface{}{[]interface{}{"a"}},
				[][]interface{}{[]interface{}{"abc    "}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 100)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 500)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 1000)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 2000)}},
				[][]interface{}{[]interface{}{strings.Repeat("a", 4000)}},
				[][]interface{}{[]interface{}{testString1}},
			},
		},

		// BLOB
		testQueryResults{
			query: "select TO_BLOB(:1) from dual",
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

	// ROWID
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, "select ROWID from dual")
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	var result [][]interface{}
	result, err = testGetRows(t, stmt, nil)
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
	data, ok := result[0][0].(string)
	if !ok {
		t.Fatal("result not string")
	}
	if len(data) != 18 {
		t.Fatal("result len not equal to 18:", len(data))
	}
}

// TestSelectGoTypesString is select :1 from dual for string Go Type
func TestSelectGoTypesString(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	// https://tour.golang.org/basics/11

	queryResults := []testQueryResults{
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

		// byte
		testQueryResults{
			query: "select :1 from dual",
			args: [][]interface{}{
				[]interface{}{byte('a')},
				[]interface{}{byte('z')},
			},
			results: [][][]interface{}{
				[][]interface{}{[]interface{}{float64(97)}},
				[][]interface{}{[]interface{}{float64(122)}},
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
				[][]interface{}{[]interface{}{float64(97)}},
				[][]interface{}{[]interface{}{float64(122)}},
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

// TestDestructiveString checks insert, select, update, and delete of string types
func TestDestructiveString(t *testing.T) {
	if TestDisableDatabase || TestDisableDestructive {
		t.SkipNow()
	}

	// https://ss64.com/ora/syntax-datatypes.html

	// VARCHAR2
	err := testExec(t, "create table VARCHAR2_"+TestTimeString+
		" ( A VARCHAR2(1), B VARCHAR2(2000), C VARCHAR2(4000) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table VARCHAR2_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into VARCHAR2_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
			[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults := []testQueryResults{
		testQueryResults{
			query: "select A, B, C from VARCHAR2_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
					[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from VARCHAR2_"+TestTimeString+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from VARCHAR2_" + TestTimeString,
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// NVARCHAR2
	err = testExec(t, "create table NVARCHAR2_"+TestTimeString+
		" ( A NVARCHAR2(1), B NVARCHAR2(1000), C NVARCHAR2(2000) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table NVARCHAR2_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into NVARCHAR2_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 1000), strings.Repeat("a", 2000)},
			[]interface{}{"b", strings.Repeat("b", 1000), strings.Repeat("b", 2000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from NVARCHAR2_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 1000), strings.Repeat("a", 2000)},
					[]interface{}{"b", strings.Repeat("b", 1000), strings.Repeat("b", 2000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from NVARCHAR2_"+TestTimeString+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from NVARCHAR2_" + TestTimeString,
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 1000), strings.Repeat("b", 2000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// CHAR
	err = testExec(t, "create table CHAR_"+TestTimeString+
		" ( A CHAR(1), B CHAR(1000), C CHAR(2000) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table CHAR_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into CHAR_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 1000), strings.Repeat("a", 2000)},
			[]interface{}{"b", strings.Repeat("b", 1000), strings.Repeat("b", 2000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from CHAR_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 1000), strings.Repeat("a", 2000)},
					[]interface{}{"b", strings.Repeat("b", 1000), strings.Repeat("b", 2000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from CHAR_"+TestTimeString+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from CHAR_" + TestTimeString,
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 1000), strings.Repeat("b", 2000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from CHAR_"+TestTimeString+" where A = :1", []interface{}{"b"})
	if err != nil {
		t.Error("delete error:", err)
	}

	err = testExecRows(t, "insert into CHAR_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 100), strings.Repeat("a", 200)},
			[]interface{}{"b", strings.Repeat("b", 100), strings.Repeat("b", 200)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from CHAR_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 100) + strings.Repeat(" ", 900), strings.Repeat("a", 200) + strings.Repeat(" ", 1800)},
					[]interface{}{"b", strings.Repeat("b", 100) + strings.Repeat(" ", 900), strings.Repeat("b", 200) + strings.Repeat(" ", 1800)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from CHAR_"+TestTimeString+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from CHAR_" + TestTimeString,
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 100) + strings.Repeat(" ", 900), strings.Repeat("b", 200) + strings.Repeat(" ", 1800)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// NCHAR
	err = testExec(t, "create table NCHAR_"+TestTimeString+
		" ( A NCHAR(1), B NCHAR(500), C NCHAR(1000) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table NCHAR_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into NCHAR_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 500), strings.Repeat("a", 1000)},
			[]interface{}{"b", strings.Repeat("b", 500), strings.Repeat("b", 1000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from NCHAR_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 500), strings.Repeat("a", 1000)},
					[]interface{}{"b", strings.Repeat("b", 500), strings.Repeat("b", 1000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from NCHAR_"+TestTimeString+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from NCHAR_" + TestTimeString,
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 500), strings.Repeat("b", 1000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from NCHAR_"+TestTimeString+" where A = :1", []interface{}{"b"})
	if err != nil {
		t.Error("delete error:", err)
	}

	err = testExecRows(t, "insert into NCHAR_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 100), strings.Repeat("a", 200)},
			[]interface{}{"b", strings.Repeat("b", 100), strings.Repeat("b", 200)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from NCHAR_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 100) + strings.Repeat(" ", 400), strings.Repeat("a", 200) + strings.Repeat(" ", 800)},
					[]interface{}{"b", strings.Repeat("b", 100) + strings.Repeat(" ", 400), strings.Repeat("b", 200) + strings.Repeat(" ", 800)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from NCHAR_"+TestTimeString+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from NCHAR_" + TestTimeString,
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 100) + strings.Repeat(" ", 400), strings.Repeat("b", 200) + strings.Repeat(" ", 800)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// RAW
	err = testExec(t, "create table RAW_"+TestTimeString+
		" ( A RAW(1), B RAW(1000), C RAW(2000) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table RAW_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into RAW_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{[]byte{}, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
			[]interface{}{[]byte{10}, []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}, testByteSlice1},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from RAW_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{[]byte{10}, []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}, testByteSlice1},
					[]interface{}{nil, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from RAW_"+TestTimeString+" where A = :1", []interface{}{[]byte{10}})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from RAW_" + TestTimeString,
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{nil, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// ROWID
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, "select ROWID from RAW_"+TestTimeString)
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	var result [][]interface{}
	result, err = testGetRows(t, stmt, nil)
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
	data, ok := result[0][0].(string)
	if !ok {
		t.Fatal("result not string")
	}
	if len(data) != 18 {
		t.Fatal("result len not equal to 18:", len(data))
	}

	// CLOB
	err = testExec(t, "create table CLOB_"+TestTimeString+
		" ( A VARCHAR2(100), B CLOB, C CLOB )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table CLOB_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into CLOB_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
			[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from CLOB_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
					[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from CLOB_"+TestTimeString+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from CLOB_" + TestTimeString,
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// NCLOB
	err = testExec(t, "create table NCLOB_"+TestTimeString+
		" ( A VARCHAR2(100), B NCLOB, C NCLOB )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table NCLOB_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into NCLOB_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
			[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from NCLOB_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
					[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from NCLOB_"+TestTimeString+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from NCLOB_" + TestTimeString,
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// BLOB
	err = testExec(t, "create table BLOB_"+TestTimeString+
		" ( A VARCHAR2(100), B BLOB, C BLOB )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer func() {
		err = testExec(t, "drop table BLOB_"+TestTimeString, nil)
		if err != nil {
			t.Error("drop table error:", err)
		}
	}()

	err = testExecRows(t, "insert into BLOB_"+TestTimeString+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", []byte{}, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			[]interface{}{"b", []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from BLOB_" + TestTimeString + " order by A",
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"a", nil, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
					[]interface{}{"b", []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from BLOB_"+TestTimeString+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = []testQueryResults{
		testQueryResults{
			query: "select A, B, C from BLOB_" + TestTimeString,
			args:  [][]interface{}{[]interface{}{}},
			results: [][][]interface{}{
				[][]interface{}{
					[]interface{}{"b", []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

}
