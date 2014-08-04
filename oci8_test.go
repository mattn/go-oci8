package oci8

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

type Fatalistic interface {
	Fatal(args ...interface{})
}

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
		{"xxmc/xxmc@107.20.30.169:1521/ORCL?loc=America%2FLos_Angeles", &DSN{Username: "xxmc", Password: "xxmc", Host: "107.20.30.169", Port: 1521, SID: "ORCL", Location: pacific}},
		{"xxmc/xxmc@107.20.30.169:1521/ORCL", &DSN{Username: "xxmc", Password: "xxmc", Host: "107.20.30.169", Port: 1521, SID: "ORCL", Location: time.Local}},
	}

	for _, tt := range dsnTests {
		actualDSN, err := ParseDSN(tt.dsnString)

		if err != nil {
			t.Errorf("ParseDSN(%) got error: %+v", tt.dsnString, err)
		}

		if !reflect.DeepEqual(actualDSN, tt.expectedDSN) {
			t.Errorf("ParseDSN(%s): expected %+v, actual %+v", tt.dsnString, tt.expectedDSN, actualDSN)
		}
	}
}

func openTestConn(t Fatalistic) *sql.DB {
	dsn := os.Getenv("ORA_DSN")
	db, err := sql.Open("oci8", dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func catchQE(rows *sql.Rows, err error) *sql.Rows {
	if err != nil {
		panic(err)
	}
	return rows
}

func expectVal(context string, t *testing.T, rows *sql.Rows, expectedVal map[int]string) {
	realVal := make(map[int]string, 2)
	var (
		k int
		v string
	)
	for rows.Next() {
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		realVal[k] = v
	}
	rows.Close()

	fmt.Printf("%s:\n    real=%#v expected=%#v\n", context, realVal, expectedVal)
	if !reflect.DeepEqual(realVal, expectedVal) {
		t.Fatalf("%s got '%v' but expected '%v'", context, realVal, expectedVal)
	}
}

func TestTx(t *testing.T) {
	const (
		testTable  = "FOO"
		updQ       = "UPDATE foo SET v=:1 WHERE k=:2"
		getQ       = "SELECT k, v FROM foo"
		initBarVal = "baz"
		tx1Val     = "tx1"
		tx2Val     = "tx2"
	)

	db := openTestConn(t)
	defer db.Close()

	var tabExists int
	row := db.QueryRow("SELECT count(1) FROM user_tables WHERE table_name=:1", testTable)
	row.Scan(&tabExists)
	if tabExists != 0 {
		db.Exec("DROP TABLE " + testTable)
	}
	db.Exec("CREATE TABLE " + testTable + " (k number(1), v varchar2(20))")
	db.Exec("INSERT INTO "+testTable+" (k, v) VALUES (:1, :2)", 1, initBarVal)
	db.Exec("INSERT INTO "+testTable+" (k, v) VALUES (:1, :2)", 2, initBarVal)

	expectVal("init", t, catchQE(db.Query(getQ)), map[int]string{1: initBarVal, 2: initBarVal})

	tx1, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx1.Exec(updQ, tx1Val, 1); err != nil {
		t.Fatal(err)
	}
	expectVal("tx1 update-1", t, catchQE(tx1.Query(getQ)), map[int]string{1: tx1Val, 2: initBarVal})

	// don't see uncommited changes
	expectVal("after tx1 update k=1", t, catchQE(db.Query(getQ)), map[int]string{1: initBarVal, 2: initBarVal})

	// start second transaction
	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	expectVal("tx2 init", t, catchQE(db.Query(getQ)), map[int]string{1: initBarVal, 2: initBarVal})
	if _, err := tx2.Exec(updQ, tx2Val, 2); err != nil {
		t.Fatal(err)
	}
	expectVal("tx2 update k=2", t, catchQE(tx2.Query(getQ)), map[int]string{1: initBarVal, 2: tx2Val})

	expectVal("base after tx2 update", t, catchQE(db.Query(getQ)), map[int]string{1: initBarVal, 2: initBarVal})

	// commit first transaction
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	// rollback second

	if err := tx2.Rollback(); err != nil {
		t.Fatal(err)
	}

	expectVal("tx1 commit, tx2 rollback", t, catchQE(db.Query(getQ)), map[int]string{1: tx1Val, 2: initBarVal})
}
