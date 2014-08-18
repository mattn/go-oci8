package oci8

import (
	"database/sql"
	"os"
	"reflect"
	"testing"
	"time"
)

const (
	getSidQuery = "SELECT sid || ',' || serial# FROM v$session WHERE audsid=sys_context('USERENV','SESSIONID')"
)

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

func openTestConn(t *testing.T) *sql.DB {
	dsn := os.Getenv("ORA_DSN")
	db, err := sql.Open("oci8", dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

type ErrCatcher struct {
	t *testing.T
}

func (e *ErrCatcher) B(tx *sql.Tx, err error) *sql.Tx {
	if err != nil {
		e.t.Fatal(err)
	}
	return tx
}

func (e *ErrCatcher) NE(err error) {
	if err != nil {
		e.t.Fatal(err)
	}
}

func (e *ErrCatcher) Q(rows *sql.Rows, err error) *sql.Rows {
	if err != nil {
		e.t.Fatal(err)
	}
	return rows
}

func (e *ErrCatcher) EX(result sql.Result, err error) sql.Result {
	if err != nil {
		e.t.Fatal(err)
	}
	return result
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

	if !reflect.DeepEqual(realVal, expectedVal) {
		t.Fatal("%s got '%v' but expected '%v'", context, realVal, expectedVal)
	}
}

type FooTable struct {
	InitVal  string
	InitVals map[int]string
	GetQ     string
	UpdQ     string
}

func makeFooTable(t *testing.T, db *sql.DB) *FooTable {
	const (
		testTable  = "FOO"
		createStmt = "CREATE TABLE FOO (k number(1), v varchar2(20))"
		insertStmt = "INSERT INTO FOO (k, v) VALUES (:1, :2)"
		initBarVal = "baz"
	)
	foo := FooTable{
		InitVal: initBarVal,
		InitVals: map[int]string{
			1: initBarVal,
			2: initBarVal,
		},
		GetQ: "SELECT k, v FROM FOO",
		UpdQ: "UPDATE FOO SET v=:1 WHERE k=:2",
	}
	var tabExists int
	catch := ErrCatcher{t}

	catch.NE(db.QueryRow(
		"SELECT count(1) FROM user_tables WHERE table_name=:1",
		testTable).Scan(&tabExists))
	if tabExists != 0 {
		db.Exec("DROP TABLE " + testTable)
	}
	catch.EX(db.Exec(createStmt))
	for k, v := range foo.InitVals {
		catch.EX(db.Exec(insertStmt, k, v))
	}

	expectVal("init", t, catch.Q(db.Query(foo.GetQ)), foo.InitVals)
	return &foo
}

func TestTx(t *testing.T) {
	const (
		tx1Val = "tx1"
		tx2Val = "tx2"
	)
	db := openTestConn(t)
	defer db.Close()
	catch := ErrCatcher{t}
	foo := makeFooTable(t, db)

	tx1 := catch.B(db.Begin())
	catch.EX(tx1.Exec(foo.UpdQ, tx1Val, 1))
	expectVal("tx1 update-1", t, catch.Q(tx1.Query(foo.GetQ)), map[int]string{
		1: tx1Val,
		2: foo.InitVal,
	})
	// don't see uncommited changes
	expectVal("after tx1 update k=1", t, catch.Q(db.Query(foo.GetQ)), foo.InitVals)

	// start second transaction
	tx2 := catch.B(db.Begin())
	expectVal("tx2 init", t, catch.Q(db.Query(foo.GetQ)), foo.InitVals)
	catch.EX(tx2.Exec(foo.UpdQ, tx2Val, 2))
	expectVal("tx2 update k=2", t, catch.Q(tx2.Query(foo.GetQ)), map[int]string{
		1: foo.InitVal,
		2: tx2Val,
	})
	expectVal("base after tx2 update", t, catch.Q(db.Query(foo.GetQ)), foo.InitVals)

	// commit first transaction
	catch.NE(tx1.Commit())
	// rollback second
	catch.NE(tx2.Rollback())
	expectVal("tx1 commit, tx2 rollback", t, catch.Q(db.Query(foo.GetQ)), map[int]string{
		1: tx1Val,
		2: foo.InitVal,
	})
}

func TestTxInOneSession(t *testing.T) {
	var (
		firstSid  string
		secondSid string
	)
	db := openTestConn(t)
	defer db.Close()
	foo := makeFooTable(t, db)
	catch := ErrCatcher{t}
	tx1 := catch.B(db.Begin())
	catch.EX(tx1.Exec(foo.UpdQ, 1, "42"))
	catch.NE(tx1.QueryRow(getSidQuery).Scan(&firstSid))
	catch.NE(tx1.Commit())

	tx2 := catch.B(db.Begin())
	catch.EX(tx2.Exec(foo.UpdQ, 2, "100500"))
	catch.NE(tx2.QueryRow(getSidQuery).Scan(&secondSid))

	if firstSid != secondSid {
		t.Fatalf("Got different SIDs first=%s second=%s", firstSid, secondSid)
	}
}

func TestReconnect(t *testing.T) {
	var (
		sid       string
		rebornSid string
	)
	catch := ErrCatcher{t}
	db1 := openTestConn(t)
	defer db1.Close()

	tx := catch.B(db1.Begin())
	catch.NE(tx.QueryRow(getSidQuery).Scan(&sid))

	db2 := openTestConn(t)
	defer db2.Close()

	catch.EX(db2.Exec("ALTER SYSTEM KILL SESSION '" + sid + "' IMMEDIATE"))

	err := tx.Rollback()
	if err == nil {
		t.Fatalf("kill session but Rollback not fail!")
	}

	catch.NE(db1.QueryRow(getSidQuery).Scan(&rebornSid))
	if sid == rebornSid {
		t.Fatalf("expected different sid, sid=%s rebornSid=%s", sid, rebornSid)
	}
}
