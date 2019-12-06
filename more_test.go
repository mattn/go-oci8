package oci8

import (
	"database/sql"
	"fmt"
	"testing"
)

type dbc interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

func sqlstest(d dbc, t *testing.T, sql string, p ...interface{}) map[string]interface{} {
	rows, err := NewS(d.Query(sql, p...))
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		rows.Close()
		t.Fatal("no row returned:", rows.Err())
	}
	err = rows.Scan()
	if err != nil {
		rows.Close()
		t.Fatal(err)
	}
	res := rows.Map()
	//res := rows.Row()
	err = rows.Close()
	if err != nil {
		rows.Close()
		t.Fatal(err)
	}
	return res
}

func TestQuestionMark(t *testing.T) {
	// skip for now
	t.SkipNow()
	a, b := 4, 5
	c := "zz"
	r := sqlstest(TestDB, t, "select ? as v1, ? as v2, ? as v3 from dual", a, b, c)
	if fmt.Sprintf("%v", r["V1"]) != fmt.Sprintf("%v", a) {
		t.Fatal(r["V1"], "!=", a)
	}
	if fmt.Sprintf("%v", r["V2"]) != fmt.Sprintf("%v", b) {
		t.Fatal(r["V2"], "!=", b)
	}
	if fmt.Sprintf("%v", r["V3"]) != fmt.Sprintf("%v", c) {
		t.Fatal(r["V3"], "!=", c)
	}
}
