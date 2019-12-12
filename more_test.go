package oci8

import (
	"database/sql"
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
