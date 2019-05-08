package oci8

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"
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

func TestColumnTypeScanType(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	timeVar := time.Date(2015, 12, 31, 23, 59, 59, 123456789, time.UTC)
	intVar := int64(0)
	floatVar := float64(0)

	rows, err := TestDB.Query("select :0 as int64 ,:1 as float64 , :2 as time from dual",
		&intVar, &floatVar, &timeVar)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ct, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range ct {
		switch c.Name() {
		case "INT64":
			if c.ScanType() != reflect.TypeOf(intVar) {
				t.Fatalf("scan type error, expect %v, get %v", reflect.TypeOf(intVar), c.ScanType())
			}
		case "TIME":
			if c.ScanType() != reflect.TypeOf(timeVar) {
				t.Fatalf("scan type error, expect %v, get %v", reflect.TypeOf(timeVar), c.ScanType())
			}
		case "FLOAT64":
			if c.ScanType() != reflect.TypeOf(floatVar) {
				t.Fatalf("scan type error, expect %v, get %v", reflect.TypeOf(floatVar), c.ScanType())
			}
		}
	}
}
