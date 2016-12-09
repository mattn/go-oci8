// +build go1.8

package oci8

import (
	"database/sql"
	"testing"
)

func TestNamedParam(t *testing.T) {
	r := sqlstest(DB(), t, "select :foo||:bar as message from dual", sql.Named("foo", "hello"), sql.Named("bar", "world"))
	if "helloworld" != r["MESSAGE"].(string) {
		t.Fatal("message should be: helloworld", r)
	}
}
