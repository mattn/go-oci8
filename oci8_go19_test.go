// +build go1.9

package oci8

import (
	"database/sql"
	"testing"
)

func TestOutputBind(t *testing.T) {
	db := DB()

	s1 := "-----------------------------"
	s2 := 11
	s3 := false
	s4 := uint64(12)
	_, err := db.Exec(`begin  :a := 42; :b := 'ddddd' ; :c := 2; :d := 18446744073709551615; end;`,
		sql.Named("a", sql.Out{Dest: &s2}),
		sql.Named("b", sql.Out{Dest: &s1}),
		sql.Named("c", sql.Out{Dest: &s3}),
		sql.Named("d", sql.Out{Dest: &s4}))
	if err != nil {
		t.Fatal(err)
	}
	s1want := "ddddd                        "
	if s1 != s1want {
		t.Fatalf("want %q but %q", s1want, s1)
	}
	if s2 != 42 {
		t.Fatalf("want %v but %v", 42, s2)
	}
	if !s3 {
		t.Fatalf("want %v but %v", true, s3)
	}
	if s4 != uint64(18446744073709551615) {
		t.Fatalf("want %v but %v", uint64(18446744073709551615), s4)
	}
}
