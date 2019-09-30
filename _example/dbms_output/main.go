package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-oci8"
)

func main() {
	db, err := sql.Open("oci8", getDSN())
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`BEGIN DBMS_OUTPUT.ENABLE(10000); END;`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`BEGIN DBMS_OUTPUT.PUT_LINE('hello'); END;`)
	if err != nil {
		log.Fatal(err)
	}

	var lines string
	var status int
	_, err = db.Exec(`BEGIN DBMS_OUTPUT.GET_LINE(:lines, :status); END;`,
		sql.Named("lines", sql.Out{Dest: &lines}),
		sql.Named("status", sql.Out{Dest: &status, In: true}))
	if err != nil {
		log.Fatal(err)
	}
	if status == 0 {
		fmt.Println(lines)
	}
}

func getDSN() string {
	var dsn string
	if len(os.Args) > 1 {
		dsn = os.Args[1]
		if dsn != "" {
			return dsn
		}
	}
	dsn = os.Getenv("GO_OCI8_CONNECT_STRING")
	if dsn != "" {
		return dsn
	}
	fmt.Fprintln(os.Stderr, `Please specifiy connection parameter in GO_OCI8_CONNECT_STRING environment variable,
or as the first argument! (The format is user/name@host:port/sid)`)
	return "scott/tiger@XE"
}
