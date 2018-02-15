package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-oci8"
)

func main() {
	db, err := sql.Open("oci8", "scott/tiger@XE")
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
