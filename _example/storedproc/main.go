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
	_, err = db.Exec(`
    CREATE OR REPLACE FUNCTION MY_SUM
    (
      P_NUM1 IN NUMBER,
      P_NUM2 IN NUMBER
    )
    RETURN NUMBER
    IS
      R_NUM NUMBER(2) DEFAULT 0;
    BEGIN
      FOR i IN 1..P_NUM2
      LOOP
        R_NUM := R_NUM + P_NUM1;
      END LOOP;
      RETURN R_NUM;
    END;
    `)
	if err != nil {
		log.Fatal(err)
	}
	rows, err := db.Query("select MY_SUM(5,6) from dual")
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var i int
		err = rows.Scan(&i)
		if err != nil {
			log.Fatal(err)
		}
		println(i)
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
