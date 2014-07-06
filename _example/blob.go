package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-oci8"
	"os"
)

func main() {
	os.Setenv("NLS_LANG", "")

	db, err := sql.Open("oci8", "scott/tiger@XE")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	db.Exec("drop table blob_example")

	_, err = db.Exec("create table blob_example(id varchar2(256) not null primary key, data blob)")
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = db.Exec("insert into blob_example(id, data) values(:1, :2)", "001", []byte("はろ"))
	if err != nil {
		fmt.Println(err)
		return
	}

	/*
		_, err = db.Exec("update blob_example set data = :2 where id = :1", "001", []byte("こんにちわ世界"))
		if err != nil {
			fmt.Println(err)
			return
		}
	*/

	rows, err := db.Query("select * from blob_example")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var data []byte
		rows.Scan(&id, &data)
		fmt.Println(id, string(data))
	}
}
