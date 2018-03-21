package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/mattn/go-oci8"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("ERROR: Please provide a DSN string in ONE argument:\n\n")
		fmt.Println("Shell-Conversion into DSN string:")
		fmt.Println("  sqlplus sys/password@tnsentry as sysdba   =>   sys/password@tnsentry?as=sysdba")
		fmt.Println("  sqlplus / as sysdba                       =>   sys/.@?as=sysdba")
		fmt.Println("instead of the tnsentry, you can also use the hostname of the IP.")
		os.Exit(1)
	}
	os.Setenv("NLS_LANG", "")

	db, err := sql.Open("oci8", os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	fmt.Println()
	var user string
	err = db.QueryRow("select user from dual").Scan(&user)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Successful 'as sysdba' connection. Current user is: %v\n", user)
}
