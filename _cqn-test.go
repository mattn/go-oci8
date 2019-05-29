package main

import "C"
import (
	"github.com/relloyd/go-oci8"
	"io/ioutil"
	"log"
	"time"
)



func main() {
	openString := "richard/richard@//192.168.56.101:1521/ORCL?prefetch_rows=500"
	driver := &oci8.OCI8DriverStruct{
		Logger: log.New(ioutil.Discard, "", 0),
	}
	db, err := driver.OpenOCI8Conn(openString)
	if err != nil {
		log.Fatal("nil db")
	}
	id, err := db.NewCqnSubscription("select a, b from t1", nil)
	if err != nil {
		log.Fatalf("unable to register query: %v", err)
	}
	log.Println("registered query =", id)
	for {
		time.Sleep(2 * time.Second)
		log.Println("waiting for notifications")
	}
}
