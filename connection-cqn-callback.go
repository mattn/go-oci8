package oci8

// #include "oci8.go.h"
import "C"
import (
	"fmt"
	"unsafe"
)

//export goCqnCallback
func goCqnCallback(ctx unsafe.Pointer, subHandle *C.OCISubscription, payload unsafe.Pointer, payl *C.ub4, descriptor unsafe.Pointer, mode C.ub4) {
	fmt.Println("callback executed yay!")

}
