package oci8


// #include "oci8.go.h"
import "C"

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"unsafe"
)

//export goCqnCallback
func goCqnCallback(ctx unsafe.Pointer, subHandle *C.OCISubscription, payload unsafe.Pointer, payl *C.ub4, descriptor unsafe.Pointer, mode C.ub4) {
	fmt.Println("callback started bad-ass...")
	// openString := "richard/richard@//192.168.56.101:1521/ORCL?prefetch_rows=500"
	// driver := &oci8.OCI8DriverStruct{
	// 	Logger: log.New(ioutil.Discard, "", 0),
	// }
	// conn, err := driver.OpenOCI8Conn(openString)
	// if err != nil {
	// 	log.Fatal("nil conn")
	// }

	var err error

	conn := OCI8Conn{
		// operationMode: dsn.operationMode,
		// logger:        oci8Driver.Logger,
	}
	if conn.logger == nil {
		conn.logger = log.New(ioutil.Discard, "", 0)
	}

	// Environment handle.
	var envP *C.OCIEnv
	envPP := &envP
	var result C.sword
	charset := C.ub2(0)
	if os.Getenv("NLS_LANG") == "" && os.Getenv("NLS_NCHAR") == "" {
		charset = defaultCharset
	}
	envCreateFlags := C.OCI_EVENTS | C.OCI_OBJECT // required for ContinuousQueryNotification.
	result = C.OCIEnvNlsCreate(
		envPP,                 // pointer to a handle to the environment
		C.ub4(envCreateFlags), // environment mode: https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci16rel001.htm#LNOCI87683
		nil,                   // Specifies the user-defined context for the memory callback routines.
		nil,                   // Specifies the user-defined memory allocation function. If mode is OCI_THREADED, this memory allocation routine must be thread-safe.
		nil,                   // Specifies the user-defined memory re-allocation function. If the mode is OCI_THREADED, this memory allocation routine must be thread safe.
		nil,                   // Specifies the user-defined memory free function. If mode is OCI_THREADED, this memory free routine must be thread-safe.
		0,                     // Specifies the amount of user memory to be allocated for the duration of the environment.
		nil,                   // Returns a pointer to the user memory of size xtramemsz allocated by the call for the user.
		charset,               // The client-side character set for the current environment handle. If it is 0, the NLS_LANG setting is used.
		charset,               // The client-side national character set for the current environment handle. If it is 0, NLS_NCHAR setting is used.
	)
	if result != C.OCI_SUCCESS {
		// return nil, errors.New("OCIEnvNlsCreate error")
		panic("OCIEnvNlsCreate error")
	}
	conn.env = *envPP

	// Defer cleanup if any error occurs.
	// defer func(errP *error) {
	// 	if *errP != nil {
	// 		conn.freeHandles()
	// 	}
	// }(&err) // pass the address of err so this is the last error assigned to err.

	// Error handle.
	var handleTemp unsafe.Pointer
	handle := &handleTemp
	result = C.OCIHandleAlloc(
		unsafe.Pointer(conn.env), // An environment handle
		handle,                   // Returns a handle
		C.OCI_HTYPE_ERROR,        // type of handle: https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci02bas.htm#LNOCI87581
		0,                        // amount of user memory to be allocated
		nil,                      // Returns a pointer to the user memory
	)
	if result != C.OCI_SUCCESS {
		// TODO: error handle not yet allocated, how to get string error from oracle?
		// return nil, errors.New("allocate error handle error")
		panic("allocate error handle error")
	}
	conn.errHandle = (*C.OCIError)(*handle)
	handle = nil // deallocate.

	// TODO: next...
	// extract reg ID from subscription.
	// get a connection
	// try handling change descriptors without preparing a tmp statement as demo code doesn't use the statement
	// implement processTableChanges() as a starter!

	// Get the registration ID.
	var regId C.ub8
	regIdSize := C.ub4(C.sizeof_ub8)
	result = C.OCIAttrGet(
		unsafe.Pointer(subHandle),  // unsafe.Pointer(stmt.stmt), // Pointer to a handle type
		C.OCI_HTYPE_SUBSCRIPTION,   // C.OCI_HTYPE_STMT,          // The handle type: OCI_DTYPE_PARAM, for a parameter descriptor
		unsafe.Pointer(&regId),     // Pointer to the storage for an attribute value
		&regIdSize,                 // The size of the attribute value.  // TODO: use sizeof()
		C.OCI_ATTR_SUBSCR_CQ_REGID, // C.OCI_ATTR_CQ_QUERYID <<< returns 0 for what I think is the first query since multiples can be registered in one subscroption. // The attribute type: https://docs.oracle.com/cd/B19306_01/appdev.102/b14250/ociaahan.htm
		conn.errHandle,             // An error handle
	)
	err = conn.getError(result)
	if err != nil {
		panic("error fetching CQN registration ID")
	} else {
		log.Println("callback fetched registration ID =", int64(regId))
	}
}
