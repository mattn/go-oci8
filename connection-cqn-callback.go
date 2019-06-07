package oci8

// #include "oci8.go.h"
// #include <string.h>
// #include <stdio.h>
// #include <stdlib.h>
// sword getTableDescriptor(OCIEnv* envhp, OCIError* errhp, OCIColl* table_changes, sb4 i, dvoid** tableDescP);
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

	// Get the notification type.
	var notificationType C.ub4
	result = C.OCIAttrGet(descriptor, C.OCI_DTYPE_CHDES, unsafe.Pointer(&notificationType), nil, C.OCI_ATTR_CHDES_NFYTYPE, conn.errHandle)
	err = conn.getError(result)
	if err != nil {
		panic("error fetching CQN notification type")
	} else {
		fmt.Println("notification type =", notificationType)
	}

	// Process changes based on notification type.
	var tableChangesPtr *C.OCIColl
	var queryChangesPtr *C.OCIColl
	if notificationType == C.OCI_EVENT_SHUTDOWN || notificationType == C.OCI_EVENT_SHUTDOWN_ANY {
		fmt.Println("SHUTDOWN NOTIFICATION RECEIVED\n");
		// notifications_processed++;
		return
	} else if notificationType == C.OCI_EVENT_STARTUP {
		fmt.Println("STARTUP NOTIFICATION RECEIVED\n");
		// notifications_processed++;
		return
	} else if notificationType == C.OCI_EVENT_OBJCHANGE { // else if we registered subscription of type OCI_SUBSCR_CQ_QOS_BEST_EFFORT...
		// Supply address of pointer tableChangesPtr *C.OCIColl to OCIAttrGet.
		// This isn't exactly clear from the documentation:
		// void* is the documented type, but (void*)(&*C.OCIColl) seems to work!
		result = C.OCIAttrGet(descriptor, C.OCI_DTYPE_CHDES, unsafe.Pointer(&tableChangesPtr), nil, C.OCI_ATTR_CHDES_TABLE_CHANGES, conn.errHandle)
		err = conn.getError(result)
		if err != nil {
			panic("error fetching CQN table changes")
		} else {
			fmt.Println("processing table changes...")
			extractTableChanges(&conn, tableChangesPtr)
		}
	} else if notificationType == C.OCI_EVENT_QUERYCHANGE { // else if we registered subscription of type OCI_SUBSCR_CQ_QOS_QUERY...
		result = C.OCIAttrGet(descriptor, C.OCI_DTYPE_CHDES, unsafe.Pointer(&queryChangesPtr), nil, C.OCI_ATTR_CHDES_QUERIES, conn.errHandle)
		err = conn.getError(result)
		if err != nil {
			panic("error fetching CQN query changes")
		} else {
			fmt.Println("processing query changes")
			// processQueryChanges(envhp, errhp, stmthp, queryChanges)
		}
	}

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
		panic("error fetching CQN registration ID in callback")
	} else {
		log.Println("callback fetched registration ID =", int64(regId))
	}
}

// extractTableChanges will extract the table changes.
// It expects conn.env and conn.errHandle to be setup in advance.
func extractTableChanges(conn *OCI8Conn, tableChanges *C.OCIColl) {
	var err error
	var result C.sword
	var numTables C.sb4
	//var exist C.boolean
	var tableDescriptionUP unsafe.Pointer
	// tableDescriptionPP := &tableDescriptionUP
	//var elemIndP unsafe.Pointer
	var tableName *C.oratext
	var tableOp C.ub4
	var rowChanges *C.OCIColl
	var idx C.sb4

	result = C.OCICollSize(conn.env, conn.errHandle, tableChanges, &numTables)
	err = conn.getError(result)
	if err != nil {
		panic("error processing CQN table changes")
	}
	fmt.Println("number of table changes is", numTables)

	for idx = 0; idx < numTables; idx++ {
		// checker(errhp, OCICollGetElem(envhp, errhp, (OCIColl *)table_changes, i, &exist, &table_descp, &elemind));
		// result = C.OCICollGetElem(conn.env, conn.errHandle, tableChanges, idx, &exist, &tableDescriptionUP, &elemIndP)
		// if err = conn.getError(result); err != nil {
		// 	panic(fmt.Sprintf("error fetching element: %v", err))
		// }
		result = C.getTableDescriptor(conn.env, conn.errHandle, tableChanges, idx, &tableDescriptionUP)
		// table_desc = *table_descp;
		// var tableDescriptionP unsafe.Pointer
		// tableDescriptionP = *tableDescriptionPP

		// checker(errhp, OCIAttrGet(table_desc,OCI_DTYPE_TABLE_CHDES, (dvoid*)&table_name,NULL, OCI_ATTR_CHDES_TABLE_NAME, errhp));
		result = C.OCIAttrGet(tableDescriptionUP, C.OCI_DTYPE_TABLE_CHDES, unsafe.Pointer(&tableName), nil, C.OCI_ATTR_CHDES_TABLE_NAME, conn.errHandle)
		if err = conn.getError(result); err != nil {
			panic("error fetching table name from element")
		}

		// C.printf("table name \n")
		// l := C.strlen(*C.char(tableName))
		// p := (*[1 << 30]byte)(unsafe.Pointer(tableName))
		// idx := 0
		// for p[idx] != 0 {
		// 	idx++
		// 	fmt.Println("searching for len tableName idx=", idx)
		// }
		// fmt.Println("table name", cGoStringN(tableName, int(l)))  // TODO: find the length for real

		// checker(errhp, OCIAttrGet(table_desc, OCI_DTYPE_TABLE_CHDES, (dvoid*)&table_op, NULL, OCI_ATTR_CHDES_TABLE_OPFLAGS, errhp));
		result = C.OCIAttrGet(tableDescriptionUP, C.OCI_DTYPE_TABLE_CHDES, unsafe.Pointer(&tableOp), nil, C.OCI_ATTR_CHDES_TABLE_OPFLAGS, conn.errHandle)
		if err = conn.getError(result); err != nil {
			panic("error fetching table operation from element")
		}

		// checker(errhp, OCIAttrGet(table_desc, OCI_DTYPE_TABLE_CHDES, (dvoid*)&row_changes, NULL, OCI_ATTR_CHDES_TABLE_ROW_CHANGES, errhp));
		result = C.OCIAttrGet(tableDescriptionUP, C.OCI_DTYPE_TABLE_CHDES, unsafe.Pointer(&rowChanges), nil, C.OCI_ATTR_CHDES_TABLE_ROW_CHANGES, conn.errHandle)
		if err = conn.getError(result); err != nil {
			panic("error fetching row changes")
		}

		fmt.Println(fmt.Sprintf("Table changed is dunnoyet; table_op 0x%x", int32(tableOp)))

		// if !(tableOp & C.ub4(C.OCI_OPCODE_ALLROWS)) > 0 {
		// 	processRowChanges(envhp, errhp, stmthp, row_changes);
		// fmt.Println("process row changes...")
		// } else {
		// 	fmt.Println("table all rows changed")
		// }
	}
}

// CGoStringN coverts C OraText to Go string
func oratextGoStringN(s *C.oratext, size int) string {
	if size == 0 {
		return ""
	}
	p := (*[1 << 30]byte)(unsafe.Pointer(s))
	buf := make([]byte, size)
	copy(buf, p[:])
	return *(*string)(unsafe.Pointer(&buf))
}
