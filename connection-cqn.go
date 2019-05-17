package oci8

/*
#include "oci8.go.h"
void myCallbackSimple(dvoid* ctx, OCISubscription* subscrhp, dvoid* payload, ub4* payl, dvoid* escriptor, ub4 mode);
*/
import "C"

import (
	"context"
	"fmt"
	"unsafe"
)

//export goCallback
func goCallback() {
	fmt.Println("callback executed!")
}

type CallbackFuncQueryChangeNotification func(
	unsafe.Pointer,
	*C.OCISubscription,
	unsafe.Pointer,
	*C.ub4,
	unsafe.Pointer,
	C.ub4)

// func (conn *OCI8Conn) createSubscription() error {
// 	return nil
// }

// callback CallbackFuncQueryChangeNotification

//export callback
func callback(unsafe.Pointer,
	*C.OCISubscription,
	unsafe.Pointer,
	*C.ub4,
	unsafe.Pointer,
	C.ub4,
) {
	fmt.Println("callback executed")
}

func (conn *OCI8Conn) RegisterQuery(query string, args []interface{}) (queryId int64, err error) {
	// Build a slice of namedValue using args.
	nv := make([]namedValue, len(args), len(args))
	for i, v := range args {
		nv[i] = namedValue{
			Name:    "",
			Ordinal: i + 1,
			Value:   v,
		}
	}
	// Allocate subscription handle.
	var namespace C.ub4 = C.OCI_SUBSCR_NAMESPACE_DBCHANGE
	var rowIds = true
	var qosFlags = C.OCI_SUBSCR_CQ_QOS_BEST_EFFORT // or use OCI_SUBSCR_CQ_QOS_QUERY for query level granularity with no false-positives; use OCI_SUBSCR_CQ_QOS_BEST_EFFORT for best-efforts
	var subscription *unsafe.Pointer
	subscription, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SUBSCRIPTION, 0)
	if err != nil {
		return 0, fmt.Errorf("allocate user session handle error: %v", err)
	}
	// Set the namespace.
	err = conn.ociAttrSet(*subscription, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&namespace), C.sizeof_ub4, C.OCI_ATTR_SUBSCR_NAMESPACE)
	if err != nil {
		return 0, err
	}
	// Associate a notification callback with the subscription.
	err = conn.ociAttrSet(*subscription, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(C.myCallbackSimple), 0, C.OCI_ATTR_SUBSCR_CALLBACK)
	if err != nil {
		return 0, err
	}
	// Allow extraction of rowid information.
	err = conn.ociAttrSet(*subscription, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&rowIds), C.sizeof_ub4, C.OCI_ATTR_CHNF_ROWIDS)
	if err != nil {
		return 0, err
	}
	// QOS Flags.
	err = conn.ociAttrSet(*subscription, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&qosFlags), C.sizeof_ub4, C.OCI_ATTR_SUBSCR_CQ_QOSFLAGS)
	if err != nil {
		return 0, err
	}
	// Create a new registration in the DBCHANGE namespace.
	var subscriptionPtr *C.OCISubscription
	subscriptionPtr = (*C.OCISubscription)(*subscription)
	err = conn.getError(C.OCISubscriptionRegister(conn.svc, &subscriptionPtr, 1, conn.errHandle, C.OCI_DEFAULT)) // this wants ptr to start of an array of subscription pointers.
	if err != nil {
		return 0, err
	}
	// Prepare the query/statement.
	stmt, err := conn.prepareStmt(query)
	if err != nil {
		return 0, err
	}

	// Define variables to receive values from the stmt.
	// It's in the sample code, but why bother if we don't want the values?!

	// var rows driver.Rows
	// rows, err = stmt.query(context.Background(), nv, true)
	// defer func() {
	// 	_ = rows.Close() // discard the rows and free the defines once we're done.
	// }()

	// Set the change notification attribute on the statement using the subscription.
	err = conn.ociAttrSet(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, *subscription, 0, C.OCI_ATTR_CHNF_REGHANDLE)
	if err != nil {
		return 0, err
	}
	// Execute the statement.
	// TODO: abort if not a SELECT statment - see query() for a check on this attr.
	err = stmt.ociStmtExecute(0, C.OCI_DEFAULT)
	if err != nil {
		return 0, err
	}
	// Get the query ID.
	var qid C.ub8
	sz := C.ub4(0)
	result := C.OCIAttrGet(
		unsafe.Pointer(stmt.stmt), // Pointer to a handle type
		C.OCI_HTYPE_STMT,          // The handle type: OCI_DTYPE_PARAM, for a parameter descriptor
		unsafe.Pointer(&qid),      // Pointer to the storage for an attribute value
		&sz,                       // The size of the attribute value.  // TODO: use sizeof()
		C.OCI_ATTR_CQ_QUERYID,     // The attribute type: https://docs.oracle.com/cd/B19306_01/appdev.102/b14250/ociaahan.htm
		conn.errHandle,            // An error handle
	)
	err = conn.getError(result)
	if err != nil {
		return 0, err
	} else {
		queryId = int64(qid)
	}

	// Commit to release the transaction. Can we rollback instead?
	conn.inTransaction = false
	if rv := C.OCITransCommit(conn.svc, conn.errHandle, 0, ); rv != C.OCI_SUCCESS {
		return 0, conn.getError(rv)
	}
	return queryId, err
}

// prepareStmt prepares a query and return the raw statement so we can access
// the statement handle.  For example, to set change notifications upon it.
// This is a duplicate of Prepare() which returns an interface.
func (conn *OCI8Conn) prepareStmt(query string) (*OCI8Stmt, error) {
	return conn.prepareStmtContext(context.Background(), query)
}

// prepareStmtContext is a duplicate of PrepareContext().
// See notes in prepareStmt().
func (conn *OCI8Conn) prepareStmtContext(ctx context.Context, query string) (*OCI8Stmt, error) {
	if conn.enableQMPlaceholders {
		query = placeholders(query)
	}

	queryP := cString(query)
	defer C.free(unsafe.Pointer(queryP))

	// statement handle
	stmt, _, err := conn.ociHandleAlloc(C.OCI_HTYPE_STMT, 0)
	if err != nil {
		return nil, fmt.Errorf("allocate statement handle error: %v", err)
	}

	if rv := C.OCIStmtPrepare(
		(*C.OCIStmt)(*stmt),
		conn.errHandle,
		queryP,
		C.ub4(len(query)),
		C.ub4(C.OCI_NTV_SYNTAX),
		C.ub4(C.OCI_DEFAULT),
	); rv != C.OCI_SUCCESS {
		C.OCIHandleFree(*stmt, C.OCI_HTYPE_STMT)
		return nil, conn.getError(rv)
	}

	return &OCI8Stmt{conn: conn, stmt: (*C.OCIStmt)(*stmt)}, nil
}

func (conn *OCI8Conn) freeHandles() {
	if conn.usrSession != nil {
		C.OCIHandleFree(unsafe.Pointer(conn.usrSession), C.OCI_HTYPE_SESSION)
		conn.usrSession = nil
	}
	if conn.svc != nil {
		C.OCIHandleFree(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX)
		conn.svc = nil
	}
	if conn.srv != nil {
		C.OCIHandleFree(unsafe.Pointer(conn.srv), C.OCI_HTYPE_SERVER)
		conn.srv = nil
	}
	if conn.errHandle != nil {
		C.OCIHandleFree(unsafe.Pointer(conn.errHandle), C.OCI_HTYPE_ERROR)
		conn.errHandle = nil
	}
	if conn.env != nil {
		C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
		conn.env = nil
	}
}
