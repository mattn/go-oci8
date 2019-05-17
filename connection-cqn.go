package oci8

import "C"

import (
	"context"
	"fmt"
	"github.com/relloyd/go-sql/database/sql/driver"
	"unsafe"
)

type CallbackFuncQueryChangeNotification func(
	unsafe.Pointer,
	*C.OCISubscription,
	unsafe.Pointer,
	*C.ub4,
	unsafe.Pointer,
	C.ub4)

func (conn *OCI8Conn) RegisterQuery(callback CallbackFuncQueryChangeNotification, query string, args []namedValue) (queryId int64, err error) {
	// Allocate subscription handle */
	var namespace C.ub4 = C.OCI_SUBSCR_NAMESPACE_DBCHANGE
	var rowIds = true
	var qosFlags = C.OCI_SUBSCR_CQ_QOS_BEST_EFFORT // or use OCI_SUBSCR_CQ_QOS_QUERY for query level granularity with no false-positives; use OCI_SUBSCR_CQ_QOS_BEST_EFFORT for best-efforts
	var subscription *unsafe.Pointer
	subscription, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SUBSCRIPTION, 0)
	if err != nil {
		return 0, fmt.Errorf("allocate user session handle error: %v", err)
	}
	// Set the namespace.
	err = conn.ociAttrSet(*subscription, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&namespace), unsafe.Sizeof(C.ub4), C.OCI_ATTR_SUBSCR_NAMESPACE)
	if err != nil {
		return 0, err
	}
	// Associate a notification callback with the subscription.
	err = conn.ociAttrSet(*subscription, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&callback), 0, C.OCI_ATTR_SUBSCR_CALLBACK)
	if err != nil {
		return 0, err
	}
	// Allow extraction of rowid information.
	err = conn.ociAttrSet(*subscription, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&rowIds), unsafe.Sizeof(C.ub4), C.OCI_ATTR_CHNF_ROWIDS)
	if err != nil {
		return 0, err
	}
	// QOS Flags.
	err = conn.ociAttrSet(*subscription, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&qosFlags), unsafe.Sizeof(C.ub4), C.OCI_ATTR_SUBSCR_CQ_QOSFLAGS)
	if err != nil {
		return 0, err
	}
	// Create a new registration in the DBCHANGE namespace.
	err = conn.getError(C.OCISubscriptionRegister(conn.svc, subscription, 1, conn.errHandle, C.OCI_DEFAULT))
	if err != nil {
		return 0, err
	}
	// Prepare the query/statement.
	stmt, err := conn.PrepareStmt(query)
	if err != nil {
		return 0, err
	}
	// Define variables to receive values from the stmt.
	// It's in the sample code, but why bother if we don't want the values?!
	var rows driver.Rows
	rows, err = stmt.query(context.Background(), args, true)
	defer func() {
		_ = rows.Close() // discard the rows and free the defines once we're done.
	}()
	// Set the change notification attribute on the statement using the subscription.
	err = conn.ociAttrSet(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, subscription, 0, C.OCI_ATTR_CHNF_REGHANDLE)
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
	qid := C.ub8(0)
	result := C.OCIAttrGet(
		unsafe.Pointer(stmt.stmt), // Pointer to a handle type
		C.OCI_HTYPE_STMT,            // The handle type: OCI_DTYPE_PARAM, for a parameter descriptor
		&qid,             // Pointer to the storage for an attribute value
		C.ub4(0),                    // The size of the attribute value.  // TODO: use sizeof()
		C.OCI_ATTR_CQ_QUERYID,       // The attribute type: https://docs.oracle.com/cd/B19306_01/appdev.102/b14250/ociaahan.htm
		conn.errHandle,              // An error handle
	)
	err = conn.getError(result)
	if err != nil {
		return 0, err
	} else {
		queryId = qid
	}

	// Commit to release the transaction. Can we rollback instead?
	xxxxxxxx continue here!!!

	subscription, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_STMT, 0)
	if err != nil {
		return 0, fmt.Errorf("allocate user session handle error: %v", err)
	}
}

// PrepareStmt prepares a query and return the raw statement so we can access
// the statement handle.  For example, to set change notifications upon it.
// This is a duplicate of Prepare() which returns an interface.
func (conn *OCI8Conn) PrepareStmt(query string) (*OCI8Stmt, error) {
	return conn.PrepareStmtContext(context.Background(), query)
}

// PrepareStmtContext is a duplicate of PrepareContext().
// See notes in PrepareStmt().
func (conn *OCI8Conn) PrepareStmtContext(ctx context.Context, query string) (*OCI8Stmt, error) {
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

func (conn *OCI8Conn) createSubscription() error {

}
