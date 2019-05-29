package oci8

/*
#cgo CXXFLAGS: -std=c++11
#include "oci8.go.h"
void cqnCallback(dvoid* ctx, OCISubscription* subscrhp, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode);
*/
import "C"

import (
	"context"
	"fmt"
	"unsafe"
)

func (conn *OCI8Conn) NewCqnSubscription() (registrationId int64, subscriptionPtr *C.OCISubscription, err error) {
	var namespace C.ub4 = C.OCI_SUBSCR_NAMESPACE_DBCHANGE
	var rowIds = true
	var qosFlags = C.OCI_SUBSCR_CQ_QOS_BEST_EFFORT // or use OCI_SUBSCR_CQ_QOS_QUERY for query level granularity with no false-positives; use OCI_SUBSCR_CQ_QOS_BEST_EFFORT for best-efforts
	var s *unsafe.Pointer
	// Defer cleanup.
	// TODO: Do we need to clean up the conn here? Check when you have brainpower.
	defer func(errP *error) {
		if *errP != nil {
			if s != nil {
				C.OCIHandleFree(*s, C.OCI_HTYPE_SUBSCRIPTION)
			}
		}
	}(&err)
	// Allocate subscription handle.
	s, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SUBSCRIPTION, 0)
	if err != nil {
		return
	}
	// Set the namespace.
	err = conn.ociAttrSet(*s, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&namespace), C.sizeof_ub4, C.OCI_ATTR_SUBSCR_NAMESPACE)
	if err != nil {
		return
	}
	// Associate a notification callback with the subscription.
	err = conn.ociAttrSet(*s, C.OCI_HTYPE_SUBSCRIPTION, C.cqnCallback, 0, C.OCI_ATTR_SUBSCR_CALLBACK)
	if err != nil {
		return
	}
	// Allow extraction of rowid information.
	err = conn.ociAttrSet(*s, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&rowIds), C.sizeof_ub4, C.OCI_ATTR_CHNF_ROWIDS)
	if err != nil {
		return
	}
	// QOS Flags.
	err = conn.ociAttrSet(*s, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&qosFlags), C.sizeof_ub4, C.OCI_ATTR_SUBSCR_CQ_QOSFLAGS)
	if err != nil {
		return
	}
	// Create a new registration in the DBCHANGE namespace.
	subscriptionPtr = (*C.OCISubscription)(*s)
	err = conn.getError(C.OCISubscriptionRegister(conn.svc, &subscriptionPtr, 1, conn.errHandle, C.OCI_DEFAULT)) // this wants ptr to start of an array of subscription pointers.
	if err != nil {
		return
	}
	// Get the registration ID.
	var regId C.ub8
	regIdSize := C.ub4(C.sizeof_ub8)
	result := C.OCIAttrGet(
		unsafe.Pointer(subscriptionPtr), // unsafe.Pointer(stmt.stmt), // Pointer to a handle type
		C.OCI_HTYPE_SUBSCRIPTION,        // C.OCI_HTYPE_STMT,          // The handle type: OCI_DTYPE_PARAM, for a parameter descriptor
		unsafe.Pointer(&regId),          // Pointer to the storage for an attribute value
		&regIdSize,                      // The size of the attribute value.  // TODO: use sizeof()
		C.OCI_ATTR_SUBSCR_CQ_REGID,      // C.OCI_ATTR_CQ_QUERYID <<< returns 0 for what I think is the first query since multiples can be registered in one subscroption. // The attribute type: https://docs.oracle.com/cd/B19306_01/appdev.102/b14250/ociaahan.htm
		conn.errHandle,                  // An error handle
	)
	err = conn.getError(result)
	if err != nil {
		return
	} else {
		registrationId = int64(regId)
	}
	return
}

func (conn *OCI8Conn) ExecuteCqn(subscription *C.OCISubscription, query string, args []interface{}) (err error) {
	// Build a slice of namedValue using args.
	nv := make([]namedValue, len(args), len(args))
	for i, v := range args {
		nv[i] = namedValue{
			Name:    "",
			Ordinal: i + 1,
			Value:   v,
		}
	}
	// Prepare the query/statement.
	var stmt *OCI8Stmt
	stmt, err = conn.prepareStmt(query)
	if err != nil {
		return
	}
	// TODO: bind the args.

	// Set the subscription on the statement.
	err = conn.ociAttrSet(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, unsafe.Pointer(subscription), 0, C.OCI_ATTR_CHNF_REGHANDLE)
	if err != nil {
		return
	}
	// Execute the statement.
	// TODO: abort if not a SELECT statement - see query() for a check on this attr.
	err = stmt.ociStmtExecute(0, C.OCI_DEFAULT)
	if err != nil {
		return
	}
	// Commit to release the transaction.
	// TODO: Rollback instead of commit after this SELECT.
	conn.inTransaction = false
	if rv := C.OCITransCommit(conn.svc, conn.errHandle, 0, ); rv != C.OCI_SUCCESS {
		err = conn.getError(rv)
		return
	}
	return
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
