package oci8

import "C"

import (
	"fmt"
	"unsafe"
)

type CallbackFuncQueryChangeNotification func(
	unsafe.Pointer,
	*C.OCISubscription,
	unsafe.Pointer,
	*C.ub4,
	unsafe.Pointer,
	C.ub4)

func (conn *OCI8Conn) RegisterQuery(query string, callback CallbackFuncQueryChangeNotification) error {
	// Allocate subscription handle */
	var namespace C.ub4 = C.OCI_SUBSCR_NAMESPACE_DBCHANGE
	var rowIds = true
	var qosFlags = C.OCI_SUBSCR_CQ_QOS_BEST_EFFORT  // or use OCI_SUBSCR_CQ_QOS_QUERY for query level granularity with no false-positives; use OCI_SUBSCR_CQ_QOS_BEST_EFFORT for best-efforts
	var sHandle *unsafe.Pointer
	var err error
	sHandle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SUBSCRIPTION, 0)
	if err != nil {
		return fmt.Errorf("allocate user session handle error: %v", err)
	}
	// Set the namespace.
	err = conn.ociAttrSet(*sHandle, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&namespace), unsafe.Sizeof(C.ub4), C.OCI_ATTR_SUBSCR_NAMESPACE)
	if err != nil {
		return err
	}
	// Associate a notification callback with the subscription.
	err = conn.ociAttrSet(*sHandle, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&callback), 0, C.OCI_ATTR_SUBSCR_CALLBACK)
	if err != nil {
		return err
	}
	// Allow extraction of rowid information.
	err = conn.ociAttrSet(*sHandle, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&rowIds), unsafe.Sizeof(C.ub4), C.OCI_ATTR_CHNF_ROWIDS)
	if err != nil {
		return err
	}
	// QOS Flags.
	err = conn.ociAttrSet(*sHandle, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&qosFlags), unsafe.Sizeof(C.ub4), C.OCI_ATTR_SUBSCR_CQ_QOSFLAGS)
	if err != nil {
		return err
	}
	// Create a new registration in the DBCHANGE namespace.
	result := C.OCISubscriptionRegister(conn.svc, sHandle, 1, conn.errHandle, C.OCI_DEFAULT)
	err = conn.getError(result)
	if err != nil {
		return err
	}

	// Prepare a statement.
	conn.Prepare(string)

	sHandle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_STMT, 0)
	if err != nil {
		return nil, fmt.Errorf("allocate user session handle error: %v", err)
	}
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
