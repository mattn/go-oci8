package oci8

import "C"

import (
	"fmt"
	"unsafe"
)

func (conn *OCI8Conn) RegisterQuery() {
	// Allocate subscription handle */
	var handle *unsafe.Pointer
	var err error
	handle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SUBSCRIPTION, 0)
	if err != nil {
		return nil, fmt.Errorf("allocate user session handle error: %v", err)
	}

	checker(errhp, OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION, (dvoid *)&namespace, sizeof(ub4),OCI_ATTR_SUBSCR_NAMESPACE, errhp));
	/* Associate a notification callback with the subscription */
	checker(errhp, OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION,(void *)myCallback, 0, OCI_ATTR_SUBSCR_CALLBACK, errhp));
	/* Allow extraction of rowid information */
	checker(errhp, OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION,(dvoid *)&rowids, sizeof(ub4),OCI_ATTR_CHNF_ROWIDS, errhp));
	checker(errhp, OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION,(dvoid *)&qosflags, sizeof(ub4),OCI_ATTR_SUBSCR_CQ_QOSFLAGS, errhp));
	/* Create a new registration in the DBCHANGE namespace */
	checker(errhp, OCISubscriptionRegister(svchp, &subscrhp, 1, errhp, OCI_DEFAULT));
	

	// Allocate a statement.
	var err error
	var result C.sword
	var handle *unsafe.Pointer
	handle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_STMT, 0)
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