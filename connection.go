package oci8

// #include "oci8.go.h"
import "C"

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"unsafe"
)

// Exec executes a query
func (conn *OCI8Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	list := make([]namedValue, len(args))
	for i, v := range args {
		list[i] = namedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return conn.exec(context.Background(), query, list)
}

func (conn *OCI8Conn) exec(ctx context.Context, query string, args []namedValue) (driver.Result, error) {
	s, err := conn.prepare(ctx, query)
	defer s.Close()
	if err != nil {
		return nil, err
	}
	res, err := s.(*OCI8Stmt).exec(ctx, args)
	if err != nil && err != driver.ErrSkip {
		return nil, err
	}
	return res, nil
}

/*
FIXME:
Queryer is disabled because of incresing cursor numbers.
See https://github.com/mattn/go-oci8/issues/151
OCIStmtExecute doesn't return anything to close resource.
This mean that OCI8Rows.Close can't close statement handle. For example,
prepared statement is called twice like below.

    stmt, _ := db.Prepare("...")
    stmt.QueryRow().Scan(&x)
    stmt.QueryRow().Scan(&x)

If OCI8Rows close handle of statement, this fails.

// Query implements Queryer.
func (conn *OCI8Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	list := make([]namedValue, len(args))
	for i, v := range args {
		list[i] = namedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	rows, err := conn.query(context.Background(), query, list)
	if err != nil {
		return nil, err
	}
	rows.(*OCI8Rows).cls = true
	return rows, err
}
*/

func (conn *OCI8Conn) query(ctx context.Context, query string, args []namedValue) (driver.Rows, error) {
	s, err := conn.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := s.(*OCI8Stmt).query(ctx, args, true)
	if err != nil && err != driver.ErrSkip {
		s.Close()
		return nil, err
	}
	return rows, nil
}

func (conn *OCI8Conn) ping(ctx context.Context) error {
	rv := C.OCIPing(
		conn.svc,
		conn.errHandle,
		C.OCI_DEFAULT)
	if rv == C.OCI_SUCCESS {
		return nil
	}
	errorCode, err := conn.ociGetError()
	if errorCode == 1010 {
		// Older versions of Oracle do not support ping,
		// but a reponse of "ORA-01010: invalid OCI operation" confirms connectivity.
		// See https://github.com/rana/ora/issues/224
		return nil
	}
	conn.logger.Print("Ping error: ", err)
	return driver.ErrBadConn
}

// Begin a transaction
func (conn *OCI8Conn) Begin() (driver.Tx, error) {
	return conn.begin(context.Background())
}

func (conn *OCI8Conn) begin(ctx context.Context) (driver.Tx, error) {
	if conn.transactionMode != C.OCI_TRANS_READWRITE {
		// transaction handle
		trans, _, err := conn.ociHandleAlloc(C.OCI_HTYPE_TRANS, 0)
		if err != nil {
			return nil, fmt.Errorf("allocate transaction handle error: %v", err)
		}

		// sets the transaction context attribute of the service context
		err = conn.ociAttrSet(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX, *trans, 0, C.OCI_ATTR_TRANS)
		if err != nil {
			C.OCIHandleFree(*trans, C.OCI_HTYPE_TRANS)
			return nil, err
		}

		// transaction handle should be freed by something once attached to the service context
		// but I cannot find anything in the documentation explicitly calling this out
		// going by examples: https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci17msc006.htm#i428845

		if rv := C.OCITransStart(
			conn.svc,
			conn.errHandle,
			0,
			conn.transactionMode, // mode is: C.OCI_TRANS_SERIALIZABLE, C.OCI_TRANS_READWRITE, or C.OCI_TRANS_READONLY
		); rv != C.OCI_SUCCESS {
			return nil, conn.getError(rv)
		}

	}

	conn.inTransaction = true

	return &OCI8Tx{conn}, nil
}

// Close a connection
func (conn *OCI8Conn) Close() error {
	if conn.closed {
		return nil
	}
	conn.closed = true

	var err error
	if useOCISessionBegin {
		if rv := C.OCISessionEnd(
			conn.svc,
			conn.errHandle,
			conn.usrSession,
			C.OCI_DEFAULT,
		); rv != C.OCI_SUCCESS {
			err = conn.getError(rv)
		}
		if rv := C.OCIServerDetach(
			conn.srv,
			conn.errHandle,
			C.OCI_DEFAULT,
		); rv != C.OCI_SUCCESS {
			err = conn.getError(rv)
		}
		C.OCIHandleFree(unsafe.Pointer(conn.usrSession), C.OCI_HTYPE_SESSION)
		C.OCIHandleFree(unsafe.Pointer(conn.srv), C.OCI_HTYPE_SERVER)
		conn.usrSession = nil
		conn.srv = nil
	} else {
		if rv := C.OCILogoff(
			conn.svc,
			conn.errHandle,
		); rv != C.OCI_SUCCESS {
			err = conn.getError(rv)
		}
	}

	C.OCIHandleFree(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX)
	C.OCIHandleFree(unsafe.Pointer(conn.errHandle), C.OCI_HTYPE_ERROR)
	C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
	conn.svc = nil
	conn.errHandle = nil
	conn.env = nil

	return err
}

// Prepare a query
func (conn *OCI8Conn) Prepare(query string) (driver.Stmt, error) {
	return conn.prepare(context.Background(), query)
}

func (conn *OCI8Conn) prepare(ctx context.Context, query string) (driver.Stmt, error) {
	if conn.enableQMPlaceholders {
		query = placeholders(query)
	}

	pquery := C.CString(query)
	defer C.free(unsafe.Pointer(pquery))

	// statement handle
	stmt, _, err := conn.ociHandleAlloc(C.OCI_HTYPE_STMT, 0)
	if err != nil {
		return nil, fmt.Errorf("allocate statement handle error: %v", err)
	}

	if rv := C.OCIStmtPrepare(
		(*C.OCIStmt)(*stmt),
		conn.errHandle,
		(*C.OraText)(unsafe.Pointer(pquery)),
		C.ub4(C.strlen(pquery)),
		C.ub4(C.OCI_NTV_SYNTAX),
		C.ub4(C.OCI_DEFAULT),
	); rv != C.OCI_SUCCESS {
		C.OCIHandleFree(*stmt, C.OCI_HTYPE_STMT)
		return nil, conn.getError(rv)
	}

	return &OCI8Stmt{conn: conn, stmt: (*C.OCIStmt)(*stmt)}, nil
}

// getError gets error from return result (sword) or OCIError
func (conn *OCI8Conn) getError(result C.sword) error {
	switch result {
	case C.OCI_SUCCESS:
		return nil
	case C.OCI_INVALID_HANDLE:
		return errors.New("OCI_INVALID_HANDLE")
	case C.OCI_SUCCESS_WITH_INFO:
		return ErrOCISuccessWithInfo
	case C.OCI_RESERVED_FOR_INT_USE:
		return errors.New("OCI_RESERVED_FOR_INT_USE")
	case C.OCI_NO_DATA:
		return errors.New("OCI_NO_DATA")
	case C.OCI_NEED_DATA:
		return errors.New("OCI_NEED_DATA")
	case C.OCI_STILL_EXECUTING:
		return errors.New("OCI_STILL_EXECUTING")
	case C.OCI_ERROR:
		errorCode, err := conn.ociGetError()
		switch errorCode {
		/*
			bad connection errors:
			ORA-00028: your session has been killed
			ORA-01012: Not logged on
			ORA-01033: ORACLE initialization or shutdown in progress
			ORA-01034: ORACLE not available
			ORA-01089: immediate shutdown in progress - no operations are permitted
			ORA-03113: end-of-file on communication channel
			ORA-03114: Not Connected to Oracle
			ORA-03135: connection lost contact
			ORA-12528: TNS:listener: all appropriate instances are blocking new connections
			ORA-12537: TNS:connection closed
		*/
		case 28, 1012, 1033, 1034, 1089, 3113, 3114, 3135, 12528, 12537:
			return driver.ErrBadConn
		}
		return err
	}
	return fmt.Errorf("received result code %d", result)
}

// ociGetError calls OCIErrorGet then returs error code and text
func (conn *OCI8Conn) ociGetError() (int, error) {
	var errorCode C.sb4
	errorText := make([]byte, 1024)

	result := C.OCIErrorGet(
		unsafe.Pointer(conn.errHandle), // error handle
		1,                           // status record number, starts from 1
		nil,                         // sqlstate, not supported in release 8.x or later
		&errorCode,                  // error code
		(*C.OraText)(&errorText[0]), // error message text
		1024,              // size of the buffer provided in number of bytes
		C.OCI_HTYPE_ERROR, // type of the handle (OCI_HTYPE_ERR or OCI_HTYPE_ENV)
	)
	if result != C.OCI_SUCCESS {
		return 3114, errors.New("OCIErrorGet failed")
	}

	index := bytes.IndexByte(errorText, 0)

	return int(errorCode), errors.New(string(errorText[:index]))
}

// ociAttrGet calls OCIAttrGet with OCIParam then returns attribute size and error.
// The attribute value is stored into passed value.
func (conn *OCI8Conn) ociAttrGet(paramHandle *C.OCIParam, value unsafe.Pointer, attributeType C.ub4) (C.ub4, error) {
	var size C.ub4

	result := C.OCIAttrGet(
		unsafe.Pointer(paramHandle), // Pointer to a handle type
		C.OCI_DTYPE_PARAM,           // The handle type: OCI_DTYPE_PARAM, for a parameter descriptor
		value,                       // Pointer to the storage for an attribute value
		&size,                       // The size of the attribute value
		attributeType,               // The attribute type: https://docs.oracle.com/cd/B19306_01/appdev.102/b14250/ociaahan.htm
		conn.errHandle,              // An error handle
	)

	return size, conn.getError(result)
}

// ociAttrSet calls OCIAttrSet.
// Only uses errHandle from conn, so can be called in conn setup after errHandle has been set.
func (conn *OCI8Conn) ociAttrSet(
	handle unsafe.Pointer,
	handleType C.ub4,
	value unsafe.Pointer,
	valueSize C.ub4,
	attributeType C.ub4,
) error {
	result := C.OCIAttrSet(
		handle,         // Pointer to a handle whose attribute gets modified
		handleType,     // The handle type
		value,          // Pointer to an attribute value
		valueSize,      // The size of an attribute value
		attributeType,  // The type of attribute being set
		conn.errHandle, // An error handle
	)

	return conn.getError(result)
}

// ociHandleAlloc calls OCIHandleAlloc then returns
// handle pointer to pointer, buffer pointer to pointer, and error
func (conn *OCI8Conn) ociHandleAlloc(handleType C.ub4, size C.size_t) (*unsafe.Pointer, *unsafe.Pointer, error) {
	var handleTemp unsafe.Pointer
	handle := &handleTemp
	var bufferTemp unsafe.Pointer
	var buffer *unsafe.Pointer
	if size > 0 {
		buffer = &bufferTemp
	}

	result := C.OCIHandleAlloc(
		unsafe.Pointer(conn.env), // An environment handle
		handle,     // Returns a handle
		handleType, // type of handle: https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci02bas.htm#LNOCI87581
		size,       // amount of user memory to be allocated
		buffer,     // Returns a pointer to the user memory
	)

	err := conn.getError(result)
	if err != nil {
		return nil, nil, err
	}

	if size > 0 {
		return handle, buffer, nil
	}

	return handle, nil, nil
}

// ociDescriptorAlloc calls OCIDescriptorAlloc then returns
// descriptor pointer to pointer, buffer pointer to pointer, and error
func (conn *OCI8Conn) ociDescriptorAlloc(descriptorType C.ub4, size C.size_t) (*unsafe.Pointer, *unsafe.Pointer, error) {
	var descriptorTemp unsafe.Pointer
	descriptor := &descriptorTemp
	var bufferTemp unsafe.Pointer
	var buffer *unsafe.Pointer
	if size > 0 {
		buffer = &bufferTemp
	}

	result := C.OCIDescriptorAlloc(
		unsafe.Pointer(conn.env), // An environment handle
		descriptor,               // Returns a descriptor or LOB locator of desired type
		descriptorType,           // Specifies the type of descriptor or LOB locator to be allocated
		size,                     // Specifies an amount of user memory to be allocated for use by the application for the lifetime of the descriptor
		buffer,                   // Returns a pointer to the user memory of size xtramem_sz allocated by the call for the user for the lifetime of the descriptor
	)

	err := conn.getError(result)
	if err != nil {
		return nil, nil, err
	}

	if size > 0 {
		return descriptor, buffer, nil
	}

	return descriptor, nil, nil
}

// ociLobRead calls OCILobRead then returns lob bytes and error.
func (conn *OCI8Conn) ociLobRead(lobLocator *C.OCILobLocator, form C.ub1) ([]byte, error) {
	readBuffer := make([]byte, lobBufferSize)
	buffer := make([]byte, 0)
	result := (C.sword)(C.OCI_NEED_DATA)

	for result == C.OCI_NEED_DATA {
		readSize := (C.ub4)(lobBufferSize)

		result = C.OCILobRead(
			conn.svc,       // The service context handle
			conn.errHandle, // An error handle
			lobLocator,     // A LOB or BFILE locator that uniquely references the LOB or BFILE
			&readSize,      // The amount in either bytes (BLOBs and BFILEs) or characters (CLOBs and NCLOBs)
			1,              // The absolute offset from the beginning of the LOB value. The first position is 1. The subsequent polling calls the offset parameter is ignored.
			unsafe.Pointer(&readBuffer[0]), // The pointer to a buffer into which the piece will be read.
			lobBufferSize,                  // The length of the buffer in bytes/characters.
			nil,                            // The context pointer for the callback function. Can be null.
			nil,                            // If this is null, then OCI_NEED_DATA will be returned for each piece.
			0,                              // If this value is 0 then csid is set to the client's NLS_LANG or NLS_CHAR value.
			form,                           // The character set form of the buffer data: SQLCS_IMPLICIT or SQLCS_NCHAR
		)

		if result == C.OCI_SUCCESS || result == C.OCI_NEED_DATA {
			buffer = append(buffer, readBuffer[:int(readSize)]...)
		}
	}

	return buffer, conn.getError(result)
}
