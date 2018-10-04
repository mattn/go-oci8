package oci8

/*
#include "oci8.go.h"
#cgo !noPkgConfig pkg-config: oci8
*/
import "C"

// noPkgConfig is a Go tag for disabling using pkg-config and using environmental settings like CGO_CFLAGS and CGO_LDFLAGS instead

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
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
		(*C.OCISvcCtx)(conn.svc),
		conn.err,
		C.OCI_DEFAULT)
	if rv == C.OCI_SUCCESS {
		return nil
	}
	if strings.HasPrefix(ociGetError(rv, conn.err).Error(), "ORA-01010") {
		// Older versions of Oracle do not support ping,
		// but a reponse of "ORA-01010: invalid OCI operation" confirms connectivity.
		// See https://github.com/rana/ora/issues/224
		return nil
	}
	return errors.New("ping failed")
}

// Begin a transaction
func (conn *OCI8Conn) Begin() (driver.Tx, error) {
	return conn.begin(context.Background())
}

func (conn *OCI8Conn) begin(ctx context.Context) (driver.Tx, error) {
	if conn.transactionMode != C.OCI_TRANS_READWRITE {
		var th unsafe.Pointer
		if rv := C.WrapOCIHandleAlloc(
			conn.env,
			C.OCI_HTYPE_TRANS,
			0,
		); rv.rv != C.OCI_SUCCESS {
			return nil, errors.New("can't allocate handle")
		} else {
			th = rv.ptr
		}

		if rv := C.OCIAttrSet(
			conn.svc,
			C.OCI_HTYPE_SVCCTX,
			th,
			0,
			C.OCI_ATTR_TRANS,
			conn.err,
		); rv != C.OCI_SUCCESS {
			C.OCIHandleFree(th, C.OCI_HTYPE_TRANS)
			return nil, ociGetError(rv, conn.err)
		}

		if rv := C.OCITransStart(
			(*C.OCISvcCtx)(conn.svc),
			conn.err,
			0,
			conn.transactionMode, // mode is: C.OCI_TRANS_SERIALIZABLE, C.OCI_TRANS_READWRITE, or C.OCI_TRANS_READONLY
		); rv != C.OCI_SUCCESS {
			C.OCIHandleFree(th, C.OCI_HTYPE_TRANS)
			return nil, ociGetError(rv, conn.err)
		}
		// TOFIX: memory leak: th needs to be saved into OCI8Tx so OCIHandleFree can be called on it

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
			(*C.OCISvcCtx)(conn.svc),
			conn.err,
			(*C.OCISession)(conn.usr_session),
			C.OCI_DEFAULT,
		); rv != C.OCI_SUCCESS {
			err = ociGetError(rv, conn.err)
		}
		if rv := C.OCIServerDetach(
			(*C.OCIServer)(conn.srv),
			conn.err,
			C.OCI_DEFAULT,
		); rv != C.OCI_SUCCESS {
			err = ociGetError(rv, conn.err)
		}
		C.OCIHandleFree(conn.usr_session, C.OCI_HTYPE_SESSION)
		C.OCIHandleFree(conn.svc, C.OCI_HTYPE_SVCCTX)
		C.OCIHandleFree(conn.srv, C.OCI_HTYPE_SERVER)
	} else {
		if rv := C.OCILogoff(
			(*C.OCISvcCtx)(conn.svc),
			conn.err,
		); rv != C.OCI_SUCCESS {
			err = ociGetError(rv, conn.err)
		}
	}

	C.OCIHandleFree(unsafe.Pointer(conn.err), C.OCI_HTYPE_ERROR)
	C.OCIHandleFree(conn.env, C.OCI_HTYPE_ENV)

	conn.svc = nil
	conn.env = nil
	conn.err = nil

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

	var s, bp, defp unsafe.Pointer
	if rv := C.WrapOCIHandleAlloc(
		conn.env,
		C.OCI_HTYPE_STMT,
		(C.size_t)(unsafe.Sizeof(bp)*2),
	); rv.rv != C.OCI_SUCCESS {
		return nil, ociGetError(rv.rv, conn.err)
	} else {
		s = rv.ptr
		bp = rv.extra
		defp = unsafe.Pointer(uintptr(rv.extra) + sizeOfNilPointer)
	}

	if rv := C.OCIStmtPrepare(
		(*C.OCIStmt)(s),
		conn.err,
		(*C.OraText)(unsafe.Pointer(pquery)),
		C.ub4(C.strlen(pquery)),
		C.ub4(C.OCI_NTV_SYNTAX),
		C.ub4(C.OCI_DEFAULT),
	); rv != C.OCI_SUCCESS {
		C.OCIHandleFree(s, C.OCI_HTYPE_STMT)
		return nil, ociGetError(rv, conn.err)
	}

	return &OCI8Stmt{conn: conn, s: s, bp: (**C.OCIBind)(bp), defp: (**C.OCIDefine)(defp)}, nil
}
