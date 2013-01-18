package oci8

/*
#include <oci.h>
#include <stdlib.h>
#include <string.h>

#cgo pkg-config: oci8
*/
import "C"
import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"unsafe"
)

func init() {
	sql.Register("oci8", &OCI8Driver{})
}

type OCI8Driver struct {
}

type OCI8Conn struct {
	svc unsafe.Pointer
	env unsafe.Pointer
	err unsafe.Pointer
}

type OCI8Tx struct {
	c *OCI8Conn
}

func (tx *OCI8Tx) Commit() error {
	if err := tx.c.exec("COMMIT"); err != nil {
		return err
	}
	return nil
}

func (tx *OCI8Tx) Rollback() error {
	if err := tx.c.exec("ROLLBACK"); err != nil {
		return err
	}
	return nil
}

func (c *OCI8Conn) exec(cmd string) error {
	stmt, err := c.Prepare(cmd)
	if err == nil {
		defer stmt.Close()
		_, err = stmt.Exec(nil)
	}
	return err
}

func (c *OCI8Conn) Begin() (driver.Tx, error) {
	if err := c.exec("BEGIN"); err != nil {
		return nil, err
	}
	return &OCI8Tx{c}, nil
}

func (d *OCI8Driver) Open(dsn string) (driver.Conn, error) {
	var conn OCI8Conn
	token := strings.SplitN(dsn, "@", 2)
	userpass := strings.SplitN(token[0], "/", 2)

	rv := C.OCIInitialize(
		C.OCI_DEFAULT,
		nil,
		nil,
		nil,
		nil)
	if rv == C.OCI_ERROR {
		return nil, ociGetError(conn.err)
	}


	rv = C.OCIEnvInit(
		(**C.OCIEnv)(unsafe.Pointer(&conn.env)),
		C.OCI_DEFAULT,
		0,
		nil)

	rv = C.OCIHandleAlloc(
		conn.env,
		&conn.err,
		C.OCI_HTYPE_ERROR,
		0,
		nil)
	if rv == C.OCI_ERROR {
		return nil, ociGetError(conn.err)
	}


	var phost *C.char
	phostlen := C.size_t(0)
	if len(token) > 1 {
		phost = C.CString(token[1])
		defer C.free(unsafe.Pointer(phost))
		phostlen = C.strlen(phost)
	}
	puser := C.CString(userpass[0])
	defer C.free(unsafe.Pointer(puser))
	ppass := C.CString(userpass[1])
	defer C.free(unsafe.Pointer(ppass))

	rv = C.OCILogon(
		(*C.OCIEnv)(conn.env),
		(*C.OCIError)(conn.err),
		(**C.OCIServer)(unsafe.Pointer(&conn.svc)),
		(*C.OraText)(unsafe.Pointer(puser)),
		C.ub4(C.strlen(puser)),
		(*C.OraText)(unsafe.Pointer(ppass)),
		C.ub4(C.strlen(ppass)),
		(*C.OraText)(unsafe.Pointer(phost)),
		C.ub4(phostlen))
	if rv == C.OCI_ERROR {
		return nil, ociGetError(conn.err)
	}

	return &conn, nil
}

func (c *OCI8Conn) Close() error {
	rv := C.OCILogoff(
		(*C.OCIServer)(c.svc),
		(*C.OCIError)(c.err))
	if rv == C.OCI_ERROR {
		return ociGetError(c.err)
	}

	C.OCIHandleFree(
		c.env,
		C.OCI_HTYPE_ENV)

	c.svc = nil
	c.env = nil
	c.err = nil
	return nil
}

type OCI8Stmt struct {
	c      *OCI8Conn
	s      unsafe.Pointer
	closed bool
}

func (c *OCI8Conn) Prepare(query string) (driver.Stmt, error) {
	pquery := C.CString(query)
	defer C.free(unsafe.Pointer(pquery))
	var s unsafe.Pointer

	rv := C.OCIHandleAlloc(
		c.env,
		&s,
		C.OCI_HTYPE_STMT,
		0,
		nil)
	if rv == C.OCI_ERROR {
		return nil, ociGetError(c.err)
	}

	rv = C.OCIStmtPrepare(
		(*C.OCIStmt)(s),
		(*C.OCIError)(c.err),
		(*C.OraText)(unsafe.Pointer(pquery)),
		C.ub4(C.strlen(pquery)),
		C.ub4(C.OCI_NTV_SYNTAX),
		C.ub4(C.OCI_DEFAULT))
	if rv == C.OCI_ERROR {
		return nil, ociGetError(c.err)
	}

	return &OCI8Stmt{c: c, s: s}, nil
}

func (s *OCI8Stmt) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	C.OCIHandleFree(
		s.s,
		C.OCI_HTYPE_STMT)
	s.s = nil

	return nil
}

func (s *OCI8Stmt) NumInput() int {
	var num C.int
	C.OCIAttrGet(
		s.s,
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&num),
		nil,
		C.OCI_ATTR_BIND_COUNT,
		(*C.OCIError)(s.c.err))
	return int(num)
}

func (s *OCI8Stmt) bind(args []driver.Value) error {
	if args == nil {
		return nil
	}

	var bp *C.OCIBind
	for i, v := range args {
		b := []byte(fmt.Sprintf("%v", v))
		b = append(b, 0)
		rv := C.OCIBindByPos(
			(*C.OCIStmt)(s.s),
			&bp,
			(*C.OCIError)(s.c.err),
			C.ub4(i+1),
			unsafe.Pointer(&b[0]),
			C.sb4(len(b)),
			C.SQLT_STR,
			nil,
			nil,
			nil,
			0,
			nil,
			C.OCI_DEFAULT)

		if rv == C.OCI_ERROR {
			return ociGetError(s.c.err)
		}
	}
	return nil
}

func (s *OCI8Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}

	var t C.int
	C.OCIAttrGet(
		s.s,
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&t),
		nil,
		C.OCI_ATTR_STMT_TYPE,
		(*C.OCIError)(s.c.err))
	iter := C.ub4(1)
	if t == C.OCI_STMT_SELECT {
		iter = 0
	}

	rv := C.OCIStmtExecute(
		(*C.OCIServer)(s.c.svc),
		(*C.OCIStmt)(s.s),
		(*C.OCIError)(s.c.err),
		iter,
		0,
		nil,
		nil,
		C.OCI_DEFAULT)
	if rv == C.OCI_ERROR {
		return nil, ociGetError(s.c.err)
	}

	var rc C.ub2
	C.OCIAttrGet(
		s.s,
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&rc),
		nil,
		C.OCI_ATTR_PARAM_COUNT,
		(*C.OCIError)(s.c.err))

	oci8cols := make([]oci8col, int(rc))
	for i := 0; i < int(rc); i++ {
		var p unsafe.Pointer
		var np *C.char
		var ns C.ub4
		var tp C.ub2
		var lp C.ub2
		C.OCIParamGet(
			s.s,
			C.OCI_HTYPE_STMT,
			(*C.OCIError)(s.c.err),
			(*unsafe.Pointer)(unsafe.Pointer(&p)),
			C.ub4(i+1))
		C.OCIAttrGet(
			p,
			C.OCI_DTYPE_PARAM,
			unsafe.Pointer(&tp),
			nil,
			C.OCI_ATTR_DATA_TYPE,
			(*C.OCIError)(s.c.err))
		C.OCIAttrGet(
			p,
			C.OCI_DTYPE_PARAM,
			unsafe.Pointer(&np),
			&ns,
			C.OCI_ATTR_NAME,
			(*C.OCIError)(s.c.err))
		C.OCIAttrGet(
			p,
			C.OCI_DTYPE_PARAM,
			unsafe.Pointer(&lp),
			nil,
			C.OCI_ATTR_DATA_SIZE,
			(*C.OCIError)(s.c.err))
		oci8cols[i].name = string((*[1 << 30]byte)(unsafe.Pointer(np))[0:int(ns)])
		oci8cols[i].kind = int(tp)
		oci8cols[i].size = int(lp)
		oci8cols[i].pbuf = make([]byte, int(lp)+1)

		var defp *C.OCIDefine
		rv = C.OCIDefineByPos(
			(*C.OCIStmt)(s.s),
			&defp,
			(*C.OCIError)(s.c.err),
			C.ub4(i+1),
			unsafe.Pointer(&oci8cols[i].pbuf[0]),
			C.sb4(lp+1),
			C.SQLT_CHR,
			nil,
			nil,
			nil,
			C.OCI_DEFAULT)
		if rv == C.OCI_ERROR {
			return nil, ociGetError(s.c.err)
		}
	}
	return &OCI8Rows{s, oci8cols, false}, nil
}

type OCI8Result struct {
	s *OCI8Stmt
}

func (r *OCI8Result) LastInsertId() (int64, error) {
	var t C.ub4
	rv := C.OCIAttrGet(
		r.s.s,
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&t),
		nil,
		C.OCI_ATTR_ROWID,
		(*C.OCIError)(r.s.c.err))
	if rv == C.OCI_ERROR {
		return 0, ociGetError(r.s.c.err)
	}
	return int64(t), nil
}

func (r *OCI8Result) RowsAffected() (int64, error) {
	var t C.ub4
	rv := C.OCIAttrGet(
		r.s.s,
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&t),
		nil,
		C.OCI_ATTR_ROW_COUNT,
		(*C.OCIError)(r.s.c.err))
	if rv == C.OCI_ERROR {
		return 0, ociGetError(r.s.c.err)
	}
	return int64(t), nil
}

func (s *OCI8Stmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}

	rv := C.OCIStmtExecute(
		(*C.OCIServer)(s.c.svc),
		(*C.OCIStmt)(s.s),
		(*C.OCIError)(s.c.err),
		1,
		0,
		nil,
		nil,
		C.OCI_DEFAULT)
	if rv == C.OCI_ERROR {
		return nil, ociGetError(s.c.err)
	}
	return &OCI8Result{s}, nil
}

type oci8col struct {
	name string
	kind int
	size int
	pbuf []byte
}

type OCI8Rows struct {
	s    *OCI8Stmt
	cols []oci8col
	e    bool
}

func (rc *OCI8Rows) Close() error {
	return rc.s.Close()
}

func (rc *OCI8Rows) Columns() []string {
	cols := make([]string, len(rc.cols))
	for i, col := range rc.cols {
		cols[i] = col.name
	}
	return cols
}

func (rc *OCI8Rows) Next(dest []driver.Value) error {
	rv := C.OCIStmtFetch(
		(*C.OCIStmt)(rc.s.s),
		(*C.OCIError)(rc.s.c.err),
		1,
		C.OCI_FETCH_NEXT,
		C.OCI_DEFAULT)
	if rv == C.OCI_ERROR {
		return ociGetError(rc.s.c.err)
	}

	if rv == C.OCI_NO_DATA {
		return io.EOF
	}

	for i := range dest {
		dest[i] = string(rc.cols[i].pbuf)
	}

	return nil
}

func ociGetError(err unsafe.Pointer) error {
	var errcode C.sb4
	var errbuff [512]C.char
	C.OCIErrorGet(
		err,
		1,
		nil,
		&errcode,
		(*C.OraText)(unsafe.Pointer(&errbuff[0])),
		512,
		C.OCI_HTYPE_ERROR)
	s := C.GoString(&errbuff[0])
	//println(s)
	return errors.New(s)
}
