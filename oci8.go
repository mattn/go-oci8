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
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type DSN struct {
	Host     string
	Port     int
	Username string
	Password string
	SID      string
	Location *time.Location
}

func init() {
	sql.Register("oci8", &OCI8Driver{})
}

type OCI8Driver struct {
}

type OCI8Conn struct {
	svc      unsafe.Pointer
	env      unsafe.Pointer
	err      unsafe.Pointer
	attrs    Values
	location *time.Location
}

type OCI8Tx struct {
	c *OCI8Conn
}

type Values map[string]interface{}

func (vs Values) Set(k string, v interface{}) {
	vs[k] = v
}

func (vs Values) Get(k string) (v interface{}) {
	v, _ = vs[k]
	return
}

//ParseDSN parses a DSN used to connect to Oracle
//It expects to receive a string in the form:
//user:password@host:port/sid?param1=value1&param2=value2
//
//Currently the only parameter supported is 'loc' which
//sets the timezone to read times in as and to marshal to when writing times to
//Oracle
func ParseDSN(dsnString string) (dsn *DSN, err error) {
	var (
		params string
	)

	dsn = &DSN{Location: time.Local}

	dsnString = strings.Replace(dsnString, "/", " / ", 2)
	dsnString = strings.Replace(dsnString, "@", " @ ", 1)
	dsnString = strings.Replace(dsnString, ":", " : ", 1)
	dsnString = strings.Replace(dsnString, "?", " ?", 1)

	if _, err = fmt.Sscanf(dsnString, "%s / %s @ %s : %d / %s", &dsn.Username, &dsn.Password, &dsn.Host, &dsn.Port, &dsn.SID); err != nil {
		panic(err)
	}

	if i := strings.Index(dsnString, "?"); i != -1 {
		params = dsnString[i+1:]
	}

	if len(params) > 0 {
		for _, v := range strings.Split(params, "&") {
			param := strings.SplitN(v, "=", 2)
			if len(param) != 2 {
				continue
			}

			if param[1], err = url.QueryUnescape(param[1]); err != nil {
				panic(err)
			}

			switch param[0] {
			case "loc":
				if dsn.Location, err = time.LoadLocation(param[1]); err != nil {
					return nil, err
				}
			}
		}
	}

	return dsn, nil
}

func (tx *OCI8Tx) Commit() error {
	rv := C.OCITransCommit(
		(*C.OCIServer)(tx.c.svc),
		(*C.OCIError)(tx.c.err),
		0)
	if rv == C.OCI_ERROR {
		return ociGetError(tx.c.err)
	}
	return nil
}

func (tx *OCI8Tx) Rollback() error {
	rv := C.OCITransRollback(
		(*C.OCIServer)(tx.c.svc),
		(*C.OCIError)(tx.c.err),
		0)
	if rv == C.OCI_ERROR {
		return ociGetError(tx.c.err)
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
	rv := C.OCITransStart(
		(*C.OCIServer)(c.svc),
		(*C.OCIError)(c.err),
		60,
		C.OCI_TRANS_NEW)
	if rv == C.OCI_ERROR {
		return nil, ociGetError(c.err)
	}
	return &OCI8Tx{c}, nil
}

func (d *OCI8Driver) Open(dsnString string) (connection driver.Conn, err error) {
	var (
		conn OCI8Conn
		dsn  *DSN
	)

	if dsn, err = ParseDSN(dsnString); err != nil {
		return nil, err
	}

	// set safe defaults
	conn.attrs = make(Values)
	conn.attrs.Set("prefetch_rows", 10)
	conn.attrs.Set("prefetch_memory", int64(0))

	for k, v := range parseEnviron(os.Environ()) {
		conn.attrs.Set(k, v)
	}

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

	phost := C.CString(fmt.Sprintf("%s:%d/%s", dsn.Host, dsn.Port, dsn.SID))
	defer C.free(unsafe.Pointer(phost))
	phostlen := C.strlen(phost)
	puser := C.CString(dsn.Username)
	defer C.free(unsafe.Pointer(puser))
	ppass := C.CString(dsn.Password)
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

	conn.location = dsn.Location

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

func (s *OCI8Stmt) bind(args []driver.Value) (freeBoundParameters func(), err error) {
	if args == nil {
    return func() {}, nil
	}

	var (
		bp              *C.OCIBind
		dty             int
		data            []byte
		cdata           *C.char
		boundParameters []*C.char
	)

	freeBoundParameters = func() {
		for _, p := range boundParameters {
			C.free(unsafe.Pointer(p))
		}
	}

	for i, v := range args {
		data = []byte{}

		switch v.(type) {
		case nil:
			dty = C.SQLT_STR
			data = []byte{0}
		case time.Time:
			dty = C.SQLT_DAT
			now := v.(time.Time).In(s.c.location)
			//TODO Handle BCE dates (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#438305)
			//TODO Handle timezones (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#443601)
			data = []byte{
				byte(now.Year()/100 + 100),
				byte(now.Year()%100 + 100),
				byte(now.Month()),
				byte(now.Day()),
				byte(now.Hour() + 1),
				byte(now.Minute() + 1),
				byte(now.Second() + 1),
			}
		default:
			dty = C.SQLT_STR
			data = []byte(fmt.Sprintf("%v", v))
			data = append(data, 0)
		}

		cdata = C.CString(string(data))
		boundParameters = append(boundParameters, cdata)
		rv := C.OCIBindByPos(
			(*C.OCIStmt)(s.s),
			&bp,
			(*C.OCIError)(s.c.err),
			C.ub4(i+1),
			unsafe.Pointer(cdata),
			C.sb4(len(data)),
			C.ub2(dty),
			nil,
			nil,
			nil,
			0,
			nil,
			C.OCI_DEFAULT)

		if rv == C.OCI_ERROR {
			defer freeBoundParameters()
			return nil, ociGetError(s.c.err)
		}
	}
	return freeBoundParameters, nil
}

func (s *OCI8Stmt) Query(args []driver.Value) (rows driver.Rows, err error) {
	var (
		freeBoundParameters func()
	)

	if freeBoundParameters, err = s.bind(args); err != nil {
		return nil, err
	}

	defer freeBoundParameters()

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

	// set the row prefetch.  Only one extra row per fetch will be returned unless this is set.
	prefetch_size := C.ub4(s.c.attrs.Get("prefetch_rows").(int))
	C.OCIAttrSet(s.s, C.OCI_HTYPE_STMT, unsafe.Pointer(&prefetch_size), 0, C.OCI_ATTR_PREFETCH_ROWS, (*C.OCIError)(s.c.err))

	// if non-zero, oci will fetch rows until the memory limit or row prefetch limit is hit.
	// useful for memory constrained systems
	prefetch_memory := C.ub4(s.c.attrs.Get("prefetch_memory").(int64))
	C.OCIAttrSet(s.s, C.OCI_HTYPE_STMT, unsafe.Pointer(&prefetch_memory), 0, C.OCI_ATTR_PREFETCH_MEMORY, (*C.OCIError)(s.c.err))

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

		switch tp {
		case C.SQLT_NUM:
			oci8cols[i].kind = C.SQLT_CHR
		default:
			oci8cols[i].kind = tp
		}
		oci8cols[i].name = string((*[1 << 30]byte)(unsafe.Pointer(np))[0:int(ns)])
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
			oci8cols[i].kind,
			unsafe.Pointer(&oci8cols[i].ind),
			&oci8cols[i].rlen,
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

func (s *OCI8Stmt) Exec(args []driver.Value) (r driver.Result, err error) {
	var (
		freeBoundParameters func()
	)

	if freeBoundParameters, err = s.bind(args); err != nil {
		return nil, err
	}

	defer freeBoundParameters()

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
	kind C.ub2
	size int
	ind  C.sb2
	rlen C.ub2
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
		err := ociGetError(rc.s.c.err)
		if err.Error()[:9] != "ORA-01405" {
			return err
		}
	}

	if rv == C.OCI_NO_DATA {
		return io.EOF
	}
	for i := range dest {
		if rc.cols[i].ind == -1 { //Null
			dest[i] = nil
			continue
		}

		buf := rc.cols[i].pbuf
		switch rc.cols[i].kind {
		case C.SQLT_DAT:
			//TODO Handle BCE dates (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#438305)
			//TODO Handle timezones (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#443601)
			dest[i] = time.Date(((int(buf[0])-100)*100)+(int(buf[1])-100), time.Month(int(buf[2])), int(buf[3]), int(buf[4])-1, int(buf[5])-1, int(buf[6])-1, 0, rc.s.c.location)
		case C.SQLT_CHR:
			switch {
			case rc.cols[i].ind == 0: //Normal
				dest[i] = string(buf)[0:rc.cols[i].rlen]
			case rc.cols[i].ind == -2 || //Field longer than type (truncated)
				rc.cols[i].ind > 0: //Field longer than type (truncated). Value is original length.
				dest[i] = string(buf)
			default:
				return errors.New(fmt.Sprintf("Unknown column indicator: %d", rc.cols[i].ind))
			}
		default:
			return errors.New(fmt.Sprintf("Unhandled column type: %d", rc.cols[i].kind))
		}
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

func parseEnviron(env []string) (out map[string]interface{}) {
	out = make(map[string]interface{})

	for _, v := range env {
		parts := strings.SplitN(v, "=", 2)

		// Better to have a type error here than later during query execution
		switch parts[0] {
		case "PREFETCH_ROWS":
			out["prefetch_rows"], _ = strconv.Atoi(parts[1])
		case "PREFETCH_MEMORY":
			out["prefetch_memory"], _ = strconv.ParseInt(parts[1], 10, 64)
		}
	}
	return out
}
