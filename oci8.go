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
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

const blobBufSize = 4000

type DSN struct {
	Host            string
	Port            int
	Username        string
	Password        string
	SID             string
	Location        *time.Location
	transactionMode C.ub4
}

func init() {
	sql.Register("oci8", &OCI8Driver{})
}

type OCI8Driver struct {
}

type OCI8Conn struct {
	svc             unsafe.Pointer
	env             unsafe.Pointer
	err             unsafe.Pointer
	attrs           Values
	location        *time.Location
	transactionMode C.ub4
	inTransaction   bool
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
//Currently the parameters supported is:
//1 'loc' which
//sets the timezone to read times in as and to marshal to when writing times to
//Oracle,
//2 'isolation' =READONLY,SERIALIZABLE,DEFAULT
func ParseDSN(dsnString string) (dsn *DSN, err error) {
	rs := []byte(dsnString)
break_loop:
	for i, r := range rs {
		if r == '/' {
			rs[i] = ':'
			dsnString = string(rs)
			break break_loop
		}
		if r == '@' {
			break break_loop
		}
	}

	if !strings.HasPrefix(dsnString, "oracle://") {
		dsnString = "oracle://" + dsnString
	}
	u, err := url.Parse(dsnString)
	if err != nil {
		return nil, err
	}
	dsn = &DSN{Location: time.Local}

	if u.User != nil {
		dsn.Username = u.User.Username()
		password, ok := u.User.Password()
		if ok {
			dsn.Password = password
		} else {
			if tok := strings.SplitN(dsn.Username, "/", 2); len(tok) >= 2 {
				dsn.Username = tok[0]
				dsn.Password = tok[1]
			}
		}
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		if err.Error() == "missing port in address" {
			return nil, fmt.Errorf("Invalid DSN: %v", err)
		}
		port = "0"
	}
	dsn.Host = host
	nport, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("Invalid DSN: %v", err)
	}
	dsn.Port = nport
	if u.Path != "" {
		dsn.SID = strings.Trim(u.Path, "/")
	} else if u.Host != "" {
		dsn.SID = u.Host
	}

	for k, v := range u.Query() {
		switch k {
		case "loc":
			if len(v) > 0 {
				if dsn.Location, err = time.LoadLocation(v[0]); err != nil {
					return nil, fmt.Errorf("Invalid loc: %v: %v", v[0], err)
				}
			}
		case "isolation":
			switch v[0] {
			case "READONLY":
				dsn.transactionMode = C.OCI_TRANS_READONLY
			case "SERIALIZABLE":
				dsn.transactionMode = C.OCI_TRANS_SERIALIZABLE
			case "DEFAULT":
				dsn.transactionMode = C.OCI_TRANS_READWRITE
			default:
				return nil, fmt.Errorf("Invalid isolation: %v", v[0])
			}
		}

	}
	return dsn, nil
}

func (tx *OCI8Tx) Commit() error {
	tx.c.inTransaction = false
	if rv := C.OCITransCommit(
		(*C.OCISvcCtx)(tx.c.svc),
		(*C.OCIError)(tx.c.err),
		0); rv != C.OCI_SUCCESS {
		return ociGetError(tx.c.err)
	}
	return nil
}

func (tx *OCI8Tx) Rollback() error {
	tx.c.inTransaction = false
	if rv := C.OCITransRollback(
		(*C.OCISvcCtx)(tx.c.svc),
		(*C.OCIError)(tx.c.err),
		0); rv != C.OCI_SUCCESS {
		return ociGetError(tx.c.err)
	}
	return nil
}

func (c *OCI8Conn) Begin() (driver.Tx, error) {
	if c.transactionMode != C.OCI_TRANS_READWRITE {
		var th unsafe.Pointer
		if rv := C.OCIHandleAlloc(
			c.env,
			&c.err,
			C.OCI_HTYPE_TRANS,
			0,
			nil); rv != C.OCI_SUCCESS {
			return nil, errors.New("cant  allocate  handle")
		} else {
			th = unsafe.Pointer(&rv)
		}
		if rv := C.OCIAttrSet(
			c.svc,
			C.OCI_HTYPE_SVCCTX,
			th,
			0,
			C.OCI_ATTR_TRANS,
			(*C.OCIError)(c.err)); rv != C.OCI_SUCCESS {
			return nil, ociGetError(c.err)
		}

		if rv := C.OCITransStart(
			(*C.OCISvcCtx)(c.svc),
			(*C.OCIError)(c.err),
			0,
			c.transactionMode); // C.OCI_TRANS_SERIALIZABLE C.OCI_TRANS_READWRITE C.OCI_TRANS_READONLY
		rv != C.OCI_SUCCESS {
			return nil, ociGetError(c.err)
		}
	}
	c.inTransaction = true
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

	if rv := C.OCIEnvInit(
		(**C.OCIEnv)(unsafe.Pointer(&conn.env)),
		C.OCI_DEFAULT|C.OCI_THREADED,
		0,
		nil); rv != C.OCI_SUCCESS {
		return nil, ociGetError(conn.err)
	}

	if rv := C.OCIHandleAlloc(
		conn.env,
		&conn.err,
		C.OCI_HTYPE_ERROR,
		0,
		nil); rv != C.OCI_SUCCESS {
		return nil, ociGetError(conn.err)
	}

	var host string
	if dsn.Host != "" && dsn.SID != "" {
		host = fmt.Sprintf("%s:%d/%s", dsn.Host, dsn.Port, dsn.SID)
	} else {
		host = dsn.SID
	}
	phost := C.CString(host)
	defer C.free(unsafe.Pointer(phost))
	phostlen := C.strlen(phost)
	puser := C.CString(dsn.Username)
	defer C.free(unsafe.Pointer(puser))
	ppass := C.CString(dsn.Password)
	defer C.free(unsafe.Pointer(ppass))

	if rv := C.OCILogon(
		(*C.OCIEnv)(conn.env),
		(*C.OCIError)(conn.err),
		(**C.OCISvcCtx)(unsafe.Pointer(&conn.svc)),
		(*C.OraText)(unsafe.Pointer(puser)),
		C.ub4(C.strlen(puser)),
		(*C.OraText)(unsafe.Pointer(ppass)),
		C.ub4(C.strlen(ppass)),
		(*C.OraText)(unsafe.Pointer(phost)),
		C.ub4(phostlen)); rv != C.OCI_SUCCESS {
		return nil, ociGetError(conn.err)
	}
	conn.location = dsn.Location
	conn.transactionMode = dsn.transactionMode
	return &conn, nil
}

func (c *OCI8Conn) Close() error {
	var err error
	if rv := C.OCILogoff(
		(*C.OCISvcCtx)(c.svc),
		(*C.OCIError)(c.err)); rv != C.OCI_SUCCESS {
		err = ociGetError(c.err)
	}

	C.OCIHandleFree(
		c.env,
		C.OCI_HTYPE_ENV)

	c.svc = nil
	c.env = nil
	c.err = nil
	return err
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

	if rv := C.OCIHandleAlloc(
		c.env,
		&s,
		C.OCI_HTYPE_STMT,
		0,
		nil); rv != C.OCI_SUCCESS {
		return nil, ociGetError(c.err)
	}

	if rv := C.OCIStmtPrepare(
		(*C.OCIStmt)(s),
		(*C.OCIError)(c.err),
		(*C.OraText)(unsafe.Pointer(pquery)),
		C.ub4(C.strlen(pquery)),
		C.ub4(C.OCI_NTV_SYNTAX),
		C.ub4(C.OCI_DEFAULT)); rv != C.OCI_SUCCESS {
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
		dty             C.ub2
		data            []byte
		cdata           *C.char
		boundParameters []oci8bind
	)

	freeBoundParameters = func() {
		for _, col := range boundParameters {
			if col.pbuf != nil {
				if col.kind == C.SQLT_CLOB || col.kind == C.SQLT_BLOB {
					C.OCIDescriptorFree(
						col.pbuf,
						C.OCI_DTYPE_LOB)
				} else {
					C.free(col.pbuf)
				}
			}
		}
	}

	for i, v := range args {
		data = []byte{}

		switch v.(type) {
		case nil:
			dty = C.SQLT_STR
			boundParameters = append(boundParameters, oci8bind{dty, nil})
			if rv := C.OCIBindByPos(
				(*C.OCIStmt)(s.s),
				&bp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				nil,
				0,
				dty,
				nil,
				nil,
				nil,
				0,
				nil,
				C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}
		case []byte:
			// FIXME: Currently, CLOB not supported
			dty = C.SQLT_BLOB
			data = v.([]byte)
			var bamt C.ub4
			var pbuf unsafe.Pointer
			if rv := C.OCIDescriptorAlloc(
				s.c.env,
				&pbuf,
				C.OCI_DTYPE_LOB,
				0,
				nil); rv != C.OCI_SUCCESS {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}

			if rv := C.OCILobCreateTemporary(
				(*C.OCISvcCtx)(s.c.svc),
				(*C.OCIError)(s.c.err),
				(*C.OCILobLocator)(pbuf),
				0,
				C.SQLCS_IMPLICIT,
				C.OCI_TEMP_BLOB,
				C.OCI_ATTR_NOCACHE,
				C.OCI_DURATION_SESSION); rv != C.OCI_SUCCESS {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}

			bamt = C.ub4(len(data))
			if rv := C.OCILobWrite(
				(*C.OCISvcCtx)(s.c.svc),
				(*C.OCIError)(s.c.err),
				(*C.OCILobLocator)(pbuf),
				&bamt,
				1,
				unsafe.Pointer(&data[0]),
				C.ub4(len(data)),
				C.OCI_ONE_PIECE,
				nil,
				nil,
				0,
				C.SQLCS_IMPLICIT); rv != C.OCI_SUCCESS {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}
			boundParameters = append(boundParameters, oci8bind{dty, pbuf})
			if rv := C.OCIBindByPos(
				(*C.OCIStmt)(s.s),
				&bp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer(&pbuf),
				0,
				dty,
				nil,
				nil,
				nil,
				0,
				nil,
				C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}
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

			cdata = C.CString(string(data))
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})
			if rv := C.OCIBindByPos(
				(*C.OCIStmt)(s.s),
				&bp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer(cdata),
				C.sb4(len(data)),
				dty,
				nil,
				nil,
				nil,
				0,
				nil,
				C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}
		default:
			dty = C.SQLT_STR
			data = []byte(fmt.Sprintf("%v", v))
			data = append(data, 0)

			cdata = C.CString(string(data))
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})
			if rv := C.OCIBindByPos(
				(*C.OCIStmt)(s.s),
				&bp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer(cdata),
				C.sb4(len(data)),
				dty,
				nil,
				nil,
				nil,
				0,
				nil,
				C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}
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

	mode := C.ub4(C.OCI_DEFAULT)
	if s.c.inTransaction == false {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
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

	// set the row prefetch.  Only one extra row per fetch will be returned unless this is set.
	prefetch_size := C.ub4(s.c.attrs.Get("prefetch_rows").(int))
	C.OCIAttrSet(s.s, C.OCI_HTYPE_STMT, unsafe.Pointer(&prefetch_size), 0, C.OCI_ATTR_PREFETCH_ROWS, (*C.OCIError)(s.c.err))

	// if non-zero, oci will fetch rows until the memory limit or row prefetch limit is hit.
	// useful for memory constrained systems
	prefetch_memory := C.ub4(s.c.attrs.Get("prefetch_memory").(int64))
	C.OCIAttrSet(s.s, C.OCI_HTYPE_STMT, unsafe.Pointer(&prefetch_memory), 0, C.OCI_ATTR_PREFETCH_MEMORY, (*C.OCIError)(s.c.err))

	mode = C.ub4(C.OCI_DEFAULT)
	if !s.c.inTransaction {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
	}
	if rv := C.OCIStmtExecute(
		(*C.OCISvcCtx)(s.c.svc),
		(*C.OCIStmt)(s.s),
		(*C.OCIError)(s.c.err),
		iter,
		0,
		nil,
		nil,
		mode); rv != C.OCI_SUCCESS {
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

		var defp *C.OCIDefine
		if tp == C.SQLT_CLOB || tp == C.SQLT_BLOB {
			if rv := C.OCIDescriptorAlloc(
				s.c.env,
				&oci8cols[i].pbuf,
				C.OCI_DTYPE_LOB,
				0,
				nil); rv != C.OCI_SUCCESS {
				return nil, ociGetError(s.c.err)
			}
			if rv := C.OCIDefineByPos(
				(*C.OCIStmt)(s.s),
				&defp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer(&oci8cols[i].pbuf),
				-1,
				oci8cols[i].kind,
				unsafe.Pointer(&oci8cols[i].ind),
				&oci8cols[i].rlen,
				nil,
				C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
				return nil, ociGetError(s.c.err)
			}
		} else {
			oci8cols[i].pbuf = C.malloc(C.size_t(lp) + 1)
			if rv := C.OCIDefineByPos(
				(*C.OCIStmt)(s.s),
				&defp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				oci8cols[i].pbuf,
				C.sb4(lp+1),
				oci8cols[i].kind,
				unsafe.Pointer(&oci8cols[i].ind),
				&oci8cols[i].rlen,
				nil,
				C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
				return nil, ociGetError(s.c.err)
			}
		}
	}
	return &OCI8Rows{s, oci8cols, false}, nil
}

// OCI_ATTR_ROWID must be get in handle -> alloc
// can be coverted to char, but not to int64

/*
func (s *OCI8Stmt) lastInsertId() (int64, error) {
	retUb4 := C.OCIAttrGetUb4(s.s, C.OCI_HTYPE_STMT, C.OCI_ATTR_ROWID, (*C.OCIError)(s.c.err))
	if retUb4.rv != C.OCI_SUCCESS {
		return 0, ociGetError(s.c.err)
	}
	return int64(retUb4.num), nil
}
*/

func (s *OCI8Stmt) lastInsertId() (int64, error) {
	var t C.ub4
	if rv := C.OCIAttrGet(
		s.s,
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&t),
		nil,
		C.OCI_ATTR_ROWID,
		(*C.OCIError)(s.c.err)); rv != C.OCI_SUCCESS {
		return 0, ociGetError(s.c.err)
	}
	return int64(t), nil
}

/*
func (s *OCI8Stmt) rowsAffected() (int64, error) {
	retUb4 := C.OCIAttrGetUb4(s.s, C.OCI_HTYPE_STMT, C.OCI_ATTR_ROW_COUNT, (*C.OCIError)(s.c.err))
	if retUb4.rv != C.OCI_SUCCESS {
		return 0, ociGetError(s.c.err)
	}
	return int64(retUb4.num), nil
}
*/

func (s *OCI8Stmt) rowsAffected() (int64, error) {
	var t C.ub4
	if rv := C.OCIAttrGet(
		s.s,
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&t),
		nil,
		C.OCI_ATTR_ROW_COUNT,
		(*C.OCIError)(s.c.err)); rv != C.OCI_SUCCESS {
		return 0, ociGetError(s.c.err)
	}
	return int64(t), nil
}

type OCI8Result struct {
	n     int64
	errn  error
	id    int64
	errid error
}

func (r *OCI8Result) LastInsertId() (int64, error) {
	return r.id, r.errid
}

func (r *OCI8Result) RowsAffected() (int64, error) {
	return r.n, r.errn
}

func (s *OCI8Stmt) Exec(args []driver.Value) (r driver.Result, err error) {
	var (
		freeBoundParameters func()
	)

	if freeBoundParameters, err = s.bind(args); err != nil {
		return nil, err
	}

	defer freeBoundParameters()

	mode := C.ub4(C.OCI_DEFAULT)
	if s.c.inTransaction == false {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
	}

	rv := C.OCIStmtExecute(
		(*C.OCISvcCtx)(s.c.svc),
		(*C.OCIStmt)(s.s),
		(*C.OCIError)(s.c.err),
		1,
		0,
		nil,
		nil,
		mode)
	if rv != C.OCI_SUCCESS {
		return nil, ociGetError(s.c.err)
	}
	n, en := s.rowsAffected()
	id, ei := s.lastInsertId()
	return &OCI8Result{n: n, errn: en, id: id, errid: ei}, nil
}

type oci8col struct {
	name string
	kind C.ub2
	size int
	ind  C.sb2
	rlen C.ub2
	pbuf unsafe.Pointer
}

type oci8bind struct {
	kind C.ub2
	pbuf unsafe.Pointer
}

type OCI8Rows struct {
	s    *OCI8Stmt
	cols []oci8col
	e    bool
}

func freeDecriptor(p unsafe.Pointer, dtype C.ub4) {
	tptr := *(*unsafe.Pointer)(p)
	C.OCIDescriptorFree(unsafe.Pointer(tptr), dtype)
}

func (rc *OCI8Rows) Close() error {
	for _, col := range rc.cols {
		switch col.kind {
		case C.SQLT_CLOB, C.SQLT_BLOB:
			freeDecriptor(col.pbuf, C.OCI_DTYPE_LOB)
		case C.SQLT_TIMESTAMP:
			freeDecriptor(col.pbuf, C.OCI_DTYPE_TIMESTAMP)
		case C.SQLT_TIMESTAMP_TZ:
			freeDecriptor(col.pbuf, C.OCI_DTYPE_TIMESTAMP_TZ)
		case C.SQLT_INTERVAL_DS:
			freeDecriptor(col.pbuf, C.OCI_DTYPE_INTERVAL_DS)
		case C.SQLT_INTERVAL_YM:
			freeDecriptor(col.pbuf, C.OCI_DTYPE_INTERVAL_YM)
		default:
			C.free(col.pbuf)
		}
	}
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

	if rv == C.OCI_NO_DATA {
		return io.EOF
	} else if rv != C.OCI_SUCCESS {
		return ociGetError(rc.s.c.err)
	}

	for i := range dest {
		if rc.cols[i].ind == -1 { //Null
			dest[i] = nil
			continue
		}

		switch rc.cols[i].kind {
		case C.SQLT_DAT: //for test, date are return as timestamp
			buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:rc.cols[i].rlen]
			//TODO Handle BCE dates (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#438305)
			//TODO Handle timezones (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#443601)
			dest[i] = time.Date(
				(int(buf[0])-100)*100+(int(buf[1])-100),
				time.Month(int(buf[2])),
				int(buf[3]),
				int(buf[4])-1,
				int(buf[5])-1,
				int(buf[6])-1,
				0,
				rc.s.c.location)
		case C.SQLT_BLOB, C.SQLT_CLOB:
			ptmp := unsafe.Pointer(uintptr(rc.cols[i].pbuf) + unsafe.Sizeof(unsafe.Pointer(nil)))
			bamt := (*C.ub4)(ptmp)
			ptmp = unsafe.Pointer(uintptr(rc.cols[i].pbuf) + unsafe.Sizeof(C.ub4(0)) + unsafe.Sizeof(unsafe.Pointer(nil)))
			b := (*[1 << 30]byte)(ptmp)[0:blobBufSize]
			var buf []byte
		again:
			rv = C.OCILobRead(
				(*C.OCISvcCtx)(rc.s.c.svc),
				(*C.OCIError)(rc.s.c.err),
				*(**C.OCILobLocator)(rc.cols[i].pbuf),
				bamt,
				1,
				ptmp,
				C.ub4(blobBufSize),
				nil,
				nil,
				0,
				C.SQLCS_IMPLICIT)
			if rv == C.OCI_NEED_DATA {
				buf = append(buf, b[:int(*bamt)]...)
				goto again
			}
			if rv != C.OCI_SUCCESS {
				return ociGetError(rc.s.c.err)
			}
			//dest[i] = append(buf, b[:int(*bamt)]...)//buf//b[:total+int(bamt)]
			if rc.cols[i].kind == C.SQLT_BLOB {
				dest[i] = append(buf, b[:int(*bamt)]...)
			} else {
				dest[i] = string(append(buf, b[:int(*bamt)]...))
			}
		case C.SQLT_CHR, C.SQLT_AFC, C.SQLT_AVC:
			buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:rc.cols[i].rlen]
			switch {
			case rc.cols[i].ind == 0: //Normal
				dest[i] = string(buf)
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
