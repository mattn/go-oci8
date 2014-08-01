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
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

const (
	// ORA-01405: fetched column value is NULL
	ERR_COLUMN_VALUE_IS_NULL = 1405
)

type DSN struct {
	Host     string
	Port     int
	Username string
	Password string
	SID      string
	Location *time.Location
	LogDebug bool
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
	logDebug bool
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
		dsn.Host = ""
		dsn.Port = 0
		if _, err = fmt.Sscanf(dsnString, "%s / %s @ %s", &dsn.Username, &dsn.Password, &dsn.SID); err != nil {
			return nil, errors.New("Invalid DSN")
		}
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
			case "debug":
				val, err := strconv.ParseBool(param[1])
				if err == nil {
					dsn.LogDebug = val
				}
			}
		}
	}

	return dsn, nil
}

func (tx *OCI8Tx) Commit() error {
	rv := C.OCITransCommit(
		(*C.OCISvcCtx)(tx.c.svc),
		(*C.OCIError)(tx.c.err),
		0)
	if err := tx.c.check(rv, "OCI8Tx.Commit"); err != nil {
		return err
	}

	return nil
}

func (tx *OCI8Tx) Rollback() error {
	rv := C.OCITransRollback(
		(*C.OCISvcCtx)(tx.c.svc),
		(*C.OCIError)(tx.c.err),
		0)
	if err := tx.c.check(rv, "OCI8Tx.Rollback"); err != nil {
		return err
	}
	return nil
}

func (c *OCI8Conn) debug(format string, args ...interface{}) {
	if c.logDebug {
		log.Printf("oci8 "+format, args...)
	}
}

func (c *OCI8Conn) check(rv C.sword, context string) error {
	c.debug("context=%s rv=%v\n", context, rv)
	if rv != C.OCI_SUCCESS && rv != C.OCI_SUCCESS_WITH_INFO {
		if rv != C.OCI_INVALID_HANDLE {
			err := ociGetError(c.err, context)
			c.debug("got error %#v\n", err)
			return err
		}
		c.debug("invalid handle\n")
		return fmt.Errorf("Invalid handler at %s", context)
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
	/*
		http://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci08sca.htm#i431599
		Many applications work with only simple local transactions.
		In these applications, an implicit transaction is created
		when the application makes database changes.
		The only transaction-specific calls needed by such applications are:
			OCITransCommit() - to commit the transaction
			OCITransRollback() - to roll back the transaction
	*/
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

	// https://github.com/mattn/go-oci8/issues/22
	rv := C.OCIEnvCreate(
		(**C.OCIEnv)(unsafe.Pointer(&conn.env)),
		C.OCI_THREADED,
		nil,
		nil,
		nil,
		nil,
		0,
		nil)
	if err := conn.check(rv, "OCIEnvCreate"); err != nil {
		return nil, err
	}

	rv = C.OCIHandleAlloc(
		conn.env,
		&conn.err,
		C.OCI_HTYPE_ERROR,
		0,
		nil)
	if err := conn.check(rv, "OCIHandleAlloc"); err != nil {
		return nil, err
	}

	var phost *C.char
	if dsn.Host != "" {
		phost = C.CString(fmt.Sprintf("%s:%d/%s", dsn.Host, dsn.Port, dsn.SID))
	} else {
		phost = C.CString(dsn.SID)
	}
	defer C.free(unsafe.Pointer(phost))
	phostlen := C.strlen(phost)
	puser := C.CString(dsn.Username)
	defer C.free(unsafe.Pointer(puser))
	ppass := C.CString(dsn.Password)
	defer C.free(unsafe.Pointer(ppass))

	rv = C.OCILogon(
		(*C.OCIEnv)(conn.env),
		(*C.OCIError)(conn.err),
		(**C.OCISvcCtx)(unsafe.Pointer(&conn.svc)),
		(*C.OraText)(unsafe.Pointer(puser)),
		C.ub4(C.strlen(puser)),
		(*C.OraText)(unsafe.Pointer(ppass)),
		C.ub4(C.strlen(ppass)),
		(*C.OraText)(unsafe.Pointer(phost)),
		C.ub4(phostlen))
	if err := conn.check(rv, "OCILogon"); err != nil {
		return nil, err
	}

	conn.location = dsn.Location
	conn.logDebug = dsn.LogDebug

	return &conn, nil
}

func (c *OCI8Conn) Close() error {
	rv := C.OCILogoff(
		(*C.OCISvcCtx)(c.svc),
		(*C.OCIError)(c.err))

	if err := c.check(rv, "OCILogoff"); err != nil {
		return err
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

	if err := c.check(rv, "Prepare.OCIHandleAlloc"); err != nil {
		return nil, err
	}

	rv = C.OCIStmtPrepare(
		(*C.OCIStmt)(s),
		(*C.OCIError)(c.err),
		(*C.OraText)(unsafe.Pointer(pquery)),
		C.ub4(C.strlen(pquery)),
		C.ub4(C.OCI_NTV_SYNTAX),
		C.ub4(C.OCI_DEFAULT))

	if err := c.check(rv, "Prepare.OCIStmtPrepare"); err != nil {
		return nil, err
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
			rv := C.OCIBindByPos(
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
				C.OCI_DEFAULT)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err, "OCI8Stmt.bind str")
			}
		case []byte:
			// FIXME: Currently, CLOB not supported
			dty = C.SQLT_BLOB
			data = v.([]byte)
			var bamt C.ub4
			var pbuf unsafe.Pointer
			rv := C.OCIDescriptorAlloc(
				s.c.env,
				&pbuf,
				C.OCI_DTYPE_LOB,
				0,
				nil)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err, "OCI8Stmt.bind lob - alloc")
			}

			rv = C.OCILobCreateTemporary(
				(*C.OCISvcCtx)(s.c.svc),
				(*C.OCIError)(s.c.err),
				(*C.OCILobLocator)(pbuf),
				0,
				C.SQLCS_IMPLICIT,
				C.OCI_TEMP_BLOB,
				C.OCI_ATTR_NOCACHE,
				C.OCI_DURATION_SESSION)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err, "OCI8Stmt.bind lob - create temp")
			}

			bamt = C.ub4(len(data))
			rv = C.OCILobWrite(
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
				C.SQLCS_IMPLICIT)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err, "OCI8Stmt.bind lob - write")
			}
			boundParameters = append(boundParameters, oci8bind{dty, pbuf})
			rv = C.OCIBindByPos(
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
				C.OCI_DEFAULT)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err, "OCI8Stmt.bind lob - bind")
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
			rv := C.OCIBindByPos(
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
				C.OCI_DEFAULT)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err, "OCI8Stmt.bind date")
			}
		default:
			dty = C.SQLT_STR
			data = []byte(fmt.Sprintf("%v", v))
			data = append(data, 0)

			cdata = C.CString(string(data))
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})
			rv := C.OCIBindByPos(
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
				C.OCI_DEFAULT)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err, "OCI8Stmt.bind other")
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
		(*C.OCISvcCtx)(s.c.svc),
		(*C.OCIStmt)(s.s),
		(*C.OCIError)(s.c.err),
		iter,
		0,
		nil,
		nil,
		C.OCI_DEFAULT)

	if err := s.c.check(rv, "OCI8Stmt.Query.OCIStmtExecute"); err != nil {
		return nil, err
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
			rv = C.OCIDescriptorAlloc(
				s.c.env,
				&oci8cols[i].pbuf,
				C.OCI_DTYPE_LOB,
				0,
				nil)
			if rv == C.OCI_ERROR {
				return nil, ociGetError(s.c.err, "OCI8Stmt.Query.OCIDescriptorAlloc")
			}
			rv = C.OCIDefineByPos(
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
				C.OCI_DEFAULT)
		} else {
			oci8cols[i].pbuf = C.malloc(C.size_t(lp) + 1)
			rv = C.OCIDefineByPos(
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
				C.OCI_DEFAULT)
		}

		if err := s.c.check(rv, "OCI8Stmt.Query at end"); err != nil {
			return nil, err
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
		return 0, ociGetError(r.s.c.err, "OCI8Result.LastInsertId")
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
		return 0, ociGetError(r.s.c.err, "OCI8Result.RowsAffected")
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
		(*C.OCISvcCtx)(s.c.svc),
		(*C.OCIStmt)(s.s),
		(*C.OCIError)(s.c.err),
		1,
		0,
		nil,
		nil,
		C.OCI_DEFAULT)
	if err := s.c.check(rv, "OCI8Stmt.Exec"); err != nil {
		return nil, err
	}

	return &OCI8Result{s}, nil
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

func (rc *OCI8Rows) Close() error {
	for _, col := range rc.cols {
		if col.kind == C.SQLT_CLOB || col.kind == C.SQLT_BLOB {
			C.OCIDescriptorFree(
				col.pbuf,
				C.OCI_DTYPE_LOB)
		} else {
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

	if rv == C.OCI_ERROR {
		err := ociGetError(rc.s.c.err, "OCI8Rows.Next")
		if err.code != ERR_COLUMN_VALUE_IS_NULL {
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

		switch rc.cols[i].kind {
		case C.SQLT_DAT:
			buf := *(*[]byte)(unsafe.Pointer(&rc.cols[i].pbuf))
			//TODO Handle BCE dates (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#438305)
			//TODO Handle timezones (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#443601)
			dest[i] = time.Date(((int(buf[0])-100)*100)+(int(buf[1])-100), time.Month(int(buf[2])), int(buf[3]), int(buf[4])-1, int(buf[5])-1, int(buf[6])-1, 0, rc.s.c.location)
		case C.SQLT_BLOB, C.SQLT_CLOB:
			var bamt C.ub4
			b := make([]byte, rc.cols[i].size)
			rv = C.OCILobRead(
				(*C.OCISvcCtx)(rc.s.c.svc),
				(*C.OCIError)(rc.s.c.err),
				(*C.OCILobLocator)(rc.cols[i].pbuf),
				&bamt,
				1,
				unsafe.Pointer(&b[0]),
				C.ub4(rc.cols[i].size),
				nil,
				nil,
				0,
				C.SQLCS_IMPLICIT)
			if rv == C.OCI_ERROR {
				return ociGetError(rc.s.c.err, "OCI8Rows.Next clob")
			}
			dest[i] = b
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

type OCIError struct {
	code    int
	msg     string
	context string
}

func (o *OCIError) Error() string {
	return o.msg
}

func ociGetError(err unsafe.Pointer, context string) *OCIError {
	var errcode C.sb4
	var errbuff [512]C.char
	if rv := C.OCIErrorGet(
		err,
		1,
		nil,
		&errcode,
		(*C.OraText)(unsafe.Pointer(&errbuff[0])),
		512,
		C.OCI_HTYPE_ERROR); rv != C.OCI_SUCCESS {
		return &OCIError{0, "no oracle error?", ""}
	}

	s := C.GoString(&errbuff[0])
	return &OCIError{int(errcode), s, context}
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
