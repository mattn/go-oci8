package oci8

/*
#include "oci8.go.h"
#cgo !noPkgConfig pkg-config: oci8
*/
import "C"

// noPkgConfig is a Go tag for disabling using pkg-config and using environmental settings like CGO_CFLAGS and CGO_LDFLAGS instead

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// ParseDSN parses a DSN used to connect to Oracle
// It expects to receive a string in the form:
// [username/[password]@]host[:port][/instance_name][?param1=value1&...&paramN=valueN]
//
// Currently the parameters supported is:
// 1 'loc' which
// sets the timezone to read times in as and to marshal to when writing times to
// Oracle date,
// 2 'isolation' =READONLY,SERIALIZABLE,DEFAULT
// 3 'prefetch_rows'
// 4 'prefetch_memory'
// 5 'questionph' =YES,NO,TRUE,FALSE enable question-mark placeholders, default to false
func ParseDSN(dsnString string) (dsn *DSN, err error) {

	dsn = &DSN{Location: time.Local}

	if dsnString == "" {
		return nil, errors.New("empty dsn")
	}

	const prefix = "oracle://"

	if strings.HasPrefix(dsnString, prefix) {
		dsnString = dsnString[len(prefix):]
	}

	authority, dsnString := splitRight(dsnString, "@")
	if authority != "" {
		dsn.Username, dsn.Password, err = parseAuthority(authority)
		if err != nil {
			return nil, err
		}
	}

	host, params := splitRight(dsnString, "?")

	if host, err = unescape(host, encodeHost); err != nil {
		return nil, err
	}

	dsn.Connect = host

	// set safe defaults
	dsn.prefetchRows = 10
	dsn.prefetchMemory = 0
	dsn.operationMode = C.OCI_DEFAULT

	qp, err := ParseQuery(params)
	for k, v := range qp {
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
		case "questionph":
			switch v[0] {
			case "YES", "TRUE":
				dsn.enableQMPlaceholders = true
			case "NO", "FALSE":
				dsn.enableQMPlaceholders = false
			default:
				return nil, fmt.Errorf("Invalid questionpm: %v", v[0])
			}
		case "prefetch_rows":
			z, err := strconv.ParseUint(v[0], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid prefetch_rows: %v", v[0])
			}
			dsn.prefetchRows = uint32(z)
		case "prefetch_memory":
			z, err := strconv.ParseUint(v[0], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid prefetch_memory: %v", v[0])
			}
			dsn.prefetchMemory = uint32(z)
		case "as":
			switch v[0] {
			case "SYSDBA", "sysdba":
				dsn.operationMode = C.OCI_SYSDBA
			case "SYSASM", "sysasm":
				dsn.operationMode = C.OCI_SYSASM
			case "SYSOPER", "sysoper":
				dsn.operationMode = C.OCI_SYSOPER
			default:
				return nil, fmt.Errorf("Invalid as: %v", v[0])
			}

		}
	}

	if len(dsn.Username)+len(dsn.Password)+len(dsn.Connect) == 0 {
		dsn.externalauthentication = true
	}
	return dsn, nil
}

// Commit transaction commit
func (tx *OCI8Tx) Commit() error {
	tx.conn.inTransaction = false
	if rv := C.OCITransCommit(
		tx.conn.svc,
		tx.conn.err,
		0); rv != C.OCI_SUCCESS {
		return ociGetError(rv, tx.conn.err)
	}
	return nil
}

// Rollback transaction rollback
func (tx *OCI8Tx) Rollback() error {
	tx.conn.inTransaction = false
	if rv := C.OCITransRollback(
		tx.conn.svc,
		tx.conn.err,
		0); rv != C.OCI_SUCCESS {
		return ociGetError(rv, tx.conn.err)
	}
	return nil
}

// Open opens a new database connection
func (oci8Driver *OCI8DriverStruct) Open(dsnString string) (connection driver.Conn, err error) {
	var dsn *DSN
	if dsn, err = ParseDSN(dsnString); err != nil {
		return
	}

	conn := OCI8Conn{
		operationMode: dsn.operationMode,
		logger:        oci8Driver.Logger,
	}
	if conn.logger == nil {
		conn.logger = log.New(ioutil.Discard, "", 0)
	}

	if rv := C.WrapOCIEnvCreate(
		C.OCI_DEFAULT|C.OCI_THREADED,
		0,
	); rv.rv != C.OCI_SUCCESS && rv.rv != C.OCI_SUCCESS_WITH_INFO {
		// TODO: error handle not yet allocated, we can't get string error from oracle
		err = errors.New("can't OCIEnvCreate")
		return
	} else {
		conn.env = (*C.OCIEnv)(rv.ptr)
		// conn allocations: env
	}

	if rv := C.WrapOCIHandleAlloc(
		unsafe.Pointer(conn.env),
		C.OCI_HTYPE_ERROR,
		0,
	); rv.rv != C.OCI_SUCCESS {
		err = errors.New("cant allocate error handle")
		C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
		return
	} else {
		conn.err = (*C.OCIError)(rv.ptr)
		// conn allocations: env, err
	}

	phost := C.CString(dsn.Connect)
	defer C.free(unsafe.Pointer(phost))
	puser := C.CString(dsn.Username)
	defer C.free(unsafe.Pointer(puser))
	ppass := C.CString(dsn.Password)
	defer C.free(unsafe.Pointer(ppass))

	if useOCISessionBegin {
		if rv := C.WrapOCIHandleAlloc(
			unsafe.Pointer(conn.env),
			C.OCI_HTYPE_SERVER,
			0,
		); rv.rv != C.OCI_SUCCESS {
			err = errors.New("cant allocate server handle")
			C.OCIHandleFree(unsafe.Pointer(conn.err), C.OCI_HTYPE_ERROR)
			C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
			return
		} else {
			conn.srv = (*C.OCIServer)(rv.ptr)
			// conn allocations: env, err, srv
		}

		if dsn.externalauthentication {
			C.WrapOCIServerAttach(
				conn.srv,
				conn.err,
				nil,
				0,
				C.OCI_DEFAULT)
		} else {
			C.WrapOCIServerAttach(
				conn.srv,
				conn.err,
				(*C.text)(unsafe.Pointer(phost)),
				C.ub4(len(dsn.Connect)),
				C.OCI_DEFAULT)
		}

		if rv := C.WrapOCIHandleAlloc(
			unsafe.Pointer(conn.env),
			C.OCI_HTYPE_SVCCTX,
			0,
		); rv.rv != C.OCI_SUCCESS {
			err = errors.New("cant allocate service handle")
			C.OCIHandleFree(unsafe.Pointer(conn.srv), C.OCI_HTYPE_SERVER)
			C.OCIHandleFree(unsafe.Pointer(conn.err), C.OCI_HTYPE_ERROR)
			C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
			return
		} else {
			conn.svc = (*C.OCISvcCtx)(rv.ptr)
			// conn allocations: env, err, srv, svc
		}

		if rv := C.OCIAttrSet(
			unsafe.Pointer(conn.svc),
			C.OCI_HTYPE_SVCCTX,
			unsafe.Pointer(conn.srv),
			0,
			C.OCI_ATTR_SERVER,
			conn.err,
		); rv != C.OCI_SUCCESS {
			err = ociGetError(rv, conn.err)
			C.OCIHandleFree(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX)
			C.OCIHandleFree(unsafe.Pointer(conn.srv), C.OCI_HTYPE_SERVER)
			C.OCIHandleFree(unsafe.Pointer(conn.err), C.OCI_HTYPE_ERROR)
			C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
			return
		}

		// allocate a user session handle
		if rv := C.WrapOCIHandleAlloc(
			unsafe.Pointer(conn.env),
			C.OCI_HTYPE_SESSION,
			0,
		); rv.rv != C.OCI_SUCCESS {
			err = errors.New("cant allocate user session handle")
			C.OCIHandleFree(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX)
			C.OCIHandleFree(unsafe.Pointer(conn.srv), C.OCI_HTYPE_SERVER)
			C.OCIHandleFree(unsafe.Pointer(conn.err), C.OCI_HTYPE_ERROR)
			C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
			return
		} else {
			conn.usrSession = (*C.OCISession)(rv.ptr)
			// conn allocations: env, err, srv, svc, usrSession
		}

		if !dsn.externalauthentication {
			//  set username attribute in user session handle
			if rv := C.OCIAttrSet(
				unsafe.Pointer(conn.usrSession),
				C.OCI_HTYPE_SESSION,
				(unsafe.Pointer(puser)),
				C.ub4(len(dsn.Username)),
				C.OCI_ATTR_USERNAME,
				conn.err,
			); rv != C.OCI_SUCCESS {
				err = ociGetError(rv, conn.err)
				C.OCIHandleFree(unsafe.Pointer(conn.usrSession), C.OCI_HTYPE_SESSION)
				C.OCIHandleFree(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX)
				C.OCIHandleFree(unsafe.Pointer(conn.srv), C.OCI_HTYPE_SERVER)
				C.OCIHandleFree(unsafe.Pointer(conn.err), C.OCI_HTYPE_ERROR)
				C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
				return
			}

			// set password attribute in the user session handle
			if rv := C.OCIAttrSet(
				unsafe.Pointer(conn.usrSession),
				C.OCI_HTYPE_SESSION,
				(unsafe.Pointer(ppass)),
				C.ub4(len(dsn.Password)),
				C.OCI_ATTR_PASSWORD,
				conn.err,
			); rv != C.OCI_SUCCESS {
				err = ociGetError(rv, conn.err)
				C.OCIHandleFree(unsafe.Pointer(conn.usrSession), C.OCI_HTYPE_SESSION)
				C.OCIHandleFree(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX)
				C.OCIHandleFree(unsafe.Pointer(conn.srv), C.OCI_HTYPE_SERVER)
				C.OCIHandleFree(unsafe.Pointer(conn.err), C.OCI_HTYPE_ERROR)
				C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
				return
			}

			// begin the session
			C.WrapOCISessionBegin(
				conn.svc,
				conn.err,
				conn.usrSession,
				C.OCI_CRED_RDBMS,
				conn.operationMode)
		} else {
			// external authentication
			C.WrapOCISessionBegin(
				conn.svc,
				conn.err,
				conn.usrSession,
				C.OCI_CRED_EXT,
				conn.operationMode)
		}

		// set the user session attribute in the service context handle
		if rv := C.OCIAttrSet(
			unsafe.Pointer(conn.svc),
			C.OCI_HTYPE_SVCCTX,
			unsafe.Pointer(conn.usrSession),
			0,
			C.OCI_ATTR_SESSION,
			conn.err,
		); rv != C.OCI_SUCCESS {
			err = ociGetError(rv, conn.err)
			C.OCIHandleFree(unsafe.Pointer(conn.usrSession), C.OCI_HTYPE_SESSION)
			C.OCIHandleFree(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX)
			C.OCIHandleFree(unsafe.Pointer(conn.srv), C.OCI_HTYPE_SERVER)
			C.OCIHandleFree(unsafe.Pointer(conn.err), C.OCI_HTYPE_ERROR)
			C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
			return
		}

	} else {
		if rv := C.WrapOCILogon(
			conn.env,
			conn.err,
			(*C.OraText)(unsafe.Pointer(puser)),
			C.ub4(len(dsn.Username)),
			(*C.OraText)(unsafe.Pointer(ppass)),
			C.ub4(len(dsn.Password)),
			(*C.OraText)(unsafe.Pointer(phost)),
			C.ub4(len(dsn.Connect)),
		); rv.rv != C.OCI_SUCCESS && rv.rv != C.OCI_SUCCESS_WITH_INFO {
			err = ociGetError(rv.rv, conn.err)
			C.OCIHandleFree(unsafe.Pointer(conn.err), C.OCI_HTYPE_ERROR)
			C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
			return
		} else {
			conn.svc = (*C.OCISvcCtx)(rv.ptr)
			// conn allocations: env, err
		}

	}

	conn.location = dsn.Location
	conn.transactionMode = dsn.transactionMode
	conn.prefetchRows = dsn.prefetchRows
	conn.prefetchMemory = dsn.prefetchMemory
	conn.enableQMPlaceholders = dsn.enableQMPlaceholders

	connection = &conn

	return
}

func freeBoundParameters(boundParameters []oci8bind) {
	for _, col := range boundParameters {
		if col.pbuf != nil {
			switch col.kind {
			case C.SQLT_CLOB, C.SQLT_BLOB:
				freeDecriptor(col.pbuf, C.OCI_DTYPE_LOB)
			case C.SQLT_TIMESTAMP:
				freeDecriptor(col.pbuf, C.OCI_DTYPE_TIMESTAMP)
			case C.SQLT_TIMESTAMP_TZ:
				freeDecriptor(col.pbuf, C.OCI_DTYPE_TIMESTAMP_TZ)
			case C.SQLT_TIMESTAMP_LTZ:
				freeDecriptor(col.pbuf, C.OCI_DTYPE_TIMESTAMP_LTZ)
			case C.SQLT_INTERVAL_DS:
				freeDecriptor(col.pbuf, C.OCI_DTYPE_INTERVAL_DS)
			case C.SQLT_INTERVAL_YM:
				freeDecriptor(col.pbuf, C.OCI_DTYPE_INTERVAL_YM)
			default:
				C.free(col.pbuf)
			}
			col.pbuf = nil
		}
	}
}

func getInt64(p unsafe.Pointer) int64 {
	return int64(*(*C.sb8)(p))
}

func getUint64(p unsafe.Pointer) uint64 {
	return uint64(*(*C.sb8)(p))
}

func outputBoundParameters(boundParameters []oci8bind) {
	for _, col := range boundParameters {
		if col.pbuf != nil {
			switch v := col.out.(type) {
			case *string:
				*v = C.GoString((*C.char)(col.pbuf))

			case *int:
				*v = int(getInt64(col.pbuf))
			case *int64:
				*v = getInt64(col.pbuf)
			case *int32:
				*v = int32(getInt64(col.pbuf))
			case *int16:
				*v = int16(getInt64(col.pbuf))
			case *int8:
				*v = int8(getInt64(col.pbuf))

			case *uint:
				*v = uint(getUint64(col.pbuf))
			case *uint64:
				*v = getUint64(col.pbuf)
			case *uint32:
				*v = uint32(getUint64(col.pbuf))
			case *uint16:
				*v = uint16(getUint64(col.pbuf))
			case *uint8:
				*v = uint8(getUint64(col.pbuf))

			case *float64:

				buf := (*[1 << 30]byte)(col.pbuf)[0:8]
				f := uint64(buf[7])
				f |= uint64(buf[6]) << 8
				f |= uint64(buf[5]) << 16
				f |= uint64(buf[4]) << 24
				f |= uint64(buf[3]) << 32
				f |= uint64(buf[2]) << 40
				f |= uint64(buf[1]) << 48
				f |= uint64(buf[0]) << 56

				// Don't know why bits are inverted that way, but it works
				if buf[0]&0x80 == 0 {
					f ^= 0xffffffffffffffff
				} else {
					f &= 0x7fffffffffffffff
				}

				*v = math.Float64frombits(f)

			case *bool:
				buf := (*[1 << 30]byte)(col.pbuf)[0:1]
				*v = buf[0] != 0
			}
		}
	}
}

// GetLastInsertId retuns last inserted ID
func GetLastInsertId(id int64) string {
	return *(*string)(unsafe.Pointer(uintptr(id)))
}

// LastInsertId returns last inserted ID
func (r *OCI8Result) LastInsertId() (int64, error) {
	return r.id, r.errid
}

// RowsAffected returns rows affected
func (r *OCI8Result) RowsAffected() (int64, error) {
	return r.n, r.errn
}

// freeDecriptor calles C OCIDescriptorFree
func freeDecriptor(p unsafe.Pointer, dtype C.ub4) {
	tptr := *(*unsafe.Pointer)(p)
	C.OCIDescriptorFree(unsafe.Pointer(tptr), dtype)
}

// ociGetErrorS gets error.
// Also calls isBadConnection to check if bad connection error
func ociGetErrorS(err *C.OCIError) error {
	rv := C.WrapOCIErrorGet(err)
	s := C.GoString(&rv.err[0])
	if isBadConnection(s) {
		return driver.ErrBadConn
	}
	return errors.New(s)
}

// isBadConnection checks the error string for ORA errors that would mean the connection is bad
func isBadConnection(error string) bool {
	if len(error) < 9 || error[0:4] != "ORA-" {
		// if error is less than 9 and is not an ORA error
		return false
	}
	// only check number part, ORA is already checked
	switch error[4:9] {
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
	case "00028", "01012", "01033", "01034", "01089", "03113", "03114", "03135", "12528", "12537":
		// bad connection
		return true
	}
	return false
}

func ociGetError(rv C.sword, err *C.OCIError) error {
	switch rv {
	case C.OCI_INVALID_HANDLE:
		return errors.New("OCI_INVALID_HANDLE")
	case C.OCI_SUCCESS_WITH_INFO:
		return errors.New("OCI_SUCCESS_WITH_INFO")
	case C.OCI_RESERVED_FOR_INT_USE:
		return errors.New("OCI_RESERVED_FOR_INT_USE")
	case C.OCI_NO_DATA:
		return errors.New("OCI_NO_DATA")
	case C.OCI_NEED_DATA:
		return errors.New("OCI_NEED_DATA")
	case C.OCI_STILL_EXECUTING:
		return errors.New("OCI_STILL_EXECUTING")
	case C.OCI_SUCCESS:
		panic("ociGetError called with no error")
	case C.OCI_ERROR:
		return ociGetErrorS(err)
	}
	return fmt.Errorf("oracle return error code %d", rv)
}

// CByte comverts byte slice to C char
func CByte(b []byte) *C.char {
	p := C.malloc(C.size_t(len(b)))
	pp := (*[1 << 30]byte)(p)
	copy(pp[:], b)
	return (*C.char)(p)
}

// converts "?" characters to  :1, :2, ... :n
func placeholders(sql string) string {
	n := 0
	return phre.ReplaceAllStringFunc(sql, func(string) string {
		n++
		return ":" + strconv.Itoa(n)
	})
}
