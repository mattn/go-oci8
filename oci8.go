package oci8

// #include "oci8.go.h"
import "C"

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
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
			dsn.prefetchRows = C.ub4(z)
		case "prefetch_memory":
			z, err := strconv.ParseUint(v[0], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid prefetch_memory: %v", v[0])
			}
			dsn.prefetchMemory = C.ub4(z)
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
		tx.conn.errHandle,
		0,
	); rv != C.OCI_SUCCESS {
		return tx.conn.getError(rv)
	}
	return nil
}

// Rollback transaction rollback
func (tx *OCI8Tx) Rollback() error {
	tx.conn.inTransaction = false
	if rv := C.OCITransRollback(
		tx.conn.svc,
		tx.conn.errHandle,
		0,
	); rv != C.OCI_SUCCESS {
		return tx.conn.getError(rv)
	}
	return nil
}

// Open opens a new database connection
func (oci8Driver *OCI8DriverStruct) Open(dsnString string) (driver.Conn, error) {
	var err error
	var dsn *DSN
	if dsn, err = ParseDSN(dsnString); err != nil {
		return nil, err
	}

	conn := OCI8Conn{
		operationMode: dsn.operationMode,
		logger:        oci8Driver.Logger,
	}
	if conn.logger == nil {
		conn.logger = log.New(ioutil.Discard, "", 0)
	}

	// get NLS_LANG character set ID from environment variable
	var charset C.ub2
	result := C.OCINlsEnvironmentVariableGet(
		unsafe.Pointer(&charset), // Returns a value of a globalization support environment variable such as the NLS_LANG character set ID or the NLS_NCHAR character set ID.
		0,                    // Specifies the size of the given output value, which is applicable only to string data.
		C.OCI_NLS_CHARSET_ID, // Specifies one of the following values to get from the globalization support environment variable: OCI_NLS_CHARSET_ID: NLS_LANG character set ID in ub2 datatype. OCI_NLS_NCHARSET_ID: NLS_NCHAR character set ID in ub2 datatype.
		0,                    // Specifies the character set ID for retrieved string data.
		nil,                  // The length of the return value in bytes.
	)
	if result != C.OCI_SUCCESS {
		return nil, errors.New("OCINlsEnvironmentVariableGet NLS_LANG error")
	}

	// get NLS_NCHAR character set ID from environment variable
	var ncharset C.ub2
	result = C.OCINlsEnvironmentVariableGet(
		unsafe.Pointer(&ncharset), // Returns a value of a globalization support environment variable such as the NLS_LANG character set ID or the NLS_NCHAR character set ID.
		0, // Specifies the size of the given output value, which is applicable only to string data.
		C.OCI_NLS_NCHARSET_ID, // Specifies one of the following values to get from the globalization support environment variable: OCI_NLS_CHARSET_ID: NLS_LANG character set ID in ub2 datatype. OCI_NLS_NCHARSET_ID: NLS_NCHAR character set ID in ub2 datatype.
		0,   // Specifies the character set ID for retrieved string data.
		nil, // The length of the return value in bytes.
	)
	if result != C.OCI_SUCCESS {
		return nil, errors.New("OCINlsEnvironmentVariableGet NLS_NCHAR error")
	}

	// environment handle
	var envP *C.OCIEnv
	envPP := &envP
	result = C.OCIEnvNlsCreate(
		envPP,          // pointer to a handle to the environment
		C.OCI_THREADED, // environment mode: https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci16rel001.htm#LNOCI87683
		nil,            // Specifies the user-defined context for the memory callback routines.
		nil,            // Specifies the user-defined memory allocation function. If mode is OCI_THREADED, this memory allocation routine must be thread-safe.
		nil,            // Specifies the user-defined memory re-allocation function. If the mode is OCI_THREADED, this memory allocation routine must be thread safe.
		nil,            // Specifies the user-defined memory free function. If mode is OCI_THREADED, this memory free routine must be thread-safe.
		0,              // Specifies the amount of user memory to be allocated for the duration of the environment.
		nil,            // Returns a pointer to the user memory of size xtramemsz allocated by the call for the user.
		charset,        // The client-side character set for the current environment handle. If it is 0, the NLS_LANG setting is used. OCI_UTF16ID is a valid setting; it is used by the metadata and the CHAR data.
		ncharset,       // The client-side national character set for the current environment handle. If it is 0, NLS_NCHAR setting is used. OCI_UTF16ID is a valid setting; it is used by the NCHAR data.
	)
	if result != C.OCI_SUCCESS {
		return nil, errors.New("OCIEnvNlsCreate error")
	}
	conn.env = *envPP

	// defer on error handle free
	defer func(errP *error) {
		if *errP != nil {
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
			C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
		}
	}(&err)

	// error handle
	var handleTemp unsafe.Pointer
	handle := &handleTemp
	result = C.OCIHandleAlloc(
		unsafe.Pointer(conn.env), // An environment handle
		handle,            // Returns a handle
		C.OCI_HTYPE_ERROR, // type of handle: https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci02bas.htm#LNOCI87581
		0,                 // amount of user memory to be allocated
		nil,               // Returns a pointer to the user memory
	)
	if result != C.OCI_SUCCESS {
		// TODO: error handle not yet allocated, how to get string error from oracle?
		return nil, errors.New("allocate error handle error")
	}
	conn.errHandle = (*C.OCIError)(*handle)

	phost := C.CString(dsn.Connect)
	defer C.free(unsafe.Pointer(phost))
	puser := C.CString(dsn.Username)
	defer C.free(unsafe.Pointer(puser))
	ppass := C.CString(dsn.Password)
	defer C.free(unsafe.Pointer(ppass))

	if useOCISessionBegin {
		// server handle
		handle = nil
		handle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SERVER, 0)
		if err != nil {
			return nil, fmt.Errorf("allocate server handle error: %v", err)
		}
		conn.srv = (*C.OCIServer)(*handle)

		if dsn.externalauthentication {
			C.WrapOCIServerAttach(
				conn.srv,
				conn.errHandle,
				nil,
				0,
				C.OCI_DEFAULT)
		} else {
			C.WrapOCIServerAttach(
				conn.srv,
				conn.errHandle,
				(*C.text)(unsafe.Pointer(phost)),
				C.ub4(len(dsn.Connect)),
				C.OCI_DEFAULT)
		}

		// service handle
		handle = nil
		handle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SVCCTX, 0)
		if err != nil {
			return nil, fmt.Errorf("allocate service handle error: %v", err)
		}
		conn.svc = (*C.OCISvcCtx)(*handle)

		// sets the server context attribute of the service context
		err = conn.ociAttrSet(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX, unsafe.Pointer(conn.srv), 0, C.OCI_ATTR_SERVER)
		if err != nil {
			return nil, err
		}

		// user session handle
		handle = nil
		handle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SESSION, 0)
		if err != nil {
			return nil, fmt.Errorf("allocate user session handle error: %v", err)
		}
		conn.usrSession = (*C.OCISession)(*handle)

		if !dsn.externalauthentication {
			// specifies a username to use for authentication
			err = conn.ociAttrSet(unsafe.Pointer(conn.usrSession), C.OCI_HTYPE_SESSION, unsafe.Pointer(puser), C.ub4(len(dsn.Username)), C.OCI_ATTR_USERNAME)
			if err != nil {
				return nil, err
			}

			// specifies a password to use for authentication
			err = conn.ociAttrSet(unsafe.Pointer(conn.usrSession), C.OCI_HTYPE_SESSION, unsafe.Pointer(ppass), C.ub4(len(dsn.Password)), C.OCI_ATTR_PASSWORD)
			if err != nil {
				return nil, err
			}

			// begin the session
			C.WrapOCISessionBegin(
				conn.svc,
				conn.errHandle,
				conn.usrSession,
				C.OCI_CRED_RDBMS,
				conn.operationMode)
		} else {
			// external authentication
			C.WrapOCISessionBegin(
				conn.svc,
				conn.errHandle,
				conn.usrSession,
				C.OCI_CRED_EXT,
				conn.operationMode)
		}

		// sets the authentication context attribute of the service context
		err = conn.ociAttrSet(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX, unsafe.Pointer(conn.usrSession), 0, C.OCI_ATTR_SESSION)
		if err != nil {
			return nil, err
		}

	} else {

		if rv := C.WrapOCILogon(
			conn.env,
			conn.errHandle,
			(*C.OraText)(unsafe.Pointer(puser)),
			C.ub4(len(dsn.Username)),
			(*C.OraText)(unsafe.Pointer(ppass)),
			C.ub4(len(dsn.Password)),
			(*C.OraText)(unsafe.Pointer(phost)),
			C.ub4(len(dsn.Connect)),
		); rv.rv != C.OCI_SUCCESS && rv.rv != C.OCI_SUCCESS_WITH_INFO {
			return nil, conn.getError(rv.rv)
		} else {
			conn.svc = (*C.OCISvcCtx)(rv.ptr)
		}

	}

	conn.location = dsn.Location
	conn.transactionMode = dsn.transactionMode
	conn.prefetchRows = dsn.prefetchRows
	conn.prefetchMemory = dsn.prefetchMemory
	conn.enableQMPlaceholders = dsn.enableQMPlaceholders

	return &conn, nil
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

// converts "?" characters to  :1, :2, ... :n
func placeholders(sql string) string {
	n := 0
	return phre.ReplaceAllStringFunc(sql, func(string) string {
		n++
		return ":" + strconv.Itoa(n)
	})
}
