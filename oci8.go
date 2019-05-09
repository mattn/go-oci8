package oci8

// #include "oci8.go.h"
import "C"

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// ParseDSN parses a DSN used to connect to Oracle
//
// It expects to receive a string in the form:
//
// [username/[password]@]host[:port][/instance_name][?param1=value1&...&paramN=valueN]
//
// Supported parameters are:
//
// loc - the time location for timezone when reading/writing Go time/Oracle date
//
// isolation - the isolation level that can be set to: READONLY, SERIALIZABLE, or DEFAULT
//
// prefetch_rows - the number of top level rows to be prefetched. Defaults to 0. A 0 means unlimited rows.
//
// prefetch_memory - the max memory for top level rows to be prefetched. Defaults to 4096. A 0 means unlimited memory.
//
// questionph - when true, enables question mark placeholders. Defaults to false. (uses strconv.ParseBool to check for true)
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
	dsn.prefetchRows = 0
	dsn.prefetchMemory = 4096
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
			dsn.enableQMPlaceholders, err = strconv.ParseBool(v[0])
			if err != nil {
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

	// environment handle
	var envP *C.OCIEnv
	envPP := &envP
	var result C.sword
	charset := C.ub2(0)

	if os.Getenv("NLS_LANG") == "" && os.Getenv("NLS_NCHAR") == "" {
		charset = defaultCharset
	}

	result = C.OCIEnvNlsCreate(
		envPP,          // pointer to a handle to the environment
		C.OCI_THREADED, // environment mode: https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci16rel001.htm#LNOCI87683
		nil,            // Specifies the user-defined context for the memory callback routines.
		nil,            // Specifies the user-defined memory allocation function. If mode is OCI_THREADED, this memory allocation routine must be thread-safe.
		nil,            // Specifies the user-defined memory re-allocation function. If the mode is OCI_THREADED, this memory allocation routine must be thread safe.
		nil,            // Specifies the user-defined memory free function. If mode is OCI_THREADED, this memory free routine must be thread-safe.
		0,              // Specifies the amount of user memory to be allocated for the duration of the environment.
		nil,            // Returns a pointer to the user memory of size xtramemsz allocated by the call for the user.
		charset,        // The client-side character set for the current environment handle. If it is 0, the NLS_LANG setting is used.
		charset,        // The client-side national character set for the current environment handle. If it is 0, NLS_NCHAR setting is used.
	)
	if result != C.OCI_SUCCESS {
		return nil, errors.New("OCIEnvNlsCreate error")
	}
	conn.env = *envPP

	// defer on error handle free
	var doneSessionBegin bool
	var doneServerAttach bool
	var doneLogon bool
	defer func(errP *error) {
		if *errP != nil {
			if doneSessionBegin {
				C.OCISessionEnd(
					conn.svc,
					conn.errHandle,
					conn.usrSession,
					C.OCI_DEFAULT,
				)
			}
			if doneLogon {
				C.OCILogoff(
					conn.svc,
					conn.errHandle,
				)
			}
			if doneServerAttach {
				C.OCIServerDetach(
					conn.srv,
					conn.errHandle,
					C.OCI_DEFAULT,
				)
			}
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
		handle,                   // Returns a handle
		C.OCI_HTYPE_ERROR,        // type of handle: https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci02bas.htm#LNOCI87581
		0,                        // amount of user memory to be allocated
		nil,                      // Returns a pointer to the user memory
	)
	if result != C.OCI_SUCCESS {
		// TODO: error handle not yet allocated, how to get string error from oracle?
		err = errors.New("allocate error handle error")
		return nil, err
	}
	conn.errHandle = (*C.OCIError)(*handle)

	host := cString(dsn.Connect)
	defer C.free(unsafe.Pointer(host))
	username := cString(dsn.Username)
	defer C.free(unsafe.Pointer(username))
	password := cString(dsn.Password)
	defer C.free(unsafe.Pointer(password))

	if useOCISessionBegin {
		// server handle
		handle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SERVER, 0)
		if err != nil {
			return nil, fmt.Errorf("allocate server handle error: %v", err)
		}
		conn.srv = (*C.OCIServer)(*handle)

		if dsn.externalauthentication {
			result = C.OCIServerAttach(
				conn.srv,       // uninitialized server handle, which gets initialized by this call. Passing in an initialized server handle causes an error.
				conn.errHandle, // error handle
				nil,            // database server to use
				0,              //  length of the database server
				C.OCI_DEFAULT,  // mode of operation: OCI_DEFAULT or OCI_CPOOL
			)
		} else {
			result = C.OCIServerAttach(
				conn.srv,                // uninitialized server handle, which gets initialized by this call. Passing in an initialized server handle causes an error.
				conn.errHandle,          // error handle
				host,                    // database server to use
				C.sb4(len(dsn.Connect)), //  length of the database server
				C.OCI_DEFAULT,           // mode of operation: OCI_DEFAULT or OCI_CPOOL
			)
		}
		if result != C.OCI_SUCCESS {
			err = conn.getError(result)
			return nil, conn.getError(result)
		}
		doneServerAttach = true

		// service handle
		handle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SVCCTX, 0)
		if err != nil {
			return nil, fmt.Errorf("allocate service handle error: %v", err)
		}
		conn.svc = (*C.OCISvcCtx)(*handle)

		// sets the server context attribute of the service context
		err = conn.ociAttrSet(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX, unsafe.Pointer(conn.srv), 0, C.OCI_ATTR_SERVER)
		if err != nil {
			return nil, fmt.Errorf("server context attribute set error: %v", err)
		}

		// user session handle
		handle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SESSION, 0)
		if err != nil {
			return nil, fmt.Errorf("allocate user session handle error: %v", err)
		}
		conn.usrSession = (*C.OCISession)(*handle)

		credentialType := C.ub4(C.OCI_CRED_EXT)
		if !dsn.externalauthentication {
			// specifies a username to use for authentication
			err = conn.ociAttrSet(unsafe.Pointer(conn.usrSession), C.OCI_HTYPE_SESSION, unsafe.Pointer(username), C.ub4(len(dsn.Username)), C.OCI_ATTR_USERNAME)
			if err != nil {
				return nil, fmt.Errorf("username attribute set error: %v", err)
			}

			// specifies a password to use for authentication
			err = conn.ociAttrSet(unsafe.Pointer(conn.usrSession), C.OCI_HTYPE_SESSION, unsafe.Pointer(password), C.ub4(len(dsn.Password)), C.OCI_ATTR_PASSWORD)
			if err != nil {
				return nil, fmt.Errorf("password attribute set error: %v", err)
			}

			credentialType = C.OCI_CRED_RDBMS
		}

		result = C.OCISessionBegin(
			conn.svc,           // service context
			conn.errHandle,     // error handle
			conn.usrSession,    // user session context
			credentialType,     // type of credentials to use for establishing the user session: OCI_CRED_RDBMS or OCI_CRED_EXT
			conn.operationMode, // mode of operation. https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci16rel001.htm#LNOCI87690
		)
		if result != C.OCI_SUCCESS && result != C.OCI_SUCCESS_WITH_INFO {
			err = conn.getError(result)
			return nil, err
		}
		doneSessionBegin = true

		// sets the authentication context attribute of the service context
		err = conn.ociAttrSet(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX, unsafe.Pointer(conn.usrSession), 0, C.OCI_ATTR_SESSION)
		if err != nil {
			return nil, fmt.Errorf("authentication context attribute set error: %v", err)
		}

	} else {

		var svcCtxP *C.OCISvcCtx
		svcCtxPP := &svcCtxP
		result = C.OCILogon(
			conn.env,                 // environment handle
			conn.errHandle,           // error handle
			svcCtxPP,                 // service context pointer
			username,                 // user name. Must be in the encoding specified by the charset parameter of a previous call to OCIEnvNlsCreate().
			C.ub4(len(dsn.Username)), // length of user name, in number of bytes, regardless of the encoding
			password,                 // user's password. Must be in the encoding specified by the charset parameter of a previous call to OCIEnvNlsCreate().
			C.ub4(len(dsn.Password)), // length of password, in number of bytes, regardless of the encoding.
			host,                     // name of the database to connect to. Must be in the encoding specified by the charset parameter of a previous call to OCIEnvNlsCreate().
			C.ub4(len(dsn.Connect)),  // length of dbname, in number of bytes, regardless of the encoding.
		)
		if result != C.OCI_SUCCESS && result != C.OCI_SUCCESS_WITH_INFO {
			err = conn.getError(result)
			return nil, err
		}
		conn.svc = *svcCtxPP
		doneLogon = true

	}

	conn.location = dsn.Location
	conn.transactionMode = dsn.transactionMode
	conn.prefetchRows = dsn.prefetchRows
	conn.prefetchMemory = dsn.prefetchMemory
	conn.enableQMPlaceholders = dsn.enableQMPlaceholders

	return &conn, nil
}

// GetLastInsertId returns rowid from LastInsertId
func GetLastInsertId(id int64) string {
	return *(*string)(unsafe.Pointer(uintptr(id)))
}

// LastInsertId returns last inserted ID
func (result *OCI8Result) LastInsertId() (int64, error) {
	return int64(uintptr(unsafe.Pointer(&result.rowid))), result.rowidErr
}

// RowsAffected returns rows affected
func (result *OCI8Result) RowsAffected() (int64, error) {
	return result.rowsAffected, result.rowsAffectedErr
}

// converts "?" characters to  :1, :2, ... :n
func placeholders(sql string) string {
	n := 0
	return phre.ReplaceAllStringFunc(sql, func(string) string {
		n++
		return ":" + strconv.Itoa(n)
	})
}
