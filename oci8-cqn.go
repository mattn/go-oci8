package oci8

// #include "oci8.go.h"
import "C"

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"unsafe"
)

// OpenOCI8Conn opens a connection to the given Oracle database.
// Supply flags == C.OCI_EVENTS | C.OCI_OBJECT for Continuous Query Notification.
func (oci8Driver *OCI8DriverStruct) OpenOCI8Conn(dsnString string, envCreateFlags C.ub4) (*OCI8Conn, error) {
	// Set up.
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
	// Environment handle.
	var envP *C.OCIEnv
	envPP := &envP
	var result C.sword
	charset := C.ub2(0)
	if os.Getenv("NLS_LANG") == "" && os.Getenv("NLS_NCHAR") == "" {
		charset = defaultCharset
	}
	// Create OCI env.
	result = C.OCIEnvNlsCreate(
		envPP,          // pointer to a handle to the environment
		envCreateFlags, // environment mode: https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci16rel001.htm#LNOCI87683
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
	// Defer cleanup if any error occurs.
	defer func(errP *error) {
		if *errP != nil {
			conn.freeHandles()
		}
	}(&err) // pass the address of err so this is the last error assigned to err.
	// Error handle.
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
		return nil, errors.New("allocate error handle error")
	}
	conn.errHandle = (*C.OCIError)(*handle)
	handle = nil // deallocate.
	// Session setup.
	host := cString(dsn.Connect)
	defer C.free(unsafe.Pointer(host))
	username := cString(dsn.Username)
	defer C.free(unsafe.Pointer(username))
	password := cString(dsn.Password)
	defer C.free(unsafe.Pointer(password))
	if useOCISessionBegin {
		// Server handle.
		handle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SERVER, 0)
		if err != nil {
			return nil, fmt.Errorf("allocate server handle error: %v", err)
		}
		conn.srv = (*C.OCIServer)(*handle)
		handle = nil // deallocate.
		// Auth type.
		if dsn.externalauthentication { // if we should use external auth...
			result = C.OCIServerAttach(
				conn.srv,       // uninitialized server handle, which gets initialized by this call. Passing in an initialized server handle causes an error.
				conn.errHandle, // error handle
				nil,            // database server to use
				0,              //  length of the database server
				C.OCI_DEFAULT,  // mode of operation: OCI_DEFAULT or OCI_CPOOL
			)
		} else { // else if we have username and password auth...
			result = C.OCIServerAttach(
				conn.srv,                // uninitialized server handle, which gets initialized by this call. Passing in an initialized server handle causes an error.
				conn.errHandle,          // error handle
				host,                    // database server to use
				C.sb4(len(dsn.Connect)), //  length of the database server
				C.OCI_DEFAULT,           // mode of operation: OCI_DEFAULT or OCI_CPOOL
			)
		}
		if result != C.OCI_SUCCESS {
			return nil, conn.getError(result)
		}
		// Service handle.
		handle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SVCCTX, 0)
		if err != nil {
			return nil, fmt.Errorf("allocate service handle error: %v", err)
		}
		conn.svc = (*C.OCISvcCtx)(*handle)
		handle = nil // deallocate.
		// Set the server context attribute of the service context.
		err = conn.ociAttrSet(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX, unsafe.Pointer(conn.srv), 0, C.OCI_ATTR_SERVER)
		if err != nil {
			return nil, err
		}
		// User Session.
		handle, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SESSION, 0)
		if err != nil {
			return nil, fmt.Errorf("allocate user session handle error: %v", err)
		}
		conn.usrSession = (*C.OCISession)(*handle)
		handle = nil                            // dealloc.
		credentialType := C.ub4(C.OCI_CRED_EXT) // assume ext auth for now.
		if !dsn.externalauthentication { // if we're using username/password auth...
			// Set the username & password.
			err = conn.ociAttrSet(unsafe.Pointer(conn.usrSession), C.OCI_HTYPE_SESSION, unsafe.Pointer(username), C.ub4(len(dsn.Username)), C.OCI_ATTR_USERNAME)
			if err != nil {
				return nil, err
			}
			err = conn.ociAttrSet(unsafe.Pointer(conn.usrSession), C.OCI_HTYPE_SESSION, unsafe.Pointer(password), C.ub4(len(dsn.Password)), C.OCI_ATTR_PASSWORD)
			if err != nil {
				return nil, err
			}
			credentialType = C.OCI_CRED_RDBMS // use username/password auth.
		}
		result = C.OCISessionBegin(
			conn.svc,           // service context
			conn.errHandle,     // error handle
			conn.usrSession,    // user session context
			credentialType,     // type of credentials to use for establishing the user session: OCI_CRED_RDBMS or OCI_CRED_EXT
			conn.operationMode, // mode of operation. https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci16rel001.htm#LNOCI87690
		)
		if result != C.OCI_SUCCESS { // if there was a failure starting a session...
			return nil, conn.getError(result)
		}
		// Set the authentication context attribute of the service context.
		err = conn.ociAttrSet(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX, unsafe.Pointer(conn.usrSession), 0, C.OCI_ATTR_SESSION)
		if err != nil {
			return nil, err
		}
	} else { // else we must start a session using OCILogon...
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
		if result != C.OCI_SUCCESS {
			return nil, conn.getError(result)
		}
		conn.svc = *svcCtxPP
	}
	conn.location = dsn.Location
	conn.transactionMode = dsn.transactionMode
	conn.prefetchRows = dsn.prefetchRows
	conn.prefetchMemory = dsn.prefetchMemory
	conn.enableQMPlaceholders = dsn.enableQMPlaceholders
	return &conn, nil
}

