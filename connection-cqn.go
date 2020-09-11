package oci8

/*
#cgo CXXFLAGS: -std=c++11
#include "oci8.go.h"
void cqnCallback(dvoid* ctx, OCISubscription* subscrhp, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode);
*/
import "C"

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/relloyd/go-oci8/types"
	"github.com/relloyd/go-sql/database/sql/driver"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"unsafe"
)

type CqnConn struct {
	conn                *OCI8Conn
	stmt                *OCI8Stmt
	subscriptionPtr     *C.OCISubscription
	registrationId      int64
	subscriptionCreated bool
	m                   sync.Mutex
}

// cqnSubscriptionHandlerMap is a global store for SubscriptionHandler interfaces supplied by
// users of CqnConn.Execute().
var cqnSubscriptionHandlerMap struct {
	sync.RWMutex
	m map[int64]types.SubscriptionHandler
}

// openOCI8Conn opens a connection to the given Oracle database.
// Uses flags == C.OCI_EVENTS | C.OCI_OBJECT for Continuous Query Notification.
// Mostly a duplicate of Open() but needed since the flags above differ.
func (oci8Driver *OCI8DriverStruct) openOCI8Conn4Cqn(dsnString string) (*OCI8Conn, error) {
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
	envCreateFlags := C.OCI_EVENTS | C.OCI_OBJECT // required for ContinuousQueryNotification.
	result = C.OCIEnvNlsCreate(
		envPP,                 // pointer to a handle to the environment
		C.ub4(envCreateFlags), // environment mode: https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci16rel001.htm#LNOCI87683
		nil,                   // Specifies the user-defined context for the memory callback routines.
		nil,                   // Specifies the user-defined memory allocation function. If mode is OCI_THREADED, this memory allocation routine must be thread-safe.
		nil,                   // Specifies the user-defined memory re-allocation function. If the mode is OCI_THREADED, this memory allocation routine must be thread safe.
		nil,                   // Specifies the user-defined memory free function. If mode is OCI_THREADED, this memory free routine must be thread-safe.
		0,                     // Specifies the amount of user memory to be allocated for the duration of the environment.
		nil,                   // Returns a pointer to the user memory of size xtramemsz allocated by the call for the user.
		charset,               // The client-side character set for the current environment handle. If it is 0, the NLS_LANG setting is used.
		charset,               // The client-side national character set for the current environment handle. If it is 0, NLS_NCHAR setting is used.
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
		if !dsn.externalauthentication {        // if we're using username/password auth...
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

// NewCqnSubscription registers a new CQN subscription, returning a handle/pointer to the subscription and its
// registration ID. A Go callback function of type SubscriptionHandler needs to be registered in global map
// cqnSubscriptionHandlerMap.m, using the registrationId which is returned by this function.
// ExecuteCqn() should then be called with the subscription handle pointer, SQL and args.
// Background:
// The subscription uses a central C function that will be executed by Oracle when query change notification events are raised.
// The C callback function executes a Go function, which searches global variable cqnSubscriptionHandlerMap for an instance function
// to execute and supply the notification payload to.
// Here's an example, to register and use a CQN:
// 1) call NewCqnSubscription().
// 2) save an interface of type SubscriptionHandler in global map cqnSubscriptionHandlerMap.m using the registration ID.
// 3) call ExecuteCqn() with the query you wish to register for CQN.
// 4) Oracle calls C.cqnCallback() when a CQN event occurs.
// 5) C.cqnCallback() passes the payload to Go function goCqnCallback().
// 6) goCqnCallback() uses the registration ID to look up a SubscriptionHandler interface.
// 7) The Go SubscriptionHandler is executed with the CQN payload and descriptor so you can route the event where its required.
func (conn *OCI8Conn) registerCqnSubscription(i types.SubscriptionHandler) (registrationId int64, subscriptionPtr *C.OCISubscription, err error) {
	var namespace C.ub4 = C.OCI_SUBSCR_NAMESPACE_DBCHANGE
	var rowIds = true
	var qosFlags = C.OCI_SUBSCR_CQ_QOS_BEST_EFFORT // or use OCI_SUBSCR_CQ_QOS_QUERY for query level granularity with no false-positives; use OCI_SUBSCR_CQ_QOS_BEST_EFFORT for best-efforts
	var s *unsafe.Pointer
	// Defer cleanup.
	// TODO: Do we need to clean up the conn here? Check when you have brainpower.
	defer func(errP *error) {
		if *errP != nil {
			if s != nil {
				C.OCIHandleFree(*s, C.OCI_HTYPE_SUBSCRIPTION)
			}
		}
	}(&err)
	// Allocate subscription handle.
	s, _, err = conn.ociHandleAlloc(C.OCI_HTYPE_SUBSCRIPTION, 0)
	if err != nil {
		return
	}
	// Set the namespace.
	err = conn.ociAttrSet(*s, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&namespace), C.sizeof_ub4, C.OCI_ATTR_SUBSCR_NAMESPACE)
	if err != nil {
		return
	}
	// Associate a notification callback with the subscription.
	err = conn.ociAttrSet(*s, C.OCI_HTYPE_SUBSCRIPTION, C.cqnCallback, 0, C.OCI_ATTR_SUBSCR_CALLBACK)
	if err != nil {
		return
	}
	// Allow extraction of rowid information.
	err = conn.ociAttrSet(*s, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&rowIds), C.sizeof_ub4, C.OCI_ATTR_CHNF_ROWIDS)
	if err != nil {
		return
	}
	// QOS Flags.
	err = conn.ociAttrSet(*s, C.OCI_HTYPE_SUBSCRIPTION, unsafe.Pointer(&qosFlags), C.sizeof_ub4, C.OCI_ATTR_SUBSCR_CQ_QOSFLAGS)
	if err != nil {
		return
	}
	// Create a new registration in the DBCHANGE namespace.
	subscriptionPtr = (*C.OCISubscription)(*s)
	err = conn.getError(C.OCISubscriptionRegister(conn.svc, &subscriptionPtr, 1, conn.errHandle, C.OCI_DEFAULT)) // this wants ptr to start of an array of subscription pointers.
	if err != nil {
		return
	}
	// Get the registration ID.
	var regId C.ub8
	regIdSize := C.ub4(C.sizeof_ub8)
	result := C.OCIAttrGet(
		unsafe.Pointer(subscriptionPtr), // unsafe.Pointer(stmt.stmt), // Pointer to a handle type
		C.OCI_HTYPE_SUBSCRIPTION,        // C.OCI_HTYPE_STMT,          // The handle type: OCI_DTYPE_PARAM, for a parameter descriptor
		unsafe.Pointer(&regId),          // Pointer to the storage for an attribute value
		&regIdSize,                      // The size of the attribute value.
		C.OCI_ATTR_SUBSCR_CQ_REGID,      // C.OCI_ATTR_CQ_QUERYID <<< returns 0 for what I think is the first query since multiples can be registered in one subscroption. // The attribute type: https://docs.oracle.com/cd/B19306_01/appdev.102/b14250/ociaahan.htm
		conn.errHandle,                  // An error handle
	)
	if err = conn.getError(result); err != nil { // if there was an error registering...
		return
	} else { // else we have a successful registration...
		// Save the registration ID and supplied interface so the Go callback can use it later.
		// See the C callback used above first: C.cqnCallback(), which leads to the go callback.
		registrationId = int64(regId)           // set the return value.
		if cqnSubscriptionHandlerMap.m == nil { // if the map needs initialising...
			cqnSubscriptionHandlerMap.m = make(map[int64]types.SubscriptionHandler)
		}
		cqnSubscriptionHandlerMap.Lock()
		cqnSubscriptionHandlerMap.m[registrationId] = i
		cqnSubscriptionHandlerMap.Unlock()
	}
	return
}

// ExecuteCqn prepares the query, binds the arguments if []args are provided and executes the CQN query.
// The return value is an error if one occurred else nil.
func (conn *OCI8Conn) executeCqnQuery(subscription *C.OCISubscription, query string, args []interface{}) (stmt *OCI8Stmt, rows driver.Rows, err error) {
	// Build a slice of namedValue using args.
	argsNv := argsToNamedValue(args)

	// Prepare the query/statement.
	stmt, err = conn.prepareStmt(query)
	if err != nil {
		return
	}

	// Set the subscription attribute on the statement.
	err = conn.ociAttrSet(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, unsafe.Pointer(subscription), 0, C.OCI_ATTR_CHNF_REGHANDLE)
	if err != nil {
		return
	}

	conn.inTransaction = false // cause query() to use mode = OCI_COMMIT_ON_SUCCESS
	rows, err = stmt.query(context.Background(), argsNv, false)
	if err != nil { // if there was a query error...
		return
	}

	// // Execute the statement.
	// // TODO: abort if not a SELECT statement - see query() for a check on this attr.
	// err = stmt.ociStmtExecute(0, C.OCI_DEFAULT)
	// if err != nil {
	// 	return
	// }

	// // Commit to release the transaction.
	// // TODO: Rollback instead of commit after this SELECT.
	// if rv := C.OCITransCommit(conn.svc, conn.errHandle, 0, ); rv != C.OCI_SUCCESS {
	// 	err = conn.getError(rv)
	// 	return
	// }

	return
}

// prepareStmt prepares a query and return the raw statement so we can access
// the statement handle.  For example, to set change notifications upon it.
// This is a duplicate of Prepare() which returns an interface.
func (conn *OCI8Conn) prepareStmt(query string) (*OCI8Stmt, error) {
	return conn.prepareStmtContext(context.Background(), query)
}

// prepareStmtContext is a duplicate of PrepareContext().
// See notes in prepareStmt().
func (conn *OCI8Conn) prepareStmtContext(ctx context.Context, query string) (*OCI8Stmt, error) {
	if conn.enableQMPlaceholders {
		query = placeholders(query)
	}

	queryP := cString(query)
	defer C.free(unsafe.Pointer(queryP))

	// statement handle
	stmt, _, err := conn.ociHandleAlloc(C.OCI_HTYPE_STMT, 0)
	if err != nil {
		return nil, fmt.Errorf("allocate statement handle error: %v", err)
	}

	if rv := C.OCIStmtPrepare(
		(*C.OCIStmt)(*stmt),
		conn.errHandle,
		queryP,
		C.ub4(len(query)),
		C.ub4(C.OCI_NTV_SYNTAX),
		C.ub4(C.OCI_DEFAULT),
	); rv != C.OCI_SUCCESS {
		C.OCIHandleFree(*stmt, C.OCI_HTYPE_STMT)
		return nil, conn.getError(rv)
	}

	return &OCI8Stmt{conn: conn, stmt: (*C.OCIStmt)(*stmt)}, nil
}

// freeHandles will clean up any handles allocated in C.
func (conn *OCI8Conn) freeHandles() {
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
	if conn.env != nil {
		C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
		conn.env = nil
	}
}

func (conn *OCI8Conn) unregisterCqn(subscriptionPtr *C.OCISubscription) {
	if subscriptionPtr != nil {
		C.OCISubscriptionUnRegister(conn.svc, subscriptionPtr, conn.errHandle, C.OCI_DEFAULT)
	}
}

// OpenCqnConnection opens a OCI driver connection directly given the supplied DSN.
// The returned CqnConn can be used to execute a Continuous Query Notification statement.
// The user should call CqnConn.Close() to unregister the statement and free private C handles when done.
func OpenCqnConnection(dsnString string) (c CqnConn, err error) {
	d := &OCI8DriverStruct{
		Logger: log.New(ioutil.Discard, "", 0),
	}
	// Connect.
	c.conn, err = d.openOCI8Conn4Cqn(dsnString)
	if err != nil {
		err = errors.Wrap(err, "unable to open Oracle OCI connection for Continuous Query Notification")
		return
	}
	return
}

func (c *CqnConn) Execute(h types.SubscriptionHandler, query string, args []interface{}) (rows driver.Rows, err error) {
	// Create CQN subscription.
	c.m.Lock()
	if !c.subscriptionCreated { // if a subscription hasn't already been created...
		// Create a new subscription and register the handler/callback interface.
		c.registrationId, c.subscriptionPtr, err = c.conn.registerCqnSubscription(h) // saves the handler in a global map using key = the registrationId
		if err != nil {
			return nil, errors.Wrap(err, "error registering query")
		}
		c.subscriptionCreated = true
		// Execute the CQN query.
		// Save the stmt so we can clean up later.
		c.stmt, rows, err = c.conn.executeCqnQuery(c.subscriptionPtr, query, args)
		if err != nil {
			return nil, errors.Wrap(err, "error executing query")
		}
	} else {
		c.m.Unlock()
		return nil, errors.New("CQN subscription exists; call Close() or create a new instance")
	}
	c.m.Unlock()
	return
}

func (c *CqnConn) RemoveSubscription() {
	c.m.Lock()
	defer c.m.Unlock()
	if c.subscriptionCreated {
		// Unregister the CQN.
		c.conn.unregisterCqn(c.subscriptionPtr)
		// Free the subscription handle.
		freeSubscriptionHandle(c.subscriptionPtr)
		// Free the statement handle.
		err := c.stmt.Close()
		if err != nil {
			err = errors.Wrap(err, "error closing statement")
		}
		// Remove the subscription handler interface from our global map.
		cqnSubscriptionHandlerMap.Lock()
		delete(cqnSubscriptionHandlerMap.m, c.registrationId)
		cqnSubscriptionHandlerMap.Unlock()
		// Flag that we're clean.
		c.subscriptionCreated = false
	}
}

func (c *CqnConn) CloseCqnConnection() error {
	c.RemoveSubscription()
	return c.conn.Close()
}

func freeSubscriptionHandle(subscriptionPtr *C.OCISubscription) {
	if subscriptionPtr != nil {
		C.OCIHandleFree(unsafe.Pointer(subscriptionPtr), C.OCI_HTYPE_SUBSCRIPTION)
	}
}

func argsToNamedValue(args []interface{}) []namedValue {
	nv := make([]namedValue, len(args), len(args))
	for i, v := range args {
		nv[i] = namedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return nv
}
