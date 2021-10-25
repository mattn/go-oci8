package oci8

// #cgo darwin LDFLAGS: -L /usr/local/lib -lclntsh
// #cgo darwin CFLAGS: -I/usr/local/include
// #cgo linux LDFLAGS: -L /usr/local/lib -lclntsh
// #cgo linux CFLAGS: -I/usr/local/include
// #cgo freebsd LDFLAGS: -L /usr/local/lib -lclntsh
// #cgo freebsd CFLAGS: -I/usr/local/include
// #include "oci8.go.h"
import "C"

import (
	"errors"
	"io/ioutil"
	"log"
	"regexp"
	"time"
	"unsafe"

	"github.com/relloyd/go-sql/database/sql"
	"github.com/relloyd/go-sql/database/sql/driver"
)

const (
	lobBufferSize      = 4000
	useOCISessionBegin = true
	sizeOfNilPointer   = unsafe.Sizeof(unsafe.Pointer(nil))
)

type (
	// DSN is Oracle Data Source Name
	DSN struct {
		Connect                string
		Username               string
		Password               string
		prefetchRows           C.ub4
		prefetchMemory         C.ub4
		Location               *time.Location
		transactionMode        C.ub4
		enableQMPlaceholders   bool
		operationMode          C.ub4
		externalauthentication bool
	}

	// OCI8DriverStruct is Oracle driver struct
	OCI8DriverStruct struct {
		// Logger is used to log connection ping errors, defaults to discard
		// To log set it to something like: log.New(os.Stderr, "oci8 ", log.Ldate|log.Ltime|log.LUTC|log.Llongfile)
		Logger *log.Logger
	}

	// OCI8Connector is the sql driver connector
	OCI8Connector struct {
		// Logger is used to log connection ping errors
		Logger *log.Logger
	}

	// OCI8Conn is Oracle connection
	OCI8Conn struct {
		svc                  *C.OCISvcCtx
		srv                  *C.OCIServer
		env                  *C.OCIEnv
		errHandle            *C.OCIError
		usrSession           *C.OCISession
		prefetchRows         C.ub4
		prefetchMemory       C.ub4
		location             *time.Location
		transactionMode      C.ub4
		operationMode        C.ub4
		inTransaction        bool
		enableQMPlaceholders bool
		closed               bool
		logger               *log.Logger
	}

	// OCI8Tx is Oracle transaction
	OCI8Tx struct {
		conn *OCI8Conn
	}

	namedValue struct {
		Name    string
		Ordinal int
		Value   driver.Value
	}

	outValue struct {
		Dest interface{}
		In   bool
	}

	// OCI8Stmt is Oracle statement
	OCI8Stmt struct {
		conn   *OCI8Conn
		stmt   *C.OCIStmt
		closed bool
		pbind  []oci8Bind // bind params
	}

	// OCI8Result is Oracle result
	OCI8Result struct {
		rowsAffected    int64
		rowsAffectedErr error
		rowid           string
		rowidErr        error
		stmt            *OCI8Stmt
	}

	oci8Define struct {
		name         string
		dataType     C.ub2
		pbuf         unsafe.Pointer
		maxSize      C.sb4
		length       *C.ub2
		indicator    *C.sb2
		defineHandle *C.OCIDefine
	}

	oci8Bind struct {
		dataType   C.ub2
		pbuf       unsafe.Pointer
		maxSize    C.sb4
		length     *C.ub2 // for batch (array bind) mode, this is the OCI parameter "alenp", which needs to be a ptr to an array of actual lengths, one per col value.
		indicator  *C.sb2 // for batch (array bind) mode, this is the array of flags that tells OCI whether a value is null (for example, specify -1 to cause a null to be inserted)
		bindHandle *C.OCIBind
		out        interface{} // original binded data type
		iters      int         // store number of values held in pbuf (also matches the len of length when it is an array for batch binding).
	}

	// OCI8Rows is Oracle rows
	OCI8Rows struct {
		stmt    *OCI8Stmt
		defines []oci8Define
		e       bool
		closed  bool
		done    chan struct{}
		cls     bool
	}
)

var (
	// ErrOCISuccessWithInfo is OCI_SUCCESS_WITH_INFO
	ErrOCISuccessWithInfo = errors.New("OCI_SUCCESS_WITH_INFO")
	// ErrNoRowid is result has no rowid
	ErrNoRowid = errors.New("result has no rowid")

	phre           = regexp.MustCompile(`\?`)
	defaultCharset = C.ub2(0)

	// OCI8Driver is the sql driver
	OCI8Driver = &OCI8DriverStruct{
		Logger: log.New(ioutil.Discard, "", 0),
	}
)

func init() {
	sql.Register("oci8", OCI8Driver)

	// set defaultCharset to AL32UTF8
	var envP *C.OCIEnv
	envPP := &envP
	var result C.sword
	result = C.OCIEnvCreate(envPP, C.OCI_DEFAULT, nil, nil, nil, nil, 0, nil)
	if result != C.OCI_SUCCESS {
		panic("OCIEnvCreate error")
	}
	nlsLang := cString("AL32UTF8")
	defaultCharset = C.OCINlsCharSetNameToId(unsafe.Pointer(*envPP), (*C.oratext)(nlsLang))
	C.free(unsafe.Pointer(nlsLang))
	C.OCIHandleFree(unsafe.Pointer(*envPP), C.OCI_HTYPE_ENV)
}

/*
OCI Documentation Notes

Datatypes:
https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci03typ.htm#CEGIEEJI

Handle and Descriptor Attributes:
https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/ociaahan.htm#i442199

OCI Function Server Round Trips:
https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/ociacrou.htm#g430405

OCI examples:
https://github.com/alexeyvo/oracle_oci_examples

Oracle datatypes:
https://ss64.com/ora/syntax-datatypes.html
*/
