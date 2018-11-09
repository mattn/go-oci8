package oci8

/*
#cgo !noPkgConfig pkg-config: oci8
#include "oci8.go.h"
*/
import "C"

// noPkgConfig is a Go tag for disabling using pkg-config and using environmental settings like CGO_CFLAGS and CGO_LDFLAGS instead

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io/ioutil"
	"log"
	"regexp"
	"time"
	"unsafe"
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
		pbind  []oci8Bind //bind params
	}

	// OCI8Result is Oracle result
	OCI8Result struct {
		n     int64
		errn  error
		id    int64
		errid error
		stmt  *OCI8Stmt
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
		length     *C.ub2
		indicator  *C.sb2
		bindHandle *C.OCIBind
		out        interface{} // original binded data type
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

	phre = regexp.MustCompile(`\?`)

	// OCI8Driver is the sql driver
	OCI8Driver = &OCI8DriverStruct{
		Logger: log.New(ioutil.Discard, "", 0),
	}
)

func init() {
	sql.Register("oci8", OCI8Driver)
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
