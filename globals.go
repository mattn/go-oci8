package oci8

/*
#include "oci8.go.h"
#cgo !noPkgConfig pkg-config: oci8
*/
import "C"

// noPkgConfig is a Go tag for disabling using pkg-config and using environmental settings like CGO_CFLAGS and CGO_LDFLAGS instead

import (
	"database/sql"
	"database/sql/driver"
	"io/ioutil"
	"log"
	"regexp"
	"time"
	"unsafe"
)

const (
	blobBufSize        = 4000
	useOCISessionBegin = true
	sizeOfNilPointer   = unsafe.Sizeof(unsafe.Pointer(nil))
)

type (
	// DSN is Oracle Data Source Name
	DSN struct {
		Connect                string
		Username               string
		Password               string
		prefetchRows           uint32
		prefetchMemory         uint32
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
		err                  *C.OCIError
		usrSession           *C.OCISession
		prefetchRows         uint32
		prefetchMemory       uint32
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
		bp     **C.OCIBind
		defp   **C.OCIDefine
		pbind  []oci8bind //bind params
	}

	// OCI8Result is Oracle result
	OCI8Result struct {
		n     int64
		errn  error
		id    int64
		errid error
		stmt  *OCI8Stmt
	}

	oci8col struct {
		name string
		kind C.ub2
		size int
		ind  *C.sb2
		rlen *C.ub2
		pbuf unsafe.Pointer
	}

	oci8bind struct {
		kind C.ub2
		pbuf unsafe.Pointer
		clen C.sb4
		out  interface{} // original binded data type
	}

	// OCI8Rows is Oracle rows
	OCI8Rows struct {
		stmt       *OCI8Stmt
		cols       []oci8col
		e          bool
		indrlenptr unsafe.Pointer
		closed     bool
		done       chan struct{}
		cls        bool
	}
)

var (
	phre = regexp.MustCompile(`\?`)

	// OCI8Driver is the sql driver
	OCI8Driver = &OCI8DriverStruct{
		Logger: log.New(ioutil.Discard, "", 0),
	}
)

func init() {
	sql.Register("oci8", OCI8Driver)
}
