package oci8

/*
#cgo !noPkgConfig pkg-config: oci8
#include "oci8.go.h"
*/
import "C"

// noPkgConfig is a Go tag for disabling using pkg-config and using environmental settings like CGO_CFLAGS and CGO_LDFLAGS instead

import (
	"context"
	"database/sql"
	"errors"
	"io/ioutil"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"sync"
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
		Connect              string
		Username             string
		Password             string
		prefetchRows         C.ub4
		prefetchMemory       C.ub4
		timeLocation         *time.Location
		transactionMode      C.ub4
		enableQMPlaceholders bool
		operationMode        C.ub4
		stmtCacheSize        C.ub4
	}

	// DriverStruct is Oracle driver struct
	DriverStruct struct {
		// Logger is used to log connection ping errors, defaults to discard
		// To log set it to something like: log.New(os.Stderr, "oci8 ", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile)
		Logger *log.Logger
	}

	// Connector is the sql driver connector
	Connector struct {
		// Logger is used to log connection ping errors
		Logger *log.Logger
	}

	// Conn is Oracle connection
	Conn struct {
		svc                  *C.OCISvcCtx
		srv                  *C.OCIServer
		env                  *C.OCIEnv
		errHandle            *C.OCIError
		usrSession           *C.OCISession
		prefetchRows         C.ub4
		prefetchMemory       C.ub4
		transactionMode      C.ub4
		operationMode        C.ub4
		stmtCacheSize        C.ub4
		inTransaction        bool
		enableQMPlaceholders bool
		closed               bool
		timeLocation         *time.Location
		logger               *log.Logger
	}

	// Tx is Oracle transaction
	Tx struct {
		conn *Conn
	}

	// Stmt is Oracle statement
	Stmt struct {
		conn        *Conn
		stmt        *C.OCIStmt
		closed      bool
		ctx         context.Context
		cacheKey    string // if statement caching is enabled, this is the key for this statement into the cache
		releaseMode C.ub4
	}

	// Rows is Oracle rows
	Rows struct {
		stmt    *Stmt
		defines []defineStruct
		closed  bool
	}

	// Result is Oracle result
	Result struct {
		rowsAffected    int64
		rowsAffectedErr error
		rowid           string
		rowidErr        error
		stmt            *Stmt
	}

	defineStruct struct {
		name         string
		dataType     C.ub2
		pbuf         unsafe.Pointer
		maxSize      C.sb4
		length       *C.ub2
		indicator    *C.sb2
		defineHandle *C.OCIDefine
		subDefines   []defineStruct
	}

	bindStruct struct {
		dataType   C.ub2
		pbuf       unsafe.Pointer
		maxSize    C.sb4
		length     *C.ub2
		indicator  *C.sb2
		bindHandle *C.OCIBind
		out        sql.Out
	}
)

var (
	// ErrOCIInvalidHandle is OCI_INVALID_HANDLE
	ErrOCIInvalidHandle = errors.New("OCI_INVALID_HANDLE")
	// ErrOCISuccessWithInfo is OCI_SUCCESS_WITH_INFO
	ErrOCISuccessWithInfo = errors.New("OCI_SUCCESS_WITH_INFO")
	// ErrOCIReservedForIntUse is OCI_RESERVED_FOR_INT_USE
	ErrOCIReservedForIntUse = errors.New("OCI_RESERVED_FOR_INT_USE")
	// ErrOCINoData is OCI_NO_DATA
	ErrOCINoData = errors.New("OCI_NO_DATA")
	// ErrOCINeedData is OCI_NEED_DATA
	ErrOCINeedData = errors.New("OCI_NEED_DATA")
	// ErrOCIStillExecuting is OCI_STILL_EXECUTING
	ErrOCIStillExecuting = errors.New("OCI_STILL_EXECUTING")

	// ErrNoRowid is result has no rowid
	ErrNoRowid = errors.New("result has no rowid")

	phre           = regexp.MustCompile(`\?`)
	defaultCharset = C.ub2(0)

	typeNil       = reflect.TypeOf(nil)
	typeString    = reflect.TypeOf("a")
	typeSliceByte = reflect.TypeOf([]byte{})
	typeInt64     = reflect.TypeOf(int64(1))
	typeFloat64   = reflect.TypeOf(float64(1))
	typeTime      = reflect.TypeOf(time.Time{})

	// Driver is the sql driver
	Driver = &DriverStruct{
		Logger: log.New(ioutil.Discard, "", 0),
	}

	timeLocations []*time.Location

	byteBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, lobBufferSize)
		},
	}
)

func init() {
	sql.Register("oci8", Driver)

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

	// build timeLocations: GMT -12 to 14
	timeLocationNames := []string{"Etc/GMT+12", "Pacific/Pago_Pago", // -12 to -11
		"Pacific/Honolulu", "Pacific/Gambier", "Pacific/Pitcairn", "America/Phoenix", "America/Costa_Rica", // -10 to -6
		"America/Panama", "America/Puerto_Rico", "America/Punta_Arenas", "America/Noronha", "Atlantic/Cape_Verde", // -5 to -1
		"GMT",                                                                         // 0
		"Africa/Lagos", "Africa/Cairo", "Europe/Moscow", "Asia/Dubai", "Asia/Karachi", // 1 to 5
		"Asia/Dhaka", "Asia/Jakarta", "Asia/Shanghai", "Asia/Tokyo", "Australia/Brisbane", // 6 to 10
		"Pacific/Noumea", "Asia/Anadyr", "Pacific/Enderbury", "Pacific/Kiritimati", // 11 to 14
	}
	var err error
	timeLocations = make([]*time.Location, len(timeLocationNames))
	for i := 0; i < len(timeLocations); i++ {
		timeLocations[i], err = time.LoadLocation(timeLocationNames[i])
		if err != nil {
			name := "GMT"
			if i < 12 {
				name += strconv.FormatInt(int64(i-12), 10)
			} else if i > 12 {
				name += "+" + strconv.FormatInt(int64(i-12), 10)
			}
			timeLocations[i] = time.FixedZone(name, 3600*(i-12))
		}
	}
}

/*
OCI Documentation Notes

Datatypes:
https://docs.oracle.com/en/database/oracle/oracle-database/12.2/lnoci/data-types.html

Handle and Descriptor Attributes:
https://docs.oracle.com/en/database/oracle/oracle-database/12.2/lnoci/handle-and-descriptor-attributes.html

OCI Function Server Round Trips:
https://docs.oracle.com/en/database/oracle/oracle-database/12.2/lnoci/oci-function-server-round-trips.html

OCI examples:
https://github.com/alexeyvo/oracle_oci_examples

Oracle datatypes:
https://ss64.com/ora/syntax-datatypes.html
*/
