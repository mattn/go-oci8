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
	"strconv"
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
		length     *C.ub2
		indicator  *C.sb2
		bindHandle *C.OCIBind
		out        sql.Out
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

	timeLocations []*time.Location
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
