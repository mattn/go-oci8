package oci8

/*
#include "oci8.go.h"
#cgo !noPkgConfig pkg-config: oci8
*/
import "C"

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"unsafe"
)

// noPkgConfig is a Go tag for disabling using pkg-config and using environmental settings like CGO_CFLAGS and CGO_LDFLAGS instead

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

// freeDecriptor calles C OCIDescriptorFree
func freeDecriptor(p unsafe.Pointer, dtype C.ub4) {
	tptr := *(*unsafe.Pointer)(p)
	C.OCIDescriptorFree(unsafe.Pointer(tptr), dtype)
}

// getError gets error from return value (sword) or OCIError
func getError(rv C.sword, err *C.OCIError) error {
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
		errorCode, err := ociGetError(err)
		switch errorCode {
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
		case 28, 1012, 1033, 1034, 1089, 3113, 3114, 3135, 12528, 12537:
			return driver.ErrBadConn
		}
		return err
	}
	return fmt.Errorf("oracle return error code %d", rv)
}

// ociGetError gets error code and text
func ociGetError(err *C.OCIError) (int, error) {
	var errorCode C.sb4
	errorText := make([]byte, 1024)

	C.OCIErrorGet(
		unsafe.Pointer(err),         // error handle
		1,                           // status record number, starts from 1
		nil,                         // sqlstate, not supported in release 8.x or later
		&errorCode,                  // error code
		(*C.OraText)(&errorText[0]), // error message text
		1024,              // size of the buffer provided in number of bytes
		C.OCI_HTYPE_ERROR, // type of the handle (OCI_HTYPE_ERR or OCI_HTYPE_ENV)
	)

	index := bytes.IndexByte(errorText, 0)

	return int(errorCode), errors.New(string(errorText[:index]))
}

// CByte comverts byte slice to C char
func CByte(b []byte) *C.char {
	p := C.malloc(C.size_t(len(b)))
	pp := (*[1 << 30]byte)(p)
	copy(pp[:], b)
	return (*C.char)(p)
}
