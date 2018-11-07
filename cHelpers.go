package oci8

// #include "oci8.go.h"
import "C"

import (
	"unsafe"
)

// getInt64 gets int64 from pointer
func getInt64(p unsafe.Pointer) int64 {
	return int64(*(*C.sb8)(p))
}

// getUint64 gets uint64 from pointer
func getUint64(p unsafe.Pointer) uint64 {
	return uint64(*(*C.sb8)(p))
}

// CByte comverts byte slice to C char
func CByte(b []byte) *C.char {
	p := C.malloc(C.size_t(len(b)))
	pp := (*[1 << 30]byte)(p)
	copy(pp[:], b)
	return (*C.char)(p)
}

// CStringN coverts string to C string with size
func CStringN(s string, size int) *C.char {
	p := C.malloc(C.size_t(size))
	pp := (*[1 << 30]byte)(p)
	copy(pp[:], s)
	if len(s) < size {
		pp[len(s)] = 0
	} else {
		pp[size-1] = 0
	}
	return (*C.char)(p)
}

// freeDefines frees defines
func freeDefines(defines []oci8Define) {
	for _, define := range defines {
		if define.pbuf != nil {
			switch define.dataType {
			case C.SQLT_CLOB, C.SQLT_BLOB:
				freeDecriptor(define.pbuf, C.OCI_DTYPE_LOB)
			case C.SQLT_TIMESTAMP:
				freeDecriptor(define.pbuf, C.OCI_DTYPE_TIMESTAMP)
			case C.SQLT_TIMESTAMP_TZ:
				freeDecriptor(define.pbuf, C.OCI_DTYPE_TIMESTAMP_TZ)
			case C.SQLT_INTERVAL_DS:
				freeDecriptor(define.pbuf, C.OCI_DTYPE_INTERVAL_DS)
			case C.SQLT_INTERVAL_YM:
				freeDecriptor(define.pbuf, C.OCI_DTYPE_INTERVAL_YM)
			default:
				C.free(define.pbuf)
			}
			define.pbuf = nil
		}
		if define.length != nil {
			C.free(unsafe.Pointer(define.length))
			define.length = nil
		}
		if define.indicator != nil {
			C.free(unsafe.Pointer(define.indicator))
			define.indicator = nil
		}
		define.defineHandle = nil // should be freed by oci statment close
	}
}

// freeBinds frees binds
func freeBinds(binds []oci8Bind) {
	for _, bind := range binds {
		if bind.pbuf != nil {
			switch bind.dataType {
			case C.SQLT_CLOB, C.SQLT_BLOB:
				freeDecriptor(bind.pbuf, C.OCI_DTYPE_LOB)
			case C.SQLT_TIMESTAMP:
				freeDecriptor(bind.pbuf, C.OCI_DTYPE_TIMESTAMP)
			case C.SQLT_TIMESTAMP_TZ:
				freeDecriptor(bind.pbuf, C.OCI_DTYPE_TIMESTAMP_TZ)
			case C.SQLT_TIMESTAMP_LTZ:
				freeDecriptor(bind.pbuf, C.OCI_DTYPE_TIMESTAMP_LTZ)
			case C.SQLT_INTERVAL_DS:
				freeDecriptor(bind.pbuf, C.OCI_DTYPE_INTERVAL_DS)
			case C.SQLT_INTERVAL_YM:
				freeDecriptor(bind.pbuf, C.OCI_DTYPE_INTERVAL_YM)
			default:
				C.free(bind.pbuf)
			}
			bind.pbuf = nil
		}
		if bind.length != nil {
			C.free(unsafe.Pointer(bind.length))
			bind.length = nil
		}
		if bind.indicator != nil {
			C.free(unsafe.Pointer(bind.indicator))
			bind.indicator = nil
		}
		bind.bindHandle = nil // freed by oci statment close
	}
}

// freeDecriptor calles OCIDescriptorFree
func freeDecriptor(p unsafe.Pointer, dtype C.ub4) {
	tptr := *(*unsafe.Pointer)(p)
	C.OCIDescriptorFree(unsafe.Pointer(tptr), dtype)
}
