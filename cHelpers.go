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

// freeBoundParameters frees bound parameters
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
			if col.bindHandle != nil {
				*col.bindHandle = nil
				col.bindHandle = nil
			}
		}
	}
}

// freeDecriptor calles OCIDescriptorFree
func freeDecriptor(p unsafe.Pointer, dtype C.ub4) {
	tptr := *(*unsafe.Pointer)(p)
	C.OCIDescriptorFree(unsafe.Pointer(tptr), dtype)
}
