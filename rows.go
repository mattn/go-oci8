package oci8

/*
#include "oci8.go.h"
#cgo !noPkgConfig pkg-config: oci8
*/
import "C"

// noPkgConfig is a Go tag for disabling using pkg-config and using environmental settings like CGO_CFLAGS and CGO_LDFLAGS instead

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"time"
	"unsafe"
)

// Close closes rows
func (rows *OCI8Rows) Close() error {
	if rows.closed {
		return nil
	}
	rows.closed = true

	close(rows.done)

	if rows.cls {
		rows.stmt.Close()
	}

	C.free(rows.indrlenptr)
	for _, col := range rows.cols {
		switch col.kind {
		case C.SQLT_CLOB, C.SQLT_BLOB:
			freeDecriptor(col.pbuf, C.OCI_DTYPE_LOB)
		case C.SQLT_TIMESTAMP:
			freeDecriptor(col.pbuf, C.OCI_DTYPE_TIMESTAMP)
		case C.SQLT_TIMESTAMP_TZ:
			freeDecriptor(col.pbuf, C.OCI_DTYPE_TIMESTAMP_TZ)
		case C.SQLT_INTERVAL_DS:
			freeDecriptor(col.pbuf, C.OCI_DTYPE_INTERVAL_DS)
		case C.SQLT_INTERVAL_YM:
			freeDecriptor(col.pbuf, C.OCI_DTYPE_INTERVAL_YM)
		default:
			C.free(col.pbuf)
		}
		col.pbuf = nil
	}
	return nil
}

// Columns returns columns
func (rows *OCI8Rows) Columns() []string {
	cols := make([]string, len(rows.cols))
	for i, col := range rows.cols {
		cols[i] = col.name
	}
	return cols
}

// Next gets next row
func (rows *OCI8Rows) Next(dest []driver.Value) error {
	if rows.closed {
		return nil
	}

	rv := C.OCIStmtFetch2(
		rows.stmt.stmt,
		rows.stmt.conn.err,
		1,
		C.OCI_FETCH_NEXT,
		0,
		C.OCI_DEFAULT)
	if rv == C.OCI_NO_DATA {
		return io.EOF
	} else if rv != C.OCI_SUCCESS && rv != C.OCI_SUCCESS_WITH_INFO {
		return getError(rv, rows.stmt.conn.err)
	}

	for i := range dest {
		// TODO: switch rows.cols[i].ind
		if *rows.cols[i].ind == -1 { // Null
			dest[i] = nil
			continue
		} else if *rows.cols[i].ind != 0 {
			return fmt.Errorf("Unknown column indicator: %d, col %s", rows.cols[i].ind, rows.cols[i].name)
		}

		switch rows.cols[i].kind {

		// SQLT_DAT
		case C.SQLT_DAT: // for test, date are return as timestamp
			buf := (*[1 << 30]byte)(rows.cols[i].pbuf)[0:*rows.cols[i].rlen]
			// TODO: Handle BCE dates (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#438305)
			// TODO: Handle timezones (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#443601)
			dest[i] = time.Date(
				(int(buf[0])-100)*100+(int(buf[1])-100),
				time.Month(int(buf[2])),
				int(buf[3]),
				int(buf[4])-1,
				int(buf[5])-1,
				int(buf[6])-1,
				0,
				rows.stmt.conn.location)

		// SQLT_BLOB and SQLT_CLOB
		case C.SQLT_BLOB, C.SQLT_CLOB:
			// get character set form
			csfrm := C.ub1(C.SQLCS_IMPLICIT)
			rv = C.OCILobCharSetForm(
				rows.stmt.conn.env,
				rows.stmt.conn.err,
				*(**C.OCILobLocator)(rows.cols[i].pbuf),
				&csfrm,
			)
			if rv != C.OCI_SUCCESS {
				return getError(rv, rows.stmt.conn.err)
			}

			ptmp := unsafe.Pointer(uintptr(rows.cols[i].pbuf) + sizeOfNilPointer)
			bamt := (*C.ub4)(ptmp)
			ptmp = unsafe.Pointer(uintptr(rows.cols[i].pbuf) + unsafe.Sizeof(C.ub4(0)) + sizeOfNilPointer)
			b := (*[1 << 30]byte)(ptmp)[0:blobBufSize]
			var buf []byte

			// get lob
			for {
				// read lob while OCI_NEED_DATA
				*bamt = 0
				rv = C.OCILobRead(
					rows.stmt.conn.svc,
					rows.stmt.conn.err,
					*(**C.OCILobLocator)(rows.cols[i].pbuf),
					bamt,               // The amount/length in bytes/characters
					1,                  // The absolute offset from the beginning of the LOB value. The subsequent polling calls the offset parameter is ignored.
					ptmp,               // The pointer to a buffer into which the piece will be read
					C.ub4(blobBufSize), // The length of the buffer in octets, in bytes/characters.
					nil,                // The context pointer for the callback function. Can be null.
					nil,                // If this is null, then OCI_NEED_DATA will be returned for each piece.
					0,                  // If this value is 0 then csid is set to the client's NLS_LANG or NLS_CHAR value.
					csfrm,              // The character set form of the buffer data: SQLCS_IMPLICIT or SQLCS_NCHAR
				)
				if rv == C.OCI_NEED_DATA {
					buf = append(buf, b[:int(*bamt)]...)
				} else {
					break
				}
			}
			if rv != C.OCI_SUCCESS {
				return getError(rv, rows.stmt.conn.err)
			}

			// set dest to buffer
			if rows.cols[i].kind == C.SQLT_BLOB {
				dest[i] = append(buf, b[:int(*bamt)]...)
			} else {
				dest[i] = string(append(buf, b[:int(*bamt)]...))
			}

		// SQLT_CHR, SQLT_AFC, and SQLT_AVC
		case C.SQLT_CHR, C.SQLT_AFC, C.SQLT_AVC:
			buf := (*[1 << 30]byte)(rows.cols[i].pbuf)[0:*rows.cols[i].rlen]
			switch {
			case *rows.cols[i].ind == 0: // Normal
				dest[i] = string(buf)
			case *rows.cols[i].ind == -2 || // Field longer than type (truncated)
				*rows.cols[i].ind > 0: // Field longer than type (truncated). Value is original length.
				dest[i] = string(buf)
			default:
				return fmt.Errorf("Unknown column indicator: %d", rows.cols[i].ind)
			}

		// SQLT_BIN
		case C.SQLT_BIN: // RAW
			buf := (*[1 << 30]byte)(rows.cols[i].pbuf)[0:*rows.cols[i].rlen]
			dest[i] = buf

		// SQLT_NUM
		case C.SQLT_NUM: // NUMBER
			buf := (*[21]byte)(rows.cols[i].pbuf)[0:*rows.cols[i].rlen]
			dest[i] = buf

		// SQLT_VNU
		case C.SQLT_VNU: // VARNUM
			buf := (*[22]byte)(rows.cols[i].pbuf)[0:*rows.cols[i].rlen]
			dest[i] = buf

		// SQLT_INT
		case C.SQLT_INT: // INT
			buf := (*[8]byte)(rows.cols[i].pbuf)[0:*rows.cols[i].rlen]
			var data int64
			err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, &data)
			if err != nil {
				return fmt.Errorf("binary read for column %v - error: %v", i, err)
			}
			dest[i] = data

		// SQLT_BDOUBLE
		case C.SQLT_BDOUBLE: // native double
			buf := (*[8]byte)(rows.cols[i].pbuf)[0:*rows.cols[i].rlen]
			var data float64
			err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, &data)
			if err != nil {
				return fmt.Errorf("binary read for column %v - error: %v", i, err)
			}
			dest[i] = data

		// SQLT_LNG
		case C.SQLT_LNG: // LONG
			buf := (*[1 << 30]byte)(rows.cols[i].pbuf)[0:*rows.cols[i].rlen]
			dest[i] = buf

		// SQLT_TIMESTAMP
		case C.SQLT_TIMESTAMP:
			if rv := C.WrapOCIDateTimeGetDateTime(
				rows.stmt.conn.env,
				rows.stmt.conn.err,
				*(**C.OCIDateTime)(rows.cols[i].pbuf),
			); rv.rv != C.OCI_SUCCESS {
				return getError(rv.rv, rows.stmt.conn.err)
			} else {
				dest[i] = time.Date(
					int(rv.y),
					time.Month(rv.m),
					int(rv.d),
					int(rv.hh),
					int(rv.mm),
					int(rv.ss),
					int(rv.ff),
					rows.stmt.conn.location,
				)
			}

		// SQLT_TIMESTAMP_TZ and SQLT_TIMESTAMP_LTZ
		case C.SQLT_TIMESTAMP_TZ, C.SQLT_TIMESTAMP_LTZ:
			tptr := *(**C.OCIDateTime)(rows.cols[i].pbuf)
			rv := C.WrapOCIDateTimeGetDateTime(
				rows.stmt.conn.env,
				rows.stmt.conn.err,
				tptr)
			if rv.rv != C.OCI_SUCCESS {
				return getError(rv.rv, rows.stmt.conn.err)
			}
			rvz := C.WrapOCIDateTimeGetTimeZoneNameOffset(
				rows.stmt.conn.env,
				rows.stmt.conn.err,
				tptr)
			if rvz.rv != C.OCI_SUCCESS {
				return getError(rvz.rv, rows.stmt.conn.err)
			}
			nnn := C.GoStringN((*C.char)((unsafe.Pointer)(&rvz.zone[0])), C.int(rvz.zlen))
			loc, err := time.LoadLocation(nnn)
			if err != nil {
				// TODO: reuse locations
				loc = time.FixedZone(nnn, int(rvz.h)*60*60+int(rvz.m)*60)
			}
			dest[i] = time.Date(
				int(rv.y),
				time.Month(rv.m),
				int(rv.d),
				int(rv.hh),
				int(rv.mm),
				int(rv.ss),
				int(rv.ff),
				loc)

		// SQLT_INTERVAL_DS
		case C.SQLT_INTERVAL_DS:
			iptr := *(**C.OCIInterval)(rows.cols[i].pbuf)
			rv := C.WrapOCIIntervalGetDaySecond(
				rows.stmt.conn.env,
				rows.stmt.conn.err,
				iptr)
			if rv.rv != C.OCI_SUCCESS {
				return getError(rv.rv, rows.stmt.conn.err)
			}
			dest[i] = int64(time.Duration(rv.d)*time.Hour*24 + time.Duration(rv.hh)*time.Hour + time.Duration(rv.mm)*time.Minute + time.Duration(rv.ss)*time.Second + time.Duration(rv.ff))

		// SQLT_INTERVAL_YM
		case C.SQLT_INTERVAL_YM:
			iptr := *(**C.OCIInterval)(rows.cols[i].pbuf)
			rv := C.WrapOCIIntervalGetYearMonth(
				rows.stmt.conn.env,
				rows.stmt.conn.err,
				iptr)
			if rv.rv != C.OCI_SUCCESS {
				return getError(rv.rv, rows.stmt.conn.err)
			}
			dest[i] = int64(rv.y)*12 + int64(rv.m)

		// default
		default:
			return fmt.Errorf("Unhandled column type: %d", rows.cols[i].kind)

		}

	}

	return nil
}

// ColumnTypeDatabaseTypeName implement RowsColumnTypeDatabaseTypeName.
func (rows *OCI8Rows) ColumnTypeDatabaseTypeName(i int) string {
	var p unsafe.Pointer
	var tp C.ub2

	rp := C.WrapOCIParamGet(unsafe.Pointer(rows.stmt.stmt), C.OCI_HTYPE_STMT, rows.stmt.conn.err, C.ub4(i+1))
	if rp.rv == C.OCI_SUCCESS {
		p = rp.ptr
	}

	tpr := C.WrapOCIAttrGetUb2(p, C.OCI_DTYPE_PARAM, C.OCI_ATTR_DATA_TYPE, rows.stmt.conn.err)
	if tpr.rv == C.OCI_SUCCESS {
		tp = tpr.num
	}

	switch tp {
	case C.SQLT_CHR:
		return "SQLT_CHR"
	case C.SQLT_NUM:
		return "SQLT_NUM"
	case C.SQLT_INT:
		return "SQLT_INT"
	case C.SQLT_FLT:
		return "SQLT_FLT"
	case C.SQLT_STR:
		return "SQLT_STR"
	case C.SQLT_VNU:
		return "SQLT_VNU"
	case C.SQLT_LNG:
		return "SQLT_LNG"
	case C.SQLT_VCS:
		return "SQLT_VCS"
	case C.SQLT_DAT:
		return "SQLT_DAT"
	case C.SQLT_VBI:
		return "SQLT_VBI"
	case C.SQLT_BFLOAT:
		return "SQLT_BFLOAT"
	case C.SQLT_BDOUBLE:
		return "SQLT_BDOUBLE"
	case C.SQLT_BIN:
		return "SQLT_BIN"
	case C.SQLT_LBI:
		return "SQLT_LBI"
	case C.SQLT_UIN:
		return "SQLT_UIN"
	case C.SQLT_LVC:
		return "SQLT_LVC"
	case C.SQLT_LVB:
		return "SQLT_LVB"
	case C.SQLT_AFC:
		return "SQLT_AFC"
	case C.SQLT_AVC:
		return "SQLT_AVC"
	case C.SQLT_RDD:
		return "SQLT_RDD"
	case C.SQLT_NTY:
		return "SQLT_NTY"
	case C.SQLT_REF:
		return "SQLT_REF"
	case C.SQLT_CLOB:
		return "SQLT_CLOB"
	case C.SQLT_BLOB:
		return "SQLT_BLOB"
	case C.SQLT_FILE:
		return "SQLT_FILE"
	case C.SQLT_VST:
		return "SQLT_VST"
	case C.SQLT_ODT:
		return "SQLT_ODT"
	case C.SQLT_DATE:
		return "SQLT_DATE"
	case C.SQLT_TIMESTAMP:
		return "SQLT_TIMESTAMP"
	case C.SQLT_TIMESTAMP_TZ:
		return "SQLT_TIMESTAMP_TZ"
	case C.SQLT_INTERVAL_YM:
		return "SQLT_INTERVAL_YM"
	case C.SQLT_INTERVAL_DS:
		return "SQLT_INTERVAL_DS"
	case C.SQLT_TIMESTAMP_LTZ:
		return "SQLT_TIMESTAMP_LTZ"
	}
	return ""
}

// ColumnTypeLength returns column length
func (rows *OCI8Rows) ColumnTypeLength(i int) (length int64, ok bool) {
	var p unsafe.Pointer
	var lp C.ub2

	rp := C.WrapOCIParamGet(unsafe.Pointer(rows.stmt.stmt), C.OCI_HTYPE_STMT, rows.stmt.conn.err, C.ub4(i+1))
	if rp.rv != C.OCI_SUCCESS {
		return 0, false
	}
	p = rp.ptr

	lpr := C.WrapOCIAttrGetUb2(p, C.OCI_DTYPE_PARAM, C.OCI_ATTR_DATA_SIZE, rows.stmt.conn.err)
	if lpr.rv != C.OCI_SUCCESS {
		return 0, false
	}
	lp = lpr.num

	return int64(lp), true
}

/*
func (rows *OCI8Rows) ColumnTypePrecisionScale(i int) (precision, scale int64, ok bool) {
	return 0, 0, false
}
*/

// ColumnTypeNullable implement RowsColumnTypeNullable.
func (rows *OCI8Rows) ColumnTypeNullable(i int) (nullable, ok bool) {
	retUb4 := C.WrapOCIAttrGetUb4(unsafe.Pointer(rows.stmt.stmt), C.OCI_HTYPE_STMT, C.OCI_ATTR_IS_NULL, rows.stmt.conn.err)
	if retUb4.rv != C.OCI_SUCCESS {
		return false, false
	}
	return retUb4.num != 0, true
}

// ColumnTypeScanType implement RowsColumnTypeScanType.
func (rows *OCI8Rows) ColumnTypeScanType(i int) reflect.Type {
	var p unsafe.Pointer
	var tp C.ub2

	rp := C.WrapOCIParamGet(unsafe.Pointer(rows.stmt.stmt), C.OCI_HTYPE_STMT, rows.stmt.conn.err, C.ub4(i+1))
	if rp.rv == C.OCI_SUCCESS {
		p = rp.ptr
	}
	tpr := C.WrapOCIAttrGetUb2(p, C.OCI_DTYPE_PARAM, C.OCI_ATTR_DATA_TYPE, rows.stmt.conn.err)
	if tpr.rv == C.OCI_SUCCESS {
		tp = tpr.num
	}

	switch tp {
	case C.SQLT_CHR, C.SQLT_AFC, C.SQLT_VCS, C.SQLT_AVC:
		return reflect.SliceOf(reflect.TypeOf(""))
	case C.SQLT_BIN:
		return reflect.SliceOf(reflect.TypeOf(byte(0)))
	case C.SQLT_NUM:
		return reflect.TypeOf(int64(0))
	case C.SQLT_IBDOUBLE, C.SQLT_IBFLOAT:
		return reflect.TypeOf(float64(0))
	case C.SQLT_CLOB, C.SQLT_BLOB:
		return reflect.SliceOf(reflect.TypeOf(byte(0)))
	case C.SQLT_TIMESTAMP, C.SQLT_DAT:
		return reflect.TypeOf(time.Time{})
	case C.SQLT_TIMESTAMP_TZ, C.SQLT_TIMESTAMP_LTZ:
		return reflect.TypeOf(time.Time{})
	case C.SQLT_INTERVAL_DS:
		return reflect.TypeOf(time.Duration(0))
	case C.SQLT_INTERVAL_YM:
		return reflect.TypeOf(time.Duration(0))
	case C.SQLT_RDD: // rowid
		return reflect.SliceOf(reflect.TypeOf(""))
	default:
		return reflect.SliceOf(reflect.TypeOf(""))
	}
	return reflect.SliceOf(reflect.TypeOf(byte(0)))
}
