package oci8

// #include "oci8.go.h"
import "C"

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

	freeDefines(rows.defines)

	return nil
}

// Columns returns column names
func (rows *OCI8Rows) Columns() []string {
	names := make([]string, len(rows.defines))
	for i, define := range rows.defines {
		names[i] = define.name
	}
	return names
}

// Next gets next row
func (rows *OCI8Rows) Next(dest []driver.Value) error {
	if rows.closed {
		return nil
	}

	result := C.OCIStmtFetch2(
		rows.stmt.stmt,
		rows.stmt.conn.errHandle,
		1,
		C.OCI_FETCH_NEXT,
		0,
		C.OCI_DEFAULT)
	if result == C.OCI_NO_DATA {
		return io.EOF
	} else if result != C.OCI_SUCCESS && result != C.OCI_SUCCESS_WITH_INFO {
		return rows.stmt.conn.getError(result)
	}

	for i := range dest {
		if *rows.defines[i].indicator == -1 { // Null
			dest[i] = nil
			continue
		} else if *rows.defines[i].indicator != 0 {
			return fmt.Errorf("unknown indicator %d for column %s", *rows.defines[i].indicator, rows.defines[i].name)
		}

		switch rows.defines[i].dataType {

		// SQLT_DAT
		case C.SQLT_DAT: // for test, date are return as timestamp
			buf := (*[1 << 30]byte)(rows.defines[i].pbuf)[0:*rows.defines[i].length]
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
			lobLocator := (**C.OCILobLocator)(rows.defines[i].pbuf)

			// set character set form
			form := C.ub1(C.SQLCS_IMPLICIT)
			result = C.OCILobCharSetForm(
				rows.stmt.conn.env,       // environment handle
				rows.stmt.conn.errHandle, // error handle
				*lobLocator,              // LOB locator
				&form,                    // character set form
			)
			if result != C.OCI_SUCCESS {
				return rows.stmt.conn.getError(result)
			}

			buffer, err := rows.stmt.conn.ociLobRead(*lobLocator, form)
			if err != nil {
				return err
			}

			// set dest to buffer
			if rows.defines[i].dataType == C.SQLT_BLOB {
				dest[i] = buffer
			} else {
				dest[i] = string(buffer)
			}

		// SQLT_CHR, SQLT_STR, SQLT_AFC, SQLT_AVC, and SQLT_LNG
		case C.SQLT_CHR, C.SQLT_STR, C.SQLT_AFC, C.SQLT_AVC, C.SQLT_LNG:
			dest[i] = C.GoStringN((*C.char)(rows.defines[i].pbuf), C.int(*rows.defines[i].length))

		// SQLT_BIN
		case C.SQLT_BIN: // RAW
			buf := (*[1 << 30]byte)(rows.defines[i].pbuf)[0:*rows.defines[i].length]
			dest[i] = buf

		// SQLT_NUM
		case C.SQLT_NUM: // NUMBER
			buf := (*[21]byte)(rows.defines[i].pbuf)[0:*rows.defines[i].length]
			dest[i] = buf

		// SQLT_VNU
		case C.SQLT_VNU: // VARNUM
			buf := (*[22]byte)(rows.defines[i].pbuf)[0:*rows.defines[i].length]
			dest[i] = buf

		// SQLT_INT
		case C.SQLT_INT: // INT
			buf := (*[8]byte)(rows.defines[i].pbuf)[0:*rows.defines[i].length]
			var data int64
			err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, &data)
			if err != nil {
				return fmt.Errorf("binary read for column %v - error: %v", i, err)
			}
			dest[i] = data

		// SQLT_BDOUBLE
		case C.SQLT_BDOUBLE: // native double
			buf := (*[8]byte)(rows.defines[i].pbuf)[0:*rows.defines[i].length]
			var data float64
			err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, &data)
			if err != nil {
				return fmt.Errorf("binary read for column %v - error: %v", i, err)
			}
			dest[i] = data

		// SQLT_TIMESTAMP
		case C.SQLT_TIMESTAMP:
			aTime, err := rows.stmt.conn.ociDateTimeToTime(*(**C.OCIDateTime)(rows.defines[i].pbuf), false)
			if err != nil {
				return fmt.Errorf("ociDateTimeToTime for column %v - error: %v", i, err)
			}
			dest[i] = *aTime

		// SQLT_TIMESTAMP_TZ and SQLT_TIMESTAMP_LTZ
		case C.SQLT_TIMESTAMP_TZ, C.SQLT_TIMESTAMP_LTZ:
			aTime, err := rows.stmt.conn.ociDateTimeToTime(*(**C.OCIDateTime)(rows.defines[i].pbuf), true)
			if err != nil {
				return fmt.Errorf("ociDateTimeToTime for column %v - error: %v", i, err)
			}
			dest[i] = *aTime

		// SQLT_INTERVAL_DS
		case C.SQLT_INTERVAL_DS:
			var days C.sb4
			var hours C.sb4
			var minutes C.sb4
			var seconds C.sb4
			var fracSeconds C.sb4
			interval := *(**C.OCIInterval)(rows.defines[i].pbuf)
			result = C.OCIIntervalGetDaySecond(
				unsafe.Pointer(rows.stmt.conn.env), // environment handle
				rows.stmt.conn.errHandle,           // error handle
				&days,                              // days
				&hours,                             // hours
				&minutes,                           // minutes
				&seconds,                           // seconds
				&fracSeconds,                       // fractional seconds
				interval,                           // interval
			)
			if result != C.OCI_SUCCESS {
				return rows.stmt.conn.getError(result)
			}

			dest[i] = (int64(days) * 24 * int64(time.Hour)) + (int64(hours) * int64(time.Hour)) +
				(int64(minutes) * int64(time.Minute)) + (int64(seconds) * int64(time.Second)) + int64(fracSeconds)

		// SQLT_INTERVAL_YM
		case C.SQLT_INTERVAL_YM:
			var years C.sb4
			var months C.sb4
			interval := *(**C.OCIInterval)(rows.defines[i].pbuf)
			result = C.OCIIntervalGetYearMonth(
				unsafe.Pointer(rows.stmt.conn.env), // environment handle
				rows.stmt.conn.errHandle,           // error handle
				&years,                             // year
				&months,                            // month
				interval,                           // interval
			)
			if result != C.OCI_SUCCESS {
				return rows.stmt.conn.getError(result)
			}
			dest[i] = (int64(years) * 12) + int64(months)

		// default
		default:
			return fmt.Errorf("Unhandled column type: %d", rows.defines[i].dataType)

		}

	}

	return nil
}

// ColumnTypeDatabaseTypeName implement RowsColumnTypeDatabaseTypeName.
func (rows *OCI8Rows) ColumnTypeDatabaseTypeName(i int) string {
	param, err := rows.stmt.ociParamGet(C.ub4(i + 1))
	if err != nil {
		// TOFIX: return an error
		return ""
	}
	defer C.OCIDescriptorFree(unsafe.Pointer(param), C.OCI_DTYPE_PARAM)

	var dataType C.ub2 // external datatype of the column: https://docs.oracle.com/cd/E11882_01/appdev.112/e10646/oci03typ.htm#CEGIEEJI
	_, err = rows.stmt.conn.ociAttrGet(param, unsafe.Pointer(&dataType), C.OCI_ATTR_DATA_TYPE)
	if err != nil {
		// TOFIX: return an error
		return ""
	}

	switch dataType {
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
	param, err := rows.stmt.ociParamGet(C.ub4(i + 1))
	if err != nil {
		return 0, false
	}
	defer C.OCIDescriptorFree(unsafe.Pointer(param), C.OCI_DTYPE_PARAM)

	var dataSize C.ub4 // Maximum size in bytes of the external data for the column. This can affect conversion buffer sizes.
	_, err = rows.stmt.conn.ociAttrGet(param, unsafe.Pointer(&dataSize), C.OCI_ATTR_DATA_SIZE)
	if err != nil {
		return 0, false
	}

	return int64(dataSize), true
}

/*
func (rows *OCI8Rows) ColumnTypePrecisionScale(i int) (precision, scale int64, ok bool) {
	return 0, 0, false
}
*/

// ColumnTypeNullable implement RowsColumnTypeNullable.
func (rows *OCI8Rows) ColumnTypeNullable(i int) (nullable, ok bool) {
	var isNull C.ub1 // returns 0 if null values are not permitted for the column
	_, err := rows.stmt.ociAttrGet(unsafe.Pointer(&isNull), C.OCI_ATTR_IS_NULL)
	if err != nil {
		return false, false
	}
	return isNull != 0, true
}

// ColumnTypeScanType implement RowsColumnTypeScanType.
func (rows *OCI8Rows) ColumnTypeScanType(i int) reflect.Type {
	param, err := rows.stmt.ociParamGet(C.ub4(i + 1))
	if err != nil {
		// TOFIX: return an error
		return reflect.SliceOf(reflect.TypeOf(""))
	}
	defer C.OCIDescriptorFree(unsafe.Pointer(param), C.OCI_DTYPE_PARAM)

	var dataType C.ub2 // external datatype of the column: https://docs.oracle.com/cd/E11882_01/appdev.112/e10646/oci03typ.htm#CEGIEEJI
	_, err = rows.stmt.conn.ociAttrGet(param, unsafe.Pointer(&dataType), C.OCI_ATTR_DATA_TYPE)
	if err != nil {
		// TOFIX: return an error
		return reflect.SliceOf(reflect.TypeOf(""))
	}

	switch dataType {
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
	}

	return reflect.SliceOf(reflect.TypeOf(""))
}
