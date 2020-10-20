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
func (rows *Rows) Close() error {
	if rows.closed {
		return nil
	}

	rows.closed = true

	freeDefines(rows.defines)

	return nil
}

// Columns returns column names
func (rows *Rows) Columns() []string {
	names := make([]string, len(rows.defines))
	for i := 0; i < len(rows.defines); i++ {
		names[i] = rows.defines[i].name
	}
	return names
}

// Next gets next row
func (rows *Rows) Next(dest []driver.Value) error {
	if rows.closed {
		return nil
	}

	if rows.stmt.ctx.Err() != nil {
		return rows.stmt.ctx.Err()
	}

	done := make(chan struct{})
	defer close(done)
	go rows.stmt.conn.ociBreakDone(rows.stmt.ctx, done)
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
				rows.stmt.conn.timeLocation)

		// SQLT_BLOB and SQLT_CLOB
		case C.SQLT_BLOB, C.SQLT_CLOB:
			lobLocator := (**C.OCILobLocator)(rows.defines[i].pbuf)
			buffer, err := rows.stmt.conn.ociLobRead(*lobLocator, C.SQLCS_IMPLICIT)
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

		// SQLT_RSET - ref cursor
		case C.SQLT_RSET:
			stmtP := (**C.OCIStmt)(rows.defines[i].pbuf)
			subStmt := &Stmt{conn: rows.stmt.conn, stmt: *stmtP, ctx: rows.stmt.ctx, releaseMode: C.ub4(C.OCI_DEFAULT)}
			if rows.defines[i].subDefines == nil {
				var err error
				rows.defines[i].subDefines, err = subStmt.makeDefines()
				if err != nil {
					return err
				}
			}
			subRows := &Rows{
				stmt:    subStmt,
				defines: rows.defines[i].subDefines,
			}
			dest[i] = subRows

		// default
		default:
			return fmt.Errorf("Unhandled column type: %d", rows.defines[i].dataType)

		}
	}

	return nil
}

// ColumnTypeDatabaseTypeName implement RowsColumnTypeDatabaseTypeName.
func (rows *Rows) ColumnTypeDatabaseTypeName(i int) string {
	if len(rows.defines) < i+1 {
		return ""
	}

	switch rows.defines[i].dataType {
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

// ColumnTypeLength is returning OCI_ATTR_DATA_SIZE, which is max data size in bytes.
// Note this is not returing length of the column type, like the 20 in FLOAT(20), which is what is normally expected.
// TODO: Should / can it be changed to return length of the column type?
func (rows *Rows) ColumnTypeLength(i int) (int64, bool) {
	if len(rows.defines) < i+1 {
		return 0, false
	}

	if rows.defines[i].dataType == C.SQLT_AFC {
		return int64(rows.defines[i].maxSize / 2), true
	}
	return int64(rows.defines[i].maxSize), true
}

// ColumnTypeScanType implement RowsColumnTypeScanType.
func (rows *Rows) ColumnTypeScanType(i int) reflect.Type {
	if len(rows.defines) < i+1 {
		return typeNil
	}

	switch rows.defines[i].dataType {
	case C.SQLT_AFC, C.SQLT_CHR, C.SQLT_VCS, C.SQLT_AVC, C.SQLT_CLOB, C.SQLT_RDD:
		return typeString
	case C.SQLT_BIN, C.SQLT_BLOB:
		return typeSliceByte
	case C.SQLT_INT:
		return typeInt64
	case C.SQLT_BDOUBLE, C.SQLT_IBDOUBLE, C.SQLT_BFLOAT, C.SQLT_IBFLOAT, C.SQLT_NUM:
		return typeFloat64
	case C.SQLT_TIMESTAMP, C.SQLT_DAT, C.SQLT_TIMESTAMP_TZ, C.SQLT_TIMESTAMP_LTZ:
		return typeTime
	case C.SQLT_INTERVAL_DS, C.SQLT_INTERVAL_YM:
		return typeInt64
	}

	return typeNil
}
