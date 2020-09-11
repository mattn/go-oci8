package oci8

// #include "oci8.go.h"
import "C"

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/relloyd/go-sql/database/sql/driver"
	"strings"
	"time"
	"unsafe"
)

// Close closes the statment
func (stmt *OCI8Stmt) Close() error {
	if stmt.closed {
		return nil
	}
	stmt.closed = true

	C.OCIHandleFree(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT)

	stmt.stmt = nil
	stmt.pbind = nil

	return nil
}

// NumInput returns the number of input
func (stmt *OCI8Stmt) NumInput() int {
	var bindCount C.ub4 // number of bind position
	_, err := stmt.ociAttrGet(unsafe.Pointer(&bindCount), C.OCI_ATTR_BIND_COUNT)
	if err != nil {
		return -1
	}

	return int(bindCount)
}

// bind binds the varables / arguments
func (stmt *OCI8Stmt) bind(ctx context.Context, args []namedValue) ([]oci8Bind, error) {
	if len(args) == 0 {
		return nil, nil
	}

	var binds []oci8Bind
	var err error
	var outIn bool

	for i, uv := range args {
		var sbind oci8Bind
		sbind.length = (*C.ub2)(C.malloc(C.sizeof_ub2))
		*sbind.length = 0
		sbind.indicator = (*C.sb2)(C.malloc(C.sizeof_sb2))
		*sbind.indicator = 0

		vv := uv.Value
		if out, ok := handleOutput(vv); ok {
			sbind.out = out.Dest
			outIn = out.In
			vv, err = driver.DefaultParameterConverter.ConvertValue(out.Dest)
			if err != nil {
				binds = append(binds, sbind)
				freeBinds(binds)
				return nil, err
			}
		}

		switch v := vv.(type) {

		case nil:
			sbind.dataType = C.SQLT_AFC
			sbind.pbuf = nil
			sbind.maxSize = 0
			*sbind.indicator = -1 // set to null

		case []byte:
			if sbind.out != nil {

				sbind.dataType = C.SQLT_BIN
				sbind.pbuf = unsafe.Pointer(cByteN(v, 32768))
				sbind.maxSize = 32767
				if !outIn {
					*sbind.indicator = -1 // set to null
				} else {
					*sbind.length = C.ub2(len(v))
				}

			} else {
				sbind.dataType = C.SQLT_BIN
				sbind.pbuf = unsafe.Pointer(cByte(v))
				sbind.maxSize = C.sb4(len(v))
				*sbind.length = C.ub2(len(v))
			}

		case time.Time:
			sbind.dataType = C.SQLT_TIMESTAMP_TZ
			sbind.maxSize = C.sb4(sizeOfNilPointer)
			*sbind.length = C.ub2(sizeOfNilPointer)

			// TODO: wrap up date time construction into Go function

			var timestampP *unsafe.Pointer
			timestampP, _, err = stmt.conn.ociDescriptorAlloc(C.OCI_DTYPE_TIMESTAMP_TZ, 0)
			if err != nil {
				freeBinds(binds)
				return nil, err
			}
			pt := unsafe.Pointer(timestampP)

			zone, offset := v.Zone()
			size := len(zone)
			if size < 16 {
				size = 16
			}
			zoneText := cStringN(zone, size)
			defer C.free(unsafe.Pointer(zoneText))

			tryagain := false

			rv := C.OCIDateTimeConstruct(
				unsafe.Pointer(stmt.conn.env),
				stmt.conn.errHandle,
				(*C.OCIDateTime)(*(*unsafe.Pointer)(pt)),
				C.sb2(v.Year()),
				C.ub1(v.Month()),
				C.ub1(v.Day()),
				C.ub1(v.Hour()),
				C.ub1(v.Minute()),
				C.ub1(v.Second()),
				C.ub4(v.Nanosecond()),
				zoneText,
				C.size_t(len(zone)),
			)
			if rv != C.OCI_SUCCESS {
				tryagain = true
			} else {
				// check if oracle timezone offset is same ?
				rvz := C.WrapOCIDateTimeGetTimeZoneNameOffset(
					stmt.conn.env,
					stmt.conn.errHandle,
					(*C.OCIDateTime)(*(*unsafe.Pointer)(pt)))
				if rvz.rv != C.OCI_SUCCESS {
					binds = append(binds, sbind)
					freeBinds(binds)
					return nil, stmt.conn.getError(rvz.rv)
				}
				if offset != int(rvz.h)*60*60+int(rvz.m)*60 {
					// fmt.Println("oracle timezone offset dont match", zone, offset, int(rvz.h)*60*60+int(rvz.m)*60)
					tryagain = true
				}
			}

			if tryagain {
				sign := '+'
				if offset < 0 {
					offset = -offset
					sign = '-'
				}
				offset /= 60
				// oracle accept zones "[+-]hh:mm", try second time
				zone = fmt.Sprintf("%c%02d:%02d", sign, offset/60, offset%60)
				if size < len(zone) {
					size = len(zone)
					zoneText = cStringN(zone, size)
					defer C.free(unsafe.Pointer(zoneText))
				} else {
					copy((*[1 << 30]byte)(unsafe.Pointer(zoneText))[:len(zone)], zone)
				}

				rv := C.OCIDateTimeConstruct(
					unsafe.Pointer(stmt.conn.env),
					stmt.conn.errHandle,
					(*C.OCIDateTime)(*(*unsafe.Pointer)(pt)),
					C.sb2(v.Year()),
					C.ub1(v.Month()),
					C.ub1(v.Day()),
					C.ub1(v.Hour()),
					C.ub1(v.Minute()),
					C.ub1(v.Second()),
					C.ub4(v.Nanosecond()),
					zoneText,
					C.size_t(len(zone)),
				)
				if rv != C.OCI_SUCCESS {
					binds = append(binds, sbind)
					freeBinds(binds)
					return nil, stmt.conn.getError(rv)
				}
			}

			sbind.pbuf = unsafe.Pointer((*C.char)(pt))

		case string:
			if sbind.out != nil {

				sbind.dataType = C.SQLT_CHR
				sbind.pbuf = unsafe.Pointer(cStringN(v, 32768))
				sbind.maxSize = 32767
				if !outIn {
					*sbind.indicator = -1 // set to null
				} else {
					*sbind.length = C.ub2(len(v))
				}

			} else {
				sbind.dataType = C.SQLT_AFC
				sbind.pbuf = unsafe.Pointer(C.CString(v))
				sbind.maxSize = C.sb4(len(v))
				*sbind.length = C.ub2(len(v))
			}

		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
			buffer := bytes.Buffer{}
			err = binary.Write(&buffer, binary.LittleEndian, v)
			if err != nil {
				return nil, fmt.Errorf("binary read for column %v - error: %v", i, err)
			}
			sbind.dataType = C.SQLT_INT
			sbind.pbuf = unsafe.Pointer(cByte(buffer.Bytes()))
			sbind.maxSize = C.sb4(buffer.Len())
			*sbind.length = C.ub2(buffer.Len())

		case float32, float64:
			buffer := bytes.Buffer{}
			err = binary.Write(&buffer, binary.LittleEndian, v)
			if err != nil {
				return nil, fmt.Errorf("binary read for column %v - error: %v", i, err)
			}
			sbind.dataType = C.SQLT_BDOUBLE
			sbind.pbuf = unsafe.Pointer(cByte(buffer.Bytes()))
			sbind.maxSize = C.sb4(buffer.Len())
			*sbind.length = C.ub2(buffer.Len())

		case bool: // oracle does not have bool, handle as 0/1 int
			sbind.dataType = C.SQLT_INT
			if v {
				sbind.pbuf = unsafe.Pointer(cByte([]byte{1}))
			} else {
				sbind.pbuf = unsafe.Pointer(cByte([]byte{0}))
			}
			sbind.maxSize = 1
			*sbind.length = 1

		default:
			if sbind.out != nil {
				// TODO: should this error instead of setting to null?
				sbind.dataType = C.SQLT_AFC
				sbind.pbuf = nil
				sbind.maxSize = 0
				*sbind.length = 0
				*sbind.indicator = -1 // set to null
			} else {
				d := fmt.Sprintf("%v", v)
				sbind.dataType = C.SQLT_AFC
				sbind.pbuf = unsafe.Pointer(C.CString(d))
				sbind.maxSize = C.sb4(len(d))
				*sbind.length = C.ub2(len(d))
			}
		}

		// add to binds now so if error will be freed by freeBinds call
		binds = append(binds, sbind)

		if uv.Name != "" {
			err = stmt.ociBindByName([]byte(":"+uv.Name), &sbind)
		} else {
			err = stmt.ociBindByPos(C.ub4(i+1), &sbind)
		}
		if err != nil {
			freeBinds(binds)
			return nil, err
		}

	}

	return binds, nil
}

// Query runs a query
func (stmt *OCI8Stmt) Query(args []driver.Value) (rows driver.Rows, err error) {
	list := make([]namedValue, len(args))
	for i, v := range args {
		list[i] = namedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return stmt.query(context.Background(), list, false)
}

func (stmt *OCI8Stmt) query(ctx context.Context, args []namedValue, closeRows bool) (driver.Rows, error) {
	var binds []oci8Bind
	var err error

	binds, err = stmt.bind(ctx, args)
	if err != nil {
		return nil, err
	}

	defer freeBinds(binds)

	var stmtType C.ub2
	_, err = stmt.ociAttrGet(unsafe.Pointer(&stmtType), C.OCI_ATTR_STMT_TYPE)
	if err != nil {
		return nil, err
	}

	iter := C.ub4(1)
	if stmtType == C.OCI_STMT_SELECT {
		iter = 0
	}

	if stmt.conn.prefetchRows != 1 {
		prefetchRows := stmt.conn.prefetchRows
		// OCI_ATTR_PREFETCH_ROWS sets the number of top level rows to be prefetched. The default value is 1 row.
		err = stmt.conn.ociAttrSet(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, unsafe.Pointer(&prefetchRows), 0, C.OCI_ATTR_PREFETCH_ROWS)
		if err != nil {
			return nil, err
		}
	}

	if stmt.conn.prefetchMemory > 0 {
		prefetchMemory := stmt.conn.prefetchMemory
		// OCI_ATTR_PREFETCH_MEMORY sets the memory level for top level rows to be prefetched. Rows up to the specified top level row count are fetched if it occupies no more than the specified memory usage limit.
		// The default value is 0, which means that memory size is not included in computing the number of rows to prefetch.
		err = stmt.conn.ociAttrSet(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, unsafe.Pointer(&prefetchMemory), 0, C.OCI_ATTR_PREFETCH_MEMORY)
		if err != nil {
			return nil, err
		}
	}

	mode := C.ub4(C.OCI_DEFAULT)
	if !stmt.conn.inTransaction {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
	}

	done := make(chan struct{})
	go stmt.ociBreak(ctx, done)
	err = stmt.ociStmtExecute(iter, mode)
	close(done)
	if err != nil {
		return nil, err
	}

	var paramCountUb4 C.ub4 // number of columns in the select-list
	_, err = stmt.ociAttrGet(unsafe.Pointer(&paramCountUb4), C.OCI_ATTR_PARAM_COUNT)
	if err != nil {
		return nil, err
	}
	paramCount := int(paramCountUb4)

	defines := make([]oci8Define, paramCount)

	for i := 0; i < paramCount; i++ {
		var param *C.OCIParam
		param, err = stmt.ociParamGet(C.ub4(i + 1))
		if err != nil {
			freeDefines(defines)
			return nil, err
		}
		defer C.OCIDescriptorFree(unsafe.Pointer(param), C.OCI_DTYPE_PARAM)

		var dataType C.ub2 // external datatype of the column: https://docs.oracle.com/cd/E11882_01/appdev.112/e10646/oci03typ.htm#CEGIEEJI
		_, err = stmt.conn.ociAttrGet(param, unsafe.Pointer(&dataType), C.OCI_ATTR_DATA_TYPE)
		if err != nil {
			freeDefines(defines)
			return nil, err
		}

		var columnName *C.OraText // name of the column
		var size C.ub4
		size, err = stmt.conn.ociAttrGet(param, unsafe.Pointer(&columnName), C.OCI_ATTR_NAME)
		if err != nil {
			freeDefines(defines)
			return nil, err
		}
		defines[i].name = cGoStringN(columnName, int(size))

		var maxSize C.ub4 // Maximum size in bytes of the external data for the column. This can affect conversion buffer sizes.
		_, err = stmt.conn.ociAttrGet(param, unsafe.Pointer(&maxSize), C.OCI_ATTR_DATA_SIZE)
		if err != nil {
			freeDefines(defines)
			return nil, err
		}

		defines[i].length = (*C.ub2)(C.malloc(C.sizeof_ub2))
		*defines[i].length = 0
		defines[i].indicator = (*C.sb2)(C.malloc(C.sizeof_sb2))
		*defines[i].indicator = 0

		// switch on dataType
		switch dataType {

		case C.SQLT_CHR, C.SQLT_AFC, C.SQLT_VCS, C.SQLT_AVC:
			defines[i].dataType = C.SQLT_AFC
			defines[i].maxSize = C.sb4(maxSize)
			defines[i].pbuf = C.malloc(C.size_t(defines[i].maxSize))

		case C.SQLT_BIN:
			defines[i].dataType = C.SQLT_BIN
			defines[i].maxSize = C.sb4(maxSize)
			defines[i].pbuf = C.malloc(C.size_t(defines[i].maxSize))

		case C.SQLT_NUM:
			var precision C.sb2 // the precision
			_, err = stmt.conn.ociAttrGet(param, unsafe.Pointer(&precision), C.OCI_ATTR_PRECISION)
			if err != nil {
				freeDefines(defines)
				return nil, err
			}

			var scale C.sb1 // the scale (number of digits to the right of the decimal point)
			_, err = stmt.conn.ociAttrGet(param, unsafe.Pointer(&scale), C.OCI_ATTR_SCALE)
			if err != nil {
				freeDefines(defines)
				return nil, err
			}

			// The precision of numeric type attributes. If the precision is nonzero and scale is -127, then it is a FLOAT;
			// otherwise, it is a NUMBER(precision, scale).
			// When precision is 0, NUMBER(precision, scale) can be represented simply as NUMBER.
			// https://docs.oracle.com/cd/E11882_01/appdev.112/e10646/oci06des.htm#LNOCI16458

			if (precision == 0 && scale == 0) || scale > 0 || scale == -127 {
				defines[i].dataType = C.SQLT_BDOUBLE
				defines[i].maxSize = 8
				defines[i].pbuf = C.malloc(C.size_t(defines[i].maxSize))
			} else {
				defines[i].dataType = C.SQLT_INT
				defines[i].maxSize = 8
				defines[i].pbuf = C.malloc(C.size_t(defines[i].maxSize))
			}

		case C.SQLT_INT:
			defines[i].dataType = C.SQLT_INT
			defines[i].maxSize = 8
			defines[i].pbuf = C.malloc(C.size_t(defines[i].maxSize))

		case C.SQLT_BFLOAT, C.SQLT_IBFLOAT, C.SQLT_BDOUBLE, C.SQLT_IBDOUBLE:
			defines[i].dataType = C.SQLT_BDOUBLE
			defines[i].maxSize = 8
			defines[i].pbuf = C.malloc(C.size_t(defines[i].maxSize))

		case C.SQLT_LNG:
			defines[i].dataType = C.SQLT_LNG
			defines[i].maxSize = 4000
			defines[i].pbuf = C.malloc(C.size_t(defines[i].maxSize))

		case C.SQLT_CLOB, C.SQLT_BLOB:
			defines[i].dataType = dataType
			defines[i].maxSize = C.sb4(sizeOfNilPointer)
			var lobP *unsafe.Pointer
			lobP, _, err = stmt.conn.ociDescriptorAlloc(C.OCI_DTYPE_LOB, 0)
			if err != nil {
				freeDefines(defines)
				return nil, err
			}
			defines[i].pbuf = unsafe.Pointer(lobP)

		case C.SQLT_TIMESTAMP, C.SQLT_DAT:
			defines[i].dataType = C.SQLT_TIMESTAMP
			defines[i].maxSize = C.sb4(sizeOfNilPointer)
			var timestampP *unsafe.Pointer
			timestampP, _, err = stmt.conn.ociDescriptorAlloc(C.OCI_DTYPE_TIMESTAMP, 0)
			if err != nil {
				freeDefines(defines)
				return nil, err
			}
			defines[i].pbuf = unsafe.Pointer(timestampP)

		case C.SQLT_TIMESTAMP_TZ, C.SQLT_TIMESTAMP_LTZ:
			defines[i].dataType = C.SQLT_TIMESTAMP_TZ
			defines[i].maxSize = C.sb4(sizeOfNilPointer)
			var timestampP *unsafe.Pointer
			timestampP, _, err = stmt.conn.ociDescriptorAlloc(C.OCI_DTYPE_TIMESTAMP_TZ, 0)
			if err != nil {
				freeDefines(defines)
				return nil, err
			}
			defines[i].pbuf = unsafe.Pointer(timestampP)

		case C.SQLT_INTERVAL_DS:
			defines[i].dataType = C.SQLT_INTERVAL_DS
			defines[i].maxSize = C.sb4(sizeOfNilPointer)
			var intervalP *unsafe.Pointer
			intervalP, _, err = stmt.conn.ociDescriptorAlloc(C.OCI_DTYPE_INTERVAL_DS, 0)
			if err != nil {
				freeDefines(defines)
				return nil, err
			}
			defines[i].pbuf = unsafe.Pointer(intervalP)

		case C.SQLT_INTERVAL_YM:
			defines[i].dataType = C.SQLT_INTERVAL_YM
			defines[i].maxSize = C.sb4(sizeOfNilPointer)
			var intervalP *unsafe.Pointer
			intervalP, _, err = stmt.conn.ociDescriptorAlloc(C.OCI_DTYPE_INTERVAL_YM, 0)
			if err != nil {
				freeDefines(defines)
				return nil, err
			}
			defines[i].pbuf = unsafe.Pointer(intervalP)

		case C.SQLT_RDD: // rowid
			defines[i].dataType = C.SQLT_AFC
			defines[i].maxSize = 40
			defines[i].pbuf = C.malloc(C.size_t(defines[i].maxSize))

		default:
			defines[i].dataType = C.SQLT_AFC
			defines[i].maxSize = C.sb4(maxSize)
			defines[i].pbuf = C.malloc(C.size_t(defines[i].maxSize))
		}

		result := C.OCIDefineByPos(
			stmt.stmt,                            // statement handle
			&defines[i].defineHandle,             // pointer to a pointer to a define handle. If NULL, this call implicitly allocates the define handle.
			stmt.conn.errHandle,                  // error handle
			C.ub4(i+1),                           // position of this value in the select list. Positions are 1-based and are numbered from left to right.
			defines[i].pbuf,                      // pointer to a buffer
			defines[i].maxSize,                   // size of each valuep buffer in bytes
			defines[i].dataType,                  // datatype
			unsafe.Pointer(defines[i].indicator), // pointer to an indicator variable or array
			defines[i].length,                    // pointer to array of length of data fetched
			nil,                                  // pointer to array of column-level return codes
			C.OCI_DEFAULT,                        // mode - OCI_DEFAULT - This is the default mode.
		)
		if result != C.OCI_SUCCESS {
			freeDefines(defines)
			return nil, stmt.conn.getError(result)
		}
	}

	rows := &OCI8Rows{
		stmt:    stmt,
		defines: defines,
		done:    make(chan struct{}),
		cls:     closeRows,
	}

	go stmt.ociBreak(ctx, rows.done)

	return rows, nil
}

// getRowid returns the rowid
func (stmt *OCI8Stmt) getRowid() (string, error) {
	rowidP, _, err := stmt.conn.ociDescriptorAlloc(C.OCI_DTYPE_ROWID, 0)
	if err != nil {
		return "", err
	}

	// OCI_ATTR_ROWID returns the ROWID descriptor allocated with OCIDescriptorAlloc()
	_, err = stmt.ociAttrGet(*rowidP, C.OCI_ATTR_ROWID)
	if err != nil {
		return "", err
	}

	rowid := cStringN("", 18)
	defer C.free(unsafe.Pointer(rowid))
	rowidLength := C.ub2(18)
	result := C.OCIRowidToChar((*C.OCIRowid)(*rowidP), rowid, &rowidLength, stmt.conn.errHandle)
	err = stmt.conn.getError(result)
	if err != nil {
		return "", err
	}

	return cGoStringN(rowid, int(rowidLength)), nil
}

// rowsAffected returns the number of rows affected
func (stmt *OCI8Stmt) rowsAffected() (int64, error) {
	var rowCount C.ub4 // Number of rows processed so far after SELECT statements. For INSERT, UPDATE, and DELETE statements, it is the number of rows processed by the most recent statement. The default value is 1.
	_, err := stmt.ociAttrGet(unsafe.Pointer(&rowCount), C.OCI_ATTR_ROW_COUNT)
	if err != nil {
		return -1, err
	}
	return int64(rowCount), nil
}

// Exec runs an exec query
func (stmt *OCI8Stmt) Exec(args []driver.Value) (r driver.Result, err error) {
	list := make([]namedValue, len(args))
	for i, v := range args {
		list[i] = namedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return stmt.exec(context.Background(), list)
}

// exec runs an exec query
func (stmt *OCI8Stmt) exec(ctx context.Context, args []namedValue) (driver.Result, error) {
	binds, err := stmt.bind(ctx, args)
	if err != nil {
		return nil, err
	}

	defer freeBinds(binds)

	mode := C.ub4(C.OCI_DEFAULT)
	if stmt.conn.inTransaction == false {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
	}

	done := make(chan struct{})
	go stmt.ociBreak(ctx, done)
	err = stmt.ociStmtExecute(1, mode)
	close(done)
	if err != nil && err != ErrOCISuccessWithInfo {
		return nil, err
	}

	result := OCI8Result{stmt: stmt}

	result.rowsAffected, result.rowsAffectedErr = stmt.rowsAffected()
	if result.rowsAffectedErr != nil || result.rowsAffected < 1 {
		result.rowidErr = ErrNoRowid
	} else {
		result.rowid, result.rowidErr = stmt.getRowid()
	}

	err = stmt.outputBoundParameters(binds)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

// outputBoundParameters sets bound parameters
func (stmt *OCI8Stmt) outputBoundParameters(binds []oci8Bind) error {
	var err error

	for i, bind := range binds {
		if bind.pbuf != nil {
			switch v := bind.out.(type) {
			case *string:
				switch {
				case *bind.indicator > 0: // indicator variable is the actual length before truncation
					spaces := int(*bind.indicator) - int(*bind.length)
					if spaces < 0 {
						return fmt.Errorf("spaces less than 0 for column %v", i)
					}
					*v = C.GoStringN((*C.char)(bind.pbuf), C.int(*bind.length)) + strings.Repeat(" ", spaces)
				case *bind.indicator == 0: // Normal
					*v = C.GoStringN((*C.char)(bind.pbuf), C.int(*bind.length))
				case *bind.indicator == -1: // The selected value is null
					*v = "" // best attempt at Go nil string
				case *bind.indicator == -2: // Item is greater than the length of the output variable; the item has been truncated.
					*v = C.GoStringN((*C.char)(bind.pbuf), C.int(*bind.length))
					// TODO: should this be an error?
				default:
					return fmt.Errorf("unknown column indicator %d for column %v", *bind.indicator, i)
				}

			case *int:
				*v = int(getInt64(bind.pbuf))
			case *int64:
				*v = getInt64(bind.pbuf)
			case *int32:
				*v = int32(getInt64(bind.pbuf))
			case *int16:
				*v = int16(getInt64(bind.pbuf))
			case *int8:
				*v = int8(getInt64(bind.pbuf))

			case *uint:
				*v = uint(getUint64(bind.pbuf))
			case *uint64:
				*v = getUint64(bind.pbuf)
			case *uint32:
				*v = uint32(getUint64(bind.pbuf))
			case *uint16:
				*v = uint16(getUint64(bind.pbuf))
			case *uint8:
				*v = uint8(getUint64(bind.pbuf))
			case *uintptr:
				*v = uintptr(getUint64(bind.pbuf))

			case *float64:
				buf := (*[8]byte)(bind.pbuf)[0:8]
				var data float64
				err = binary.Read(bytes.NewReader(buf), binary.LittleEndian, &data)
				if err != nil {
					return fmt.Errorf("binary read for column %v - error: %v", i, err)
				}
				*v = data
			case *float32:
				// statment is using SQLT_BDOUBLE to bind
				// need to read as float64 because of the 8 bits
				buf := (*[8]byte)(bind.pbuf)[0:8]
				var data float64
				err = binary.Read(bytes.NewReader(buf), binary.LittleEndian, &data)
				if err != nil {
					return fmt.Errorf("binary read for column %v - error: %v", i, err)
				}
				*v = float32(data)

			case *bool:
				buf := (*[1 << 30]byte)(bind.pbuf)[0:1]
				*v = buf[0] != 0

			case *[]byte:
				switch {
				case *bind.indicator > 0: // indicator variable is the actual length before truncation
					if int(*bind.indicator)-int(*bind.length) < 0 {
						return fmt.Errorf("spaces less than 0 for column %v", i)
					}
					*v = C.GoBytes(bind.pbuf, C.int(*bind.indicator))
				case *bind.indicator == 0: // Normal
					*v = C.GoBytes(bind.pbuf, C.int(*bind.length))
				case *bind.indicator == -1: // The selected value is null
					*v = nil
				case *bind.indicator == -2: // Item is greater than the length of the output variable; the item has been truncated.
					*v = C.GoBytes(bind.pbuf, C.int(*bind.length))
					// TODO: should this be an error?
				default:
					return fmt.Errorf("unknown column indicator %d for column %v", *bind.indicator, i)
				}
			}

		}
	}

	return nil
}

// ociParamGet calls OCIParamGet then returns OCIParam and error.
// OCIDescriptorFree must be called on returned OCIParam.
func (stmt *OCI8Stmt) ociParamGet(position C.ub4) (*C.OCIParam, error) {
	var paramTemp *C.OCIParam
	param := &paramTemp

	result := C.OCIParamGet(
		unsafe.Pointer(stmt.stmt),                // A statement handle or describe handle
		C.OCI_HTYPE_STMT,                         // Handle type: OCI_HTYPE_STMT, for a statement handle
		stmt.conn.errHandle,                      // An error handle
		(*unsafe.Pointer)(unsafe.Pointer(param)), // A descriptor of the parameter at the position
		position,                                 // Position number in the statement handle or describe handle. A parameter descriptor will be returned for this position.
	)

	err := stmt.conn.getError(result)
	if err != nil {
		return nil, err
	}

	return *param, nil
}

// ociAttrGet calls OCIAttrGet with OCIStmt then returns attribute size and error.
// The attribute value is stored into passed value.
func (stmt *OCI8Stmt) ociAttrGet(value unsafe.Pointer, attributeType C.ub4) (C.ub4, error) {
	var size C.ub4

	result := C.OCIAttrGet(
		unsafe.Pointer(stmt.stmt), // Pointer to a handle type
		C.OCI_HTYPE_STMT,          // The handle type: OCI_HTYPE_STMT, for a statement handle
		value,                     // Pointer to the storage for an attribute value
		&size,                     // The size of the attribute value
		attributeType,             // The attribute type: https://docs.oracle.com/cd/B19306_01/appdev.102/b14250/ociaahan.htm
		stmt.conn.errHandle,       // An error handle
	)

	return size, stmt.conn.getError(result)
}

// ociBindByName calls OCIBindByName, then returns bind handle and error.
func (stmt *OCI8Stmt) ociBindByName(name []byte, bind *oci8Bind) error {
	result := C.OCIBindByName(
		stmt.stmt,                      // The statement handle
		&bind.bindHandle,               // The bind handle that is implicitly allocated by this call. The handle is freed implicitly when the statement handle is deallocated.
		stmt.conn.errHandle,            // An error handle
		(*C.OraText)(&name[0]),         // The placeholder, specified by its name, that maps to a variable in the statement associated with the statement handle.
		C.sb4(len(name)),               // The length of the name specified in placeholder, in number of bytes regardless of the encoding.
		bind.pbuf,                      // The pointer to a data value or an array of data values of type specified in the dty parameter
		bind.maxSize,                   // The maximum size possible in bytes of any data value for this bind variable
		bind.dataType,                  // The data type of the values being bound
		unsafe.Pointer(bind.indicator), // Pointer to an indicator variable or array
		bind.length,                    // lengths are in bytes in general
		nil,                            // Pointer to the array of column-level return codes
		0,                              // A maximum array length parameter
		nil,                            // Current array length parameter
		C.OCI_DEFAULT,                  // The mode. Recommended to set to OCI_DEFAULT, which makes the bind variable have the same encoding as its statement.
	)

	return stmt.conn.getError(result)
}

// ociBindByPos calls OCIBindByPos, then returns bind handle and error.
func (stmt *OCI8Stmt) ociBindByPos(position C.ub4, bind *oci8Bind) error {
	result := C.OCIBindByPos(
		stmt.stmt,                      // The statement handle
		&bind.bindHandle,               // The bind handle that is implicitly allocated by this call. The handle is freed implicitly when the statement handle is deallocated.
		stmt.conn.errHandle,            // An error handle
		position,                       // The placeholder attributes are specified by position if OCIBindByPos() is being called.
		bind.pbuf,                      // void* valuep - An address of a data value or an array of data values
		bind.maxSize,                   // The maximum size possible in bytes of any data value for this bind variable
		bind.dataType,                  // The data type of the values being bound
		unsafe.Pointer(bind.indicator), // Pointer to an indicator variable or array
		bind.length,                    // ub2* alenp - lengths are in bytes in general - pointer to an array of actual array length elements
		nil,                            // Pointer to the array of column-level return codes
		0,                              // A maximum array length parameter
		nil,                            // Current array length parameter
		C.OCI_DEFAULT,                  // The mode. Recommended to set to OCI_DEFAULT, which makes the bind variable have the same encoding as its statement.
	)

	return stmt.conn.getError(result)
}

// ociStmtExecute calls OCIStmtExecute
func (stmt *OCI8Stmt) ociStmtExecute(iters C.ub4, mode C.ub4) error {
	result := C.OCIStmtExecute(
		stmt.conn.svc,       // Service context handle
		stmt.stmt,           // A statement handle
		stmt.conn.errHandle, // An error handle
		iters,               // For non-SELECT statements, the number of times this statement is executed equals iters - rowoff. For SELECT statements, if iters is nonzero, then defines must have been done for the statement handle.
		0,                   // The starting index from which the data in an array bind is relevant for this multiple row execution
		nil,                 // This parameter is optional. If it is supplied, it must point to a snapshot descriptor of type OCI_DTYPE_SNAP
		nil,                 // This parameter is optional. If it is supplied, it must point to a descriptor of type OCI_DTYPE_SNAP.
		mode,                // The mode: https://docs.oracle.com/cd/E11882_01/appdev.112/e10646/oci17msc001.htm#LNOCI17163
	)

	return stmt.conn.getError(result)
}

// ociBreak calls OCIBreak if ctx.Done is finished before done chan is closed
func (stmt *OCI8Stmt) ociBreak(ctx context.Context, done chan struct{}) {
	select {
	case <-done:
	case <-ctx.Done():
		// select again to avoid race condition if both are done
		select {
		case <-done:
		default:
			result := C.OCIBreak(
				unsafe.Pointer(stmt.conn.svc), // The service context handle or the server context handle.
				stmt.conn.errHandle,           // An error handle
			)
			err := stmt.conn.getError(result)
			if err != nil {
				stmt.conn.logger.Print("OCIBreak error: ", err)
			}
		}
	}
}
