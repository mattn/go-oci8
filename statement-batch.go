package oci8

// #include "oci8.go.h"
import "C"

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/relloyd/go-sql/database/sql/driver"
	"time"
	"unsafe"
)

// type DMLBatcher interface {
// 	BindToBatch(args []driver.Value) error
// 	BindToBatchContext(ctx context.Context, args []driver.Value) error
// 	ExecBatch() (driver.Result, error)
// 	ExecBatchContext(ctx context.Context) (driver.Result, error)
// }

// BindToBatch will add the bind variables to the batch.
func (stmt *OCI8Stmt) BindToBatchContext(ctx context.Context, vals []driver.NamedValue) error {
	if len(vals) == 0 {
		return nil
	}

	args := make([]namedValue, len(vals), len(vals))
	for i, v := range vals {
		args[i] = namedValue{
			Ordinal: i + 1,
			Value:   v.Value,
		}
	}

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
				stmt.pbind = append(stmt.pbind, sbind)
				freeBinds(stmt.pbind)
				return err
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
				freeBinds(stmt.pbind)
				return err
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
					stmt.pbind = append(stmt.pbind, sbind)
					freeBinds(stmt.pbind)
					return stmt.conn.getError(rvz.rv)
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
					stmt.pbind = append(stmt.pbind, sbind)
					freeBinds(stmt.pbind)
					return stmt.conn.getError(rv)
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
				return fmt.Errorf("binary read for column %v - error: %v", i, err)
			}
			sbind.dataType = C.SQLT_INT
			sbind.pbuf = unsafe.Pointer(cByte(buffer.Bytes()))
			sbind.maxSize = C.sb4(buffer.Len())
			*sbind.length = C.ub2(buffer.Len())

		case float32, float64:
			buffer := bytes.Buffer{}
			err = binary.Write(&buffer, binary.LittleEndian, v)
			if err != nil {
				return fmt.Errorf("binary read for column %v - error: %v", i, err)
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

		// add to stmt.pbind now so if error will be freed by freeBinds call
		stmt.pbind = append(stmt.pbind, sbind)

		if uv.Name != "" {
			err = stmt.ociBindByName([]byte(":"+uv.Name), &sbind)
		} else {
			err = stmt.ociBindByPos(C.ub4(i+1), &sbind)
		}
		if err != nil {
			freeBinds(stmt.pbind)
			return err
		}

	}

	return nil
}

func (stmt *OCI8Stmt) ExecBatchContext(ctx context.Context) (driver.Result, error) {
	return stmt.execBatch(ctx)
}

// execBatch runs an exec query
func (stmt *OCI8Stmt) execBatch(ctx context.Context) (driver.Result, error) {
	var err error
	// var binds []oci8Bind
	// binds, err = stmt.bind(ctx, args)
	// if err != nil {
	// 	return nil, err
	// }
	defer func() {
		freeBinds(stmt.pbind)
		stmt.pbind = nil  // free the current pbind slice for next batch.
	}()
	mode := C.ub4(C.OCI_BATCH_ERRORS)
	if stmt.conn.inTransaction == false {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
	}
	done := make(chan struct{})
	go stmt.ociBreak(ctx, done)
	err = stmt.ociStmtExecute(C.ub4(len(stmt.pbind)), mode) // iters is the length of pbind.
	fmt.Println("dump error after exec: ", err)
	close(done)
	if err != nil && err != ErrOCISuccessWithInfo { // if we executed unsuccessfully...
		var numErrors C.ub4
		_, err2 := stmt.ociAttrGet(unsafe.Pointer(&numErrors), C.OCI_ATTR_NUM_DML_ERRORS) // get num DML errors using the statement handle.
		if err2 != nil {
			// fmt.Println("error received while allocating handle of type OCI_HTYPE_ERROR")
			return nil, fmt.Errorf("error allocating handle of type OCI_HTYPE_ERROR")
		}
		if numErrors > 0 { // if there are errors...
			// Allocate a new handle to fetch the errors.
			// OCIHandleAlloc( (void *)envhp, (void **)&errhndl[i],(ub4) OCI_HTYPE_ERROR, (ub2)0, 0)
			dmlError, _, err2 := stmt.conn.ociHandleAlloc(C.OCI_HTYPE_ERROR, 0) // alloc one handle for reuse in for loop below.
			defer C.OCIHandleFree(*dmlError, C.OCI_HTYPE_ERROR)
			var rowOffset C.ub4
			i := C.ub4(0) // get the first error for now.
			// for i := 0; i < numErrors; i++ { // for each error...
			var errHandle *C.OCIError // placeholder pointer to handle that will be allocated for this OCIParamGet.
			// OCIParamGet(errhp, OCI_HTYPE_ERROR, errhp2, &errhndl[i], i)
			result := C.OCIParamGet(
				unsafe.Pointer(stmt.conn.errHandle), // the error handle produced by OCIStmtExecute above.
				C.OCI_HTYPE_ERROR,                   // handle type.
				errHandle,                           // an error handle that will be allocated for us, to explain an error from this call if applicable.
				dmlError,                            // pointer to unsafe.Pointer: this will be filled with info about the DML error.
				i) // error position in the statement handle.
			err2 = stmt.conn.getError(result)
			if err2 != nil {
				return nil, err2
			}
			// OCIAttrGet(errhndl[i], OCI_HTYPE_ERROR, &row_off[i], 0, OCI_ATTR_DML_ROW_OFFSET, errhp2)
			zero := C.ub4(0)
			result = C.OCIAttrGet(
				*dmlError,                  // Pointer to a handle type
				C.OCI_HTYPE_ERROR,          // The handle type: OCI_HTYPE_ERROR
				unsafe.Pointer(&rowOffset), // Pointer to the storage for an attribute value
				&zero,                      // The size of the attribute value
				C.OCI_ATTR_DML_ROW_OFFSET,  // The attribute type: OCI_ATTR_DML_ROW_OFFSET
				errHandle,                  // An error handle
			)
			err2 = stmt.conn.getError(result)
			if err2 != nil {
				return nil, err2
			}
			// Get server diagnostics for each DML error.
			// OCIErrorGet(..., errhndl[i], ...)
			var errorCode C.sb4
			errorText := make([]byte, C.OCI_ERROR_MAXMSG_SIZE2)
			result = C.OCIErrorGet(
				*dmlError,                   // error handle
				i,                           // status record number, starts from 1
				nil,                         // sqlstate, not supported in release 8.x or later
				&errorCode,                  // error code
				(*C.OraText)(&errorText[0]), // error message text
				C.OCI_ERROR_MAXMSG_SIZE2,    // size of the buffer provided in number of bytes
				C.OCI_HTYPE_ERROR,           // type of the handle (OCI_HTYPE_ERR or OCI_HTYPE_ENV)
			)
			if result != C.OCI_SUCCESS {
				return nil, errors.New("OCIErrorGet failed trying to fetch batch DML error details")
			}
			index := bytes.IndexByte(errorText, 0) // what is this search for '0' doing?  is it the C string terminator? If so, what about other zeros in the text?!?
			// TODO: build full list of errors in this loop.
			return nil, fmt.Errorf("batch DML error code: %v; text: %v", int(errorCode), string(errorText[:index]))
			// }
		} else {  // else we have no DML errors but there is some other error...
			// return the first error.
			return nil, err
		}
	} else if err == ErrOCISuccessWithInfo {
		// OCIErrorGet ((void  *) errhp, (ub4) 1, (text *) NULL, &errcode, errbuf, (ub4) sizeof(errbuf), (ub4) OCI_HTYPE_ERROR);
		// printf("Error - %s\n", errbuf);
		_, err := stmt.conn.ociGetError()
		if err != nil  {
			return nil, err
		}
	} else {  // else unknown stuff happened...
		return nil, err
	}
	result := OCI8Result{stmt: stmt}
	result.rowsAffected, result.rowsAffectedErr = stmt.rowsAffected()
	if result.rowsAffectedErr != nil || result.rowsAffected < 1 {
		result.rowidErr = ErrNoRowid
	} else {
		result.rowid, result.rowidErr = stmt.getRowid()
	}
	return &result, nil
}
