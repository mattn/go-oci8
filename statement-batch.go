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
	"reflect"
	"time"
	"unsafe"
)

// BindToBatch will add the bind variables to the batch.
// vals is expected to be a slice of values for a single column (not a row).
// position is expected to be the column number starting at position 0.
func (stmt *OCI8Stmt) BindToBatchContext(ctx context.Context, position int, vals []driver.NamedValue) error {
	if len(vals) == 0 {
		return nil
	}
	var err error
	// var outIn bool  // TODO: fix OUT processing for now in batch mode?

	// TODO: remove use of namedValue here as it's not fully used in this func.
	// Why are we making a slice of namedValue and filling it from the vals?
	// just to add the ordinal index? where is the index used?
	// nv := make([]namedValue, len(vals), len(vals))
	// for i, v := range vals {
	// 	nv[i] = namedValue{
	// 		Ordinal: i + 1,
	// 		Value:   v.Value,
	// 	}
	// }

	// Setup a new sbind with correct length array preallocated (and indexable) ready for data about each value in this column.
	// TODO: find a way to track mallocs since we don't append sbind to pbind until later on below.
	// therefore we may end up not freeing these mallocs.
	// add to stmt.pbind now so if error will be freed by freeBinds call
	doFreeSBind := true // assume worst case and toggle freeBind() on via defer func.
	doFreePBind := true
	var sbind oci8Bind
	defer func() {
		if doFreeSBind {
			freeBind(&sbind) // free any mallocs done in this func before sbind is appended to stmt.pbind.
		}
		if doFreePBind {
			freeBinds(stmt.pbind) // and free any previously bound columns.
		}
	}()
	// Allocate storage for the full column batch of values.
	// OCI seems to use sbind.maxsize to increment through the data array (sbind.pbuf, i.e. valuep).
	// The array of lengths (see ptrLen) is used to read within each data array element.
	sbind.length = (*C.ub2)(C.malloc(C.size_t(int(C.sizeof_sb2) * len(vals)))) // allocate an array of c.ub2, one element per col value held in the buffer.
	ptrLen := (*[1 << 30]C.ub2)(unsafe.Pointer(sbind.length))                  // cast length to void* and then to a ptr of [1073741824]c.ub2 so we can index the array later.
	sbind.indicator = (*C.sb2)(C.malloc(C.size_t(int(C.sizeof_sb2) * len(vals))))
	ptrInd := (*[1 << 30]C.sb2)(unsafe.Pointer(sbind.indicator)) // cast length to void* and then to a ptr of [1073741824]c.ub2 so we can index the array later.
	sbind.iters = len(vals)
	sbind.maxSize = 0 // set maxSize = 0 for the case where all col values are null.
	buffer := bytes.Buffer{}
	foundColType := false
	leadingNullCount := 0
	leadingNullsApplied := false
	// Func to write 0s where null is required to be represented.
	writeNull := func(maxSizeBytes C.sb4) {
		err = binary.Write(&buffer, binary.LittleEndian, make([]byte, maxSizeBytes, maxSizeBytes)) // write 0s to the buffer of count maxSizeBytes.
	}
	// Func to write 0s for each leading null found in the supplied input column.
	applyNulls := func(maxSizeBytes C.sb4) {
		if !leadingNullsApplied {
			for idx := 0; idx < leadingNullCount; idx++ { // for each leading null...
				writeNull(maxSizeBytes) // write it to the buffer with correct size.
			}
			leadingNullsApplied = true
		}
	}
	// Func for handling string type in multiple places.
	handleString := func(idx int, v string) error {
		// TODO: confirm there aren't stored up problems around char set conversion - check unicode chars are written to the databsae okay considering NLS settings as well!
		if !foundColType {
			sbind.dataType = C.SQLT_AFC // SQLT_AFC is type CHAR (not varchar2) - why is this okay?
			// Find the maximum string len in the batch.
			for idy := idx; idy < len(vals); idy++ { // for each remaining value in batch...
				switch vv := vals[idy].Value.(type) {
				case string:
					l := C.sb4(len(vv))
					if l > sbind.maxSize { // if we have a new maxsize...
						sbind.maxSize = l // save the new size.
					}
					foundColType = true
				case nil: // skip nil values - we assume we have arrived here because the column value was of type string already.
				}
			}
			applyNulls(sbind.maxSize)
		}
		b := make([]byte, sbind.maxSize, sbind.maxSize) // make a byte slice the width of the max string in this batch.
		copy(b, v)                                      // copy v into b, where b may have trailing junk
		_, err2 := buffer.Write(b)
		if err2 != nil {
			return fmt.Errorf("binary write for column %v - error: %v", position, err2)
		}
		ptrInd[idx] = C.sb2(0)      // use -1 to specify a null.
		ptrLen[idx] = C.ub2(len(v)) // save the byte size of the col value casted to ub2. this implies 2 bytes are enough to store the max size.
		// fmt.Printf("idx = %v; char = %v; len = %v\n", idx+1, v, ptrLen[idx])
		return nil
	}
	// Process the batch.
	for idx, nv := range vals { // for each column value...
		// Set the C length and indicator array elements, and write to a Golang buffer.
		// Assume the column data type from the first non nil value seen.
		switch v := nv.Value.(type) { // get concrete type from the nv.Value interface{} type.
		case nil:
			if !foundColType { // if we haven't found a column type and its maxSize yet...
				// Save the null to apply later.
				leadingNullCount++
			} else { // else write the null to our buffer with the known maxsize...
				writeNull(sbind.maxSize)
			}
			// Always set the length and indicator for this idx.
			ptrInd[idx] = C.sb2(-1) // use -1 to specify a null.
			ptrLen[idx] = C.ub2(0)  // save the byte size of the col value casted to ub2. this implies 2 bytes are enough to store the max size.
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
			if !foundColType { // if we haven't found the column type yet...
				// Save the data type and max size.
				// use Reflect to get the deepest underlying type. unsafe.SizeOf() is not doing this as it sees Driver.Value instead.
				// TODO: can we add unit test to ensure unsafe.SizeOf() is not used?
				sbind.dataType = C.SQLT_INT
				sbind.maxSize = C.sb4(reflect.TypeOf(v).Size()) // this should be set to the max size of any single entry in the bind array - which should all be the same!
				applyNulls(sbind.maxSize)                       // apply nulls now we have the maxSize.
				foundColType = true
			}
			// Write the concrete type to the buffer.
			err = binary.Write(&buffer, binary.LittleEndian, v)
			if err != nil {
				return fmt.Errorf("binary write for column %v - error: %v", position, err)
			}
			// Set the indicator and length for this idx.
			ptrInd[idx] = C.sb2(0)                        // use -1 to specify a null.
			ptrLen[idx] = C.ub2(reflect.TypeOf(v).Size()) // save the byte size of the col value casted to ub2. this implies 2 bytes are enough to store the max size.
			// fmt.Println("reflect type pf v = ", reflect.TypeOf(v))
			// fmt.Println("reflect size of v = ", reflect.TypeOf(v).Size())
			// fmt.Println("bytes = ", buffer.Bytes())
		case []byte:
			// TODO: test this!
			if !foundColType {
				sbind.dataType = C.SQLT_BIN
				sz := reflect.TypeOf(v).Size()
				if sbind.maxSize < C.sb4(sz) {
					sbind.maxSize = C.sb4(sz)
				}
				applyNulls(sbind.maxSize) // apply nulls now we have the maxSize.
				foundColType = true
			}
			// Write the concrete type to the buffer.
			err = binary.Write(&buffer, binary.LittleEndian, v)
			if err != nil {
				return fmt.Errorf("binary write for column %v - error: %v", position, err)
			}
			// Set the indicator and length for this idx.
			ptrInd[idx] = C.sb2(0)                        // use -1 to specify a null.  // TODO: test loading of BLOB []byte greater than size 2^16 bytes as implied by type C.ub2.
			ptrLen[idx] = C.ub2(reflect.TypeOf(v).Size()) // save the byte size of the col value casted to ub2. this implies 2 bytes are enough to store the max size.
		case time.Time:
			if !foundColType {
				sbind.dataType = C.SQLT_TIMESTAMP_TZ
				sbind.maxSize = C.sb4(sizeOfNilPointer)
				applyNulls(sbind.maxSize) // apply nulls now we have the maxSize.
				foundColType = true
			}
			// TODO: wrap up date-time construction into Go function
			var timestampP *unsafe.Pointer
			timestampP, _, err = stmt.conn.ociDescriptorAlloc(C.OCI_DTYPE_TIMESTAMP_TZ, 0)
			if err != nil {
				return err // freeing sbind and pbind will be called by earlier defer()
			}
			pt := unsafe.Pointer(timestampP)
			// Try #1 to construct time zone using Go time zone.
			zone, offset := v.Zone()
			size := len(zone)
			if size < 16 {
				size = 16
			}
			zoneText := cStringN(zone, size)
			defer C.free(unsafe.Pointer(zoneText))
			tryAgain := false
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
			if rv != C.OCI_SUCCESS { // if we failed...
				tryAgain = true
			} else { // else we're okay so double-check if Oracle timezone offset is same...
				rvz := C.WrapOCIDateTimeGetTimeZoneNameOffset(
					stmt.conn.env,
					stmt.conn.errHandle,
					(*C.OCIDateTime)(*(*unsafe.Pointer)(pt)))
				if rvz.rv != C.OCI_SUCCESS { // if we failed...
					// Freeing sbind and pbind will be done by ealier defer().
					return stmt.conn.getError(rvz.rv)
				}
				if offset != int(rvz.h)*60*60+int(rvz.m)*60 {
					// fmt.Println("oracle timezone offset dont match", zone, offset, int(rvz.h)*60*60+int(rvz.m)*60)
					tryAgain = true
				}
			}
			if tryAgain { // if we should try zones with format, "[+-]hh:mm"...
				sign := '+'
				if offset < 0 {
					offset = -offset
					sign = '-'
				}
				offset /= 60
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
					// Freeing of sbind, pbind will be done via earlier defer().
					return stmt.conn.getError(rv)
				}
			}
			// Save the time data to the buffer.
			// sbind.pbuf = unsafe.Pointer((*C.char)(pt))
			slice := (*[1 << 30]byte)(unsafe.Pointer((*C.char)(pt)))[:sizeOfNilPointer:sizeOfNilPointer] // create []byte the length of a pointer.
			_, err = buffer.Write(slice)                                                                 // save the ptr address to the buffer.  should this be little endian like the rest?
			if err != nil {
				return fmt.Errorf("binary write for column %v - error: %v", position, err)
			}
			ptrInd[idx] = C.sb2(0)                // use -1 to specify a null.
			ptrLen[idx] = C.ub2(sizeOfNilPointer) // save the byte size of the col value casted to ub2. this implies 2 bytes are enough to store the max size.
		case string:
			err = handleString(idx, v)
			if err != nil {
				return err
			}
		case float32, float64:
			if !foundColType {
				sbind.dataType = C.SQLT_BDOUBLE
				sbind.maxSize = C.sb4(reflect.TypeOf(v).Size())
				applyNulls(sbind.maxSize) // apply nulls now we have the maxSize.
				foundColType = true
			}
			err = binary.Write(&buffer, binary.LittleEndian, v)
			if err != nil {
				return fmt.Errorf("binary write for column %v - error: %v", position, err)
			}
			// Set the indicator and length for this idx.
			ptrInd[idx] = C.sb2(0)                        // use -1 to specify a null.
			ptrLen[idx] = C.ub2(reflect.TypeOf(v).Size()) // save the byte size of the col value casted to ub2. this implies 2 bytes are enough to store the max size.
		case bool: // oracle does not have bool, handle as 0/1 int
			if !foundColType {
				sbind.dataType = C.SQLT_INT
				sbind.maxSize = 1
				applyNulls(sbind.maxSize) // apply nulls now we have the maxSize.
				foundColType = true
			}
			if v {
				err = binary.Write(&buffer, binary.LittleEndian, int8(1))
				// buffer.Write([1]byte{1}[:])
			} else {
				err = binary.Write(&buffer, binary.LittleEndian, int8(0))
			}
			if err != nil {
				return fmt.Errorf("binary write for column %v - error: %v", position, err)
			}
			// Set the indicator and length for this idx.
			ptrInd[idx] = C.sb2(0) // use -1 to specify a null.
			ptrLen[idx] = C.ub2(1) // save the byte size of the col value casted to ub2. this implies 2 bytes are enough to store the max size.
		default:
			// Try to convert v to a string.
			v2 := fmt.Sprintf("%v", v)
			err = handleString(idx, v2)
			if err != nil {
				return err
			}
		}
	}

	if !foundColType { // if each column value was nil, everything was null...
		// No data type was found so use CHAR type.
		// See also "case nil:" above.
		sbind.dataType = C.SQLT_AFC  // SQLT_AFC is type CHAR (not varchar2)
		sbind.maxSize = C.sizeof_ub2 // save the max size (Sizeof C.ub2 was used for the -1 nil above)
		applyNulls(sbind.maxSize)
	}

	sbind.pbuf = unsafe.Pointer(cByte(buffer.Bytes())) // original mattn malloc. this will malloc and store in memory (needs freeing later)  // TODO: how can we avoid double copy of data?
	stmt.pbind = append(stmt.pbind, sbind)             // save sbind so we free the allocated store later.
	doFreeSBind = false
	// Richard Lloyd - disable bind by name for array binds.
	// if uv.Name != "" {
	// 	err = stmt.ociBindByName([]byte(":"+uv.Name), &sbind)
	// } else {
	//  err = stmt.ociBindByPos(C.ub4(i+1), &sbind)
	// }
	err = stmt.ociBindByPos(C.ub4(position+1), &sbind)
	if err != nil {
		// Freeing binds will be done by earlier defer().
		return err
	} else { // else there was no error so we should keep the malloc'ed data until batch is executed.
		doFreePBind = false
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
		stmt.pbind = nil // free the current pbind slice for next batch.
	}()
	mode := C.ub4(C.OCI_BATCH_ERRORS)
	if stmt.conn.inTransaction == false {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
	}
	done := make(chan struct{})
	go stmt.ociBreak(ctx, done)
	err = stmt.ociStmtExecute(C.ub4(stmt.pbind[0].iters), mode) // iters must be the length of pbind.pbuf i.e the data bind batch/array.
	// fmt.Println("dumping error after exec: ", err)
	// fmt.Println("Dump pbind ", stmt.pbind)
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
				i)                                   // error position in the statement handle.
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
		} else { // else we have no DML errors but there is some other error...
			// return the first error.
			return nil, err
		}
	} else if err == ErrOCISuccessWithInfo {
		// OCIErrorGet ((void  *) errhp, (ub4) 1, (text *) NULL, &errcode, errbuf, (ub4) sizeof(errbuf), (ub4) OCI_HTYPE_ERROR);
		// printf("Error - %s\n", errbuf);
		_, err := stmt.conn.ociGetError()
		if err != nil {
			return nil, err
		}
	} else { // else unknown stuff happened...
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

// ociBindByPos calls OCIBindByPos, then returns bind handle and error.
func (stmt *OCI8Stmt) ociBindArrayByPos(position C.ub4, bind *oci8Bind) error {
	result := C.OCIBindByPos(
		stmt.stmt,                      // The statement handle
		&bind.bindHandle,               // The bind handle that is implicitly allocated by this call. The handle is freed implicitly when the statement handle is deallocated.
		stmt.conn.errHandle,            // An error handle
		position,                       // The placeholder attributes are specified by position if OCIBindByPos() is being called.
		bind.pbuf,                      // An address of a data value or an array of data values
		bind.maxSize,                   // The maximum size possible in bytes of any data value for this bind variable
		bind.dataType,                  // The data type of the values being bound
		unsafe.Pointer(bind.indicator), // Void pointer to an indicator variable or array
		bind.length,                    // alenp lengths are in bytes in general
		nil,                            // Pointer to the array of column-level return codes used for OUT variables
		0,                              // A maximum array length parameter used for PL/SQL
		nil,                            // Current array length parameter used only for PL/SQL index table bindings
		C.OCI_DEFAULT,                  // The mode. Recommended to set to OCI_DEFAULT, which makes the bind variable have the same encoding as its statement.
	)
	return stmt.conn.getError(result)
}
