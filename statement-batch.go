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
	"unsafe"
)

// type DMLBatcher interface {
// 	BindToBatch(args []driver.Value) error
// 	BindToBatchContext(ctx context.Context, args []driver.Value) error
// 	ExecBatch() (driver.Result, error)
// 	ExecBatchContext(ctx context.Context) (driver.Result, error)
// }

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
	var sbind oci8Bind

	// fmt.Println("len vals = ", len(vals))
	// sbind.length = (*C.ub2)(C.malloc(C.size_t(int(unsafe.Sizeof(C.ub2(0))) * len(vals)))) // allocate an array of c.ub2, one element per col value held in the buffer.
	sbind.length = (*C.ub2)(C.malloc(C.size_t(int(C.sizeof_sb2) * len(vals)))) // allocate an array of c.ub2, one element per col value held in the buffer.
	ptrLen := (*[1 << 30]C.ub2)(unsafe.Pointer(sbind.length))                  // cast length to void* and then to a ptr of [1073741824]c.ub2 so we can index the array later.
	sbind.indicator = (*C.sb2)(C.malloc(C.size_t(int(C.sizeof_sb2) * len(vals))))
	ptrInd := (*[1 << 30]C.sb2)(unsafe.Pointer(sbind.indicator)) // cast length to void* and then to a ptr of [1073741824]c.ub2 so we can index the array later.
	// *sbind.indicator = 0
	sbind.iters = len(vals)
	buffer := bytes.Buffer{}

	switch firstColVal := vals[0].Value.(type) { // inspect the first column value and do initial setup, assuming the other cols are the same type...
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
		sbind.dataType = C.SQLT_INT
		// assume all col values have the same size - use the first one - use Reflect to get the deepest underlying type. unsafe.SizeOf() is not doing this as it sees Driver.Value instead.
		sbind.maxSize = C.sb4(reflect.TypeOf(firstColVal).Size()) // this should be set to the max size of any single entry in the bind array - which should all be the same!
		for idx, nv := range vals { // for each column value...
			switch v := nv.Value.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
				err = binary.Write(&buffer, binary.LittleEndian, v) // write the concrete typed value to the buffer.
				if err != nil {
					return fmt.Errorf("binary write for column %v - error: %v", position, err)
				}
				sz := reflect.TypeOf(v).Size()
				// fmt.Println("reflect type pf v = ", reflect.TypeOf(v))
				// fmt.Println("reflect size of v = ", sz)
				ptrLen[idx] = C.ub2(sz) // save the byte size of the col value casted to ub2. this implies 2 bytes are enough to store the max size.
				ptrInd[idx] = C.sb2(0)  // use -1 to specify a null.
				// fmt.Println("bytes = ", buffer.Bytes())
			}
		}
		sbind.pbuf = unsafe.Pointer(cByte(buffer.Bytes())) // original mattn malloc. this will malloc and store in memory (needs freeing later)  // TODO: how can we avoid double copy of data?
		// sbind.pbuf = unsafe.Pointer(C.CBytes(buffer.Bytes())) // native cgo helper for malloc. // this will malloc and store in memory (needs freeing later)  // TODO: how can we avoid double copy of data?
		// slice := (*[1 << 30]byte)(sbind.pbuf)[:80:80]
		// fmt.Println("dump pbuf mem: ", slice)
		// TODO: how about dumping memory address for 80 bytes around this void ptr?
		// save sbind so we free the allocated store later.
		stmt.pbind = append(stmt.pbind, sbind)
	}

	// assume the column values are all of the same type for now.
	// use the type of the value in the first slice entry [0].
	// for i, uv := range nv { // for each value in a single column...
	// 	vv := uv.Value
	// Richard Lloyd - comment handling of Sql.out bind variables.
	// if out, ok := handleOutput(vv); ok {
	// 	sbind.out = out.Dest
	// 	outIn = out.In
	// 	vv, err = driver.DefaultParameterConverter.ConvertValue(out.Dest)
	// 	if err != nil {
	// 		stmt.pbind = append(stmt.pbind, sbind)
	// 		freeBinds(stmt.pbind)
	// 		return err
	// 	}
	// }
	// switch colVal := vv.(type) {
	// case nil:
	// 	sbind.dataType = C.SQLT_AFC
	// 	sbind.pbuf = nil
	// 	sbind.maxSize = 0
	// 	*sbind.indicator = -1 // set to null
	// case []byte:
	// 	if sbind.out != nil {
	//
	// 		sbind.dataType = C.SQLT_BIN
	// 		sbind.pbuf = unsafe.Pointer(cByteN(colVal, 32768))
	// 		sbind.maxSize = 32767
	// 		if !outIn {
	// 			*sbind.indicator = -1 // set to null
	// 		} else {
	// 			*sbind.length = C.ub2(len(colVal))
	// 		}
	//
	// 	} else {
	// 		sbind.dataType = C.SQLT_BIN
	// 		sbind.pbuf = unsafe.Pointer(cByte(colVal))
	// 		sbind.maxSize = C.sb4(len(colVal))
	// 		*sbind.length = C.ub2(len(colVal))
	// 	}
	// case time.Time:
	// 	sbind.dataType = C.SQLT_TIMESTAMP_TZ
	// 	sbind.maxSize = C.sb4(sizeOfNilPointer)
	// 	*sbind.length = C.ub2(sizeOfNilPointer)
	// 	// TODO: wrap up date time construction into Go function
	// 	var timestampP *unsafe.Pointer
	// 	timestampP, _, err = stmt.conn.ociDescriptorAlloc(C.OCI_DTYPE_TIMESTAMP_TZ, 0)
	// 	if err != nil {
	// 		freeBinds(stmt.pbind)
	// 		return err
	// 	}
	// 	pt := unsafe.Pointer(timestampP)
	// 	zone, offset := colVal.Zone()
	// 	size := len(zone)
	// 	if size < 16 {
	// 		size = 16
	// 	}
	// 	zoneText := cStringN(zone, size)
	// 	defer C.free(unsafe.Pointer(zoneText))
	// 	tryagain := false
	// 	rv := C.OCIDateTimeConstruct(
	// 		unsafe.Pointer(stmt.conn.env),
	// 		stmt.conn.errHandle,
	// 		(*C.OCIDateTime)(*(*unsafe.Pointer)(pt)),
	// 		C.sb2(colVal.Year()),
	// 		C.ub1(colVal.Month()),
	// 		C.ub1(colVal.Day()),
	// 		C.ub1(colVal.Hour()),
	// 		C.ub1(colVal.Minute()),
	// 		C.ub1(colVal.Second()),
	// 		C.ub4(colVal.Nanosecond()),
	// 		zoneText,
	// 		C.size_t(len(zone)),
	// 	)
	// 	if rv != C.OCI_SUCCESS {
	// 		tryagain = true
	// 	} else {
	// 		// check if oracle timezone offset is same ?
	// 		rvz := C.WrapOCIDateTimeGetTimeZoneNameOffset(
	// 			stmt.conn.env,
	// 			stmt.conn.errHandle,
	// 			(*C.OCIDateTime)(*(*unsafe.Pointer)(pt)))
	// 		if rvz.rv != C.OCI_SUCCESS {
	// 			stmt.pbind = append(stmt.pbind, sbind)
	// 			freeBinds(stmt.pbind)
	// 			return stmt.conn.getError(rvz.rv)
	// 		}
	// 		if offset != int(rvz.h)*60*60+int(rvz.m)*60 {
	// 			// fmt.Println("oracle timezone offset dont match", zone, offset, int(rvz.h)*60*60+int(rvz.m)*60)
	// 			tryagain = true
	// 		}
	// 	}
	// 	if tryagain {
	// 		sign := '+'
	// 		if offset < 0 {
	// 			offset = -offset
	// 			sign = '-'
	// 		}
	// 		offset /= 60
	// 		// oracle accept zones "[+-]hh:mm", try second time
	// 		zone = fmt.Sprintf("%c%02d:%02d", sign, offset/60, offset%60)
	// 		if size < len(zone) {
	// 			size = len(zone)
	// 			zoneText = cStringN(zone, size)
	// 			defer C.free(unsafe.Pointer(zoneText))
	// 		} else {
	// 			copy((*[1 << 30]byte)(unsafe.Pointer(zoneText))[:len(zone)], zone)
	// 		}
	// 		rv := C.OCIDateTimeConstruct(
	// 			unsafe.Pointer(stmt.conn.env),
	// 			stmt.conn.errHandle,
	// 			(*C.OCIDateTime)(*(*unsafe.Pointer)(pt)),
	// 			C.sb2(colVal.Year()),
	// 			C.ub1(colVal.Month()),
	// 			C.ub1(colVal.Day()),
	// 			C.ub1(colVal.Hour()),
	// 			C.ub1(colVal.Minute()),
	// 			C.ub1(colVal.Second()),
	// 			C.ub4(colVal.Nanosecond()),
	// 			zoneText,
	// 			C.size_t(len(zone)),
	// 		)
	// 		if rv != C.OCI_SUCCESS {
	// 			stmt.pbind = append(stmt.pbind, sbind)
	// 			freeBinds(stmt.pbind)
	// 			return stmt.conn.getError(rv)
	// 		}
	// 	}
	// 	sbind.pbuf = unsafe.Pointer((*C.char)(pt))
	// case string:
	// 	if sbind.out != nil {
	// 		sbind.dataType = C.SQLT_CHR
	// 		sbind.pbuf = unsafe.Pointer(cStringN(colVal, 32768))
	// 		sbind.maxSize = 32767
	// 		if !outIn {
	// 			*sbind.indicator = -1 // set to null
	// 		} else {
	// 			*sbind.length = C.ub2(len(colVal))
	// 		}
	// 	} else {
	// 		sbind.dataType = C.SQLT_AFC
	// 		sbind.pbuf = unsafe.Pointer(C.CString(colVal))
	// 		sbind.maxSize = C.sb4(len(colVal))
	// 		*sbind.length = C.ub2(len(colVal))
	// 	}
	// case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
	// 	// Write the entire column slice set of values to this buffer.
	// 	err = binary.Write(&buffer, binary.LittleEndian, colVal) // write the column value to the buffer.
	// 	fmt.Println("wrote to buffer. bytes = ", buffer.Bytes())
	// 	if err != nil {
	// 		return fmt.Errorf("binary write for column %v - error: %v", i, err)
	// 	}
	// case float32, float64:
	// 	buffer := bytes.Buffer{}
	// 	err = binary.Write(&buffer, binary.LittleEndian, colVal)
	// 	if err != nil {
	// 		return fmt.Errorf("binary write for column %v - error: %v", i, err)
	// 	}
	// 	sbind.dataType = C.SQLT_BDOUBLE
	// 	sbind.pbuf = unsafe.Pointer(cByte(buffer.Bytes()))
	// 	sbind.maxSize = C.sb4(buffer.Len())
	// 	*sbind.length = C.ub2(buffer.Len())
	// case bool: // oracle does not have bool, handle as 0/1 int
	// 	sbind.dataType = C.SQLT_INT
	// 	if colVal {
	// 		sbind.pbuf = unsafe.Pointer(cByte([]byte{1}))
	// 	} else {
	// 		sbind.pbuf = unsafe.Pointer(cByte([]byte{0}))
	// 	}
	// 	sbind.maxSize = 1
	// 	*sbind.length = 1
	// default:
	// 	if sbind.out != nil {
	// 		// TODO: should this error instead of setting to null?
	// 		sbind.dataType = C.SQLT_AFC
	// 		sbind.pbuf = nil
	// 		sbind.maxSize = 0
	// 		*sbind.length = 0
	// 		*sbind.indicator = -1 // set to null
	// 	} else {
	// 		d := fmt.Sprintf("%v", colVal)
	// 		sbind.dataType = C.SQLT_AFC
	// 		sbind.pbuf = unsafe.Pointer(C.CString(d))
	// 		sbind.maxSize = C.sb4(len(d))
	// 		*sbind.length = C.ub2(len(d))
	// 	}
	// }
	// }

	// Richard Lloyd - disable bind by name for array binds.
	// if uv.Name != "" {
	// 	err = stmt.ociBindByName([]byte(":"+uv.Name), &sbind)
	// } else {
	//  err = stmt.ociBindByPos(C.ub4(i+1), &sbind)
	// }
	err = stmt.ociBindByPos(C.ub4(position+1), &sbind)
	if err != nil {
		freeBinds(stmt.pbind)
		return err
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

// type oci8Bind2 struct {
// 	bindHandle *C.OCIBind
// 	dataType   C.ub2
// 	pbuf       unsafe.Pointer
// 	maxSize    C.sb4
// 	length     *C.ub2
// 	indicator  *C.sb2
// 	out        interface{} // original binded data type
// }

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
