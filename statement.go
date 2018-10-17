package oci8

/*
#include "oci8.go.h"
#cgo !noPkgConfig pkg-config: oci8
*/
import "C"

// noPkgConfig is a Go tag for disabling using pkg-config and using environmental settings like CGO_CFLAGS and CGO_LDFLAGS instead

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
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
	r := C.WrapOCIAttrGetInt(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, C.OCI_ATTR_BIND_COUNT, stmt.conn.err)
	if r.rv != C.OCI_SUCCESS {
		return -1
	}
	return int(r.num)
}

// bind binds the varables / arguments
func (stmt *OCI8Stmt) bind(args []namedValue) ([]oci8bind, error) {
	if len(args) == 0 {
		return nil, nil
	}

	var (
		boundParameters []oci8bind
		err             error
	)
	*stmt.bp = nil
	for i, uv := range args {
		var sbind oci8bind

		vv := uv.Value
		if out, ok := handleOutput(vv); ok {
			sbind.out = out.Dest
			vv, err = driver.DefaultParameterConverter.ConvertValue(out.Dest)
			if err != nil {
				defer freeBoundParameters(boundParameters)
				return nil, err
			}
		}

		switch v := vv.(type) {
		case nil:
			sbind.kind = C.SQLT_STR
			sbind.pbuf = nil
			sbind.clen = 0
		case []byte:
			sbind.kind = C.SQLT_BIN
			sbind.pbuf = unsafe.Pointer(CByte(v))
			sbind.clen = C.sb4(len(v))

		case time.Time:

			var pt unsafe.Pointer
			var zp unsafe.Pointer

			zone, offset := v.Zone()

			size := len(zone)
			if size < 8 {
				size = 8
			}
			size += int(sizeOfNilPointer)
			if ret := C.WrapOCIDescriptorAlloc(
				unsafe.Pointer(stmt.conn.env),
				C.OCI_DTYPE_TIMESTAMP_TZ,
				C.size_t(size)); ret.rv != C.OCI_SUCCESS {
				defer freeBoundParameters(boundParameters)
				return nil, getError(ret.rv, stmt.conn.err)
			} else {
				sbind.kind = C.SQLT_TIMESTAMP_TZ
				sbind.clen = C.sb4(unsafe.Sizeof(pt))
				pt = ret.extra
				*(*unsafe.Pointer)(ret.extra) = ret.ptr
				zp = unsafe.Pointer(uintptr(ret.extra) + sizeOfNilPointer)
			}

			tryagain := false

			copy((*[1 << 30]byte)(zp)[0:len(zone)], zone)
			rv := C.OCIDateTimeConstruct(
				unsafe.Pointer(stmt.conn.env),
				stmt.conn.err,
				(*C.OCIDateTime)(*(*unsafe.Pointer)(pt)),
				C.sb2(v.Year()),
				C.ub1(v.Month()),
				C.ub1(v.Day()),
				C.ub1(v.Hour()),
				C.ub1(v.Minute()),
				C.ub1(v.Second()),
				C.ub4(v.Nanosecond()),
				(*C.OraText)(zp),
				C.size_t(len(zone)),
			)
			if rv != C.OCI_SUCCESS {
				tryagain = true
			} else {
				//check if oracle timezone offset is same ?
				rvz := C.WrapOCIDateTimeGetTimeZoneNameOffset(
					stmt.conn.env,
					stmt.conn.err,
					(*C.OCIDateTime)(*(*unsafe.Pointer)(pt)))
				if rvz.rv != C.OCI_SUCCESS {
					defer freeBoundParameters(boundParameters)
					return nil, getError(rvz.rv, stmt.conn.err)
				}
				if offset != int(rvz.h)*60*60+int(rvz.m)*60 {
					//fmt.Println("oracle timezone offset dont match", zone, offset, int(rvz.h)*60*60+int(rvz.m)*60)
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

				copy((*[1 << 30]byte)(zp)[0:len(zone)], zone)
				rv := C.OCIDateTimeConstruct(
					unsafe.Pointer(stmt.conn.env),
					stmt.conn.err,
					(*C.OCIDateTime)(*(*unsafe.Pointer)(pt)),
					C.sb2(v.Year()),
					C.ub1(v.Month()),
					C.ub1(v.Day()),
					C.ub1(v.Hour()),
					C.ub1(v.Minute()),
					C.ub1(v.Second()),
					C.ub4(v.Nanosecond()),
					(*C.OraText)(zp),
					C.size_t(len(zone)),
				)
				if rv != C.OCI_SUCCESS {
					defer freeBoundParameters(boundParameters)
					return nil, getError(rv, stmt.conn.err)
				}
			}

			sbind.pbuf = unsafe.Pointer((*C.char)(pt))

		case string:
			if sbind.out != nil {
				sbind.kind = C.SQLT_STR
				sbind.clen = 2048 //4 * C.sb4(len(*v))
				sbind.pbuf = unsafe.Pointer((*C.char)(C.malloc(C.size_t(sbind.clen))))
			} else {
				sbind.kind = C.SQLT_AFC // don't trim strings !!!
				sbind.pbuf = unsafe.Pointer(C.CString(v))
				sbind.clen = C.sb4(len(v))
			}

		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
			buffer := bytes.Buffer{}
			err = binary.Write(&buffer, binary.LittleEndian, v)
			if err != nil {
				return nil, fmt.Errorf("binary read for column %v - error: %v", i, err)
			}
			sbind.kind = C.SQLT_INT
			sbind.clen = C.sb4(buffer.Len())
			sbind.pbuf = unsafe.Pointer(CByte(buffer.Bytes()))

		case float32, float64:
			buffer := bytes.Buffer{}
			err = binary.Write(&buffer, binary.LittleEndian, v)
			if err != nil {
				return nil, fmt.Errorf("binary read for column %v - error: %v", i, err)
			}
			sbind.kind = C.SQLT_BDOUBLE
			sbind.clen = C.sb4(buffer.Len())
			sbind.pbuf = unsafe.Pointer(CByte(buffer.Bytes()))

		case bool: // oracle does not have bool, handle as 0/1 int
			sbind.kind = C.SQLT_INT
			sbind.clen = C.sb4(1)
			if v {
				sbind.pbuf = unsafe.Pointer(CByte([]byte{1}))
			} else {
				sbind.pbuf = unsafe.Pointer(CByte([]byte{0}))
			}

		default:
			if sbind.out != nil {
				sbind.kind = C.SQLT_STR
			} else {
				sbind.kind = C.SQLT_CHR
				d := fmt.Sprintf("%v", v)
				sbind.clen = C.sb4(len(d))
				sbind.pbuf = unsafe.Pointer(C.CString(d))
			}
		}

		if uv.Name != "" {
			name := ":" + uv.Name
			cname := C.CString(name)
			defer C.free(unsafe.Pointer(cname))
			if rv := C.OCIBindByName(
				stmt.stmt,
				stmt.bp,
				stmt.conn.err,
				(*C.OraText)(unsafe.Pointer(cname)),
				C.sb4(len(name)),
				unsafe.Pointer(sbind.pbuf),
				sbind.clen,
				sbind.kind,
				nil,
				nil,
				nil,
				0,
				nil,
				C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
				defer freeBoundParameters(stmt.pbind)
				return nil, getError(rv, stmt.conn.err)
			}
		} else {
			if rv := C.OCIBindByPos(
				stmt.stmt,
				stmt.bp,
				stmt.conn.err,
				C.ub4(i+1),
				unsafe.Pointer(sbind.pbuf),
				sbind.clen,
				sbind.kind,
				nil,
				nil,
				nil,
				0,
				nil,
				C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
				defer freeBoundParameters(stmt.pbind)
				return nil, getError(rv, stmt.conn.err)
			}
		}
		boundParameters = append(boundParameters, sbind)
	}
	return boundParameters, nil
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
	var (
		fbp []oci8bind
		err error
	)

	if fbp, err = stmt.bind(args); err != nil {
		return nil, err
	}

	defer freeBoundParameters(fbp)

	iter := C.ub4(1)
	if retUb2 := C.WrapOCIAttrGetUb2(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, C.OCI_ATTR_STMT_TYPE, stmt.conn.err); retUb2.rv != C.OCI_SUCCESS {
		return nil, getError(retUb2.rv, stmt.conn.err)
	} else if retUb2.num == C.OCI_STMT_SELECT {
		iter = 0
	}

	// set the row prefetch.  Only one extra row per fetch will be returned unless this is set.
	if stmt.conn.prefetchRows > 0 {
		if rv := C.WrapOCIAttrSetUb4(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, C.ub4(stmt.conn.prefetchRows), C.OCI_ATTR_PREFETCH_ROWS, stmt.conn.err); rv != C.OCI_SUCCESS {
			return nil, getError(rv, stmt.conn.err)
		}
	}

	// if non-zero, oci will fetch rows until the memory limit or row prefetch limit is hit.
	// useful for memory constrained systems
	if stmt.conn.prefetchMemory > 0 {
		if rv := C.WrapOCIAttrSetUb4(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, C.ub4(stmt.conn.prefetchMemory), C.OCI_ATTR_PREFETCH_MEMORY, stmt.conn.err); rv != C.OCI_SUCCESS {
			return nil, getError(rv, stmt.conn.err)
		}
	}

	mode := C.ub4(C.OCI_DEFAULT)
	if !stmt.conn.inTransaction {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			// select again to avoid race condition if both are done
			select {
			case <-done:
			default:
				C.OCIBreak(
					unsafe.Pointer(stmt.conn.svc),
					stmt.conn.err)
			}

		}
	}()
	rv := C.OCIStmtExecute(
		stmt.conn.svc,
		stmt.stmt,
		stmt.conn.err,
		iter,
		0,
		nil,
		nil,
		mode)
	close(done)
	if rv != C.OCI_SUCCESS {
		return nil, getError(rv, stmt.conn.err)
	}

	var rc int
	if retUb2 := C.WrapOCIAttrGetUb2(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, C.OCI_ATTR_PARAM_COUNT, stmt.conn.err); retUb2.rv != C.OCI_SUCCESS {
		return nil, getError(retUb2.rv, stmt.conn.err)
	} else {
		rc = int(retUb2.num)
	}

	oci8cols := make([]oci8col, rc)
	indrlenptr := C.calloc(C.size_t(rc), C.sizeof_indrlen)
	indrlen := (*[1 << 16]C.indrlen)(indrlenptr)[0:rc]
	for i := 0; i < rc; i++ {
		var p unsafe.Pointer
		var tp C.ub2
		var lp C.ub2

		if rp := C.WrapOCIParamGet(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, stmt.conn.err, C.ub4(i+1)); rp.rv != C.OCI_SUCCESS {
			C.free(indrlenptr)
			return nil, getError(rp.rv, stmt.conn.err)
		} else {
			// A descriptor of the parameter at the position given in the pos parameter, of handle type OCI_DTYPE_PARAM.
			p = rp.ptr
		}

		if tpr := C.WrapOCIAttrGetUb2(p, C.OCI_DTYPE_PARAM, C.OCI_ATTR_DATA_TYPE, stmt.conn.err); tpr.rv != C.OCI_SUCCESS {
			C.free(indrlenptr)
			return nil, getError(tpr.rv, stmt.conn.err)
		} else {
			// external datatype of the column. Valid datatypes are: SQLT_CHR, SQLT_DATE, etc...
			tp = tpr.num
		}

		if nsr := C.WrapOCIAttrGetString(p, C.OCI_DTYPE_PARAM, C.OCI_ATTR_NAME, stmt.conn.err); nsr.rv != C.OCI_SUCCESS {
			C.free(indrlenptr)
			return nil, getError(nsr.rv, stmt.conn.err)
		} else {
			// the name of the column that is being loaded.
			oci8cols[i].name = string((*[1 << 30]byte)(unsafe.Pointer(nsr.ptr))[0:int(nsr.size)])
		}

		if lpr := C.WrapOCIAttrGetUb2(p, C.OCI_DTYPE_PARAM, C.OCI_ATTR_DATA_SIZE, stmt.conn.err); lpr.rv != C.OCI_SUCCESS {
			C.free(indrlenptr)
			return nil, getError(lpr.rv, stmt.conn.err)
		} else {
			// Maximum size in bytes of the external data for the column. This can affect conversion buffer sizes.
			lp = lpr.num
		}
		*stmt.defp = nil
		switch tp {

		case C.SQLT_CHR, C.SQLT_AFC, C.SQLT_VCS, C.SQLT_AVC:
			// TODO: transfer as clob, read all bytes in loop
			// lp *= 4 // utf8 enc
			oci8cols[i].kind = C.SQLT_CHR  // tp
			oci8cols[i].size = int(lp) * 4 // utf8 enc
			oci8cols[i].pbuf = C.malloc(C.size_t(oci8cols[i].size) + 1)

		case C.SQLT_BIN:
			oci8cols[i].kind = C.SQLT_BIN
			oci8cols[i].size = int(lp)
			oci8cols[i].pbuf = C.malloc(C.size_t(oci8cols[i].size))

		case C.SQLT_NUM:
			var precision int
			var scale int
			if rv := C.WrapOCIAttrGetInt(p, C.OCI_DTYPE_PARAM, C.OCI_ATTR_PRECISION, stmt.conn.err); rv.rv != C.OCI_SUCCESS {
				C.free(indrlenptr)
				return nil, getError(rv.rv, stmt.conn.err)
			} else {
				// The precision of numeric type attributes.
				precision = int(rv.num)
			}
			if rv := C.WrapOCIAttrGetInt(p, C.OCI_DTYPE_PARAM, C.OCI_ATTR_SCALE, stmt.conn.err); rv.rv != C.OCI_SUCCESS {
				C.free(indrlenptr)
				return nil, getError(rv.rv, stmt.conn.err)
			} else {
				// The scale of numeric type attributes.
				scale = int(rv.num)
			}
			// The precision of numeric type attributes. If the precision is nonzero and scale is -127, then it is a FLOAT;
			// otherwise, it is a NUMBER(precision, scale).
			// When precision is 0, NUMBER(precision, scale) can be represented simply as NUMBER.
			// https://docs.oracle.com/cd/E11882_01/appdev.112/e10646/oci06des.htm#LNOCI16458

			if (precision == 0 && scale == 0) || scale > 0 || scale == -127 {
				oci8cols[i].kind = C.SQLT_BDOUBLE
				oci8cols[i].size = 8
				oci8cols[i].pbuf = C.malloc(C.size_t(oci8cols[i].size))
			} else {
				oci8cols[i].kind = C.SQLT_INT
				oci8cols[i].size = 8
				oci8cols[i].pbuf = C.malloc(C.size_t(oci8cols[i].size))
			}

		case C.SQLT_INT:
			oci8cols[i].kind = C.SQLT_INT
			oci8cols[i].size = 8
			oci8cols[i].pbuf = C.malloc(C.size_t(oci8cols[i].size))

		case C.SQLT_BFLOAT, C.SQLT_IBFLOAT, C.SQLT_BDOUBLE, C.SQLT_IBDOUBLE:
			oci8cols[i].kind = C.SQLT_BDOUBLE
			oci8cols[i].size = 8
			oci8cols[i].pbuf = C.malloc(C.size_t(oci8cols[i].size))

		case C.SQLT_LNG:
			oci8cols[i].kind = C.SQLT_BIN
			oci8cols[i].size = 2000
			oci8cols[i].pbuf = C.malloc(C.size_t(oci8cols[i].size))

		case C.SQLT_CLOB, C.SQLT_BLOB:
			// allocate + io buffers + ub4
			size := int(unsafe.Sizeof(unsafe.Pointer(nil)) + unsafe.Sizeof(C.ub4(0)))
			if oci8cols[i].size < blobBufSize {
				size += blobBufSize
			} else {
				size += oci8cols[i].size
			}
			if ret := C.WrapOCIDescriptorAlloc(unsafe.Pointer(stmt.conn.env), C.OCI_DTYPE_LOB, C.size_t(size)); ret.rv != C.OCI_SUCCESS {
				C.free(indrlenptr)
				return nil, getError(ret.rv, stmt.conn.err)
			} else {
				oci8cols[i].kind = tp
				oci8cols[i].size = int(sizeOfNilPointer)
				oci8cols[i].pbuf = ret.extra
				*(*unsafe.Pointer)(ret.extra) = ret.ptr
			}

			//      testing
			//		case C.SQLT_DAT:
			//
			//			oci8cols[i].kind = C.SQLT_DAT
			//			oci8cols[i].size = int(lp)
			//			oci8cols[i].pbuf = C.malloc(C.size_t(lp))
			//

		case C.SQLT_TIMESTAMP, C.SQLT_DAT:
			if ret := C.WrapOCIDescriptorAlloc(unsafe.Pointer(stmt.conn.env), C.OCI_DTYPE_TIMESTAMP, C.size_t(sizeOfNilPointer)); ret.rv != C.OCI_SUCCESS {
				C.free(indrlenptr)
				return nil, getError(ret.rv, stmt.conn.err)
			} else {

				oci8cols[i].kind = C.SQLT_TIMESTAMP
				oci8cols[i].size = int(sizeOfNilPointer)
				oci8cols[i].pbuf = ret.extra
				*(*unsafe.Pointer)(ret.extra) = ret.ptr
			}

		case C.SQLT_TIMESTAMP_TZ, C.SQLT_TIMESTAMP_LTZ:
			if ret := C.WrapOCIDescriptorAlloc(unsafe.Pointer(stmt.conn.env), C.OCI_DTYPE_TIMESTAMP_TZ, C.size_t(sizeOfNilPointer)); ret.rv != C.OCI_SUCCESS {
				C.free(indrlenptr)
				return nil, getError(ret.rv, stmt.conn.err)
			} else {

				oci8cols[i].kind = C.SQLT_TIMESTAMP_TZ
				oci8cols[i].size = int(sizeOfNilPointer)
				oci8cols[i].pbuf = ret.extra
				*(*unsafe.Pointer)(ret.extra) = ret.ptr
			}

		case C.SQLT_INTERVAL_DS:
			if ret := C.WrapOCIDescriptorAlloc(unsafe.Pointer(stmt.conn.env), C.OCI_DTYPE_INTERVAL_DS, C.size_t(sizeOfNilPointer)); ret.rv != C.OCI_SUCCESS {
				C.free(indrlenptr)
				return nil, getError(ret.rv, stmt.conn.err)
			} else {

				oci8cols[i].kind = C.SQLT_INTERVAL_DS
				oci8cols[i].size = int(sizeOfNilPointer)
				oci8cols[i].pbuf = ret.extra
				*(*unsafe.Pointer)(ret.extra) = ret.ptr
			}

		case C.SQLT_INTERVAL_YM:
			if ret := C.WrapOCIDescriptorAlloc(unsafe.Pointer(stmt.conn.env), C.OCI_DTYPE_INTERVAL_YM, C.size_t(sizeOfNilPointer)); ret.rv != C.OCI_SUCCESS {
				return nil, getError(ret.rv, stmt.conn.err)
			} else {

				oci8cols[i].kind = C.SQLT_INTERVAL_YM
				oci8cols[i].size = int(sizeOfNilPointer)
				oci8cols[i].pbuf = ret.extra
				*(*unsafe.Pointer)(ret.extra) = ret.ptr
			}

		case C.SQLT_RDD: // rowid
			lp = 40
			oci8cols[i].pbuf = C.malloc(C.size_t(lp) + 1)
			oci8cols[i].kind = C.SQLT_CHR // tp
			oci8cols[i].size = int(lp + 1)

		default:
			oci8cols[i].pbuf = C.malloc(C.size_t(lp) + 1)
			oci8cols[i].kind = C.SQLT_CHR // tp
			oci8cols[i].size = int(lp + 1)
		}

		oci8cols[i].ind = &indrlen[i].ind
		oci8cols[i].rlen = &indrlen[i].rlen

		if rv := C.OCIDefineByPos(
			stmt.stmt,
			stmt.defp,
			stmt.conn.err,
			C.ub4(i+1),
			oci8cols[i].pbuf,
			C.sb4(oci8cols[i].size),
			oci8cols[i].kind,
			unsafe.Pointer(oci8cols[i].ind),
			oci8cols[i].rlen,
			nil,
			C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
			C.free(indrlenptr)
			return nil, getError(rv, stmt.conn.err)
		}
	}

	rows := &OCI8Rows{
		stmt:       stmt,
		cols:       oci8cols,
		e:          false,
		indrlenptr: indrlenptr,
		closed:     false,
		done:       make(chan struct{}),
		cls:        closeRows,
	}

	go func() {
		select {
		case <-rows.done:
		case <-ctx.Done():
			// select again to avoid race condition if both are done
			select {
			case <-rows.done:
			default:
				C.OCIBreak(unsafe.Pointer(stmt.conn.svc), stmt.conn.err)
				rows.Close()
			}
		}
	}()

	return rows, nil
}

// lastInsertId returns the last inserted ID
func (stmt *OCI8Stmt) lastInsertId() (int64, error) {
	// OCI_ATTR_ROWID must be get in handle -> alloc
	// can be coverted to char, but not to int64
	retRowid := C.WrapOCIAttrRowId(unsafe.Pointer(stmt.conn.env), unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, C.OCI_ATTR_ROWID, stmt.conn.err)
	if retRowid.rv == C.OCI_SUCCESS {
		bs := make([]byte, 18)
		for i, b := range retRowid.rowid[:18] {
			bs[i] = byte(b)
		}
		rowid := string(bs)
		return int64(uintptr(unsafe.Pointer(&rowid))), nil
	}
	return int64(0), nil
}

// rowsAffected returns the number of rows affected
func (stmt *OCI8Stmt) rowsAffected() (int64, error) {
	retUb4 := C.WrapOCIAttrGetUb4(unsafe.Pointer(stmt.stmt), C.OCI_HTYPE_STMT, C.OCI_ATTR_ROW_COUNT, stmt.conn.err)
	if retUb4.rv != C.OCI_SUCCESS {
		return 0, getError(retUb4.rv, stmt.conn.err)
	}
	return int64(retUb4.num), nil
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
	var err error
	var fbp []oci8bind

	if fbp, err = stmt.bind(args); err != nil {
		return nil, err
	}

	defer freeBoundParameters(fbp)

	mode := C.ub4(C.OCI_DEFAULT)
	if stmt.conn.inTransaction == false {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			// select again to avoid race condition if both are done
			select {
			case <-done:
			default:
				C.OCIBreak(
					unsafe.Pointer(stmt.conn.svc),
					stmt.conn.err)
			}
		}
	}()

	rv := C.OCIStmtExecute(
		stmt.conn.svc,
		stmt.stmt,
		stmt.conn.err,
		1,
		0,
		nil,
		nil,
		mode)
	close(done)
	if rv != C.OCI_SUCCESS && rv != C.OCI_SUCCESS_WITH_INFO {
		return nil, getError(rv, stmt.conn.err)
	}

	n, en := stmt.rowsAffected()
	var id int64
	var ei error
	if n > 0 {
		id, ei = stmt.lastInsertId()
	}

	err = outputBoundParameters(fbp)
	if err != nil {
		return nil, err
	}

	return &OCI8Result{stmt: stmt, n: n, errn: en, id: id, errid: ei}, nil
}
