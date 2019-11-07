package oci8

// #include "oci8.go.h"
import "C"

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unsafe"
)

// Ping database connection
func (conn *OCI8Conn) Ping(ctx context.Context) error {
	done := make(chan struct{})
	go conn.ociBreak(ctx, done)
	result := C.OCIPing(conn.svc, conn.errHandle, C.OCI_DEFAULT)
	close(done)
	if result == C.OCI_SUCCESS || result == C.OCI_SUCCESS_WITH_INFO {
		return nil
	}
	errorCode, err := conn.ociGetError()
	if errorCode == 1010 {
		// Older versions of Oracle do not support ping,
		// but a response of "ORA-01010: invalid OCI operation" confirms connectivity.
		// See https://github.com/rana/ora/issues/224
		return nil
	}

	conn.logger.Print("Ping error: ", err)
	return driver.ErrBadConn
}

// Close a connection
func (conn *OCI8Conn) Close() error {
	if conn.closed {
		return nil
	}
	conn.closed = true

	var err error
	if useOCISessionBegin {
		if rv := C.OCISessionEnd(
			conn.svc,
			conn.errHandle,
			conn.usrSession,
			C.OCI_DEFAULT,
		); rv != C.OCI_SUCCESS {
			err = conn.getError(rv)
		}
		if rv := C.OCIServerDetach(
			conn.srv,
			conn.errHandle,
			C.OCI_DEFAULT,
		); rv != C.OCI_SUCCESS {
			err = conn.getError(rv)
		}
		C.OCIHandleFree(unsafe.Pointer(conn.usrSession), C.OCI_HTYPE_SESSION)
		C.OCIHandleFree(unsafe.Pointer(conn.srv), C.OCI_HTYPE_SERVER)
		conn.usrSession = nil
		conn.srv = nil
	} else {
		if rv := C.OCILogoff(
			conn.svc,
			conn.errHandle,
		); rv != C.OCI_SUCCESS {
			err = conn.getError(rv)
		}
	}

	C.OCIHandleFree(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX)
	C.OCIHandleFree(unsafe.Pointer(conn.errHandle), C.OCI_HTYPE_ERROR)
	C.OCIHandleFree(unsafe.Pointer(conn.env), C.OCI_HTYPE_ENV)
	conn.svc = nil
	conn.errHandle = nil
	conn.env = nil

	return err
}

// Prepare prepares a query
func (conn *OCI8Conn) Prepare(query string) (driver.Stmt, error) {
	return conn.PrepareContext(context.Background(), query)
}

// PrepareContext prepares a query with context
func (conn *OCI8Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if conn.enableQMPlaceholders {
		query = placeholders(query)
	}

	queryP := cString(query)
	defer C.free(unsafe.Pointer(queryP))

	// statement handle
	stmt, _, err := conn.ociHandleAlloc(C.OCI_HTYPE_STMT, 0)
	if err != nil {
		return nil, fmt.Errorf("allocate statement handle error: %v", err)
	}

	if rv := C.OCIStmtPrepare(
		(*C.OCIStmt)(*stmt),
		conn.errHandle,
		queryP,
		C.ub4(len(query)),
		C.ub4(C.OCI_NTV_SYNTAX),
		C.ub4(C.OCI_DEFAULT),
	); rv != C.OCI_SUCCESS {
		C.OCIHandleFree(*stmt, C.OCI_HTYPE_STMT)
		return nil, conn.getError(rv)
	}

	return &OCI8Stmt{conn: conn, stmt: (*C.OCIStmt)(*stmt)}, nil
}

// Begin starts a transaction
func (conn *OCI8Conn) Begin() (driver.Tx, error) {
	return conn.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a transaction
func (conn *OCI8Conn) BeginTx(ctx context.Context, txOptions driver.TxOptions) (driver.Tx, error) {
	if conn.transactionMode != C.OCI_TRANS_READWRITE {
		// transaction handle
		trans, _, err := conn.ociHandleAlloc(C.OCI_HTYPE_TRANS, 0)
		if err != nil {
			return nil, fmt.Errorf("allocate transaction handle error: %v", err)
		}

		// sets the transaction context attribute of the service context
		err = conn.ociAttrSet(unsafe.Pointer(conn.svc), C.OCI_HTYPE_SVCCTX, *trans, 0, C.OCI_ATTR_TRANS)
		if err != nil {
			C.OCIHandleFree(*trans, C.OCI_HTYPE_TRANS)
			return nil, err
		}

		// transaction handle should be freed by something once attached to the service context
		// but I cannot find anything in the documentation explicitly calling this out
		// going by examples: https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci17msc006.htm#i428845

		if rv := C.OCITransStart(
			conn.svc,
			conn.errHandle,
			0,
			conn.transactionMode, // mode is: C.OCI_TRANS_SERIALIZABLE, C.OCI_TRANS_READWRITE, or C.OCI_TRANS_READONLY
		); rv != C.OCI_SUCCESS {
			return nil, conn.getError(rv)
		}

	}

	conn.inTransaction = true

	return &OCI8Tx{conn}, nil
}

// getError gets error from return result (sword) or OCIError
func (conn *OCI8Conn) getError(result C.sword) error {
	switch result {
	case C.OCI_SUCCESS:
		return nil
	case C.OCI_INVALID_HANDLE:
		return ErrOCIInvalidHandle
	case C.OCI_SUCCESS_WITH_INFO:
		return ErrOCISuccessWithInfo
	case C.OCI_RESERVED_FOR_INT_USE:
		return ErrOCIReservedForIntUse
	case C.OCI_NO_DATA:
		return ErrOCINoData
	case C.OCI_NEED_DATA:
		return ErrOCINeedData
	case C.OCI_STILL_EXECUTING:
		return ErrOCIStillExecuting
	case C.OCI_ERROR:
		errorCode, err := conn.ociGetError()
		switch errorCode {
		/*
			bad connection errors:
			ORA-00028: your session has been killed
			ORA-01012: Not logged on
			ORA-01033: ORACLE initialization or shutdown in progress
			ORA-01034: ORACLE not available
			ORA-01089: immediate shutdown in progress - no operations are permitted
			ORA-03113: end-of-file on communication channel
			ORA-03114: Not Connected to Oracle
			ORA-03135: connection lost contact
			ORA-12528: TNS:listener: all appropriate instances are blocking new connections
			ORA-12537: TNS:connection closed
		*/
		case 28, 1012, 1033, 1034, 1089, 3113, 3114, 3135, 12528, 12537:
			return driver.ErrBadConn
		}
		return err
	}
	return fmt.Errorf("received result code %d", result)
}

// ociGetError calls OCIErrorGet then returs error code and text
func (conn *OCI8Conn) ociGetError() (int, error) {
	var errorCode C.sb4
	errorText := make([]byte, 1024)

	result := C.OCIErrorGet(
		unsafe.Pointer(conn.errHandle), // error handle
		1,                              // status record number, starts from 1
		nil,                            // sqlstate, not supported in release 8.x or later
		&errorCode,                     // error code
		(*C.OraText)(&errorText[0]),    // error message text
		1024,                           // size of the buffer provided in number of bytes
		C.OCI_HTYPE_ERROR,              // type of the handle (OCI_HTYPE_ERR or OCI_HTYPE_ENV)
	)
	if result != C.OCI_SUCCESS {
		return 3114, errors.New("OCIErrorGet failed")
	}

	index := bytes.IndexByte(errorText, 0)

	return int(errorCode), errors.New(string(errorText[:index]))
}

// ociAttrGet calls OCIAttrGet with OCIParam then returns attribute size and error.
// The attribute value is stored into passed value.
func (conn *OCI8Conn) ociAttrGet(paramHandle *C.OCIParam, value unsafe.Pointer, attributeType C.ub4) (C.ub4, error) {
	var size C.ub4

	result := C.OCIAttrGet(
		unsafe.Pointer(paramHandle), // Pointer to a handle type
		C.OCI_DTYPE_PARAM,           // The handle type: OCI_DTYPE_PARAM, for a parameter descriptor
		value,                       // Pointer to the storage for an attribute value
		&size,                       // The size of the attribute value
		attributeType,               // The attribute type: https://docs.oracle.com/cd/B19306_01/appdev.102/b14250/ociaahan.htm
		conn.errHandle,              // An error handle
	)

	return size, conn.getError(result)
}

// ociAttrSet calls OCIAttrSet.
// Only uses errHandle from conn, so can be called in conn setup after errHandle has been set.
func (conn *OCI8Conn) ociAttrSet(
	handle unsafe.Pointer,
	handleType C.ub4,
	value unsafe.Pointer,
	valueSize C.ub4,
	attributeType C.ub4,
) error {
	result := C.OCIAttrSet(
		handle,         // Pointer to a handle whose attribute gets modified
		handleType,     // The handle type
		value,          // Pointer to an attribute value
		valueSize,      // The size of an attribute value
		attributeType,  // The type of attribute being set
		conn.errHandle, // An error handle
	)

	return conn.getError(result)
}

// ociHandleAlloc calls OCIHandleAlloc then returns
// handle pointer to pointer, buffer pointer to pointer, and error
func (conn *OCI8Conn) ociHandleAlloc(handleType C.ub4, size C.size_t) (*unsafe.Pointer, *unsafe.Pointer, error) {
	var handleTemp unsafe.Pointer
	handle := &handleTemp
	var bufferTemp unsafe.Pointer
	var buffer *unsafe.Pointer
	if size > 0 {
		buffer = &bufferTemp
	}

	result := C.OCIHandleAlloc(
		unsafe.Pointer(conn.env), // An environment handle
		handle,                   // Returns a handle
		handleType,               // type of handle: https://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci02bas.htm#LNOCI87581
		size,                     // amount of user memory to be allocated
		buffer,                   // Returns a pointer to the user memory
	)

	err := conn.getError(result)
	if err != nil {
		return nil, nil, err
	}

	if size > 0 {
		return handle, buffer, nil
	}

	return handle, nil, nil
}

// ociDescriptorAlloc calls OCIDescriptorAlloc then returns
// descriptor pointer to pointer, buffer pointer to pointer, and error
func (conn *OCI8Conn) ociDescriptorAlloc(descriptorType C.ub4, size C.size_t) (*unsafe.Pointer, *unsafe.Pointer, error) {
	var descriptorTemp unsafe.Pointer
	descriptor := &descriptorTemp
	var bufferTemp unsafe.Pointer
	var buffer *unsafe.Pointer
	if size > 0 {
		buffer = &bufferTemp
	}

	result := C.OCIDescriptorAlloc(
		unsafe.Pointer(conn.env), // An environment handle
		descriptor,               // Returns a descriptor or LOB locator of desired type
		descriptorType,           // Specifies the type of descriptor or LOB locator to be allocated
		size,                     // Specifies an amount of user memory to be allocated for use by the application for the lifetime of the descriptor
		buffer,                   // Returns a pointer to the user memory of size xtramem_sz allocated by the call for the user for the lifetime of the descriptor
	)

	err := conn.getError(result)
	if err != nil {
		return nil, nil, err
	}

	if size > 0 {
		return descriptor, buffer, nil
	}

	return descriptor, nil, nil
}

// ociLobCreateTemporary calls OCILobCreateTemporary then returns error
func (conn *OCI8Conn) ociLobCreateTemporary(lobLocator *C.OCILobLocator, form C.ub1, lobType C.ub1) error {

	result := C.OCILobCreateTemporary(
		conn.svc,               // service context handle
		conn.errHandle,         // error handle
		lobLocator,             // locator that points to the temporary LOB
		C.OCI_DEFAULT,          // LOB character set ID. For Oracle8i or later, pass as OCI_DEFAULT.
		form,                   // character set form
		lobType,                // type of LOB to create: OCI_TEMP_BLOB or OCI_TEMP_CLOB
		C.TRUE,                 // Pass TRUE if the temporary LOB should be read into the cache; pass FALSE if it should not. FALSE for NOCACHE functionality
		C.OCI_DURATION_SESSION, //  duration of the temporary LOB: OCI_DURATION_SESSION or OCI_DURATION_CALL
	)

	return conn.getError(result)
}

// ociLobRead calls OCILobRead then returns lob bytes and error.
func (conn *OCI8Conn) ociLobRead(lobLocator *C.OCILobLocator, form C.ub1) ([]byte, error) {
	buffer := make([]byte, 0)

	// set character set form
	result := C.OCILobCharSetForm(
		conn.env,       // environment handle
		conn.errHandle, // error handle
		lobLocator,     // LOB locator
		&form,          // character set form
	)
	if result != C.OCI_SUCCESS {
		return buffer, conn.getError(result)
	}

	readBuffer := byteBufferPool.Get().([]byte)
	piece := (C.ub1)(C.OCI_FIRST_PIECE)
	result = C.OCI_NEED_DATA

	for result == C.OCI_NEED_DATA {
		readBytes := (C.oraub8)(0)

		// If both byte_amtp and char_amtp are set to point to zero and OCI_FIRST_PIECE is passed then polling mode is assumed and data is read till the end of the LOB
		result = C.OCILobRead2(
			conn.svc,                       // service context handle
			conn.errHandle,                 // error handle
			lobLocator,                     // LOB or BFILE locator
			&readBytes,                     // number of bytes to read. Used for BLOB and BFILE always. For CLOB and NCLOB, it is used only when char_amtp is zero.
			nil,                            // number of characters to read
			1,                              // the offset in the first call and in subsequent polling calls the offset parameter is ignored
			unsafe.Pointer(&readBuffer[0]), // pointer to a buffer into which the piece will be read
			lobBufferSize,                  // length of the buffer
			piece,                          // For polling, pass OCI_FIRST_PIECE the first time and OCI_NEXT_PIECE in subsequent calls.
			nil,                            // context pointer for the callback function
			nil,                            // If this is null, then OCI_NEED_DATA will be returned for each piece.
			0,                              // character set ID of the buffer data. If this value is 0 then csid is set to the client's NLS_LANG or NLS_CHAR value, depending on the value of csfrm.
			form,                           // character set form of the buffer data
		)

		if piece == C.OCI_FIRST_PIECE {
			piece = C.OCI_NEXT_PIECE
		}

		if result == C.OCI_SUCCESS || result == C.OCI_NEED_DATA {
			buffer = append(buffer, readBuffer[:int(readBytes)]...)
		}
	}

	return buffer, conn.getError(result)
}

// ociLobWrite calls OCILobWrite then returns error.
func (conn *OCI8Conn) ociLobWrite(lobLocator *C.OCILobLocator, form C.ub1, data []byte) error {
	start := 0
	writeBuffer := byteBufferPool.Get().([]byte)
	piece := (C.ub1)(C.OCI_FIRST_PIECE)
	writeBytes := (C.oraub8)(len(data))
	if len(data) <= lobBufferSize {
		piece = (C.ub1)(C.OCI_ONE_PIECE)
		copy(writeBuffer, data)
	} else {
		copy(writeBuffer, data[0:lobBufferSize])
	}

	for {
		result := C.OCILobWrite2(
			conn.svc,                        // service context handle
			conn.errHandle,                  // error handle
			lobLocator,                      // LOB or BFILE locator
			&writeBytes,                     // IN - The number of bytes to write to the database. OUT - The number of bytes written to the database.
			nil,                             // maximum number of characters to write
			(C.oraub8)(1),                   // the offset in the first call and in subsequent polling calls the offset parameter is ignored
			unsafe.Pointer(&writeBuffer[0]), // pointer to a buffer from which the piece is written
			(C.oraub8)(lobBufferSize),       // length, in bytes, of the data in the buffer
			piece,                           // which piece of the buffer is being written. OCI_ONE_PIECE, indicating that the buffer is written in a single piece. Piecewise or callback mode: OCI_FIRST_PIECE, OCI_NEXT_PIECE, and OCI_LAST_PIECE.
			nil,                             // callback function
			nil,                             // callback that can be registered
			0,                               // character set ID
			form,                            // character set form
		)

		if result != C.OCI_SUCCESS && result != C.OCI_NEED_DATA {
			err := conn.getError(result)
			fmt.Println(err)
			return err
		}

		start += lobBufferSize

		if start >= len(data) {
			break
		}

		if start+lobBufferSize < len(data) {
			piece = C.OCI_NEXT_PIECE
			copy(writeBuffer, data[start:start+lobBufferSize])
		} else {
			piece = C.OCI_LAST_PIECE
			copy(writeBuffer, data[start:])
		}
	}

	return nil
}

// ociDateTimeToTime coverts OCIDateTime to Go Time
// if useOCITimeZone is true, will use OCIDateTime time zone, otherwise will use conn.location
func (conn *OCI8Conn) ociDateTimeToTime(dateTime *C.OCIDateTime, useOCITimeZone bool) (*time.Time, error) {
	// get date
	var year C.sb2
	var month C.ub1
	var day C.ub1
	result := C.OCIDateTimeGetDate(
		unsafe.Pointer(conn.env), // environment handle
		conn.errHandle,           // error handle
		dateTime,                 // pointer to an OCIDateTime
		&year,                    // year
		&month,                   // month
		&day,                     // day
	)
	err := conn.getError(result)
	if err != nil {
		return nil, err
	}

	// get time
	var hour C.ub1
	var min C.ub1
	var sec C.ub1
	var fsec C.ub4
	result = C.OCIDateTimeGetTime(
		unsafe.Pointer(conn.env), // environment handle
		conn.errHandle,           // error handle
		dateTime,                 // pointer to an OCIDateTime
		&hour,                    // hour
		&min,                     // min
		&sec,                     // sec
		&fsec,                    // fsec
	)
	err = conn.getError(result)
	if err != nil {
		return nil, err
	}

	if !useOCITimeZone {
		// return Go Time with conn.location
		aTime := time.Date(int(year), time.Month(month), int(day), int(hour), int(min), int(sec), int(fsec), conn.location)
		return &aTime, nil
	}

	// get OCI time zone offset
	var timeZoneHour C.sb1
	var timeZoneMin C.sb1
	result = C.OCIDateTimeGetTimeZoneOffset(
		unsafe.Pointer(conn.env), // environment handle
		conn.errHandle,           // error handle
		dateTime,                 // pointer to an OCIDateTime
		&timeZoneHour,            // time zone hour
		&timeZoneMin,             // time zone minute
	)
	err = conn.getError(result)
	if err != nil {
		return nil, err
	}

	var location *time.Location
	if timeZoneMin != 0 || timeZoneHour > 14 || timeZoneHour < -12 {
		// create location with FixedZone
		var timeZoneName string
		if timeZoneHour < 0 {
			timeZoneName = strconv.FormatInt(int64(timeZoneHour), 10) + ":"
		} else {
			timeZoneName = "+" + strconv.FormatInt(int64(timeZoneHour), 10) + ":"
		}
		if timeZoneMin == 0 {
			timeZoneName += "00"
		} else {
			if timeZoneMin < 10 {
				timeZoneName += "0"
			}
			timeZoneName += strconv.FormatInt(int64(timeZoneMin), 10)
		}
		location = time.FixedZone(timeZoneName, (3600*int(timeZoneHour))+(60*int(timeZoneMin)))
	} else {
		// use location from timeLocations cache
		location = timeLocations[12+timeZoneHour]
	}

	// return Go Time using OCI time zone offset
	aTime := time.Date(int(year), time.Month(month), int(day), int(hour), int(min), int(sec), int(fsec), location)
	return &aTime, nil
}

// timeToOCIDateTime coverts Go Time to OCIDateTime
func (conn *OCI8Conn) timeToOCIDateTime(aTime *time.Time) (*unsafe.Pointer, error) {
	var err error
	var dateTimePP *unsafe.Pointer
	dateTimePP, _, err = conn.ociDescriptorAlloc(C.OCI_DTYPE_TIMESTAMP_TZ, 0)
	if err != nil {
		return nil, err
	}
	dateTimeP := (*C.OCIDateTime)(*dateTimePP)

	// make time zone string formated: [+|-][HH:MM]
	_, offset := aTime.Zone()
	timeZone := make([]byte, 0, 6)
	if offset < 0 {
		timeZone = append(timeZone, '-')
		offset = -offset
	} else {
		timeZone = append(timeZone, '+')
	}
	// hours
	timeZone = appendSmallInt(timeZone, offset/3600)
	offset %= 3600
	timeZone = append(timeZone, ':')
	// minutes
	timeZone = appendSmallInt(timeZone, offset/60)

	result := C.OCIDateTimeConstruct(
		unsafe.Pointer(conn.env),   // environment handle
		conn.errHandle,             // error handle
		dateTimeP,                  // an OCIDateTime pointer
		C.sb2(aTime.Year()),        // year
		C.ub1(aTime.Month()),       // month
		C.ub1(aTime.Day()),         // day
		C.ub1(aTime.Hour()),        // hour
		C.ub1(aTime.Minute()),      // minute
		C.ub1(aTime.Second()),      // second
		C.ub4(aTime.Nanosecond()),  // fractional second
		(*C.OraText)(&timeZone[0]), // time zone string formated: [+|-][HH:MM]
		C.size_t(6),                //  time zone string length
	)
	err = conn.getError(result)
	if err != nil {
		return nil, err
	}

	return dateTimePP, nil
}

// appendSmallInt takes small int and returns an appended byte slice
// if int is > 99 or < 0 the result may not be as expected
func appendSmallInt(slice []byte, num int) []byte {
	if num == 0 {
		return append(slice, '0', '0')
	}
	if num < 10 {
		return append(slice, '0', byte('0'+num))
	}
	return append(slice, byte('0'+num/10), byte('0'+(num%10)))
}

// ociBreak calls OCIBreak if ctx.Done is finished before done chan is closed
func (conn *OCI8Conn) ociBreak(ctx context.Context, done chan struct{}) {
	select {
	case <-done:
	case <-ctx.Done():
		// select again to avoid race condition if both are done
		select {
		case <-done:
		default:
			result := C.OCIBreak(
				unsafe.Pointer(conn.svc), // The service context handle or the server context handle.
				conn.errHandle,           // An error handle
			)
			err := conn.getError(result)
			if err != nil {
				conn.logger.Print("OCIBreak error: ", err)
			}
		}
	}
}
