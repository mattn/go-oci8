// package oci8
// import "C"
// import (
// 	"unsafe"
// )
//
// func (stmt *OCI8Stmt) ociDefineByPos(position C.ub4, ) error {
// 	result := C.OCIBindByPos(
// 		stmt.stmt,                      // The statement handle
// 		&bind.bindHandle,               // The bind handle that is implicitly allocated by this call. The handle is freed implicitly when the statement handle is deallocated.
// 		stmt.conn.errHandle,            // An error handle
// 		position,                       // The placeholder attributes are specified by position if OCIBindByPos() is being called.
// 		bind.pbuf,                      // void* valuep - An address of a data value or an array of data values
// 		bind.maxSize,                   // The maximum size possible in bytes of any data value for this bind variable
// 		bind.dataType,                  // The data type of the values being bound
// 		unsafe.Pointer(bind.indicator), // Pointer to an indicator variable or array
// 		bind.length,                    // ub2* alenp - lengths are in bytes in general - pointer to an array of actual array length elements
// 		nil,                            // Pointer to the array of column-level return codes
// 		0,                              // A maximum array length parameter
// 		nil,                            // Current array length parameter
// 		C.OCI_DEFAULT,                  // The mode. Recommended to set to OCI_DEFAULT, which makes the bind variable have the same encoding as its statement.
// 	)
//
// 	return stmt.conn.getError(result)
// }