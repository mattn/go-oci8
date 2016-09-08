package oci8

/*
#include <oci.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#cgo pkg-config: oci8

typedef struct {
  char err[1024];
  sb4 errcode;
} retErr;

static retErr
WrapOCIErrorGet(OCIError *err) {
  retErr vvv;
  OCIErrorGet(err, 1, NULL, &vvv.errcode, (OraText*) vvv.err, sizeof(vvv.err), OCI_HTYPE_ERROR);
  return vvv;
}

typedef struct {
  int num;
  sword rv;
} retInt;

static retInt
WrapOCIAttrGetInt(dvoid *ss, ub4 hType, ub4 aType, OCIError *err) {
  retInt vvv = {0, 0};
  vvv.rv = OCIAttrGet(
    ss,
    hType,
    &vvv.num,
    NULL,
    aType,
    err);
  return vvv;
}

typedef struct {
  ub2 num;
  sword rv;
} retUb2;

static retUb2
WrapOCIAttrGetUb2(dvoid *ss, ub4 hType, ub4 aType, OCIError *err) {
  retUb2 vvv = {0, 0};
  vvv.rv = OCIAttrGet(
    ss,
    hType,
    &vvv.num,
    NULL,
    aType,
    err);
  return vvv;
}

typedef struct {
  ub4 num;
  sword rv;
} retUb4;

static retUb4
WrapOCIAttrGetUb4(dvoid *ss, ub4 hType, ub4 aType, OCIError *err) {
  retUb4 vvv = {0,0};
  vvv.rv = OCIAttrGet(
    ss,
    hType,
    &vvv.num,
    NULL,
    aType,
    err);
  return vvv;
}

typedef struct {
  OraText rowid[19];
  sword rv;
} retRowid;

static retRowid
WrapOCIAttrRowId(dvoid *ss, dvoid *st, ub4 hType, ub4 aType, OCIError *err) {
  OCIRowid *ptr;
  ub4 size;
  retRowid vvv;
  vvv.rv = OCIDescriptorAlloc(
    ss,
    (dvoid*)&ptr,
    OCI_DTYPE_ROWID,
    0,
    NULL);
  if (vvv.rv == OCI_SUCCESS) {
    vvv.rv = OCIAttrGet(
      st,
      hType,
      ptr,
      &size,
      aType,
      err);
    if (vvv.rv == OCI_SUCCESS) {
      ub2 idsize = 18;
      memset(vvv.rowid, 0, sizeof(vvv.rowid));
      vvv.rv = OCIRowidToChar(ptr, vvv.rowid, &idsize, err);
    }
  }
  return vvv;
}

typedef struct {
  char *ptr;
  ub4 size;
  sword rv;
} retString;

static retString
WrapOCIAttrGetString(dvoid *ss, ub4 hType, ub4 aType, OCIError *err) {
  retString vvv = {NULL, 0, 0};
  vvv.rv = OCIAttrGet(
    ss,
    hType,
    &vvv.ptr,
    &vvv.size,
    aType,
    err);
  return vvv;
}

typedef struct {
  dvoid *ptr;
  sword rv;
} ret1ptr;

typedef struct {
  dvoid *ptr;
  dvoid *extra;
  sword rv;
} ret2ptr;

static ret1ptr
WrapOCIParamGet(dvoid *ss, ub4 hType, OCIError *err, ub4 pos) {
  ret1ptr vvv = {NULL, 0};
  vvv.rv = OCIParamGet(
    ss,
    hType,
    err,
    &vvv.ptr,
    pos);
  return vvv;
}

static ret2ptr
WrapOCIDescriptorAlloc(dvoid *env, ub4 type, size_t extra) {
  ret2ptr vvv = {NULL, NULL, 0};
  void *ptr;
  if (extra == 0) {
    ptr = NULL;
  } else {
    ptr = &vvv.extra;
  }
  vvv.rv = OCIDescriptorAlloc(
    env,
    &vvv.ptr,
    type,
    extra,
    ptr);
  return vvv;
}

static ret2ptr
WrapOCIHandleAlloc(dvoid *parrent, ub4 type, size_t extra) {
  ret2ptr vvv = {NULL, NULL, 0};
  void *ptr;
  if (extra == 0) {
    ptr = NULL;
  } else {
    ptr = &vvv.extra;
  }
  vvv.rv = OCIHandleAlloc(
    parrent,
    &vvv.ptr,
    type,
    extra,
    ptr);
  return vvv;
}

static ret2ptr
WrapOCIEnvCreate(ub4 mode, size_t extra) {
  ret2ptr vvv = {NULL, NULL, 0};
  void *ptr;
  if (extra == 0)  {
    ptr = NULL;
  } else {
    ptr = &vvv.extra;
  }
  vvv.rv = OCIEnvCreate(
    (OCIEnv**)(&vvv.ptr),
    mode,
    NULL,
    NULL,
    NULL,
    NULL,
    extra,
    ptr);
  return vvv;
}

static ret1ptr
WrapOCILogon(OCIEnv *env, OCIError *err, OraText *u, ub4 ulen, OraText *p, ub4 plen, OraText *h, ub4 hlen) {
  ret1ptr vvv = {NULL, 0};
  vvv.rv = OCILogon(
    env,
    err,
    (OCISvcCtx**)(&vvv.ptr),
    u,
    ulen,
    p,
    plen,
    h,
    hlen);
  return vvv;
}

typedef struct {
  ub4 ff;
  sb2 y;
  ub1 m, d, hh, mm, ss;
  sword rv;
} retTime;

static retTime
WrapOCIDateTimeGetDateTime(OCIEnv *env, OCIError *err, OCIDateTime *tptr) {
  retTime vvv;

  vvv.rv = OCIDateTimeGetDate(
    env,
    err,
    tptr,
    &vvv.y,
    &vvv.m,
    &vvv.d);
  if (vvv.rv != OCI_SUCCESS) {
    return vvv;
  }
  vvv.rv = OCIDateTimeGetTime(
    env,
    err,
    tptr,
    &vvv.hh,
    &vvv.mm,
    &vvv.ss,
    &vvv.ff);
  return vvv;
}

typedef struct {
  sb1 h, m;
  ub1 zone[90]; // = max timezone name len
  ub4 zlen;
  sword rv;
} retZone;

static retZone
WrapOCIDateTimeGetTimeZoneNameOffset(OCIEnv *env, OCIError *err, OCIDateTime *tptr) {
  retZone vvv;
  vvv.zlen = sizeof(vvv.zone);

  vvv.rv = OCIDateTimeGetTimeZoneName(
    env,
    err,
    tptr,
    vvv.zone,
    &vvv.zlen);
  if (vvv.rv != OCI_SUCCESS) {
    return vvv;
  }
  vvv.rv = OCIDateTimeGetTimeZoneOffset(
    env,
    err,
    tptr,
    &vvv.h,
    &vvv.m);
  return vvv;
}

typedef struct {
  sb4 d, hh, mm, ss, ff;
  sword rv;
} retIntervalDS;

static retIntervalDS
WrapOCIIntervalGetDaySecond(OCIEnv *env, OCIError *err, OCIInterval *ptr) {
  retIntervalDS vvv;
  vvv.rv = OCIIntervalGetDaySecond(
    env,
    err,
    &vvv.d,
    &vvv.hh,
    &vvv.mm,
    &vvv.ss,
    &vvv.ff,
    ptr);
  return vvv;
}

typedef struct {
  sb4 y, m;
  sword rv;
} retIntervalYM;

static retIntervalYM
WrapOCIIntervalGetYearMonth(OCIEnv *env, OCIError *err, OCIInterval *ptr) {
  retIntervalYM vvv;
  vvv.rv = OCIIntervalGetYearMonth(
    env,
    err,
    &vvv.y,
    &vvv.m,
    ptr);
  return vvv;
}

static sword
WrapOCIAttrSetUb4(dvoid *h, ub4 type, ub4 value, ub4  attrtype, OCIError *err) {
  return OCIAttrSet(h, type, &value, 0, attrtype, err);
}

typedef struct  {
       sb2 ind;
       ub2 rlen;
} indrlen;

*/
import "C"
import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

const (
	blobBufSize                                     = 4000
	NOT_LOGGED_ON_ERROR_CODE                        = 1012
	ORACLE_NOT_AVAILABLE_ERROR_CODE                 = 1034
	END_OF_FILE_ON_COMMUNICATION_CHANNEL_ERROR_CODE = 3113
	NOT_CONNECTED_TO_ORACLE_ERROR_CODE              = 3114
	CONNECTION_LOST_CONTACT_ERROR_CODE              = 3135
	EXCEPTION_ENCOUNTERED_CORE_DUMP_ERROR_CODE      = 7445
)

type DSN struct {
	Connect              string
	Username             string
	Password             string
	prefetch_rows        uint32
	prefetch_memory      uint32
	Location             *time.Location
	transactionMode      C.ub4
	enableQMPlaceholders bool
}

func init() {
	sql.Register("oci8", &OCI8Driver{})
}

type OCI8Driver struct {
}

type OCI8Conn struct {
	svc                  unsafe.Pointer
	env                  unsafe.Pointer
	err                  unsafe.Pointer
	prefetch_rows        uint32
	prefetch_memory      uint32
	location             *time.Location
	transactionMode      C.ub4
	inTransaction        bool
	enableQMPlaceholders bool
	closed               bool
}

type OCI8Tx struct {
	c *OCI8Conn
}

// ParseDSN parses a DSN used to connect to Oracle
// It expects to receive a string in the form:
// user:password@host:port/sid?param1=value1&param2=value2
//
// Currently the parameters supported is:
// 1 'loc' which
// sets the timezone to read times in as and to marshal to when writing times to
// Oracle date,
// 2 'isolation' =READONLY,SERIALIZABLE,DEFAULT
// 3 'prefetch_rows'
// 4 'prefetch_memory'
// 5 'questionph' =YES,NO,TRUE,FALSE enable question-mark placeholders, default to false
func ParseDSN(dsnString string) (dsn *DSN, err error) {

	dsn = &DSN{Location: time.Local}

	if dsnString == "" {
		return nil, errors.New("empty dsn")
	}

	const prefix = "oracle://"

	if strings.HasPrefix(dsnString, prefix) {
		dsnString = dsnString[len(prefix):]
	}

	authority, dsnString := split(dsnString, "@")
	if authority != "" {
		dsn.Username, dsn.Password, err = parseAuthority(authority)
		if err != nil {
			return nil, err
		}
	}

	host, params := split(dsnString, "?")

	if host, err = unescape(host, encodeHost); err != nil {
		return nil, err
	}

	dsn.Connect = host
	// set safe defaults
	dsn.prefetch_rows = 10
	dsn.prefetch_memory = 0

	qp, err := ParseQuery(params)
	for k, v := range qp {
		switch k {
		case "loc":
			if len(v) > 0 {
				if dsn.Location, err = time.LoadLocation(v[0]); err != nil {
					return nil, fmt.Errorf("Invalid loc: %v: %v", v[0], err)
				}
			}
		case "isolation":
			switch v[0] {
			case "READONLY":
				dsn.transactionMode = C.OCI_TRANS_READONLY
			case "SERIALIZABLE":
				dsn.transactionMode = C.OCI_TRANS_SERIALIZABLE
			case "DEFAULT":
				dsn.transactionMode = C.OCI_TRANS_READWRITE
			default:
				return nil, fmt.Errorf("Invalid isolation: %v", v[0])
			}
		case "questionph":
			switch v[0] {
			case "YES", "TRUE":
				dsn.enableQMPlaceholders = true
			case "NO", "FALSE":
				dsn.enableQMPlaceholders = false
			default:
				return nil, fmt.Errorf("Invalid questionpm: %v", v[0])
			}
		case "prefetch_rows":
			z, err := strconv.ParseUint(v[0], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid prefetch_rows: %v", v[0])
			}
			dsn.prefetch_rows = uint32(z)
		case "prefetch_memory":
			z, err := strconv.ParseUint(v[0], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid prefetch_memory: %v", v[0])
			}
			dsn.prefetch_memory = uint32(z)
			//default:
			//log.Println("unused parameter", k)

		}
	}
	return dsn, nil
}

func (tx *OCI8Tx) Commit() error {
	tx.c.inTransaction = false
	if rv := C.OCITransCommit(
		(*C.OCISvcCtx)(tx.c.svc),
		(*C.OCIError)(tx.c.err),
		0); rv != C.OCI_SUCCESS {
		return ociGetError(rv, tx.c.err)
	}
	return nil
}

func (tx *OCI8Tx) Rollback() error {
	tx.c.inTransaction = false
	if rv := C.OCITransRollback(
		(*C.OCISvcCtx)(tx.c.svc),
		(*C.OCIError)(tx.c.err),
		0); rv != C.OCI_SUCCESS {
		return ociGetError(rv, tx.c.err)
	}
	return nil
}

func (c *OCI8Conn) Begin() (driver.Tx, error) {
	if c.transactionMode != C.OCI_TRANS_READWRITE {
		var th unsafe.Pointer
		if rv := C.WrapOCIHandleAlloc(
			c.env,
			C.OCI_HTYPE_TRANS,
			0); rv.rv != C.OCI_SUCCESS {
			return nil, errors.New("can't allocate handle")
		} else {
			th = rv.ptr
		}
		if rv := C.OCIAttrSet(
			c.svc,
			C.OCI_HTYPE_SVCCTX,
			th,
			0,
			C.OCI_ATTR_TRANS,
			(*C.OCIError)(c.err)); rv != C.OCI_SUCCESS {
			return nil, ociGetError(rv, c.err)
		}

		if rv := C.OCITransStart(
			(*C.OCISvcCtx)(c.svc),
			(*C.OCIError)(c.err),
			0,
			c.transactionMode); // C.OCI_TRANS_SERIALIZABLE C.OCI_TRANS_READWRITE C.OCI_TRANS_READONLY
		rv != C.OCI_SUCCESS {
			return nil, ociGetError(rv, c.err)
		}
	}
	c.inTransaction = true
	return &OCI8Tx{c}, nil
}

func (d *OCI8Driver) Open(dsnString string) (connection driver.Conn, err error) {
	var (
		conn OCI8Conn
		dsn  *DSN
	)

	if dsn, err = ParseDSN(dsnString); err != nil {
		return nil, err
	}

	if rv := C.WrapOCIEnvCreate(
		C.OCI_DEFAULT|C.OCI_THREADED,
		0); rv.rv != C.OCI_SUCCESS && rv.rv != C.OCI_SUCCESS_WITH_INFO {
		// TODO: error handle not yet allocated, we can't get string error from oracle
		return nil, errors.New("can't OCIEnvCreate")
	} else {
		conn.env = rv.ptr
	}

	if rv := C.WrapOCIHandleAlloc(
		conn.env,
		C.OCI_HTYPE_ERROR,
		0); rv.rv != C.OCI_SUCCESS {
		return nil, errors.New("cant allocate error handle")
	} else {
		conn.err = rv.ptr
	}

	phost := C.CString(dsn.Connect)
	defer C.free(unsafe.Pointer(phost))
	puser := C.CString(dsn.Username)
	defer C.free(unsafe.Pointer(puser))
	ppass := C.CString(dsn.Password)
	defer C.free(unsafe.Pointer(ppass))

	if rv := C.WrapOCILogon(
		(*C.OCIEnv)(conn.env),
		(*C.OCIError)(conn.err),
		(*C.OraText)(unsafe.Pointer(puser)),
		C.ub4(len(dsn.Username)),
		(*C.OraText)(unsafe.Pointer(ppass)),
		C.ub4(len(dsn.Password)),
		(*C.OraText)(unsafe.Pointer(phost)),
		C.ub4(len(dsn.Connect))); rv.rv != C.OCI_SUCCESS && rv.rv != C.OCI_SUCCESS_WITH_INFO {
		return nil, ociGetError(rv.rv, conn.err)
	} else {
		conn.svc = rv.ptr
	}
	conn.location = dsn.Location
	conn.transactionMode = dsn.transactionMode
	conn.prefetch_rows = dsn.prefetch_rows
	conn.prefetch_memory = dsn.prefetch_memory
	conn.enableQMPlaceholders = dsn.enableQMPlaceholders
	return &conn, nil
}

func (c *OCI8Conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true

	var err error
	if rv := C.OCILogoff(
		(*C.OCISvcCtx)(c.svc),
		(*C.OCIError)(c.err)); rv != C.OCI_SUCCESS {
		err = ociGetError(rv, c.err)
	}

	C.OCIHandleFree(
		c.env,
		C.OCI_HTYPE_ENV)

	c.svc = nil
	c.env = nil
	c.err = nil
	return err
}

type OCI8Stmt struct {
	c      *OCI8Conn
	s      unsafe.Pointer
	closed bool
	bp     **C.OCIBind
	defp   **C.OCIDefine
}

func (c *OCI8Conn) Prepare(query string) (driver.Stmt, error) {
	if c.enableQMPlaceholders {
		query = placeholders(query)
	}
	pquery := C.CString(query)
	defer C.free(unsafe.Pointer(pquery))
	var s, bp, defp unsafe.Pointer

	if rv := C.WrapOCIHandleAlloc(
		c.env,
		C.OCI_HTYPE_STMT,
		(C.size_t)(unsafe.Sizeof(bp)*2)); rv.rv != C.OCI_SUCCESS {
		return nil, ociGetError(rv.rv, c.err)
	} else {
		s = rv.ptr
		bp = rv.extra
		defp = unsafe.Pointer(uintptr(rv.extra) + unsafe.Sizeof(unsafe.Pointer(nil)))
	}

	if rv := C.OCIStmtPrepare(
		(*C.OCIStmt)(s),
		(*C.OCIError)(c.err),
		(*C.OraText)(unsafe.Pointer(pquery)),
		C.ub4(C.strlen(pquery)),
		C.ub4(C.OCI_NTV_SYNTAX),
		C.ub4(C.OCI_DEFAULT)); rv != C.OCI_SUCCESS {
		return nil, ociGetError(rv, c.err)
	}

	return &OCI8Stmt{c: c, s: s, bp: (**C.OCIBind)(bp), defp: (**C.OCIDefine)(defp)}, nil
}

func (s *OCI8Stmt) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	C.OCIHandleFree(
		s.s,
		C.OCI_HTYPE_STMT)
	s.s = nil
	return nil
}

func (s *OCI8Stmt) NumInput() int {
	r := C.WrapOCIAttrGetInt(s.s, C.OCI_HTYPE_STMT, C.OCI_ATTR_BIND_COUNT, (*C.OCIError)(s.c.err))
	if r.rv != C.OCI_SUCCESS {
		return -1
	}
	return int(r.num)
}

func freeBoundParameters(boundParameters []oci8bind) {
	for _, col := range boundParameters {
		if col.pbuf != nil {
			switch col.kind {
			case C.SQLT_CLOB, C.SQLT_BLOB:
				freeDecriptor(col.pbuf, C.OCI_DTYPE_LOB)
			case C.SQLT_TIMESTAMP:
				freeDecriptor(col.pbuf, C.OCI_DTYPE_TIMESTAMP)
			case C.SQLT_TIMESTAMP_TZ:
				freeDecriptor(col.pbuf, C.OCI_DTYPE_TIMESTAMP_TZ)
			case C.SQLT_TIMESTAMP_LTZ:
				freeDecriptor(col.pbuf, C.OCI_DTYPE_TIMESTAMP_LTZ)
			case C.SQLT_INTERVAL_DS:
				freeDecriptor(col.pbuf, C.OCI_DTYPE_INTERVAL_DS)
			case C.SQLT_INTERVAL_YM:
				freeDecriptor(col.pbuf, C.OCI_DTYPE_INTERVAL_YM)
			default:
				C.free(col.pbuf)
			}
		}
	}
}

func (s *OCI8Stmt) bind(args []driver.Value) (boundParameters []oci8bind, err error) {
	if len(args) == 0 {
		return nil, nil
	}

	var (
		dty   C.ub2
		cdata *C.char
		clen  C.sb4
	)
	*s.bp = nil
	for i, uv := range args {

		switch v := uv.(type) {
		case nil:
			dty = C.SQLT_STR
			cdata = nil
			clen = 0
		case []byte:
			dty = C.SQLT_BIN
			cdata = CByte(v)
			clen = C.sb4(len(v))
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})

		case float64:
			fb := math.Float64bits(v)
			if fb&0x8000000000000000 != 0 {
				fb ^= 0xffffffffffffffff
			} else {
				fb |= 0x8000000000000000
			}
			dty = C.SQLT_IBDOUBLE
			cdata = CByte([]byte{byte(fb >> 56), byte(fb >> 48), byte(fb >> 40), byte(fb >> 32), byte(fb >> 24), byte(fb >> 16), byte(fb >> 8), byte(fb)})
			clen = 8
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})

		case time.Time:

			var pt unsafe.Pointer
			var zp unsafe.Pointer

			zone, offset := v.Zone()

			size := len(zone)
			if size < 8 {
				size = 8
			}
			size += int(unsafe.Sizeof(unsafe.Pointer(nil)))
			if ret := C.WrapOCIDescriptorAlloc(
				s.c.env,
				C.OCI_DTYPE_TIMESTAMP_TZ,
				C.size_t(size)); ret.rv != C.OCI_SUCCESS {
				defer freeBoundParameters(boundParameters)
				return nil, ociGetError(ret.rv, s.c.err)
			} else {
				dty = C.SQLT_TIMESTAMP_TZ
				clen = C.sb4(unsafe.Sizeof(pt))
				pt = ret.extra
				*(*unsafe.Pointer)(ret.extra) = ret.ptr
				zp = unsafe.Pointer(uintptr(ret.extra) + unsafe.Sizeof(unsafe.Pointer(nil)))
				boundParameters = append(boundParameters, oci8bind{dty, pt})

			}

			tryagain := false

			copy((*[1 << 30]byte)(zp)[0:len(zone)], zone)
			rv := C.OCIDateTimeConstruct(
				s.c.env,
				(*C.OCIError)(s.c.err),
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
					(*C.OCIEnv)(s.c.env),
					(*C.OCIError)(s.c.err),
					(*C.OCIDateTime)(*(*unsafe.Pointer)(pt)))
				if rvz.rv != C.OCI_SUCCESS {
					return nil, ociGetError(rvz.rv, s.c.err)
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
					s.c.env,
					(*C.OCIError)(s.c.err),
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
					return nil, ociGetError(rv, s.c.err)
				}
			}

			cdata = (*C.char)(pt)

		case string:
			dty = C.SQLT_AFC // don't trim strings !!!
			cdata = C.CString(v)
			clen = C.sb4(len(v))
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})
		case int64:
			dty = C.SQLT_INT
			clen = C.sb4(8) // not tested on i386. may only work on amd64
			cdata = (*C.char)(C.malloc(8))
			buf := (*[1 << 30]byte)(unsafe.Pointer(cdata))[0:8]
			buf[0] = byte(v & 0x0ff)
			buf[1] = byte(v >> 8 & 0x0ff)
			buf[2] = byte(v >> 16 & 0x0ff)
			buf[3] = byte(v >> 24 & 0x0ff)
			buf[4] = byte(v >> 32 & 0x0ff)
			buf[5] = byte(v >> 40 & 0x0ff)
			buf[6] = byte(v >> 48 & 0x0ff)
			buf[7] = byte(v >> 56 & 0x0ff)
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})

		case bool: // oracle dont have bool, handle as 0/1
			dty = C.SQLT_INT
			clen = C.sb4(1)
			cdata = (*C.char)(C.malloc(10))
			if v {
				*cdata = 1
			} else {
				*cdata = 0
			}
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})

		default:
			dty = C.SQLT_CHR
			d := fmt.Sprintf("%v", v)
			clen = C.sb4(len(d))
			cdata = C.CString(d)
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})
		}

		if rv := C.OCIBindByPos(
			(*C.OCIStmt)(s.s),
			s.bp,
			(*C.OCIError)(s.c.err),
			C.ub4(i+1),
			unsafe.Pointer(cdata),
			clen,
			dty,
			nil,
			nil,
			nil,
			0,
			nil,
			C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
			defer freeBoundParameters(boundParameters)
			return nil, ociGetError(rv, s.c.err)
		}
	}
	return boundParameters, nil
}

func (s *OCI8Stmt) Query(args []driver.Value) (rows driver.Rows, err error) {
	var (
		fbp []oci8bind
	)

	if fbp, err = s.bind(args); err != nil {
		return nil, err
	}

	defer freeBoundParameters(fbp)

	iter := C.ub4(1)
	if retUb2 := C.WrapOCIAttrGetUb2(s.s, C.OCI_HTYPE_STMT, C.OCI_ATTR_STMT_TYPE, (*C.OCIError)(s.c.err)); retUb2.rv != C.OCI_SUCCESS {
		return nil, ociGetError(retUb2.rv, s.c.err)
	} else if retUb2.num == C.OCI_STMT_SELECT {
		iter = 0
	}

	// set the row prefetch.  Only one extra row per fetch will be returned unless this is set.
	if s.c.prefetch_rows > 0 {
		if rv := C.WrapOCIAttrSetUb4(s.s, C.OCI_HTYPE_STMT, C.ub4(s.c.prefetch_rows), C.OCI_ATTR_PREFETCH_ROWS, (*C.OCIError)(s.c.err)); rv != C.OCI_SUCCESS {
			return nil, ociGetError(rv, s.c.err)
		}
	}

	// if non-zero, oci will fetch rows until the memory limit or row prefetch limit is hit.
	// useful for memory constrained systems
	if s.c.prefetch_memory > 0 {
		if rv := C.WrapOCIAttrSetUb4(s.s, C.OCI_HTYPE_STMT, C.ub4(s.c.prefetch_memory), C.OCI_ATTR_PREFETCH_MEMORY, (*C.OCIError)(s.c.err)); rv != C.OCI_SUCCESS {
			return nil, ociGetError(rv, s.c.err)
		}
	}

	mode := C.ub4(C.OCI_DEFAULT)
	if !s.c.inTransaction {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
	}

	if rv := C.OCIStmtExecute(
		(*C.OCISvcCtx)(s.c.svc),
		(*C.OCIStmt)(s.s),
		(*C.OCIError)(s.c.err),
		iter,
		0,
		nil,
		nil,
		mode); rv != C.OCI_SUCCESS {
		return nil, ociGetError(rv, s.c.err)
	}

	var rc int
	if retUb2 := C.WrapOCIAttrGetUb2(s.s, C.OCI_HTYPE_STMT, C.OCI_ATTR_PARAM_COUNT, (*C.OCIError)(s.c.err)); retUb2.rv != C.OCI_SUCCESS {
		return nil, ociGetError(retUb2.rv, s.c.err)
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

		if rp := C.WrapOCIParamGet(s.s, C.OCI_HTYPE_STMT, (*C.OCIError)(s.c.err), C.ub4(i+1)); rp.rv != C.OCI_SUCCESS {
			return nil, ociGetError(rp.rv, s.c.err)
		} else {
			p = rp.ptr
		}

		if tpr := C.WrapOCIAttrGetUb2(p, C.OCI_DTYPE_PARAM, C.OCI_ATTR_DATA_TYPE, (*C.OCIError)(s.c.err)); tpr.rv != C.OCI_SUCCESS {
			return nil, ociGetError(tpr.rv, s.c.err)
		} else {
			tp = tpr.num
		}

		if nsr := C.WrapOCIAttrGetString(p, C.OCI_DTYPE_PARAM, C.OCI_ATTR_NAME, (*C.OCIError)(s.c.err)); nsr.rv != C.OCI_SUCCESS {
			return nil, ociGetError(nsr.rv, s.c.err)
		} else {
			oci8cols[i].name = string((*[1 << 30]byte)(unsafe.Pointer(nsr.ptr))[0:int(nsr.size)])
		}

		if lpr := C.WrapOCIAttrGetUb2(p, C.OCI_DTYPE_PARAM, C.OCI_ATTR_DATA_SIZE, (*C.OCIError)(s.c.err)); lpr.rv != C.OCI_SUCCESS {
			return nil, ociGetError(lpr.rv, s.c.err)
		} else {
			lp = lpr.num
		}
		/*
		   var (
		          defp *C.OCIDefine
		   )
		*/
		*s.defp = nil
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
			oci8cols[i].kind = C.SQLT_CHR
			oci8cols[i].size = int(lp * 4)
			oci8cols[i].pbuf = C.malloc(C.size_t(oci8cols[i].size) + 1)

		case C.SQLT_IBDOUBLE, C.SQLT_IBFLOAT:
			oci8cols[i].kind = C.SQLT_IBDOUBLE
			oci8cols[i].size = int(8)
			oci8cols[i].pbuf = C.malloc(8)

		case C.SQLT_CLOB, C.SQLT_BLOB:
			// allocate +io buffers + ub4
			size := int(unsafe.Sizeof(unsafe.Pointer(nil)) + unsafe.Sizeof(C.ub4(0)))
			if oci8cols[i].size < blobBufSize {
				size += blobBufSize
			} else {
				size += oci8cols[i].size
			}
			if ret := C.WrapOCIDescriptorAlloc(s.c.env, C.OCI_DTYPE_LOB, C.size_t(size)); ret.rv != C.OCI_SUCCESS {
				return nil, ociGetError(ret.rv, s.c.err)
			} else {

				oci8cols[i].kind = tp
				oci8cols[i].size = int(unsafe.Sizeof(unsafe.Pointer(nil)))
				oci8cols[i].pbuf = ret.extra
				*(*unsafe.Pointer)(ret.extra) = ret.ptr

			}

		//      testing
		//            case C.SQLT_DAT:
		//
		//                   oci8cols[i].kind = C.SQLT_DAT
		//                   oci8cols[i].size = int(lp)
		//                   oci8cols[i].pbuf = C.malloc(C.size_t(lp))
		//

		case C.SQLT_TIMESTAMP, C.SQLT_DAT:
			if ret := C.WrapOCIDescriptorAlloc(s.c.env, C.OCI_DTYPE_TIMESTAMP, C.size_t(unsafe.Sizeof(unsafe.Pointer(nil)))); ret.rv != C.OCI_SUCCESS {
				return nil, ociGetError(ret.rv, s.c.err)
			} else {

				oci8cols[i].kind = C.SQLT_TIMESTAMP
				oci8cols[i].size = int(unsafe.Sizeof(unsafe.Pointer(nil)))
				oci8cols[i].pbuf = ret.extra
				*(*unsafe.Pointer)(ret.extra) = ret.ptr
			}

		case C.SQLT_TIMESTAMP_TZ, C.SQLT_TIMESTAMP_LTZ:
			if ret := C.WrapOCIDescriptorAlloc(s.c.env, C.OCI_DTYPE_TIMESTAMP_TZ, C.size_t(unsafe.Sizeof(unsafe.Pointer(nil)))); ret.rv != C.OCI_SUCCESS {
				return nil, ociGetError(ret.rv, s.c.err)
			} else {

				oci8cols[i].kind = C.SQLT_TIMESTAMP_TZ
				oci8cols[i].size = int(unsafe.Sizeof(unsafe.Pointer(nil)))
				oci8cols[i].pbuf = ret.extra
				*(*unsafe.Pointer)(ret.extra) = ret.ptr
			}

		case C.SQLT_INTERVAL_DS:
			if ret := C.WrapOCIDescriptorAlloc(s.c.env, C.OCI_DTYPE_INTERVAL_DS, C.size_t(unsafe.Sizeof(unsafe.Pointer(nil)))); ret.rv != C.OCI_SUCCESS {
				return nil, ociGetError(ret.rv, s.c.err)
			} else {

				oci8cols[i].kind = C.SQLT_INTERVAL_DS
				oci8cols[i].size = int(unsafe.Sizeof(unsafe.Pointer(nil)))
				oci8cols[i].pbuf = ret.extra
				*(*unsafe.Pointer)(ret.extra) = ret.ptr
			}

		case C.SQLT_INTERVAL_YM:
			if ret := C.WrapOCIDescriptorAlloc(s.c.env, C.OCI_DTYPE_INTERVAL_YM, C.size_t(unsafe.Sizeof(unsafe.Pointer(nil)))); ret.rv != C.OCI_SUCCESS {
				return nil, ociGetError(ret.rv, s.c.err)
			} else {

				oci8cols[i].kind = C.SQLT_INTERVAL_YM
				oci8cols[i].size = int(unsafe.Sizeof(unsafe.Pointer(nil)))
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
			(*C.OCIStmt)(s.s),
			s.defp,
			(*C.OCIError)(s.c.err),
			C.ub4(i+1),
			oci8cols[i].pbuf,
			C.sb4(oci8cols[i].size),
			oci8cols[i].kind,
			unsafe.Pointer(oci8cols[i].ind),
			oci8cols[i].rlen,
			nil,
			C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
			C.free(indrlenptr)
			return nil, ociGetError(rv, s.c.err)
		}
	}
	return &OCI8Rows{s, oci8cols, false, indrlenptr, false}, nil
}

// OCI_ATTR_ROWID must be get in handle -> alloc
// can be coverted to char, but not to int64

func (s *OCI8Stmt) lastInsertId() (int64, error) {
	retRowid := C.WrapOCIAttrRowId(s.c.env, s.s, C.OCI_HTYPE_STMT, C.OCI_ATTR_ROWID, (*C.OCIError)(s.c.err))
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

func GetLastInsertId(id int64) string {
	return *(*string)(unsafe.Pointer(uintptr(id)))
}

func (s *OCI8Stmt) rowsAffected() (int64, error) {
	retUb4 := C.WrapOCIAttrGetUb4(s.s, C.OCI_HTYPE_STMT, C.OCI_ATTR_ROW_COUNT, (*C.OCIError)(s.c.err))
	if retUb4.rv != C.OCI_SUCCESS {
		return 0, ociGetError(retUb4.rv, s.c.err)
	}
	return int64(retUb4.num), nil
}

type OCI8Result struct {
	n     int64
	errn  error
	id    int64
	errid error
	s     *OCI8Stmt
}

func (r *OCI8Result) LastInsertId() (int64, error) {
	return r.id, r.errid
}

func (r *OCI8Result) RowsAffected() (int64, error) {
	return r.n, r.errn
}

func (s *OCI8Stmt) Exec(args []driver.Value) (r driver.Result, err error) {
	var (
		fbp []oci8bind
	)

	if fbp, err = s.bind(args); err != nil {
		return nil, err
	}

	defer freeBoundParameters(fbp)

	mode := C.ub4(C.OCI_DEFAULT)
	if s.c.inTransaction == false {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
	}

	rv := C.OCIStmtExecute(
		(*C.OCISvcCtx)(s.c.svc),
		(*C.OCIStmt)(s.s),
		(*C.OCIError)(s.c.err),
		1,
		0,
		nil,
		nil,
		mode)
	if rv != C.OCI_SUCCESS {
		return nil, ociGetError(rv, s.c.err)
	}

	n, en := s.rowsAffected()
	var id int64
	var ei error
	if n > 0 {
		id, ei = s.lastInsertId()
	}
	return &OCI8Result{s: s, n: n, errn: en, id: id, errid: ei}, nil
}

type oci8col struct {
	name string
	kind C.ub2
	size int
	ind  *C.sb2
	rlen *C.ub2
	pbuf unsafe.Pointer
}

type oci8bind struct {
	kind C.ub2
	pbuf unsafe.Pointer
}

type OCI8Rows struct {
	s          *OCI8Stmt
	cols       []oci8col
	e          bool
	indrlenptr unsafe.Pointer
	closed     bool
}

func freeDecriptor(p unsafe.Pointer, dtype C.ub4) {
	tptr := *(*unsafe.Pointer)(p)
	C.OCIDescriptorFree(unsafe.Pointer(tptr), dtype)
}

func (rc *OCI8Rows) Close() error {
	if rc.closed {
		return nil
	}
	rc.closed = true

	C.free(rc.indrlenptr)
	for _, col := range rc.cols {
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
	}
	return nil
}

func (rc *OCI8Rows) Columns() []string {
	cols := make([]string, len(rc.cols))
	for i, col := range rc.cols {
		cols[i] = col.name
	}
	return cols
}

func (rc *OCI8Rows) Next(dest []driver.Value) error {
	rv := C.OCIStmtFetch(
		(*C.OCIStmt)(rc.s.s),
		(*C.OCIError)(rc.s.c.err),
		1,
		C.OCI_FETCH_NEXT,
		C.OCI_DEFAULT)

	if rv == C.OCI_NO_DATA {
		return io.EOF
	} else if rv != C.OCI_SUCCESS && rv != C.OCI_SUCCESS_WITH_INFO {
		return ociGetError(rv, rc.s.c.err)
	}

	for i := range dest {
		// TODO: switch rc.cols[i].ind
		if *rc.cols[i].ind == -1 { // Null
			dest[i] = nil
			continue
		} else if *rc.cols[i].ind != 0 {
			return errors.New(fmt.Sprintf("Unknown column indicator: %d, col %s", rc.cols[i].ind, rc.cols[i].name))
		}

		switch rc.cols[i].kind {
		case C.SQLT_DAT: // for test, date are return as timestamp
			buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:*rc.cols[i].rlen]
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
				rc.s.c.location)
		case C.SQLT_BLOB, C.SQLT_CLOB:
			ptmp := unsafe.Pointer(uintptr(rc.cols[i].pbuf) + unsafe.Sizeof(unsafe.Pointer(nil)))
			bamt := (*C.ub4)(ptmp)
			ptmp = unsafe.Pointer(uintptr(rc.cols[i].pbuf) + unsafe.Sizeof(C.ub4(0)) + unsafe.Sizeof(unsafe.Pointer(nil)))
			b := (*[1 << 30]byte)(ptmp)[0:blobBufSize]
			var buf []byte
		again:
			*bamt = 0
			rv = C.OCILobRead(
				(*C.OCISvcCtx)(rc.s.c.svc),
				(*C.OCIError)(rc.s.c.err),
				*(**C.OCILobLocator)(rc.cols[i].pbuf),
				bamt,
				1,
				ptmp,
				C.ub4(blobBufSize),
				nil,
				nil,
				0,
				C.SQLCS_IMPLICIT)
			if rv == C.OCI_NEED_DATA {
				buf = append(buf, b[:int(*bamt)]...)
				goto again
			}
			if rv != C.OCI_SUCCESS {
				return ociGetError(rv, rc.s.c.err)
			}
			if rc.cols[i].kind == C.SQLT_BLOB {
				dest[i] = append(buf, b[:int(*bamt)]...)
			} else {
				dest[i] = string(append(buf, b[:int(*bamt)]...))
			}
		case C.SQLT_CHR, C.SQLT_AFC, C.SQLT_AVC:
			buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:*rc.cols[i].rlen]
			switch {
			case *rc.cols[i].ind == 0: // Normal
				dest[i] = string(buf)
			case *rc.cols[i].ind == -2 || // Field longer than type (truncated)
				*rc.cols[i].ind > 0: // Field longer than type (truncated). Value is original length.
				dest[i] = string(buf)
			default:
				return errors.New(fmt.Sprintf("Unknown column indicator: %d", rc.cols[i].ind))
			}
		case C.SQLT_BIN: // RAW
			buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:*rc.cols[i].rlen]
			dest[i] = buf
		case C.SQLT_NUM: // NUMBER
			buf := (*[21]byte)(unsafe.Pointer(rc.cols[i].pbuf))
			dest[i] = buf
		case C.SQLT_VNU: // VARNUM
			buf := (*[22]byte)(unsafe.Pointer(rc.cols[i].pbuf))
			dest[i] = buf
		case C.SQLT_INT: // INT
			buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:*rc.cols[i].rlen]
			dest[i] = buf
		case C.SQLT_LNG: // LONG
			buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:*rc.cols[i].rlen]
			dest[i] = buf
		case C.SQLT_IBDOUBLE, C.SQLT_IBFLOAT:
			colsize := rc.cols[i].size
			buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:colsize]
			if colsize == 4 {
				v := uint32(buf[3])
				v |= uint32(buf[2]) << 8
				v |= uint32(buf[1]) << 16
				v |= uint32(buf[0]) << 24

				// Don't know why bits are inverted that way, but it works
				if buf[0]&0x80 == 0 {
					v ^= 0xffffffff
				} else {
					v &= 0x7fffffff
				}
				dest[i] = math.Float32frombits(v)
			} else if colsize == 8 {
				v := uint64(buf[7])
				v |= uint64(buf[6]) << 8
				v |= uint64(buf[5]) << 16
				v |= uint64(buf[4]) << 24
				v |= uint64(buf[3]) << 32
				v |= uint64(buf[2]) << 40
				v |= uint64(buf[1]) << 48
				v |= uint64(buf[0]) << 56

				// Don't know why bits are inverted that way, but it works
				if buf[0]&0x80 == 0 {
					v ^= 0xffffffffffffffff
				} else {
					v &= 0x7fffffffffffffff
				}

				dest[i] = math.Float64frombits(v)
			} else {
				return errors.New(fmt.Sprintf("Unhandled binary float size: %d", colsize))
			}
		case C.SQLT_TIMESTAMP:
			if rv := C.WrapOCIDateTimeGetDateTime(
				(*C.OCIEnv)(rc.s.c.env),
				(*C.OCIError)(rc.s.c.err),
				*(**C.OCIDateTime)(rc.cols[i].pbuf),
			); rv.rv != C.OCI_SUCCESS {
				return ociGetError(rv.rv, rc.s.c.err)
			} else {
				dest[i] = time.Date(
					int(rv.y),
					time.Month(rv.m),
					int(rv.d),
					int(rv.hh),
					int(rv.mm),
					int(rv.ss),
					int(rv.ff),
					rc.s.c.location)
			}
		case C.SQLT_TIMESTAMP_TZ, C.SQLT_TIMESTAMP_LTZ:
			tptr := *(**C.OCIDateTime)(rc.cols[i].pbuf)
			rv := C.WrapOCIDateTimeGetDateTime(
				(*C.OCIEnv)(rc.s.c.env),
				(*C.OCIError)(rc.s.c.err),
				tptr)
			if rv.rv != C.OCI_SUCCESS {
				return ociGetError(rv.rv, rc.s.c.err)
			}
			rvz := C.WrapOCIDateTimeGetTimeZoneNameOffset(
				(*C.OCIEnv)(rc.s.c.env),
				(*C.OCIError)(rc.s.c.err),
				tptr)
			if rvz.rv != C.OCI_SUCCESS {
				return ociGetError(rvz.rv, rc.s.c.err)
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
		case C.SQLT_INTERVAL_DS:
			iptr := *(**C.OCIInterval)(rc.cols[i].pbuf)
			rv := C.WrapOCIIntervalGetDaySecond(
				(*C.OCIEnv)(rc.s.c.env),
				(*C.OCIError)(rc.s.c.err),
				iptr)
			if rv.rv != C.OCI_SUCCESS {
				return ociGetError(rv.rv, rc.s.c.err)
			}
			dest[i] = int64(time.Duration(rv.d)*time.Hour*24 + time.Duration(rv.hh)*time.Hour + time.Duration(rv.mm)*time.Minute + time.Duration(rv.ss)*time.Second + time.Duration(rv.ff))
		case C.SQLT_INTERVAL_YM:
			iptr := *(**C.OCIInterval)(rc.cols[i].pbuf)
			rv := C.WrapOCIIntervalGetYearMonth(
				(*C.OCIEnv)(rc.s.c.env),
				(*C.OCIError)(rc.s.c.err),
				iptr)
			if rv.rv != C.OCI_SUCCESS {
				return ociGetError(rv.rv, rc.s.c.err)
			}
			dest[i] = int64(rv.y)*12 + int64(rv.m)
		default:
			return errors.New(fmt.Sprintf("Unhandled column type: %d", rc.cols[i].kind))
		}
	}

	return nil
}

func isBadConn(errcode int) (bool, error) {
	switch errcode {
	case NOT_LOGGED_ON_ERROR_CODE, ORACLE_NOT_AVAILABLE_ERROR_CODE,
		END_OF_FILE_ON_COMMUNICATION_CHANNEL_ERROR_CODE, NOT_CONNECTED_TO_ORACLE_ERROR_CODE,
		CONNECTION_LOST_CONTACT_ERROR_CODE, EXCEPTION_ENCOUNTERED_CORE_DUMP_ERROR_CODE:
		return true, driver.ErrBadConn
	}
	return false, nil
}

func ociGetErrorS(err unsafe.Pointer) error {
	rv := C.WrapOCIErrorGet((*C.OCIError)(err))
	if isBadConn, badConnErr := isBadConn(int(rv.errcode)); isBadConn == true {
		return badConnErr
	}
	s := C.GoString(&rv.err[0])
	return errors.New(s)
}

func ociGetError(rv C.sword, err unsafe.Pointer) error {
	switch rv {
	case C.OCI_INVALID_HANDLE:
		return errors.New("OCI_INVALID_HANDLE")
	case C.OCI_SUCCESS_WITH_INFO:
		return errors.New("OCI_SUCCESS_WITH_INFO")
	case C.OCI_RESERVED_FOR_INT_USE:
		return errors.New("OCI_RESERVED_FOR_INT_USE")
	case C.OCI_NO_DATA:
		return errors.New("OCI_NO_DATA")
	case C.OCI_NEED_DATA:
		return errors.New("OCI_NEED_DATA")
	case C.OCI_STILL_EXECUTING:
		return errors.New("OCI_STILL_EXECUTING")
	case C.OCI_SUCCESS:
		panic("ociGetError called with no error")
	case C.OCI_ERROR:
		return ociGetErrorS(err)
	}
	return fmt.Errorf("oracle return error code %d", rv)
}

func CByte(b []byte) *C.char {
	p := C.malloc(C.size_t(len(b)))
	pp := (*[1 << 30]byte)(p)
	copy(pp[:], b)
	return (*C.char)(p)
}

var phre = regexp.MustCompile(`\?`)

// converts "?" characters to  :1, :2, ... :n
func placeholders(sql string) string {
	n := 0
	return phre.ReplaceAllStringFunc(sql, func(string) string {
		n++
		return ":" + strconv.Itoa(n)
	})
}
