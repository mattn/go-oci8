#include <oci.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

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
  OCIEnv *env;
  ub2 charsetid = 0;
  ret2ptr vvv = {NULL, NULL, 0};
  void *ptr;
  if (extra == 0)  {
    ptr = NULL;
  } else {
    ptr = &vvv.extra;
  }
  if (getenv("NLS_LANG") == NULL && !OCIEnvInit(&env, OCI_DEFAULT, 0, NULL)) {
    charsetid = OCINlsCharSetNameToId(env, (const oratext*)"AL32UTF8");
    OCIHandleFree(env, OCI_HTYPE_ENV);
  }

  vvv.rv = OCIEnvNlsCreate(
    (OCIEnv**)(&vvv.ptr),
    mode,
    NULL,
    NULL,
    NULL,
    NULL,
    extra,
    ptr,
    charsetid,
    charsetid);
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

static ret1ptr
WrapOCIServerAttach(OCIServer *srv, OCIError *err, text *dblink, ub4 dblinklen, ub4 mode) {
  ret1ptr vvv = {NULL, 0};
  vvv.rv = OCIServerAttach(
    srv,
    err,
    dblink,
    dblinklen,
    mode);
  return vvv;
}

static ret1ptr
WrapOCISessionBegin(OCISvcCtx *srv, OCIError *err, OCISession *usr, ub4 credt, ub4 mode) {
  ret1ptr vvv = {NULL, 0};
  vvv.rv = OCISessionBegin(
    srv,
    err,
    usr,
    credt,
    mode);
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
