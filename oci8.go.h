#include <oci.h>
#include <stdlib.h>

typedef struct {
  ub4 ff;
  sb2 y;
  ub1 m, d, hh, mm, ss;
  sword rv;
} retTime;

static retTime WrapOCIDateTimeGetDateTime(OCIEnv* env, OCIError* err,
                                          OCIDateTime* tptr) {
  retTime vvv;

  vvv.rv = OCIDateTimeGetDate(env, err, tptr, &vvv.y, &vvv.m, &vvv.d);
  if (vvv.rv != OCI_SUCCESS) {
    return vvv;
  }
  vvv.rv =
      OCIDateTimeGetTime(env, err, tptr, &vvv.hh, &vvv.mm, &vvv.ss, &vvv.ff);
  return vvv;
}

typedef struct {
  sb1 h, m;
  ub1 zone[90];  // = max timezone name len
  ub4 zlen;
  sword rv;
} retZone;

static retZone WrapOCIDateTimeGetTimeZoneNameOffset(OCIEnv* env, OCIError* err,
                                                    OCIDateTime* tptr) {
  retZone vvv;
  vvv.zlen = sizeof(vvv.zone);

  vvv.rv = OCIDateTimeGetTimeZoneName(env, err, tptr, vvv.zone, &vvv.zlen);
  if (vvv.rv != OCI_SUCCESS) {
    return vvv;
  }
  vvv.rv = OCIDateTimeGetTimeZoneOffset(env, err, tptr, &vvv.h, &vvv.m);
  return vvv;
}
