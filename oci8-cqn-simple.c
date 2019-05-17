#ifndef S_ORACLE
#include <oratypes.h>
#endif
#include <oci.h>

extern void goCallback();

void myCallbackSimple(dvoid* ctx, OCISubscription* subscrhp, dvoid* payload, ub4* payl, dvoid* escriptor, ub4 mode) {
	goCallback();
}
