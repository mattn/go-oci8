#ifndef S_ORACLE
#include <oratypes.h>
#endif
#include <oci.h>

extern void goCqnCallback(dvoid* ctx, OCISubscription* subHandle, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode);

void cqnCallback(dvoid* ctx, OCISubscription* subscrhp, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode) {
	goCqnCallback(ctx, subscrhp, payload, payl, descriptor, mode);
}
