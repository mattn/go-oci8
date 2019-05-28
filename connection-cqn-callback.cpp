#ifdef __cplusplus
extern "C" {
#endif

#ifndef S_ORACLE
#include <oratypes.h>
#endif
#include <oci.h>
#include <stdio.h>
#include <stdlib.h>
//#include <functional>

extern void goCqnCallback(dvoid* ctx, OCISubscription* subHandle, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode);

typedef void (*FNP)(dvoid* ctx, OCISubscription* subscrhp, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode);

void cqnCallback(dvoid* ctx, OCISubscription* subscrhp, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode) {
	goCqnCallback(ctx, subscrhp, payload, payl, descriptor, mode);
//	printf("starting auto\n");
//	int a = 8;
//	auto cnt = [&]() { a+=1; return a; };
//	printf("count=%d\n", cnt());   //<<<< move this to C++ file.
}

// getCqnCallback returns a callback function that OCI can use for Continuous Query Notifications.
// The returned function is a lambda closure around the int passed in, so the callback has access to this variable
// the next time it is executed.
// this isn't convertable: FNP getCqnCallback(int functionLookupIdx) {
//std::function<int (int)>
//std::function<FNP> getCqnCallback(int functionLookupIdx) {
//auto getCqnCallback(int functionLookupIdx) {
//    auto cqnCallback2 = [&](dvoid* ctx, OCISubscription* subscrhp, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode) {
//        goCqnCallback(ctx, subscrhp, payload, payl, descriptor, mode);
//        printf("starting auto\n");
//        printf("functionLookupIdx = %d\n", functionLookupIdx);   //<<<< move this to C++ file.
//    };
//    return cqnCallback2;
//}

#ifdef __cplusplus
}
#endif