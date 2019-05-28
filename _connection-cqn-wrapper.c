#ifndef S_ORACLE
#include <oratypes.h>
#endif
#include <oci.h>
#include <stdio.h>

typedef void (*FNP)(dvoid* ctx, OCISubscription* subscrhp, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode);

// getCqnCallback returns a callback function that OCI can use for Continuous Query Notifications.
// The returned function is a lambda closure around the int passed in, so the callback has access to this variable
// the next time it is executed.
// this isn't convertable: FNP getCqnCallback(int functionLookupIdx) {
//std::function<int (int)>
std::function<FNP> getCqnCallback3(int functionLookupIdx) {
//auto getCqnCallback(int functionLookupIdx) {
    auto cqnCallback2 = [&](dvoid* ctx, OCISubscription* subscrhp, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode) {
        //goCqnCallback(ctx, subscrhp, payload, payl, descriptor, mode);
        printf("starting auto\n");
        printf("functionLookupIdx = %d\n", functionLookupIdx);   //<<<< move this to C++ file.
    };
    return cqnCallback2;
}

