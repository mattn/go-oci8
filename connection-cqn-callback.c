#ifndef S_ORACLE
#include <oratypes.h>
#endif

#include <oci.h>

extern void goCqnCallback(dvoid* ctx, OCISubscription* subHandle, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode);
//typedef void (*FNP)(dvoid* ctx, OCISubscription* subscrhp, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode);

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

// getTableDescriptor is a C wrapper around OCICollGetElem() which requires void*** to be supplied as a void**.
// See comments below.
sword getTableDescriptor(OCIEnv* envhp, OCIError* errhp, OCIColl* table_changes, ub2 i, dvoid** tableDescP) {
    sword result;
    boolean exist;
    dvoid **table_descp;
    dvoid *elemind = (dvoid *)0;

    // Weird cast of void*** to void** is required to stop compiler "warning: incompatible pointer"
    // Oracle documentation shows that an address of void** being used in calls to OCICollGetElem
    // I couldn't get this to work from Golang since it expects correct type matching - obvs.
    result = OCICollGetElem(envhp, errhp, table_changes, i, &exist, (void**)&table_descp, &elemind);  // incompatible pointer types warning!

    if (result == OCI_SUCCESS) {
        *tableDescP = *table_descp;
    }

    return result;
}
