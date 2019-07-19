#ifndef S_ORACLE
#include <oratypes.h>
#endif

#include <oci.h>

extern void goCqnCallback(dvoid* ctx, OCISubscription* subHandle, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode);

void cqnCallback(dvoid* ctx, OCISubscription* subscrhp, dvoid* payload, ub4* payl, dvoid* descriptor, ub4 mode) {
	goCqnCallback(ctx, subscrhp, payload, payl, descriptor, mode);
}

// getTableChangesCollectionElement() fetches an element from a collection of type OCIColl supplied by reference.
// The table changes are returned via reference - see tableDescP.
// The return value is the sword result that should be checked by the caller before using *tableDescP.
// Because this is used to fetch attribute OCI_DTYPE_CQDES->OCI_ATTR_CQDES_TABLE_CHANGES there is a gotcha
// that OCICollGetElem() requires a void*** to be supplied as a void**.
// See this in examples in Oracle docs and further comments below.
sword getCollectionElement(OCIEnv* envhp, OCIError* errhp, OCIColl* collection, ub2 idx, dvoid** element) {
    sword result;
    boolean exist; // set to true if the element exists after call below.
    dvoid** element_PP;
    dvoid* elemInd = (dvoid*)0;  // optional in call below.

    // Weird cast of void*** to void** is required to stop compiler "warning: incompatible pointer"
    // Oracle documentation shows that an address of void** being used in calls to OCICollGetElem
    // I couldn't get this to work from Golang since it expects correct type matching.
    result = OCICollGetElem(envhp, errhp, collection, idx, &exist, (void**)&element_PP, &elemInd);  // incompatible pointer types warning!

    if (result == OCI_SUCCESS) {
        *element = *element_PP;
    }
    return result;
}
