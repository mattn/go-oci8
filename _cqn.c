#ifndef S_ORACLE
#include <oratypes.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <oci.h>

#define MAXSTRLENGTH 1024
#define bit(a, b) ((a) & (b))

static char* server = "oracle";
static char* user = "richard";
static char* passwd = "richard";

static int notifications_processed = 0;
static OCISubscription *subhandle1 = (OCISubscription *)0;
static OCISubscription *subhandle2 = (OCISubscription *)0;
static void checker(/*_ OCIError *errhp, sword status _*/);
static void registerQuery(/*_ OCISvcCtx *svchp, OCIError *errhp, OCIStmt *stmthp, OCIEnv *envhp _*/);
static void myCallback(/*_  dvoid *ctx, OCISubscription *subscrhp, dvoid *payload, ub4 *payl, dvoid *descriptor, ub4 mode _*/);
static int NotificationDriver(/*_ int argc, char *argv[]  _*/);
static sword status;
static boolean logged_on = FALSE;
static void processRowChanges(OCIEnv *envhp, OCIError *errhp, OCIStmt *stmthp, OCIColl *row_changes);
static void processTableChanges(OCIEnv *envhp, OCIError *errhp, OCIStmt *stmthp, OCIColl *table_changes);
static void processQueryChanges(OCIEnv *envhp, OCIError *errhp, OCIStmt *stmthp, OCIColl *query_changes);
static int nonractests2(/*_ int argc, char *argv[] _*/);

//int main(int argc, char **argv)
//{
//  NotificationDriver(argc, argv);
//  return 0;
//}

int NotificationDriver(argc, argv) int argc;
char *argv[];
{
  OCIEnv *envhp;
  OCISvcCtx *svchp, *svchp2;
  OCIError *errhp, *errhp2;
  OCISession *authp, *authp2;
  OCIStmt *stmthp, *stmthp2;
  OCIDuration dur, dur2;
  int i;
  dvoid *tmp;
  OCISession *usrhp;
  OCIServer *srvhp;

//  printf("Initializing OCI Process\n");

  /* Initialize the environment. The environment has to be initialized
     with OCI_EVENTS and OCI_OBJECTS to create a continuous query notification
     registration and receive notifications.
  */
//  OCIEnvCreate((OCIEnv **)&envhp, OCI_EVENTS | OCI_OBJECT, (dvoid *)0,(dvoid * (*)(dvoid *, size_t))0,(dvoid * (*)(dvoid *, dvoid *, size_t))0,(void (*)(dvoid *, dvoid *))0,(size_t)0, (dvoid **)0);
//  OCIHandleAlloc((dvoid *)envhp, (dvoid **)&errhp, OCI_HTYPE_ERROR, (size_t)0, (dvoid **)0);
  /* server contexts */
//  OCIHandleAlloc((dvoid *)envhp, (dvoid **)&srvhp, OCI_HTYPE_SERVER,(size_t)0, (dvoid **)0);
//  checker(errhp, OCIServerAttach(srvhp, errhp, (text *)0, (sb4)0, (ub4)OCI_DEFAULT));
//  checker(errhp, OCIServerAttach(srvhp, errhp, (text *)server, (sb4)strlen(server), (ub4)OCI_DEFAULT));
  /* set attribute server context in the service context */
//  OCIHandleAlloc((dvoid *)envhp, (dvoid **)&svchp, OCI_HTYPE_SVCCTX,(size_t)0, (dvoid **)0);
//  OCIAttrSet((dvoid *)svchp, (ub4)OCI_HTYPE_SVCCTX, (dvoid *)srvhp,(ub4)0, (ub4)OCI_ATTR_SERVER, (OCIError *)errhp);
  /* allocate a user context handle */
//  OCIHandleAlloc((dvoid *)envhp, (dvoid **)&usrhp, (ub4)OCI_HTYPE_SESSION,(size_t)0, (dvoid **)0);
//  OCIAttrSet((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION,(dvoid *)((text *)user), (ub4)strlen((char *)user),OCI_ATTR_USERNAME, errhp);
//  OCIAttrSet((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION,(dvoid *)((text *)passwd), (ub4)strlen((char *)passwd),OCI_ATTR_PASSWORD, errhp);
//  checker(errhp, OCISessionBegin(svchp, errhp, usrhp, OCI_CRED_RDBMS, OCI_DEFAULT));

  /* Allocate a statement handle */
  OCIHandleAlloc((dvoid *)envhp, (dvoid **)&stmthp,(ub4)OCI_HTYPE_STMT, 52, (dvoid **)&tmp);
  OCIAttrSet((dvoid *)svchp, (ub4)OCI_HTYPE_SVCCTX, (dvoid *)usrhp, (ub4)0,OCI_ATTR_SESSION, errhp);
  registerQuery(svchp, errhp, stmthp, envhp);
  printf("Waiting for Notifications\n");
  while (notifications_processed <= 100) {
    sleep(1);
  }
  printf("Ending - unregistering...\n");
  fflush(stdout);
  /* Unregister HR */
  checker(errhp,OCISubscriptionUnRegister(svchp, subhandle1, errhp, OCI_DEFAULT));
  checker(errhp, OCISessionEnd(svchp, errhp, usrhp, (ub4)0));
  printf("HR Logged off.\n");
  if (subhandle1)
    OCIHandleFree((dvoid *)subhandle1, OCI_HTYPE_SUBSCRIPTION);
  if (stmthp)
    OCIHandleFree((dvoid *)stmthp, OCI_HTYPE_STMT);
  if (srvhp)
    OCIHandleFree((dvoid *)srvhp, (ub4)OCI_HTYPE_SERVER);
  if (svchp)
    OCIHandleFree((dvoid *)svchp, (ub4)OCI_HTYPE_SVCCTX);
  if (authp)
    OCIHandleFree((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION);
  if (errhp)
    OCIHandleFree((dvoid *)errhp, (ub4)OCI_HTYPE_ERROR);
  if (envhp)
    OCIHandleFree((dvoid *)envhp, (ub4)OCI_HTYPE_ENV);
  return 0;
}

void checker(errhp, status)
OCIError *errhp;
sword status;
{
  text errbuf[512];
  sb4 errcode = 0;
  int retval = 1;

  switch (status)
  {
  case OCI_SUCCESS:
    retval = 0;
    break;
  case OCI_SUCCESS_WITH_INFO:
    (void)printf("Error - OCI_SUCCESS_WITH_INFO\n");
    break;
  case OCI_NEED_DATA:
    (void)printf("Error - OCI_NEED_DATA\n");
    break;
  case OCI_NO_DATA:
    (void)printf("Error - OCI_NODATA\n");
    break;
  case OCI_ERROR:
    (void)OCIErrorGet((dvoid *)errhp, (ub4)1, (text *)NULL, &errcode,
                      errbuf, (ub4)sizeof(errbuf), OCI_HTYPE_ERROR);
    (void)printf("Error - %.*s\n", 512, errbuf);
    break;
  case OCI_INVALID_HANDLE:
    (void)printf("Error - OCI_INVALID_HANDLE\n");
    break;
  case OCI_STILL_EXECUTING:
    (void)printf("Error - OCI_STILL_EXECUTE\n");
    break;
  case OCI_CONTINUE:
    (void)printf("Error - OCI_CONTINUE\n");
    break;
  default:
    break;
  }
  if (retval)
  {
    exit(1);
  }
}

void processRowChanges(OCIEnv *envhp, OCIError *errhp, OCIStmt *stmthp, OCIColl *row_changes)
{
  dvoid **row_descp;
  dvoid *row_desc;
  boolean exist;
  ub2 i, j;
  dvoid *elemind = (dvoid *)0;
  oratext *row_id;
  ub4 row_op;
  char* ptrText;
  char text_insert[8] = "INSERT";
  char text_update[8] = "UPDATE";
  char text_delete[8] = "DELETE";
  char text_other[8] = "OTHER";
  sb4 num_rows;
  if (!row_changes)
    return;
  checker(errhp, OCICollSize(envhp, errhp,
                             (CONST OCIColl *)row_changes, &num_rows));
  printf("Found %d row change(s) to be processed\n", num_rows);
  for (i = 0; i < num_rows; i++)
  {
    checker(errhp, OCICollGetElem(envhp,
                                  errhp, (OCIColl *)row_changes,
                                  i, &exist, &row_descp, &elemind));

    row_desc = *row_descp;
    checker(errhp, OCIAttrGet(row_desc,
                              OCI_DTYPE_ROW_CHDES, (dvoid *)&row_id,
                              NULL, OCI_ATTR_CHDES_ROW_ROWID, errhp));
    checker(errhp, OCIAttrGet(row_desc,
                              OCI_DTYPE_ROW_CHDES, (dvoid *)&row_op,
                              NULL, OCI_ATTR_CHDES_ROW_OPFLAGS, errhp));
    switch(row_op) {
      case OCI_OPCODE_INSERT:
        ptrText = text_insert;
        break;
      case OCI_OPCODE_UPDATE:
        ptrText = text_update;
        break;
      case OCI_OPCODE_DELETE:
        ptrText = text_delete;
        break;
      default:
        ptrText = text_other;
    }
    printf("%00d: row changed is %s row_op 0x%x (%s)\n", i+1, row_id, row_op, ptrText);
  }
  fflush(stdout);
}

void processTableChanges(OCIEnv *envhp, OCIError *errhp, OCIStmt *stmthp, OCIColl *table_changes)
{
  dvoid **table_descp;
  dvoid *table_desc;
  dvoid **row_descp;
  dvoid *row_desc;
  OCIColl *row_changes = (OCIColl *)0;
  boolean exist;
  ub2 i, j;
  dvoid *elemind = (dvoid *)0;
  oratext *table_name;
  ub4 table_op;

  sb4 num_tables;
  if (!table_changes)
    return;
  checker(errhp, OCICollSize(envhp, errhp, (CONST OCIColl *)table_changes, &num_tables));
  // char op_insert[] = "INSERT";
  // char op_update[] = "UPDATE";
  // char op_delete[] = "DELETE";
  // char op_alter[] = "ALTER";
  // char op_drop[] = "DROP";
  // char op_text[50];
  for (i = 0; i < num_tables; i++)
  {
    checker(errhp, OCICollGetElem(envhp,
                                  errhp, (OCIColl *)table_changes,
                                  i, &exist, &table_descp, &elemind));

    table_desc = *table_descp;
    checker(errhp, OCIAttrGet(table_desc,
                              OCI_DTYPE_TABLE_CHDES, (dvoid *)&table_name,
                              NULL, OCI_ATTR_CHDES_TABLE_NAME, errhp));
    checker(errhp, OCIAttrGet(table_desc,
                              OCI_DTYPE_TABLE_CHDES, (dvoid *)&table_op,
                              NULL, OCI_ATTR_CHDES_TABLE_OPFLAGS, errhp));
    checker(errhp, OCIAttrGet(table_desc,
                              OCI_DTYPE_TABLE_CHDES, (dvoid *)&row_changes,
                              NULL, OCI_ATTR_CHDES_TABLE_ROW_CHANGES, errhp));
    // switch(table_op) {
      // case bit(table_op, OCI_OPCODE_INSERT):
        // snprintf(op_text, 50, "%s")
    // }
    printf("Table changed is %s table_op 0x%x\n", table_name, table_op);
    fflush(stdout);
    if (!bit(table_op, OCI_OPCODE_ALLROWS)) {
      processRowChanges(envhp, errhp, stmthp, row_changes);
    }
    else {
      printf("Table all rows changed\n");
    }
    fflush(stdout);
  }
}

void processQueryChanges(OCIEnv *envhp, OCIError *errhp, OCIStmt *stmthp, OCIColl *query_changes)
{
  sb4 num_queries;
  ub8 queryid;
  OCINumber qidnum;
  ub4 queryop;
  dvoid *elemind = (dvoid *)0;
  dvoid *query_desc;
  dvoid **query_descp;
  ub2 i;
  boolean exist;
  OCIColl *table_changes = (OCIColl *)0;

  if (!query_changes)
    return;
  checker(errhp, OCICollSize(envhp, errhp,
                             (CONST OCIColl *)query_changes, &num_queries));
  printf("Found %d queries to process\n", num_queries);
  for (i = 0; i < num_queries; i++)
  {
    checker(errhp, OCICollGetElem(envhp,
                                  errhp, (OCIColl *)query_changes,
                                  i, &exist, &query_descp, &elemind));

    query_desc = *query_descp;
    checker(errhp, OCIAttrGet(query_desc,
                              OCI_DTYPE_CQDES, (dvoid *)&queryid,
                              NULL, OCI_ATTR_CQDES_QUERYID, errhp));
    checker(errhp, OCIAttrGet(query_desc,
                              OCI_DTYPE_CQDES, (dvoid *)&queryop,
                              NULL, OCI_ATTR_CQDES_OPERATION, errhp));
    printf(" Query %lu is changed\n", queryid);
    if (queryop == OCI_EVENT_DEREG)
      printf("Query Deregistered\n");
    checker(errhp, OCIAttrGet(query_desc,
                              OCI_DTYPE_CQDES, (dvoid *)&table_changes,
                              NULL, OCI_ATTR_CQDES_TABLE_CHANGES, errhp));
    processTableChanges(envhp, errhp, stmthp, table_changes);
  }
}

void myCallback(ctx, subscrhp, payload, payl, descriptor, mode)
dvoid *ctx;
OCISubscription *subscrhp;
dvoid *payload;
ub4 *payl;
dvoid *descriptor;
ub4 mode;
{
  OCIColl *table_changes = (OCIColl *)0;
  OCIColl *row_changes = (OCIColl *)0;
  dvoid *change_descriptor = descriptor;
  ub4 notify_type;
  ub2 i, j;
  OCIEnv *envhp;
  OCIError *errhp;
  OCIColl *query_changes = (OCIColl *)0;
  OCIServer *srvhp;
  OCISvcCtx *svchp;
  OCISession *usrhp;
  dvoid *tmp;
  OCIStmt *stmthp;
  (void)OCIEnvInit((OCIEnv **)&envhp, OCI_DEFAULT, (size_t)0, (dvoid **)0);
  (void)OCIHandleAlloc((dvoid *)envhp, (dvoid **)&errhp, OCI_HTYPE_ERROR,(size_t)0, (dvoid **)0);
  /* server contexts */
  (void)OCIHandleAlloc((dvoid *)envhp, (dvoid **)&srvhp, OCI_HTYPE_SERVER,(size_t)0, (dvoid **)0);
  (void)OCIHandleAlloc((dvoid *)envhp, (dvoid **)&svchp, OCI_HTYPE_SVCCTX,(size_t)0, (dvoid **)0);

  OCIAttrGet(change_descriptor, OCI_DTYPE_CHDES, (dvoid*)&notify_type, NULL, OCI_ATTR_CHDES_NFYTYPE, errhp);
  fflush(stdout);
  if (notify_type == OCI_EVENT_SHUTDOWN ||
      notify_type == OCI_EVENT_SHUTDOWN_ANY)
  {
    printf("SHUTDOWN NOTIFICATION RECEIVED\n");
    fflush(stdout);
    notifications_processed++;
    return;
  }
  if (notify_type == OCI_EVENT_STARTUP)
  {
    printf("STARTUP NOTIFICATION RECEIVED\n");
    fflush(stdout);
    notifications_processed++;
    return;
  }
  notifications_processed++;
  checker(errhp, OCIServerAttach(srvhp, errhp, (text *)server, (sb4)strlen(server),(ub4)OCI_DEFAULT));
  OCIHandleAlloc((dvoid *)envhp, (dvoid **)&svchp, (ub4)OCI_HTYPE_SVCCTX, 52, (dvoid **)&tmp);
  /* set attribute server context in the service context */
  OCIAttrSet((dvoid *)svchp, (ub4)OCI_HTYPE_SVCCTX, (dvoid *)srvhp,(ub4)0, (ub4)OCI_ATTR_SERVER, (OCIError *)errhp);
  /* allocate a user context handle */
  OCIHandleAlloc((dvoid *)envhp, (dvoid **)&usrhp, (ub4)OCI_HTYPE_SESSION,(size_t)0, (dvoid **)0);
  OCIAttrSet((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION,(dvoid *)user, (ub4)strlen(user), OCI_ATTR_USERNAME, errhp);
  OCIAttrSet((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION,(dvoid *)passwd, (ub4)strlen(passwd),OCI_ATTR_PASSWORD, errhp);
  checker(errhp, OCISessionBegin(svchp, errhp, usrhp, OCI_CRED_RDBMS,OCI_DEFAULT));
  OCIAttrSet((dvoid *)svchp, (ub4)OCI_HTYPE_SVCCTX,(dvoid *)usrhp, (ub4)0, OCI_ATTR_SESSION, errhp);
  /* Allocate a statement handle */
  OCIHandleAlloc((dvoid *)envhp, (dvoid **)&stmthp, (ub4)OCI_HTYPE_STMT, 52, (dvoid **)&tmp);
  if (notify_type == OCI_EVENT_OBJCHANGE)
  {
    checker(errhp, OCIAttrGet(change_descriptor,OCI_DTYPE_CHDES, &table_changes, NULL, OCI_ATTR_CHDES_TABLE_CHANGES, errhp));
    processTableChanges(envhp, errhp, stmthp, table_changes);
  }
  else if (notify_type == OCI_EVENT_QUERYCHANGE)
  {
    checker(errhp, OCIAttrGet(change_descriptor,OCI_DTYPE_CHDES, &query_changes, NULL,OCI_ATTR_CHDES_QUERIES, errhp));
    processQueryChanges(envhp, errhp, stmthp, query_changes);
  }
  checker(errhp, OCISessionEnd(svchp, errhp, usrhp, OCI_DEFAULT));
  checker(errhp, OCIServerDetach(srvhp, errhp, OCI_DEFAULT));
  if (stmthp)
    OCIHandleFree((dvoid *)stmthp, OCI_HTYPE_STMT);
  if (errhp)
    OCIHandleFree((dvoid *)errhp, OCI_HTYPE_ERROR);
  if (srvhp)
    OCIHandleFree((dvoid *)srvhp, OCI_HTYPE_SERVER);
  if (svchp)
    OCIHandleFree((dvoid *)svchp, OCI_HTYPE_SVCCTX);
  if (usrhp)
    OCIHandleFree((dvoid *)usrhp, OCI_HTYPE_SESSION);
  if (envhp)
    OCIHandleFree((dvoid *)envhp, OCI_HTYPE_ENV);
}

void registerQuery(svchp, errhp, stmthp, envhp)
OCISvcCtx *svchp;
OCIError *errhp;
OCIStmt *stmthp;
OCIEnv *envhp;
{
  OCISubscription *subscrhp;
  ub4 namespace = OCI_SUBSCR_NAMESPACE_DBCHANGE;
  ub4 timeout = 60;
  OCIDefine *defnp1 = (OCIDefine *)0;
  OCIDefine *defnp2 = (OCIDefine *)0;
  OCIDefine *defnp3 = (OCIDefine *)0;
  OCIDefine *defnp4 = (OCIDefine *)0;
  OCIDefine *defnp5 = (OCIDefine *)0;
  int mgr_id = 0;
  // text query_text1[] = "select last_name, employees.department_id, department_name from employees,departments where employee_id = 200 and employees.department_id = departments.department_id";
  text query_text1[] = "select a, b, last_modified from t1";
  ub4 num_prefetch_rows = 0;
  ub4 num_reg_tables;
  OCIColl *table_names;
  ub2 i;
  boolean rowids = TRUE;
  ub4 qosflags = OCI_SUBSCR_CQ_QOS_BEST_EFFORT; //or use OCI_SUBSCR_CQ_QOS_QUERY for query level granularity with no false-positives; use OCI_SUBSCR_CQ_QOS_BEST_EFFORT for best-efforts
  int empno = 0;
  OCINumber qidnum;
  ub8 qid;
  char outstr[MAXSTRLENGTH], dname[MAXSTRLENGTH];
  int q3out;
  fflush(stdout);
  /* allocate subscription handle */
  OCIHandleAlloc((dvoid *)envhp, (dvoid **)&subscrhp, OCI_HTYPE_SUBSCRIPTION, (size_t)0, (dvoid **)0);
  /* set the namespace to DBCHANGE */
  checker(errhp, OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION, (dvoid *)&namespace, sizeof(ub4),OCI_ATTR_SUBSCR_NAMESPACE, errhp));
  /* Associate a notification callback with the subscription */
  checker(errhp, OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION,(void *)myCallback, 0, OCI_ATTR_SUBSCR_CALLBACK, errhp));
  /* Allow extraction of rowid information */
  checker(errhp, OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION,(dvoid *)&rowids, sizeof(ub4),OCI_ATTR_CHNF_ROWIDS, errhp));
  checker(errhp, OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION,(dvoid *)&qosflags, sizeof(ub4),OCI_ATTR_SUBSCR_CQ_QOSFLAGS, errhp));
  /* Create a new registration in the DBCHANGE namespace */
  checker(errhp, OCISubscriptionRegister(svchp, &subscrhp, 1, errhp, OCI_DEFAULT));
  /* Multiple queries can now be associated with the subscription */
  subhandle1 = subscrhp;
  printf("Registering query : %s\n", (const signed char *)query_text1);
  /* Prepare the statement */
  checker(errhp, OCIStmtPrepare(stmthp, errhp, query_text1, (ub4)strlen((const signed char *)query_text1), OCI_V7_SYNTAX,OCI_DEFAULT));
  // Bind
  checker(errhp,OCIDefineByPos(stmthp, &defnp1,errhp, 1, (dvoid *)outstr, MAXSTRLENGTH * sizeof(char),SQLT_STR, (dvoid *)0, (ub2 *)0, (ub2 *)0, OCI_DEFAULT));
  checker(errhp,OCIDefineByPos(stmthp, &defnp2,errhp, 2, (dvoid *)&empno, sizeof(empno),SQLT_INT, (dvoid *)0, (ub2 *)0, (ub2 *)0, OCI_DEFAULT));
  checker(errhp,OCIDefineByPos(stmthp, &defnp3,errhp, 3, (dvoid *)&dname, sizeof(dname),SQLT_STR, (dvoid *)0, (ub2 *)0, (ub2 *)0, OCI_DEFAULT));
  /* Associate the statement with the subscription handle */
  OCIAttrSet(stmthp, OCI_HTYPE_STMT, subscrhp, 0,OCI_ATTR_CHNF_REGHANDLE, errhp);
  /* Execute the statement, the execution performs object registration */
  checker(errhp, OCIStmtExecute(svchp, stmthp, errhp, (ub4)1, (ub4)0,(CONST OCISnapshot *)NULL, (OCISnapshot *)NULL,OCI_DEFAULT));
  fflush(stdout);
  OCIAttrGet(stmthp, OCI_HTYPE_STMT, &qid, (ub4 *)0,OCI_ATTR_CQ_QUERYID, errhp);
  printf("Query Id %lu\n", qid);
  /* commit */
  checker(errhp, OCITransCommit(svchp, errhp, (ub4)0));
}

static void cleanup(envhp, svchp, srvhp, errhp, usrhp)
OCIEnv *envhp;
OCISvcCtx *svchp;
OCIServer *srvhp;
OCIError *errhp;
OCISession *usrhp;
{
  /* detach from the server */
  checker(errhp, OCISessionEnd(svchp, errhp, usrhp, OCI_DEFAULT));
  checker(errhp, OCIServerDetach(srvhp, errhp, (ub4)OCI_DEFAULT));

  if (usrhp)
    (void)OCIHandleFree((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION);
  if (svchp)
    (void)OCIHandleFree((dvoid *)svchp, (ub4)OCI_HTYPE_SVCCTX);
  if (srvhp)
    (void)OCIHandleFree((dvoid *)srvhp, (ub4)OCI_HTYPE_SERVER);
  if (errhp)
    (void)OCIHandleFree((dvoid *)errhp, (ub4)OCI_HTYPE_ERROR);
  if (envhp)
    (void)OCIHandleFree((dvoid *)envhp, (ub4)OCI_HTYPE_ENV);
}