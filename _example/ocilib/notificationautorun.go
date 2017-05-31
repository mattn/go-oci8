package main

/*
#include "ocilib.h"

#cgo pkg-config: ocilib oci8

#ifdef _WINDOWS
  #define sleep(x) Sleep(x*1000)
#endif
#define wait_for_events() sleep(5)
void event_handler(OCI_Event *event);
void error_handler(OCI_Error *err);
int init()
{
    OCI_Connection   *con;
    OCI_Subscription *sub;
    OCI_Statement    *st;

    printf("=> Initializing OCILIB in event mode...\n\n");
    if (!OCI_Initialize(error_handler, NULL, OCI_ENV_EVENTS))
        return EXIT_FAILURE;
    printf("=> Connecting to usr@db...\n\n");
    con = OCI_ConnectionCreate("localhost:1521/xe.oracle.docker", "scott", "tiger", OCI_SESSION_DEFAULT);

    OCI_SetAutoCommit(con, TRUE);

    printf("=> Creating statement...\n\n");
    st  = OCI_StatementCreate(con);
    printf("=> Creating tables...\n\n");
    OCI_ExecuteStmt(st, "create table table1(code number)");
    OCI_ExecuteStmt(st, "create table table2(str varchar2(10))");
    printf("=> Registering subscription...\n\n");
    sub = OCI_SubscriptionRegister(con, "sub-00", OCI_CNT_ALL, event_handler, 5468, 0);
    printf("=> Adding queries to be notified...\n\n");
    OCI_Prepare(st, "select * from table1");
    OCI_SubscriptionAddStatement(sub, st);
    OCI_Prepare(st, "select * from table2");
    OCI_SubscriptionAddStatement(sub, st);
    printf("=> Executing some DDL operation...\n\n");
    OCI_ExecuteStmt(st, "alter table table1 add price number");
    wait_for_events();

    printf("=> Executing some DML operation...\n\n");
    OCI_ExecuteStmt(st, "insert into table1 values(1, 10.5)");
    OCI_ExecuteStmt(st, "insert into table2 values('shoes')");
    OCI_ExecuteStmt(st, "update table1 set price = 13.5 where code = 1");
    OCI_ExecuteStmt(st, "delete from table2 ");
    wait_for_events();
    printf("=> Droping tables...\n\n");
    OCI_ExecuteStmt(st, "drop table table1");
    OCI_ExecuteStmt(st, "drop table table2");
    wait_for_events();
    printf("=> Disconnecting from DB...\n\n");
    OCI_ConnectionFree(con);
    printf("=> Stopping the remote database...\n\n");
    OCI_DatabaseShutdown("db", "sys", "sys",
                         OCI_SESSION_SYSDBA,
                         OCI_DB_SDM_FULL,
                         OCI_DB_SDF_IMMEDIATE);
    wait_for_events();;
    printf("=> Starting the remote database...\n\n");
    OCI_DatabaseStartup("db", "sys", "sys",
                         OCI_SESSION_SYSDBA,
                         OCI_DB_SPM_FULL,
                         OCI_DB_SPF_FORCE,
                         NULL);
    wait_for_events();
    printf("=> Unregistering subscription...\n\n");
    OCI_SubscriptionUnregister(sub);
    printf("=> Cleaning up OCILIB resources...\n\n");
    OCI_Cleanup();
    printf("=> Done...\n\n");
    return EXIT_SUCCESS;
}
void error_handler(OCI_Error *err)
{
    int         err_type = OCI_ErrorGetType(err);
    const char *err_msg  = OCI_ErrorGetString(err);
    printf("** %s - %s\n", err_type == OCI_ERR_WARNING ? "Warning" : "Error", err_msg);
}
void event_handler(OCI_Event *event)
{
    unsigned int type     = OCI_EventGetType(event);
    unsigned int op       = OCI_EventGetOperation(event);
    OCI_Subscription *sub = OCI_EventGetSubscription(event);
    printf("** Notification      : %s\n\n", OCI_SubscriptionGetName(sub));
    printf("...... Database      : %s\n",   OCI_EventGetDatabase(event));
    switch (type)
    {
        case OCI_ENT_STARTUP:
            printf("...... Event         : Startup\n");
            break;
        case OCI_ENT_SHUTDOWN:
            printf("...... Event         : Shutdown\n");
            break;
        case OCI_ENT_SHUTDOWN_ANY:
            printf("...... Event         : Shutdown any\n");
            break;
        case OCI_ENT_DROP_DATABASE:
            printf("...... Event         : drop database\n");
            break;
        case OCI_ENT_DEREGISTER:
            printf("...... Event         : deregister\n");
            break;
         case OCI_ENT_OBJECT_CHANGED:

            printf("...... Event         : object changed\n");
            printf("........... Object   : %s\n", OCI_EventGetObject(event));

            switch (op)
            {
                case OCI_ONT_INSERT:
                    printf("........... Action   : insert\n");
                    break;
                case OCI_ONT_UPDATE:
                    printf("........... Action   : update\n");
                    break;
                case OCI_ONT_DELETE:
                    printf("........... Action   : delete\n");
                    break;
                case OCI_ONT_ALTER:
                    printf("........... Action   : alter\n");
                    break;
                case OCI_ONT_DROP:
                    printf("........... Action   : drop\n");
                    break;
            }

            if (op < OCI_ONT_ALTER)
                printf("........... Rowid    : %s\n",  OCI_EventGetRowid(event));

            break;
    }

    printf("\n");
}
*/
import "C"

func main() {
	C.init()
}
