package main

/*
#include "ocilib.h"

#cgo pkg-config: ocilib oci8

#ifdef _WINDOWS
  #define sleep(x) Sleep(x*1000)
#endif
#define wait_for_events() sleep(15)
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
    sub = OCI_SubscriptionRegister(con, "sub-00", OCI_CNT_ALL, event_handler, 5468, 0);
    printf("=> Adding queries to be notified...\n\n");
    OCI_Prepare(st, "select * from system.example");
    OCI_SubscriptionAddStatement(sub, st);
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
