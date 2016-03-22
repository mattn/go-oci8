package main

// #include "ocilib.h"
//
// #cgo pkg-config: ocilib oci8
//
// #define print_ostr(x)   printf(OTEXT("%s\n"), x)
//
// void err_handler(OCI_Error *err);
// static int nb_err  = 0;
// static int nb_warn = 0;
//
// int init()
// {
//     OCI_Connection* cn;
//     OCI_Statement* st;
//     OCI_Resultset* rs;
//
//     OCI_EnableWarnings(TRUE);
//
//     OCI_Initialize(err_handler, NULL, OCI_ENV_DEFAULT);
//
//     cn = OCI_ConnectionCreate("localhost:1521/xe.oracle.docker", "scott", "tiger", OCI_SESSION_DEFAULT);
//     st = OCI_StatementCreate(cn);
//
//     print_ostr(OCI_GetUserName(cn));
//
//     OCI_ExecuteStmt(st, "select id, name from system.example");
//
//     rs = OCI_GetResultset(st);
//
//     while (OCI_FetchNext(rs))
//     {
//         printf("%i - %s\n", OCI_GetInt(rs, 1), OCI_GetString(rs,2));
//     }
//
//     OCI_Cleanup();
//     printf("\ndone\n");
//     return EXIT_SUCCESS;
// }
//
// // * err_handler
// void err_handler(OCI_Error *err)
// {
//     int err_type = OCI_ErrorGetType(err);
//     printf("\n");
//     if (err_type == OCI_ERR_WARNING)
//     {
// 		printf("> WARNING : ");
// 		nb_warn++;
//     }
//     else
//     {
//         printf("> ERROR   : ");
//         nb_err++;
//     }
//     print_ostr(OCI_ErrorGetString(err));
//     printf("\n");
// }
import "C"

func main() {
	C.init()
}
