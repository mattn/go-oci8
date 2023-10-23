/************************************************************************
   Copyright (c) 2021 OceanBase.
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not, write to the Free
   Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02111-1301, USA 

   Part of this code includes code from PHP's mysqlnd extension
   (written by Andrey Hristov, Georg Richter and Ulf Wendel), freely
   available from http://www.php.net/software

*************************************************************************/
#include <stdint.h>

#ifdef _WIN32
typedef unsigned int uint;
typedef unsigned long	ulong;
#endif

#define MYSQL_NO_DATA 100
#define MYSQL_DATA_TRUNCATED 101
#define MYSQL_DEFAULT_PREFETCH_ROWS (unsigned long) 1

/* Bind flags */
#define MADB_BIND_DUMMY 1

#define MARIADB_STMT_BULK_SUPPORTED(stmt)\
  ((stmt)->mysql && \
  (!((stmt)->mysql->server_capabilities & CLIENT_MYSQL) &&\
    ((stmt)->mysql->extension->mariadb_server_capabilities & \
    (MARIADB_CLIENT_STMT_BULK_OPERATIONS >> 32))))

#define SET_CLIENT_STMT_ERROR(a, b, c, d) \
do { \
  (a)->last_errno= (b);\
  strncpy((a)->sqlstate, (c), SQLSTATE_LENGTH);\
  (a)->sqlstate[SQLSTATE_LENGTH]= 0;\
  strncpy((a)->last_error, (d) ? (d) : ER((b)), MYSQL_ERRMSG_SIZE);\
  (a)->last_error[MYSQL_ERRMSG_SIZE - 1]= 0;\
} while (0)

#define CLEAR_CLIENT_STMT_ERROR(a) \
do { \
  (a)->last_errno= 0;\
  strcpy((a)->sqlstate, "00000");\
  (a)->last_error[0]= 0;\
} while (0)

#define MYSQL_PS_SKIP_RESULT_W_LEN  -1
#define MYSQL_PS_SKIP_RESULT_STR    -2
#define STMT_ID_LENGTH 4

typedef struct st_mysql_stmt MYSQL_STMT;

typedef MYSQL_RES* (*mysql_stmt_use_or_store_func)(MYSQL_STMT *);

enum enum_stmt_attr_type
{
  STMT_ATTR_UPDATE_MAX_LENGTH,
  STMT_ATTR_CURSOR_TYPE,
  STMT_ATTR_PREFETCH_ROWS,
  STMT_ATTR_ARRAY_BIND,

  /* MariaDB only */
  STMT_ATTR_PREBIND_PARAMS=200,
  STMT_ATTR_ARRAY_SIZE,
  STMT_ATTR_ROW_SIZE,
  STMT_ATTR_STATE,
  STMT_ATTR_CB_USER_DATA,
  STMT_ATTR_CB_PARAM,
  STMT_ATTR_CB_RESULT
};

enum enum_cursor_type
{
  CURSOR_TYPE_NO_CURSOR= 0,
  CURSOR_TYPE_READ_ONLY= 1,
  CURSOR_TYPE_FOR_UPDATE= 2,
  CURSOR_TYPE_SCROLLABLE= 4
};

#define CURSOR_TYPE_ARRAY_BIND 8
#define CURSOR_TYPE_SAVE_EXCEPTION 16

enum enum_indicator_type
{
  STMT_INDICATOR_NTS=-1,
  STMT_INDICATOR_NONE=0,
  STMT_INDICATOR_NULL=1,
  STMT_INDICATOR_DEFAULT=2,
  STMT_INDICATOR_IGNORE=3,
  STMT_INDICATOR_IGNORE_ROW=4
};

enum enum_prepare_execute_request_ext_flag
{
  STMT_PRE_EXE_EFLAG_DEFAULT=0,
  STMT_PRE_EXE_EFLAG_NEED_ARRAYBIND_RES= 2  //need resultset for arraybinding for all types(dml/pl etc.)
};

/*
  bulk PS flags
*/
#define STMT_BULK_FLAG_CLIENT_SEND_TYPES 128
#define STMT_BULK_FLAG_INSERT_ID_REQUEST 64

typedef enum mysql_stmt_state
{
  MYSQL_STMT_INITTED = 0,
  MYSQL_STMT_PREPARED,
  MYSQL_STMT_EXECUTED,
  MYSQL_STMT_WAITING_USE_OR_STORE,
  MYSQL_STMT_USE_OR_STORE_CALLED,
  MYSQL_STMT_USER_FETCHING, /* fetch_row_buff or fetch_row_unbuf */
  MYSQL_STMT_FETCH_DONE
} enum_mysqlnd_stmt_state;

typedef struct st_mysql_bind
{
  unsigned long  *length;          /* output length pointer */
  my_bool        *is_null;         /* Pointer to null indicator */
  void           *buffer;          /* buffer to get/put data */
  /* set this if you want to track data truncations happened during fetch */
  my_bool        *error;
  union {
    unsigned char *row_ptr;        /* for the current data position */
    char *indicator;               /* indicator variable */
  } u;
  void (*store_param_func)(NET *net, struct st_mysql_bind *param);
  void (*fetch_result)(struct st_mysql_bind *, MYSQL_FIELD *,
                       unsigned char **row);
  void (*skip_result)(struct st_mysql_bind *, MYSQL_FIELD *,
          unsigned char **row);
  /* output buffer length, must be set when fetching str/binary */
  unsigned long  buffer_length;
  unsigned long  offset;           /* offset position for char/binary fetch */
  unsigned long  length_value;     /* Used if length is 0 */
  unsigned int   flags;            /* special flags, e.g. for dummy bind  */
  unsigned int   pack_length;      /* Internal length for packed data */
  enum enum_field_types buffer_type;  /* buffer type */
  my_bool        error_value;      /* used if error is 0 */
  my_bool        is_unsigned;      /* set if integer type is unsigned */
  my_bool        long_data_used;   /* If used with mysql_send_long_data */
  my_bool	       piece_data_used;	 /* If used with mysql_send_piece_data */
  my_bool        is_null_value;    /* Used if is_null is 0 */
  my_bool        is_handle_returning_into; /* is handling the result of returning into*/
  my_bool        no_need_to_parser_result;   /* skip to parse the column for this result */
  unsigned long  last_offset;
  void           *extension;
  MYSQL          *mysql;
  MA_MEM_ROOT    bind_alloc; /* use for complex type */
} MYSQL_BIND;

//add for oboracle complex type
#define COMPLEX_BIND_HEADER \
  enum enum_field_types buffer_type;\
  void *buffer; \
  my_bool is_null

#define COMPLEX_OBJECT_HEADER \
  COMPLEX_BIND_HEADER; \
  unsigned char *type_name; \
  unsigned char *owner_name; \
  unsigned long length

typedef struct st_mysql_complex_bind_header
{
  COMPLEX_BIND_HEADER;
} MYSQL_COMPLEX_BIND_HEADER;

typedef struct st_mysql_complex_bind_header MYSQL_COMPLEX_BIND_BASIC;

typedef struct st_mysql_complex_bind_string
{
  COMPLEX_BIND_HEADER;
  unsigned long length;
} MYSQL_COMPLEX_BIND_STRING;

typedef struct st_mysql_complex_bind_string MYSQL_COMPLEX_BIND_DECIMAL;

typedef struct st_mysql_complex_bind_object
{
  COMPLEX_OBJECT_HEADER;
} MYSQL_COMPLEX_BIND_OBJECT;

// add for support send pl array maxrarr_len
typedef struct st_mysql_complex_bind_plarray
{
  COMPLEX_OBJECT_HEADER;
  unsigned long maxrarr_len;
  enum enum_field_types elem_type;
} MYSQL_COMPLEX_BIND_PLARRAY;

typedef struct st_mysql_complex_bind_object MYSQL_COMPLEX_BIND_ARRAY;
//end add for oboracle complex type
#define MAX_OB_LOB_LOCATOR_HEADER_LENGTH 40

typedef struct ObLobLocator
{
  uint32_t magic_code_;
  uint32_t version_;
  int64_t snapshot_version_;
  uint64_t table_id_;
  uint32_t column_id_;
  uint16_t mode_;  // extend flag: inlin and other types
  uint16_t option_;  // storage option: compress/ encrypt / dedup
  uint32_t payload_offset_; // == rowid_size; payload = data_ + payload_offset_
  uint32_t payload_size_;
  char data_[1]; // rowid + varchar
} OB_LOB_LOCATOR;


typedef struct st_ObClientMemLobCommon
{
  uint32_t magic_; // Keep the old version consistent
  uint32_t version_ : 8;
  uint32_t type_ : 4;     // Persistent/TmpFull/TmpDelta
  uint32_t read_only_ : 1; // Whether to write
  uint32_t is_inrow_ : 1;
  uint32_t is_open_ : 1; // only persist lob could be open
  uint32_t is_simple : 1; 
  uint32_t has_extern : 1;
  uint32_t reserved_ : 15;
}ObClientMemLobCommon;
typedef struct st_ObClientMemLobExternHeader
{
  int64_t snapshot_ver_;
  uint64_t table_id_;
  uint32_t column_idx_;
  uint16_t has_tx_info : 1;
  uint16_t has_cid_hash : 1; 
  uint16_t has_view_info : 1; 
  uint16_t extern_flags_ : 13;
  uint16_t rowkey_size_; 
  uint32_t payload_offset_;
  uint32_t payload_size_;
}ObClientMemLobExternHeader;
typedef struct st_ObClientLobCommon
{
  uint32_t version_ : 8;
  uint32_t is_init_ : 1;
  uint32_t is_empty_ : 1;
  uint32_t in_row_ : 1;
  uint32_t opt_encrypt_ : 1;
  uint32_t opt_compress_ : 1;
  uint32_t opt_deduplicate_ : 1;
  uint32_t has_content_type_ : 1;
  uint32_t use_big_endian_ : 1;
  uint32_t is_mem_loc_ : 1;
  uint32_t reserve_ : 15;
}ObClientLobCommon;
typedef struct st_ObClientLobData
{
  uint64_t tablet_id_;
  uint64_t lob_id_;
  uint64_t byte_size_;
}ObClientLobData;
typedef struct ObLobLocatorV2
{
  ObClientMemLobCommon common;
  ObClientMemLobExternHeader extern_header;
  char data_[1];
}OB_LOB_LOCATOR_V2;
uint8_t get_ob_lob_locator_version(void *lob);
int64_t get_ob_lob_payload_data_len(void *lob);
int stmt_get_data_from_lobv2(MYSQL *mysql, void * lob, enum_field_types dty, 
  int64_t char_offset, int64_t byte_offset, int64_t char_len, int64_t byte_len, char *buf, const int64_t buf_len, int64_t *data_len, int64_t *act_len);



typedef struct st_mysqlnd_upsert_result
{
  unsigned int  warning_count;
  unsigned int  server_status;
  unsigned long long affected_rows;
  unsigned long long last_insert_id;
} mysql_upsert_status;

typedef struct st_mysql_cmd_buffer
{
  unsigned char   *buffer;
  size_t     length;
} MYSQL_CMD_BUFFER;

typedef struct st_mysql_error_info
{
  unsigned int error_no;
  char error[MYSQL_ERRMSG_SIZE+1];
  char sqlstate[SQLSTATE_LENGTH + 1];
} mysql_error_info;


struct st_mysqlnd_stmt_methods
{
  my_bool (*prepare)(const MYSQL_STMT * stmt, const char * const query, size_t query_len);
  my_bool (*execute)(const MYSQL_STMT * stmt);
  MYSQL_RES * (*use_result)(const MYSQL_STMT * stmt);
  MYSQL_RES * (*store_result)(const MYSQL_STMT * stmt);
  MYSQL_RES * (*get_result)(const MYSQL_STMT * stmt);
  my_bool (*free_result)(const MYSQL_STMT * stmt);
  my_bool (*seek_data)(const MYSQL_STMT * stmt, unsigned long long row);
  my_bool (*reset)(const MYSQL_STMT * stmt);
  my_bool (*close)(const MYSQL_STMT * stmt); /* private */
  my_bool (*dtor)(const MYSQL_STMT * stmt); /* use this for mysqlnd_stmt_close */

  my_bool (*fetch)(const MYSQL_STMT * stmt, my_bool * const fetched_anything);

  my_bool (*bind_param)(const MYSQL_STMT * stmt, const MYSQL_BIND bind);
  my_bool (*refresh_bind_param)(const MYSQL_STMT * stmt);
  my_bool (*bind_result)(const MYSQL_STMT * stmt, const MYSQL_BIND *bind);
  my_bool (*send_long_data)(const MYSQL_STMT * stmt, unsigned int param_num,
                            const char * const data, size_t length);
  MYSQL_RES *(*get_parameter_metadata)(const MYSQL_STMT * stmt);
  MYSQL_RES *(*get_result_metadata)(const MYSQL_STMT * stmt);
  unsigned long long (*get_last_insert_id)(const MYSQL_STMT * stmt);
  unsigned long long (*get_affected_rows)(const MYSQL_STMT * stmt);
  unsigned long long (*get_num_rows)(const MYSQL_STMT * stmt);

  unsigned int (*get_param_count)(const MYSQL_STMT * stmt);
  unsigned int (*get_field_count)(const MYSQL_STMT * stmt);
  unsigned int (*get_warning_count)(const MYSQL_STMT * stmt);

  unsigned int (*get_error_no)(const MYSQL_STMT * stmt);
  const char * (*get_error_str)(const MYSQL_STMT * stmt);
  const char * (*get_sqlstate)(const MYSQL_STMT * stmt);

  my_bool (*get_attribute)(const MYSQL_STMT * stmt, enum enum_stmt_attr_type attr_type, const void * value);
  my_bool (*set_attribute)(const MYSQL_STMT * stmt, enum enum_stmt_attr_type attr_type, const void * value);
  void (*set_error)(MYSQL_STMT *stmt, unsigned int error_nr, const char *sqlstate, const char *format, ...);
};

typedef int  (*mysql_stmt_fetch_row_func)(MYSQL_STMT *stmt, unsigned char **row);
typedef void (*ps_result_callback)(void *data, unsigned int column, unsigned char **row);
typedef my_bool *(*ps_param_callback)(void *data, MYSQL_BIND *bind, unsigned int row_nr);

struct st_mysql_stmt
{
  MA_MEM_ROOT              mem_root;
  MYSQL                    *mysql;
  unsigned long            stmt_id;
  unsigned long            flags;/* cursor is set here */
  enum_mysqlnd_stmt_state  state;
  MYSQL_FIELD              *fields;
  unsigned int             field_count;
  unsigned int             param_count;
  unsigned char            send_types_to_server;
  MYSQL_BIND               *params;
  MYSQL_BIND               *bind;
  MYSQL_FIELD              *param_fields;        /* result set metadata */
  MYSQL_DATA               result;  /* we don't use mysqlnd's result set logic */
  MYSQL_ROWS               *result_cursor;
  my_bool                  bind_result_done;
  my_bool                  bind_param_done;

  mysql_upsert_status      upsert_status;

  unsigned int last_errno;
  char last_error[MYSQL_ERRMSG_SIZE+1];
  char sqlstate[SQLSTATE_LENGTH + 1];

  my_bool                  update_max_length;
  unsigned long            prefetch_rows;
  LIST                     list;

  my_bool                  cursor_exists;

  void                     *extension;
  mysql_stmt_fetch_row_func fetch_row_func;
  unsigned int             execute_count;/* count how many times the stmt was executed */
  mysql_stmt_use_or_store_func default_rset_handler;
  struct st_mysqlnd_stmt_methods  *m;
  unsigned int             array_size;
  size_t row_size;
  unsigned int prebind_params;
  void *user_data;
  ps_result_callback result_callback;
  ps_param_callback param_callback;
  /*add for support prepare_execute protocol*/
  unsigned int  bind_size;
  unsigned int  iteration_count;
  unsigned int  execute_mode;
  unsigned int  check_sum;
  my_bool       use_prepare_execute;
  /*
   * Added for RETURNING...INTO to handle the resultset 
   **/
  my_bool       is_handle_returning_into; /* is handling the result of returning into */
  my_bool       has_added_user_fields;    /* will be set if user's field info has added */
  my_bool       is_pl_out_resultset;      /* will be set for resultset of PL out parameters. OBServer will be set since 3.2.2 */
  /*end add for support prepare_execute protocol*/
  unsigned short orientation;  /* i.e. OCI_FETCH_ABSOLUTE - Fetches the row number (specified by fetch_offset parameter) in the result set using absolute positioning.*/
  int            fetch_offset; /* The offset to be used with the orientation parameter for changing the current row position */
  unsigned long  ext_flag; /* prepare_execute extend flag which need to be send to OBServer  */
  MA_MEM_ROOT       param_fields_mem_root;       /* param root allocations */
};
/*add for support send PLArray maxrarr_len*/
enum enum_mysql_send_plarray_maxrarrlen_flag
{
  SEND_PLARRAY_MAXRARRLEN_FORCE_CLOSE = 0,
  SEND_PLARRAY_MAXRARRLEN_AUTO_OPEN,
  SEND_PLARRAY_MAXRARRLEN_FORCE_OPEN,
  SEND_PLARRAY_MAXRARRLEN_FLAG_MAX,
};
my_bool determine_send_plarray_maxrarr_len(MYSQL *mysql);
my_bool get_support_send_plarray_maxrarr_len(MYSQL *mysql);
/*end for support send PLArray maxrarr_len*/

/*add for support plarray bindbyname */
enum enum_mysql_plarray_bindbyname
{
  PLARRAY_BINDBYNAME_FORCE_CLOSE = 0,
  PLARRAY_BINDBYNAME_AUTO_OPEN,
  PLARRAY_BINDBYNAME_FORCE_OPEN,
  PLARRAY_BINDBYNAME_FLAG_MAX,
};
my_bool determine_plarray_bindbyname(MYSQL *mysql);
my_bool get_support_plarray_bindbyname(MYSQL *mysql);
/*end for support plarray bindbyname */

/*add for support protocol ob20*/
enum enum_ob20_protocol
{
  PROTOCOL_OB20_FORCE_CLOSE = 0,
  PROTOCOL_OB20_AUTO_OPEN,
  PROTOCOL_OB20_FORCE_OPEN,
  PROTOCOL_OB20_FLAG_MAX
};
my_bool determine_protocol_ob20(MYSQL *mysql);
my_bool get_use_protocol_ob20(MYSQL *mysql);

enum enum_full_link_trace
{
  PROTOCOL_FLT_FORCE_CLOSE = 0,
  PROTOCOL_FLT_AUTO_OPEN,
  PROTOCOL_FLT_FORCE_OPEN,
  PROTOCOL_FLT_FLAG_MAX
};
my_bool determine_full_link_trace(MYSQL *mysql);
my_bool get_use_full_link_trace(MYSQL *mysql);

enum enum_flt_show_trace
{
  FLT_SHOW_TRACE_FORCE_CLOSE = 0,
  FLT_SHOW_TRACE_AUTO_OPEN,
  FLT_SHOW_TRACE_FORCE_OPEN,
  FLT_SHOW_TRACE_FLAG_MAX
};
my_bool determine_flt_show_trace(MYSQL *mysql);
my_bool get_use_flt_show_trace(MYSQL *mysql);

uint32_t ob_crc32(uint64_t crc, const char *buf, int64_t len);
uint64_t ob_crc64(uint64_t crc, const char *buf, int64_t len);
/*end for support protocol ob20*/

enum enum_ob_client_lob_locatorv2
{
  OB_CLIENT_LOB_LOCATORV2_FORCE_CLOSE = 0,
  OB_CLIENT_LOB_LOCATORV2_AUTO_OPEN,
  OB_CLIENT_LOB_LOCATORV2_FORCE_OPEN,
  OB_CLIENT_LOB_LOCATORV2_FLAY_MAX
};
my_bool determine_ob_client_lob_locatorv2(MYSQL *mysql);
my_bool get_use_ob_client_lob_locatorv2(MYSQL *mysql);

my_bool set_nls_format(MYSQL *mysql);

/* add for support bindbyname for plarray */
struct prepare_extend_args_t
{
  unsigned int params_count;  // pass params_ount from caller
};
typedef struct prepare_extend_args_t PREPARE_EXTEND_ARGS;
// todo: add a switch to accept args
/* end for support bindbyname for plarray */

enum enum_mysql_prepare_execute_flag
{
  PREPARE_EXECUTE_FORCE_CLOSE = 0,
  PREPARE_EXECUTE_AUTO_OPEN,
  PREPARE_EXECUTE_FORCE_OPEN,
  PREPARE_EXECUTE_FLAG_MAX
};
/*add for support prepare_execute protocol*/
my_bool determine_use_prepare_execute(MYSQL *mysql);
my_bool get_support_send_fetch_flag(MYSQL *mysql);
my_bool get_use_prepare_execute(MYSQL* msyql);
my_bool get_use_preapre_execute(MYSQL* msyql);
/*end add for support prepare_execute protocol*/
/*add for support new prepare_execute mode*/
//same value as OCI
#define EXECUTE_BATCH_MODE             0x00000001 /* batch the oci stmt for exec */
#define EXECUTE_EXACT_FETCH            0x00000002  /* fetch exact rows specified */
/* #define                         0x00000004                      available */
#define EXECUTE_STMT_SCROLLABLE_READONLY \
                                   0x00000008 /* if result set is scrollable */
#define EXECUTE_DESCRIBE_ONLY          0x00000010 /* only describe the statement */
#define EXECUTE_COMMIT_ON_SUCCESS      0x00000020  /* commit, if successful exec */
#define EXECUTE_NON_BLOCKING           0x00000040                /* non-blocking */
#define EXECUTE_BATCH_ERRORS           0x00000080  /* batch errors in array dmls */
#define EXECUTE_PARSE_ONLY             0x00000100    /* only parse the statement */
#define EXECUTE_EXACT_FETCH_RESERVED_1 0x00000200                    /* reserved */
/*add for support new prepare_execute mode*/

/*add for support fetch flag*/
#define FETCH_RETURN_EXTRA_OK   0x00000001L
#define FETCH_HAS_PIECE_COLUMN  0x00000002L
//same as oci fetch orientation
#define CURSOR_FETCH_DEFAULT    0x00000000
#define CURSOR_FETCH_CURRENT    0x00000001      /* refetching current position  */
#define CURSOR_FETCH_NEXT       0x00000002                          /* next row */
#define CURSOR_FETCH_FIRST      0x00000004       /* first row of the result set */
#define CURSOR_FETCH_LAST       0x00000008    /* the last row of the result set */
#define CURSOR_FETCH_PRIOR      0x00000010  /* previous row relative to current */
#define CURSOR_FETCH_ABSOLUTE   0x00000020        /* absolute offset from first */
#define CURSOR__FETCH_RELATIVE   0x00000040        /* offset relative to current */
#define CURSOR__FETCH_RESERVED_1 0x00000080                          /* reserved */
#define CURSOR__FETCH_RESERVED_2 0x00000100                          /* reserved */
#define CURSOR__FETCH_RESERVED_3 0x00000200                          /* reserved */
#define CURSOR__FETCH_RESERVED_4 0x00000400                          /* reserved */
#define CURSOR__FETCH_RESERVED_5 0x00000800                          /* reserved */
#define CURSOR__FETCH_RESERVED_6 0x00001000                          /* reserved */
/*end add for support fetch flag*/

#define NEED_DATA_AT_EXEC_FLAG   0x00000001

typedef void (*ps_field_fetch_func)(MYSQL_BIND *r_param, const MYSQL_FIELD * field, unsigned char **row);
typedef struct st_mysql_perm_bind {
  ps_field_fetch_func func;
  /* should be signed int */
  int pack_len;
  unsigned long max_len;
} MYSQL_PS_CONVERSION;

extern MYSQL_PS_CONVERSION mysql_ps_fetch_functions[MYSQL_TYPE_GEOMETRY + 2];
unsigned long ma_net_safe_read(MYSQL *mysql);
void mysql_init_ps_subsystem(void);
unsigned long net_field_length(unsigned char **packet);
int ma_simple_command(MYSQL *mysql,enum enum_server_command command, const char *arg,
          	       size_t length, my_bool skipp_check, void *opt_arg);
/*
 *  function prototypes
 */
MYSQL_STMT * STDCALL mysql_stmt_init(MYSQL *mysql);
int STDCALL mysql_stmt_prepare(MYSQL_STMT *stmt, const char *query, unsigned long length);
int STDCALL mysql_stmt_execute(MYSQL_STMT *stmt);
int STDCALL mysql_stmt_prepare_v2(MYSQL_STMT *stmt, const char *query, unsigned long length, void* extend_arg);
int STDCALL mysql_stmt_execute_v2(MYSQL_STMT *stmt, const char *query, unsigned long length, unsigned int iteration_count, int execute_mode, void* extend_arg);
int STDCALL mysql_stmt_fetch(MYSQL_STMT *stmt);
int STDCALL mysql_stmt_fetch_column(MYSQL_STMT *stmt, MYSQL_BIND *bind_arg, unsigned int column, unsigned long offset);
int STDCALL mysql_stmt_store_result(MYSQL_STMT *stmt);

my_bool STDCALL mysql_stmt_send_piece_data(MYSQL_STMT *stmt, unsigned int param_number,
                                           const char *data, unsigned long length,
                                           char piece_type, char is_null);
my_bool STDCALL mysql_stmt_read_piece_data(MYSQL_STMT *stmt, unsigned int param_number,
                                           unsigned short orientation, int scroll_offset,
                                           unsigned long data_len, unsigned char *piece_type, unsigned long *ret_data_len);
/* 
 * add for RETURNING INTO resultset's flag
 **/
my_bool STDCALL is_returning_result(MYSQL_STMT *stmt);
my_bool STDCALL has_added_user_fields(MYSQL_STMT *stmt);
/* add pl out resultset flag, observer since 3.2.2 */
my_bool STDCALL is_pl_out_result(MYSQL_STMT *stmt);

unsigned long STDCALL stmt_pre_exe_req_ext_flag_get(MYSQL_STMT *stmt);
void STDCALL stmt_pre_exe_req_ext_flag_set(MYSQL_STMT *stmt, unsigned long flag);

/*
 * add oracle_mode fetch method
 */
int STDCALL mysql_stmt_fetch_oracle_cursor(MYSQL_STMT *stmt);
int STDCALL mysql_stmt_fetch_oracle_implicit_cursor(MYSQL_STMT *stmt, my_bool is_need_fetch_from_server);
int STDCALL mysql_stmt_fetch_oracle_buffered_result(MYSQL_STMT *stmt);
/*
 * end add oracle_mode fetch method
 */
unsigned long STDCALL mysql_stmt_param_count(MYSQL_STMT * stmt);
my_bool STDCALL mysql_stmt_attr_set(MYSQL_STMT *stmt, enum enum_stmt_attr_type attr_type, const void *attr);
my_bool STDCALL mysql_stmt_attr_get(MYSQL_STMT *stmt, enum enum_stmt_attr_type attr_type, void *attr);
my_bool STDCALL mysql_stmt_bind_param(MYSQL_STMT * stmt, MYSQL_BIND * bnd);
my_bool STDCALL mysql_stmt_bind_result(MYSQL_STMT * stmt, MYSQL_BIND * bnd);
my_bool STDCALL mysql_stmt_close(MYSQL_STMT * stmt);
my_bool STDCALL mysql_stmt_reset(MYSQL_STMT * stmt);
my_bool STDCALL mysql_stmt_free_result(MYSQL_STMT *stmt);
my_bool STDCALL mysql_stmt_send_long_data(MYSQL_STMT *stmt, unsigned int param_number, const char *data, unsigned long length);
MYSQL_RES *STDCALL mysql_stmt_result_metadata(MYSQL_STMT *stmt);
MYSQL_RES *STDCALL mysql_stmt_param_metadata(MYSQL_STMT *stmt);
unsigned int STDCALL mysql_stmt_errno(MYSQL_STMT * stmt);
const char *STDCALL mysql_stmt_error(MYSQL_STMT * stmt);
const char *STDCALL mysql_stmt_sqlstate(MYSQL_STMT * stmt);
MYSQL_ROW_OFFSET STDCALL mysql_stmt_row_seek(MYSQL_STMT *stmt, MYSQL_ROW_OFFSET offset);
MYSQL_ROW_OFFSET STDCALL mysql_stmt_row_tell(MYSQL_STMT *stmt);
void STDCALL mysql_stmt_data_seek(MYSQL_STMT *stmt, unsigned long long offset);
unsigned long long STDCALL mysql_stmt_num_rows(MYSQL_STMT *stmt);
unsigned long long STDCALL mysql_stmt_affected_rows(MYSQL_STMT *stmt);
unsigned long long STDCALL mysql_stmt_insert_id(MYSQL_STMT *stmt);
unsigned int STDCALL mysql_stmt_field_count(MYSQL_STMT *stmt);
int STDCALL mysql_stmt_next_result(MYSQL_STMT *stmt);
my_bool STDCALL mysql_stmt_more_results(MYSQL_STMT *stmt);
int STDCALL mariadb_stmt_execute_direct(MYSQL_STMT *stmt, const char *stmt_str, size_t length);
MYSQL_FIELD * STDCALL mariadb_stmt_fetch_fields(MYSQL_STMT *stmt);
void end_server(MYSQL *mysql);
void free_old_query(MYSQL *mysql);
