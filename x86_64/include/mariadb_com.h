/************************************************************************************
   Copyright (C) 2000, 2012 MySQL AB & MySQL Finland AB & TCX DataKonsult AB,
                 Monty Program AB
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
   License along with this library; if not see <http://www.gnu.org/licenses>
   or write to the Free Software Foundation, Inc., 
   51 Franklin St., Fifth Floor, Boston, MA 02110, USA

   Part of this code includes code from the PHP project which
   is freely available from http://www.php.net
*************************************************************************************/

/*
** Common definition between mysql server & client
*/

#ifndef _mysql_com_h
#define _mysql_com_h

#define NAME_CHAR_LEN   64
#define NAME_LEN	256		/* Field/table name length */
#define HOSTNAME_LENGTH 60
#define SYSTEM_MB_MAX_CHAR_LENGTH 4
#define USERNAME_CHAR_LENGTH 128
#define USERNAME_LENGTH (USERNAME_CHAR_LENGTH * SYSTEM_MB_MAX_CHAR_LENGTH)
#define SERVER_VERSION_LENGTH 60
#define SQLSTATE_LENGTH 5
#define SCRAMBLE_LENGTH 20
#define SCRAMBLE_LENGTH_323 8

#define LOCAL_HOST	"localhost"
#define LOCAL_HOST_NAMEDPIPE "."

#if defined(_WIN32) && !defined( _CUSTOMCONFIG_)
#define MARIADB_NAMEDPIPE "MySQL"
#define MYSQL_SERVICENAME "MySql"
#endif /* _WIN32 */

/* for use in mysql client tools only */
#define MYSQL_AUTODETECT_CHARSET_NAME "auto"
#define BINCMP_FLAG       131072

enum mysql_enum_shutdown_level
{
  SHUTDOWN_DEFAULT = 0,
  KILL_QUERY= 254,
  KILL_CONNECTION= 255
};

enum enum_server_command
{
  COM_SLEEP = 0,
  COM_QUIT,
  COM_INIT_DB,
  COM_QUERY,
  COM_FIELD_LIST,
  COM_CREATE_DB,
  COM_DROP_DB,
  COM_REFRESH,
  COM_SHUTDOWN,
  COM_STATISTICS,
  COM_PROCESS_INFO,
  COM_CONNECT,
  COM_PROCESS_KILL,
  COM_DEBUG,
  COM_PING,
  COM_TIME = 15,
  COM_DELAYED_INSERT,
  COM_CHANGE_USER,
  COM_BINLOG_DUMP,
  COM_TABLE_DUMP,
  COM_CONNECT_OUT = 20,
  COM_REGISTER_SLAVE,
  COM_STMT_PREPARE = 22,
  COM_STMT_EXECUTE = 23,
  COM_STMT_SEND_LONG_DATA = 24,
  COM_STMT_CLOSE = 25,
  COM_STMT_RESET = 26,
  COM_SET_OPTION = 27,
  COM_STMT_FETCH = 28,
  COM_DAEMON= 29,
  COM_UNSUPPORTED= 30,
  COM_RESET_CONNECTION = 31,
  COM_STMT_PREPARE_EXECUTE = 0xa1,
  COM_STMT_SEND_PIECE_DATA = 0xa2,
  COM_STMT_GET_PIECE_DATA = 0xa3,
  COM_STMT_BULK_EXECUTE = 250,
  COM_RESERVED_1 = 254, /* former COM_MULTI, now removed */
  COM_END
};

enum enum_nls_time_type
{
  NLS_DATE_FORMAT,
  NLS_TIMESTAMP_FORMAT,
  NLS_TIMESTAMP_TZ_FORMAT,
  NLS_TIME_MAX
};

enum enum_prepare_execute_extend_flag
{
  PRE_EXE_EXTEND_FLAG_RETURNING= 1,
  PRE_EXE_EXTEND_FLAG_ADD_USER_FIELD= 1<<1,
  PRE_EXE_EXTEND_FLAG_PLOUT= 1<<2,
  PRE_EXE_EXTEND_END=1<<31
};

#define NOT_NULL_FLAG	1		/* Field can't be NULL */
#define PRI_KEY_FLAG	2		/* Field is part of a primary key */
#define UNIQUE_KEY_FLAG 4		/* Field is part of a unique key */
#define MULTIPLE_KEY_FLAG 8		/* Field is part of a key */
#define BLOB_FLAG	16		/* Field is a blob */
#define UNSIGNED_FLAG	32		/* Field is unsigned */
#define ZEROFILL_FLAG	64		/* Field is zerofill */
#define BINARY_FLAG	128
/* The following are only sent to new clients */
#define ENUM_FLAG	256		/* field is an enum */
#define AUTO_INCREMENT_FLAG 512		/* field is a autoincrement field */
#define TIMESTAMP_FLAG	1024		/* Field is a timestamp */
#define SET_FLAG	2048		/* field is a set */
/* new since 3.23.58 */
#define NO_DEFAULT_VALUE_FLAG 4096	/* Field doesn't have default value */
#define ON_UPDATE_NOW_FLAG 8192         /* Field is set to NOW on UPDATE */
/* end new */
#define NUM_FLAG	32768		/* Field is num (for clients) */
#define PART_KEY_FLAG	16384		/* Intern; Part of some key */
#define GROUP_FLAG	32768		/* Intern: Group field */
#define UNIQUE_FLAG	65536		/* Intern: Used by sql_yacc */

#define REFRESH_GRANT		1	/* Refresh grant tables */
#define REFRESH_LOG		2	/* Start on new log file */
#define REFRESH_TABLES		4	/* close all tables */
#define REFRESH_HOSTS		8	/* Flush host cache */
#define REFRESH_STATUS		16	/* Flush status variables */
#define REFRESH_THREADS		32	/* Flush thread cache */
#define REFRESH_SLAVE           64      /* Reset master info and restart slave
					   thread */
#define REFRESH_MASTER          128     /* Remove all bin logs in the index
					   and truncate the index */

/* The following can't be set with mysql_refresh() */
#define REFRESH_READ_LOCK	16384	/* Lock tables for read */
#define REFRESH_FAST		32768	/* Intern flag */

#define CLIENT_MYSQL          1
#define CLIENT_FOUND_ROWS	    2	/* Found instead of affected rows */
#define CLIENT_LONG_FLAG	    4	/* Get all column flags */
#define CLIENT_CONNECT_WITH_DB	    8	/* One can specify db on connect */
#define CLIENT_NO_SCHEMA	   16	/* Don't allow database.table.column */
#define CLIENT_COMPRESS		   32	/* Can use compression protocol */
#define CLIENT_ODBC		   64	/* Odbc client */
#define CLIENT_LOCAL_FILES	  128	/* Can use LOAD DATA LOCAL */
#define CLIENT_IGNORE_SPACE	  256	/* Ignore spaces before '(' */
#define CLIENT_INTERACTIVE	  1024	/* This is an interactive client */
#define CLIENT_SSL                2048     /* Switch to SSL after handshake */
#define CLIENT_IGNORE_SIGPIPE     4096     /* IGNORE sigpipes */
#define CLIENT_TRANSACTIONS	  8192	/* Client knows about transactions */
/* added in 4.x */
#define CLIENT_PROTOCOL_41         512
#define CLIENT_RESERVED          16384
#define CLIENT_SECURE_CONNECTION 32768  
#define CLIENT_MULTI_STATEMENTS  (1UL << 16)
#define CLIENT_MULTI_RESULTS     (1UL << 17)
#define CLIENT_PS_MULTI_RESULTS  (1UL << 18)
#define CLIENT_PLUGIN_AUTH       (1UL << 19)
#define CLIENT_CONNECT_ATTRS     (1UL << 20)
#define CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA (1UL << 21)
#define CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS (1UL << 22)
#define CLIENT_SESSION_TRACKING  (1UL << 23)

/* Client no longer needs EOF packet */
#define CLIENT_DEPRECATE_EOF (1UL << 24)

#define CLIENT_SUPPORT_ORACLE_MODE (1UL << 27)
#define CLIENT_RETURN_HIDDEN_ROWID (1UL << 28)

#define CLIENT_USE_LOB_LOCATOR (1UL << 29)
// #define CLIENT_PROGRESS          (1UL << 29) /* client supports progress indicator */
#define CLIENT_PROGRESS          (1UL << 32) /* client supports progress indicator */
#define CLIENT_PROGRESS_OBSOLETE  CLIENT_PROGRESS 
#define CLIENT_SSL_VERIFY_SERVER_CERT (1UL << 30)
#define CLIENT_REMEMBER_OPTIONS  (1UL << 31)

/* MariaDB specific capabilities */
#define MARIADB_CLIENT_FLAGS 0xFFFFFFFF00000000ULL
#define MARIADB_CLIENT_PROGRESS (1ULL << 32)
#define MARIADB_CLIENT_RESERVED_1 (1ULL << 33) /* Former COM_MULTI, don't use */
#define MARIADB_CLIENT_STMT_BULK_OPERATIONS (1ULL << 34)
/* support of extended data type/format information, since 10.5.0 */
#define MARIADB_CLIENT_EXTENDED_METADATA (1ULL << 35)

#define IS_MARIADB_EXTENDED_SERVER(mysql)\
        (!(mysql->server_capabilities & CLIENT_MYSQL))

#define MARIADB_CLIENT_SUPPORTED_FLAGS (MARIADB_CLIENT_PROGRESS |\
                                       MARIADB_CLIENT_STMT_BULK_OPERATIONS|\
                                       MARIADB_CLIENT_EXTENDED_METADATA)

#define CLIENT_SUPPORTED_FLAGS  (CLIENT_MYSQL |\
                                 CLIENT_FOUND_ROWS |\
                                 CLIENT_LONG_FLAG |\
                                 CLIENT_CONNECT_WITH_DB |\
                                 CLIENT_NO_SCHEMA |\
                                 CLIENT_COMPRESS |\
                                 CLIENT_ODBC |\
                                 CLIENT_LOCAL_FILES |\
                                 CLIENT_IGNORE_SPACE |\
                                 CLIENT_INTERACTIVE |\
                                 CLIENT_SSL |\
                                 CLIENT_IGNORE_SIGPIPE |\
                                 CLIENT_TRANSACTIONS |\
                                 CLIENT_PROTOCOL_41 |\
                                 CLIENT_RESERVED |\
                                 CLIENT_SECURE_CONNECTION |\
                                 CLIENT_MULTI_STATEMENTS |\
                                 CLIENT_MULTI_RESULTS |\
                                 CLIENT_PROGRESS |\
		                 CLIENT_SSL_VERIFY_SERVER_CERT |\
                                 CLIENT_REMEMBER_OPTIONS |\
                                 CLIENT_PLUGIN_AUTH |\
                                 CLIENT_SESSION_TRACKING |\
                                 CLIENT_CONNECT_ATTRS)
#define CLIENT_CAPABILITIES	    (CLIENT_MYSQL | \
                                 CLIENT_LONG_FLAG |\
                                 CLIENT_TRANSACTIONS |\
                                 CLIENT_SECURE_CONNECTION |\
                                 CLIENT_MULTI_RESULTS | \
                                 CLIENT_PS_MULTI_RESULTS |\
                                 CLIENT_PROTOCOL_41 |\
                                 CLIENT_PLUGIN_AUTH |\
                                 CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | \
                                 CLIENT_SESSION_TRACKING |\
                                 CLIENT_CONNECT_ATTRS)

#define CLIENT_DEFAULT_FLAGS ((CLIENT_SUPPORTED_FLAGS & ~CLIENT_COMPRESS)\
                                                      & ~CLIENT_SSL)

#define SERVER_STATUS_IN_TRANS               1	/* Transaction has started */
#define SERVER_STATUS_AUTOCOMMIT             2	/* Server in auto_commit mode */
#define SERVER_ORACLE_MODE         4    /* Server in oracle mode */
#define SERVER_MORE_RESULTS_EXIST            8
#define SERVER_QUERY_NO_GOOD_INDEX_USED     16
#define SERVER_QUERY_NO_INDEX_USED          32
#define SERVER_STATUS_CURSOR_EXISTS         64
#define SERVER_STATUS_LAST_ROW_SENT        128
#define SERVER_STATUS_DB_DROPPED           256 
#define SERVER_STATUS_NO_BACKSLASH_ESCAPES 512
#define SERVER_STATUS_METADATA_CHANGED    1024
#define SERVER_QUERY_WAS_SLOW             2048
#define SERVER_PS_OUT_PARAMS              4096
#define SERVER_STATUS_IN_TRANS_READONLY   8192
#define SERVER_SESSION_STATE_CHANGED     16384
#define SERVER_STATUS_ANSI_QUOTES        32768

#define MYSQL_ERRMSG_SIZE	512
#define NET_READ_TIMEOUT	30		/* Timeout on read */
#define NET_WRITE_TIMEOUT	60		/* Timeout on write */
#define NET_WAIT_TIMEOUT	(8*60*60)	/* Wait for new query */

/* for server integration (mysqlbinlog) */
#define LIST_PROCESS_HOST_LEN 64
#define MYSQL50_TABLE_NAME_PREFIX         "#mysql50#"
#define MYSQL50_TABLE_NAME_PREFIX_LENGTH  (sizeof(MYSQL50_TABLE_NAME_PREFIX)-1)
#define SAFE_NAME_LEN (NAME_LEN + MYSQL50_TABLE_NAME_PREFIX_LENGTH)

struct st_ma_pvio;
typedef struct st_ma_pvio MARIADB_PVIO;

#ifndef MAX_CHAR_WIDTH
#define MAX_CHAR_WIDTH		255	/* Max length for a CHAR column */
#endif

#ifndef MAX_BLOB_WIDTH
#define MAX_BLOB_WIDTH		8192	/* Default width for blob */
#endif

/* the following defines were added for PHP's mysqli and pdo extensions: 
   see: CONC-56
*/
#define MAX_TINYINT_WIDTH     3
#define MAX_SMALLINT_WIDTH    5
#define MAX_MEDIUMINT_WIDTH   8
#define MAX_INT_WIDTH        10
#define MAX_BIGINT_WIDTH     20

struct st_ma_connection_plugin;
struct st_ob20protocol;

typedef struct st_net {
  MARIADB_PVIO *pvio;
  unsigned char *buff;
  unsigned char *buff_end,*write_pos,*read_pos;
  my_socket fd;					/* For Perl DBI/dbd */
  unsigned long remain_in_buf,length;
  unsigned long buf_length, where_b;
  unsigned long max_packet, max_packet_size;
  unsigned int pkt_nr, compress_pkt_nr;
  unsigned int write_timeout, read_timeout, retry_count;
  int fcntl;
  unsigned int *return_status;
  unsigned char reading_or_writing;
  char save_char;
  char unused_1;
  my_bool unused_2;
  my_bool compress;
  my_bool use_ob20protocol;
  void *unused_4;
  unsigned int last_errno;
  unsigned char error;
  my_bool unused_5;
  my_bool unused_6;
  char last_error[MYSQL_ERRMSG_SIZE];
  char sqlstate[SQLSTATE_LENGTH+1];
  struct st_mariadb_net_extension *extension;
  struct st_ob20protocol *ob20protocol;
} NET;

#define packet_error ((unsigned int) -1)

/* used by mysql_set_server_option */
enum enum_mysql_set_option
{
  MYSQL_OPTION_MULTI_STATEMENTS_ON,
  MYSQL_OPTION_MULTI_STATEMENTS_OFF
};

#define CURSOR_TYPE_ARRAY_BIND 8
#define CURSOR_TYPE_SAVE_EXCEPTION 16

enum enum_session_state_type
{
  SESSION_TRACK_SYSTEM_VARIABLES= 0,
  SESSION_TRACK_SCHEMA,
  SESSION_TRACK_STATE_CHANGE,
  /* currently not supported by MariaDB Server */
  SESSION_TRACK_GTIDS,
  SESSION_TRACK_TRANSACTION_CHARACTERISTICS,
  SESSION_TRACK_TRANSACTION_STATE /* make sure that SESSION_TRACK_END always points
                                    to last element of enum !! */
};

#define SESSION_TRACK_BEGIN 0
#define SESSION_TRACK_END SESSION_TRACK_TRANSACTION_STATE
#define SESSION_TRACK_TYPES (SESSION_TRACK_END + 1)

/* SESSION_TRACK_TRANSACTION_TYPE was renamed to SESSION_TRACK_TRANSACTION_STATE
   in 3e699a1738cdfb0a2c5b8eabfa8301b8d11cf711.
   This is a workaround to prevent breaking of travis and buildbot tests.
   TODO: Remove this after server fixes */
#define SESSION_TRACK_TRANSACTION_TYPE SESSION_TRACK_TRANSACTION_STATE

// for obproxy and observer compatibility
enum ObCapabilityFlagShift
{
  OBCLIENT_CAP_PARTITION_TABLE_SHIFT = 0,
  OBCLIENT_CAP_CHANGE_USER_SHIFT,
  OBCLIENT_CAP_READ_WEAK_SHIFT,
  OBCLIENT_CAP_CHECKSUM_SHIFT,
  OBCLIENT_CAP_SAFE_WEAK_READ_SHIFT,
  OBCLIENT_CAP_PRIORITY_HIT_SHIFT,
  OBCLIENT_CAP_CHECKSUM_SWITCH_SHIFT,
  OBCLIENT_CAP_OCJ_ENABLE_EXTRA_OK_PACKET_SHIFT,
  OBCLIENT_CAP_OB_PROTOCOL_V2_SHIFT,
  OBCLIENT_CAP_EXTRA_OK_PACKET_FOR_STATISTICS_SHIFT,
  OBCLIENT_CAP_ABUNDANT_FEEDBACK,
  OBCLIENT_CAP_PL_ROUTE_SHIFT,
  OBCLIENT_CAP_PROXY_REROUTE_SHIFT,
  OBCLIENT_CAP_PROXY_SESSION_SYNC_SHIFT,
  OBCLIENT_CAP_FULL_LINK_TRACE_SHIFT,
  OBCLIENT_CAP_PROXY_NEW_EXTRA_INFO_SHIFT,
  OBCLIENT_CAP_PROXY_SESSION_VAR_SYNC_SHIFT,
  OBCLIENT_CAP_PROXY_WEAK_STALE_FEEDBACK_SHIFT,
  OBCLIENT_CAP_PROXY_FULL_LINK_TRACE_SHOW_TRACE_SHIFT
};

#define OBCLIENT_TEST_CAPABILITY(cap, tg_cap) (((cap) & (tg_cap)) == (tg_cap))
#define OBCLIENT_CAP_GET_TYPE(i)        (1LL << i)
#define OBCLIENT_CAP_PARTITION_TABLE                  OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_PARTITION_TABLE_SHIFT)
#define OBCLIENT_CAP_CHANGE_USER                      OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_CHANGE_USER_SHIFT)
#define OBCLIENT_CAP_READ_WEAK                        OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_READ_WEAK_SHIFT)
#define OBCLIENT_CAP_CHECKSUM                         OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_CHECKSUM_SHIFT)
#define OBCLIENT_CAP_SAFE_WEAK_READ                   OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_SAFE_WEAK_READ_SHIFT)
#define OBCLIENT_CAP_PRIORITY_HIT                     OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_PRIORITY_HIT_SHIFT)
#define OBCLIENT_CAP_CHECKSUM_SWITCH                  OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_CHECKSUM_SWITCH_SHIFT)
#define OBCLIENT_CAP_OB_PROTOCOL_V2                   OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_OB_PROTOCOL_V2_SHIFT)
#define OBCLIENT_CAP_EXTRA_OK_PACKET_FOR_STATISTICS   OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_EXTRA_OK_PACKET_FOR_STATISTICS_SHIFT)
#define OBCLIENT_CAP_PL_ROUTE                         OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_PL_ROUTE_SHIFT)
#define OBCLIENT_CAP_PROXY_REROUTE                    OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_PROXY_REROUTE_SHIFT)
#define OBCLIENT_CAP_PROXY_SESSION_SYNC               OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_PROXY_SESSION_SYNC_SHIFT)
#define OBCLIENT_CAP_FULL_LINK_TRACE                  OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_FULL_LINK_TRACE_SHIFT)
#define OBCLIENT_CAP_PROXY_NEW_EXTRA_INFO             OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_PROXY_NEW_EXTRA_INFO_SHIFT)
#define OBCLIENT_CAP_PROXY_SESSION_VAR_SYNC           OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_PROXY_SESSION_VAR_SYNC_SHIFT)
#define OBCLIENT_CAP_PROXY_WEAK_STALE_FEEDBACK        OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_PROXY_WEAK_STALE_FEEDBACK_SHIFT)
#define OBCLIENT_CAP_PROXY_FULL_LINK_TRACE_SHOW_TRACE OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_PROXY_FULL_LINK_TRACE_SHOW_TRACE_SHIFT)

static const unsigned long OBPROXY_DEFAULT_CAPABILITY_FLAG =
    (OBCLIENT_CAP_OB_PROTOCOL_V2
     | OBCLIENT_CAP_FULL_LINK_TRACE
     | OBCLIENT_CAP_PROXY_NEW_EXTRA_INFO
     | OBCLIENT_CAP_PROXY_FULL_LINK_TRACE_SHOW_TRACE);

static const long OB_MAX_UINT32_BUF_LEN = 11; // string length of max uint32_t(2**32 - 1)
static const long OB_MAX_UINT64_BUF_LEN = 22; // string length of max uint64_t(2**64 - 1)
static const long OB_MAX_VERSION_BUF_LEN = 22; // string length of (xxx.xxx.xxx.xxx.xxx)
static const long OB_MAX_IP_BUF_LEN = 20; // string length of (xxx.xxx.xxx.xxx.xxx)

static const char *const OB_MYSQL_CAPABILITY_FLAG = "__proxy_capability_flag";
static const char *const OB_MYSQL_CLIENT_MODE = "__mysql_client_type";
static const char *const OB_MYSQL_CLIENT_OBPROXY_MODE = "__ob_proxy";
static const char *const OB_MYSQL_CONNECTION_ID = "__connection_id";
static const char *const OB_MYSQL_PROXY_CONNECTION_ID = "__proxy_connection_id";
static const char *const OB_MYSQL_GLOBAL_VARS_VERSION = "__global_vars_version";


enum ObClientLobLocatorVersion
{
  OBCLIENT_LOB_LOCATORV1 = 1,
  OBCLIENT_LOB_LOCATORV2 = 2,
  OBCLIENT_LOB_LOCATOR_ERROR
};
enum ObCapabilityFlagLob
{
  OBCLIENT_CAP_OB_LOB_LOCATOR_V2 = 0
};
#define OBCLIENT_CAP_OB_LOB_LOCATOR_V2 OBCLIENT_CAP_GET_TYPE(OBCLIENT_CAP_OB_LOB_LOCATOR_V2)
static const unsigned long OBCLIENT_CAP_OB_LOB_LOCATOR_V2_FLAG = OBCLIENT_CAP_OB_LOB_LOCATOR_V2;
static const char *const OB_MYSQL_LOB_LOCATOR_V2 = "__ob_client_attribute_capability_flag";


typedef enum enum_field_types { MYSQL_TYPE_DECIMAL, MYSQL_TYPE_TINY,
                        MYSQL_TYPE_SHORT,  MYSQL_TYPE_LONG,
                        MYSQL_TYPE_FLOAT,  MYSQL_TYPE_DOUBLE,
                        MYSQL_TYPE_NULL,   MYSQL_TYPE_TIMESTAMP,
                        MYSQL_TYPE_LONGLONG,MYSQL_TYPE_INT24,
                        MYSQL_TYPE_DATE,   MYSQL_TYPE_TIME,
                        MYSQL_TYPE_DATETIME, MYSQL_TYPE_YEAR,
                        MYSQL_TYPE_NEWDATE, MYSQL_TYPE_VARCHAR,
                        MYSQL_TYPE_BIT,
                        /*
                          the following types are not used by client,
                          only for mysqlbinlog!!
                        */
                        MYSQL_TYPE_TIMESTAMP2,
                        MYSQL_TYPE_DATETIME2,
                        MYSQL_TYPE_TIME2,
                        /* --------------------------------------------- */
                        /*begin add for extend ob oracle type*/
                        MYSQL_TYPE_OBJECT = 160, //0xa0
                        MYSQL_TYPE_ARRAY = 161, //0xa1
                        MYSQL_TYPE_STRUCT = 162, //0xa2
                        MYSQL_TYPE_CURSOR = 163, //0xa3
                        MYSQL_TYPE_PLARRAY = 164, //0xa4
                        MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE = 200,
                        MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE = 201,
                        MYSQL_TYPE_OB_TIMESTAMP_NANO = 202,
                        MYSQL_TYPE_OB_RAW = 203,
                        MYSQL_TYPE_OB_INTERVAL_YM = 204,
                        MYSQL_TYPE_OB_INTERVAL_DS = 205,
                        MYSQL_TYPE_OB_NUMBER_FLOAT = 206,
                        MYSQL_TYPE_OB_NVARCHAR2 = 207,
                        MYSQL_TYPE_OB_NCHAR = 208,
                        MYSQL_TYPE_OB_UROWID = 209,
                        MYSQL_TYPE_ORA_BLOB=210,
                        MYSQL_TYPE_ORA_CLOB=211,
                        /*end add for extend ob oracle type*/
                        MYSQL_TYPE_JSON=245,
                        MYSQL_TYPE_NEWDECIMAL=246,
                        MYSQL_TYPE_ENUM=247,
                        MYSQL_TYPE_SET=248,
                        MYSQL_TYPE_TINY_BLOB=249,
                        MYSQL_TYPE_MEDIUM_BLOB=250,
                        MYSQL_TYPE_LONG_BLOB=251,
                        MYSQL_TYPE_BLOB=252,
                        MYSQL_TYPE_VAR_STRING=253,
                        MYSQL_TYPE_STRING=254,
                        MYSQL_TYPE_GEOMETRY=255,
                        MAX_NO_FIELD_TYPES } enum_field_types;

#define FIELD_TYPE_CHAR FIELD_TYPE_TINY		/* For compatibility */
#define FIELD_TYPE_INTERVAL FIELD_TYPE_ENUM	/* For compatibility */
#define FIELD_TYPE_DECIMAL MYSQL_TYPE_DECIMAL
#define FIELD_TYPE_NEWDECIMAL MYSQL_TYPE_NEWDECIMAL
#define FIELD_TYPE_TINY MYSQL_TYPE_TINY
#define FIELD_TYPE_SHORT MYSQL_TYPE_SHORT
#define FIELD_TYPE_LONG MYSQL_TYPE_LONG
#define FIELD_TYPE_FLOAT MYSQL_TYPE_FLOAT
#define FIELD_TYPE_DOUBLE MYSQL_TYPE_DOUBLE
#define FIELD_TYPE_NULL MYSQL_TYPE_NULL
#define FIELD_TYPE_TIMESTAMP MYSQL_TYPE_TIMESTAMP
#define FIELD_TYPE_LONGLONG MYSQL_TYPE_LONGLONG
#define FIELD_TYPE_INT24 MYSQL_TYPE_INT24
#define FIELD_TYPE_DATE MYSQL_TYPE_DATE
#define FIELD_TYPE_TIME MYSQL_TYPE_TIME
#define FIELD_TYPE_DATETIME MYSQL_TYPE_DATETIME
#define FIELD_TYPE_YEAR MYSQL_TYPE_YEAR
#define FIELD_TYPE_NEWDATE MYSQL_TYPE_NEWDATE
#define FIELD_TYPE_ENUM MYSQL_TYPE_ENUM
#define FIELD_TYPE_SET MYSQL_TYPE_SET
#define FIELD_TYPE_TINY_BLOB MYSQL_TYPE_TINY_BLOB
#define FIELD_TYPE_MEDIUM_BLOB MYSQL_TYPE_MEDIUM_BLOB
#define FIELD_TYPE_LONG_BLOB MYSQL_TYPE_LONG_BLOB
#define FIELD_TYPE_BLOB MYSQL_TYPE_BLOB
#define FIELD_TYPE_VAR_STRING MYSQL_TYPE_VAR_STRING
#define FIELD_TYPE_STRING MYSQL_TYPE_STRING
#define FIELD_TYPE_GEOMETRY MYSQL_TYPE_GEOMETRY
#define FIELD_TYPE_BIT MYSQL_TYPE_BIT

extern unsigned long max_allowed_packet;
extern unsigned long net_buffer_length;

#define net_new_transaction(net) ((net)->pkt_nr=0)

int	ma_net_init(NET *net, MARIADB_PVIO *pvio);
void	ma_net_end(NET *net);
void	ma_net_clear(NET *net);
int	ma_net_flush(NET *net);
int	ma_net_write(NET *net,const unsigned char *packet, size_t len);
int	ma_net_write_command(NET *net,unsigned char command,const char *packet,
			  size_t len, my_bool disable_flush);
int	ma_net_real_write(NET *net,const char *packet, size_t len);
extern unsigned long ma_net_read(NET *net);

struct rand_struct {
  unsigned long seed1,seed2,max_value;
  double max_value_dbl;
};

  /* The following is for user defined functions */

enum Item_result {STRING_RESULT,REAL_RESULT,INT_RESULT,ROW_RESULT,DECIMAL_RESULT};

typedef struct st_udf_args
{
  unsigned int arg_count;		/* Number of arguments */
  enum Item_result *arg_type;		/* Pointer to item_results */
  char **args;				/* Pointer to argument */
  unsigned long *lengths;		/* Length of string arguments */
  char *maybe_null;			/* Set to 1 for all maybe_null args */
} UDF_ARGS;

  /* This holds information about the result */

typedef struct st_udf_init
{
  my_bool maybe_null;			/* 1 if function can return NULL */
  unsigned int decimals;		/* for real functions */
  unsigned int max_length;		/* For string functions */
  char	  *ptr;				/* free pointer for function data */
  my_bool const_item;			/* 0 if result is independent of arguments */
} UDF_INIT;

/* Connection types */
#define MARIADB_CONNECTION_UNIXSOCKET   0
#define MARIADB_CONNECTION_TCP          1
#define MARIADB_CONNECTION_NAMEDPIPE    2
#define MARIADB_CONNECTION_SHAREDMEM    3

  /* Constants when using compression */
#define NET_HEADER_SIZE 4		/* standard header size */
#define COMP_HEADER_SIZE 3		/* compression header extra size */
#define PIECE_HEADER_SIZE 16  /* COM_STMT_SEND_PIECE_DATA header size */
#define OB20_HEADER_SIZE 24 /* ob 20 protocol header size */
#define OB20_TAILER_SIZE 4 /* ob 20 protocol tailer size */
#define OB20_PROTOCOL_MAGIC_NUM 0x20AB /* ob 20 protocol header magic num */
#define OB20_PROTOCOL_VERSION_VALUE 20 /* ob 20 protocol header version */
#define OB20_EXTRAINFO_LENGTH_SIZE 4 /* ob 20 protocol extra_info size */

  /* Prototypes to password functions */
#define native_password_plugin_name "mysql_native_password"
#define old_password_plugin_name    "mysql_old_password"

#ifdef __cplusplus
extern "C" {
#endif
  
char *ma_scramble_323(char *to,const char *message,const char *password);
void ma_scramble_41(const unsigned char *buffer, const char *scramble, const char *password);
void ma_hash_password(unsigned long *result, const char *password, size_t len);
void ma_make_scrambled_password(char *to,const char *password);

/* Some other useful functions */

void mariadb_load_defaults(const char *conf_file, const char **groups,
		   int *argc, char ***argv);
my_bool ma_thread_init(void);
void ma_thread_end(void);

#ifdef __cplusplus
}
#endif

#define NULL_LENGTH ((unsigned long) ~0) /* For net_store_length */

#endif
