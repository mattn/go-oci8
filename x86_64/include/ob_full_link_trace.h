/*
   Copyright (c) 2000, 2018, Oracle and/or its affiliates.
   Copyright (c) 2009, 2019, MariaDB Corporation.
   Copyright (c) 2021 OceanBase.
   
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA */

#ifndef _ob_full_link_trace_h
#define _ob_full_link_trace_h

#include <stdint.h>
#include "mysql.h"
#include "ma_list.h"
#include "ob_thread_key.h"

// #define DEBUG_OB20

#define UUID4_LEN 37
#define UUID4_SERIALIZE_LEN 16

#define FLT_DECLARE                                       \
  Ob20Protocol *ob20protocol = mysql->net.ob20protocol;   \
  FLTInfo *flt = NULL;                                    \
  ObSpanCtx *span;                                        \
  ObTrace *trace;                                         \
  char ip_buffer[50] = {0};                               \
  char host_buffer[50] = {0};                             \
  int port = 0

#define FLT_BEFORE_COMMAND(span_level, tag_level, tag_str)              \
  do                                                                    \
  {                                                                     \
    if (get_use_full_link_trace(mysql) && OB_NOT_NULL(ob20protocol)) {  \
      flt = ob20protocol->flt;                                          \
      if (OB_NOT_NULL(flt)) {                                           \
        if ((TRUE == flt->control_info_.flt_show_trace_enable_)         \
          || !(mysql->server_status & SERVER_STATUS_IN_TRANS)) {        \
          BEGIN_TRACE(flt);                                             \
        } else {                                                        \
          /* A reset is required before each begin in the current transaction to prevent insufficient span*/           \
          reset_span(flt->trace_);                                      \
        }                                                               \
        span = BEGIN_SPAN(flt, span_level);                             \
        SET_TAG(flt, tag_level, tag_str);                               \
        if (NULL != mysql && get_local_ip_port(mysql->net.fd, ip_buffer, 50, &port)) {     \
          snprintf(host_buffer, 50, "\"%s:%d\"", ip_buffer, port);      \
          SET_TAG(flt, FLT_TAG_CLIENT_HOST, host_buffer);               \
        }                                                               \
        if (flt_build_request(mysql, flt)) {                            \
          /* error , but do nothing */                                  \
        }                                                               \
      }                                                                 \
    }                                                                   \
  } while (0)

#define FLT_AFTER_COMMAND                                                       \
  do                                                                            \
  {                                                                             \
    ob20protocol = mysql->net.ob20protocol;                                     \
    if (get_use_full_link_trace(mysql) && OB_NOT_NULL(ob20protocol)) {          \
      flt = ob20protocol->flt;                                                  \
      trace = OBTRACE(flt);                                                     \
      if (NULL != trace) {                                                      \
        END_SPAN(flt, span);                                                    \
        if (TRUE == trace->force_print_) {                                      \
          FLUSH_TRACE(flt);                                                     \
        } else if (TRUE == trace->slow_query_print_) {                          \
          trace->slow_query_print_ = FALSE;   /* Slow query is set to FALSE for each request */ \
          flush_first_span(trace);                                              \
        }                                                                       \
        if ((TRUE == flt->control_info_.flt_show_trace_enable_)                 \
          || !(mysql->server_status & SERVER_STATUS_IN_TRANS)) {                \
          END_TRACE(flt);                                                       \
        }                                                                       \
      }                                                                         \
    }                                                                           \
  } while (0)

#define DEFINE_FLT_SERIALIZE_FUNC(type)                                                                       \
  int flt_serialize_##type(char *buf, const int64_t len, int64_t *pos, void *flt_info);                       \
  int flt_get_serialize_size_##type(int32_t *size, void *flt_info);                                           \
  int flt_deserialize_field_##type(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,                    \
                                          const char *buf, const int64_t len, int64_t *pos, void *flt_info)

#define FLT_SERIALIZE_FUNC(type)       \
  {                                    \
    flt_deserialize_field_##type,      \
    flt_serialize_##type,              \
    flt_get_serialize_size_##type      \
  }

#define DEFINE_TO_STRING_FUNC_FOR(type) \
  int tostring_##type(char *buf, const int64_t buf_len, int64_t *pos, type *src)

// Currently maintains a size of 5k for each trace, which is the size of all space allocated during the entire trace
#define OBTRACE_DEFAULT_BUFFER_SIZE (5 * (1L << 10))
// The current driver free span is 4
#define SPAN_CACHE_COUNT (1L << 2)
// The current driver free span is 4
#define TAG_CACHE_COUNT (1L << 4)
// Since there is currently at most one SPAN, the largest LOG buffer is 1k
#define MAX_TRACE_LOG_SIZE (1L << 10)
// The current maximum size of the full link after serialization is 2k
#define MAX_FLT_SERIALIZE_SIZE (1L << 11)
// span cache begin buffer
#define TAG_BUFFER_BEGIN ((2 * MAX_TRACE_LOG_SIZE) + MAX_FLT_SERIALIZE_SIZE)
#define SPAN_BUFFER_BEGIN ((2 * MAX_TRACE_LOG_SIZE) + MAX_FLT_SERIALIZE_SIZE + TAG_CACHE_COUNT * (sizeof(ObTagCtx)))
#define INIT_OFFSET (SPAN_BUFFER_BEGIN + SPAN_CACHE_COUNT * (sizeof(LIST) + sizeof(ObSpanCtx)))

#define OBTRACE(flt) get_trace_instance(flt)
#define BEGIN_TRACE(flt) (begin_trace(OBTRACE(flt)))
#define END_TRACE(flt)         \
  do                           \
  {                            \
    end_trace(OBTRACE(flt));   \
  } while (0)
#define BEGIN_SPAN(flt, span_type) BEGIN_CHILD_SPAN(flt, span_type)
#define BEGIN_CHILD_SPAN(flt, span_type) (begin_span(OBTRACE(flt), span_type, 1, FALSE))
#define BEGIN_FOLLOW_SPAN(flt, span_type) (begin_span(OBTRACE(flt), span_type, 1, TRUE))
#define END_SPAN(flt, span_id)             \
  do                                  \
  {                                   \
    ObTrace *trace = OBTRACE(flt);    \
    (end_span(trace, span_id));       \
  } while (0)
#define SET_TAG(flt, tag_type, value)                   \
  do                                                    \
  {                                                     \
    ObSpanCtx *span = OBTRACE(flt)->last_active_span_;  \
    if (OB_NOT_NULL(span)) {                            \
      append_tag(OBTRACE(flt), span, tag_type, value);  \
    }                                                   \
  } while (0)
#define SET_TRACE_LEVEL(trace, level) (trace->level_ = level)
#define SET_AUTO_FLUSH(trace, value) (trace->auto_flush_ = value)
#define FLUSH_TRACE(flt)       \
  do                           \
  {                            \
    flush_trace(OBTRACE(flt)); \
  } while (0)


typedef struct st_obtrace ObTrace;
/*
0 ～ 999 for obclient
1000～1999 for obproxy
2000～65535 for all
*/
typedef enum enum_fulllinktraceextrainfoid
{
  FLT_DRIVER_SPAN = 1,
  FLT_DRIVER_END = 1000,
  // APP_INFO
  FLT_CLIENT_IDENTIFIER = 2001,
  FLT_MODULE,
  FLT_ACTION,
  FLT_CLIENT_INFO,
  FLT_APPINFO_TYPE,
  // QUERY_INFO
  FLT_QUERY_START_TIMESTAMP = 2010,
  FLT_QUERY_END_TIMESTAMP,
  // CONTROL_INFO
  FLT_LEVEL = 2020,
  FLT_SAMPLE_PERCENTAGE,
  FLT_RECORD_POLICY,
  FLT_PRINT_SAMPLE_PCT,
  FLT_SLOW_QUERY_THRES,
  FLT_SHOW_TRACE_ENABLE,
  // SPAN_INFO
  FLT_TRACE_ENABLE = 2030,
  FLT_FORCE_PRINT,
  FLT_TRACE_ID,
  FLT_REF_TYPE,
  FLT_SPAN_ID,
  // FLT_SHOW_TRACE
  FLT_DRV_SHOW_TRACE_SPAN = 2050,
  FLT_PROXY_SHOW_TRACE_SPAN,
  FLT_EXTRA_INFO_ID_END
} FullLinkTraceExtraInfoId;

/*
0 ～ 999 for obclient
1000～1999 for obproxy
2000～65535 for all
*/
typedef enum enum_fulllinktraceextrainfotype
{
  FLT_DRIVER_SPAN_INFO = 1,
  FLT_EXTRA_INFO_DRIVE_END = 1000,

  FLT_APP_INFO = 2001,
  FLT_QUERY_INFO,
  FLT_CONTROL_INFO,
  FLT_SPAN_INFO,
  FLT_TYPE_SHOW_TRACE_SPAN,

  FLT_EXTRA_INFO_TYPE_END
} FullLinkTraceExtraInfoType;

typedef enum enum_recordpolicy {
  RP_ALL = 1,
  RP_ONLY_SLOW_QUERY,
  RP_SAMPLE_AND_SLOW_QUERY,
  MAX_RECORD_POLICY
} RecordPolicy;

typedef struct st_fltinfobase
{
  FullLinkTraceExtraInfoType type_;
} FLTInfoBase;

typedef struct st_fltcontrolinfo
{
  FullLinkTraceExtraInfoType type_;
  int8_t level_;
  double sample_pct_;
  RecordPolicy rp_;
  double print_sample_pct_;
  int64_t slow_query_threshold_;

  int8_t  flt_show_trace_enable_;									// enable show trace or not
} FLTControlInfo;

typedef struct st_fltappinfo
{
  FullLinkTraceExtraInfoType type_;
  const char *identifier_;
  const char *module_;
  const char *action_;
  const char *client_info_;
} FLTAppInfo;

typedef struct st_fltqueryinfo
{
  FullLinkTraceExtraInfoType type_;
  int64_t query_start_timestamp_;
  int64_t query_end_timestamp_;
} FLTQueryInfo;

typedef struct st_fltspaninfo
{
  FullLinkTraceExtraInfoType type_;
  int8_t trace_enable_;
  int8_t force_print_;
  const char *trace_id_;
  int8_t ref_type_;
  const char *span_id_;
} FLTSpanInfo;

typedef struct st_fltdriverspaninfo
{
  FullLinkTraceExtraInfoType type_;
  const char *client_span_;
} FLTDriverSpanInfo;

typedef struct st_showtracespan
{
  FullLinkTraceExtraInfoType type_;
  const char *client_span_json_;
} FLTShowTraceSpan;

typedef struct st_fltvaluedata
{
  void *value_data_;
  size_t length;
} FLTValueData;

typedef struct st_fltinfo
{
  FLTShowTraceSpan show_trace_span_;
  FLTDriverSpanInfo client_span_;
  FLTControlInfo control_info_;
  FLTAppInfo app_info_;
  FLTQueryInfo query_info_;
  FLTSpanInfo span_info_;

  FLTValueData flt_value_data_;   // for set extra info

  my_bool in_trans_;
  char trace_id_[UUID4_SERIALIZE_LEN];
  char span_id_[UUID4_SERIALIZE_LEN];
  ObTrace *trace_;
} FLTInfo;

typedef int (*flt_deserialize_field_func)(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                        const char *buf, const int64_t len, int64_t *pos, void *flt_info);
typedef int (*flt_serialize_func)(char *buf, const int64_t len, int64_t *pos, void *flt_info);
typedef int (*flt_get_serialize_size_func)(int32_t *size, void *flt_info);

typedef struct st_fltfunc
{
  flt_deserialize_field_func deserialize_field_func;
  flt_serialize_func serialize_func;
  flt_get_serialize_size_func get_serialize_size_func;
} FLTFunc;

typedef struct st_uuid
{
  union {
    struct {
      uint64_t low_;
      uint64_t high_;
    };
    struct {
      uint32_t time_low;
      uint16_t time_mid;
      uint16_t time_hi_and_version;
      uint8_t clock_seq_hi_and_reserved;
      uint8_t clock_seq_low;
      uint8_t node[6];
    };
  };
} UUID;
DEFINE_TO_STRING_FUNC_FOR(UUID);

enum enum_flt_tagtype{
  FLT_TAG_COMMAND_NAME = 0,
  FLT_TAG_CLIENT_HOST = 1,
  FLT_TAG_MAX_TYPE
};

typedef struct st_obtagctx ObTagCtx;
struct st_obtagctx
{
  ObTagCtx *next_;
  uint16_t tag_type_;
  const char *data_;
};
DEFINE_TO_STRING_FUNC_FOR(ObTagCtx);

typedef struct st_obspanctx
{
  uint16_t span_type_;
  UUID span_id_;
  struct st_obspanctx *source_span_;
  my_bool is_follow_;
  int64_t start_ts_;
  int64_t end_ts_;
  ObTagCtx *tags_;
} ObSpanCtx;
DEFINE_TO_STRING_FUNC_FOR(ObSpanCtx);

struct st_obtrace
{
  uint64_t buffer_size_;
  uint64_t offset_;
  uint64_t seq_;
  my_bool in_trans_;
  my_bool trace_enable_;
  my_bool show_trace_enable_;
  my_bool force_print_;
  my_bool slow_query_print_;
  FLTInfo *flt;                 // point to flt struct
  uint64_t uuid_random_seed[2];
  UUID trace_id_;
  UUID root_span_id_;
  LIST *current_span_list_;
  LIST *free_span_list_;
  ObSpanCtx *last_active_span_;
  ObTagCtx free_tag_list_;
  union {
    uint8_t policy_;
    struct {
      uint8_t level_ : 7;
      uint8_t auto_flush_ : 1;
    };
  };
  uint64_t log_buf_offset_;
  char *log_buf_;   // size is MAX_TRACE_LOG_SIZE
  uint64_t show_trace_buf_offset_;
  char *show_trace_buf_;   // size is MAX_TRACE_LOG_SIZE
  char *flt_serialize_buf_;  // size is MAX_FLT_SERIALIZE_SIZE
  char data_[0];   // Multi-allocated space to store free span and facilitate subsequent tagging
};

int trace_init();
void trace_end();
void begin_trace(ObTrace *trace);
void end_trace(ObTrace *trace);
ObSpanCtx* begin_span(ObTrace *trace, uint32_t span_type, uint8_t level, my_bool is_follow);
void end_span(ObTrace *trace, ObSpanCtx *span);
void reset_span(ObTrace *trace);
void append_tag(ObTrace *trace, ObSpanCtx *span, uint16_t tag_type, const char *str);
void reset_tag(ObTrace *trace, ObSpanCtx *span, ObTagCtx *tag);
ObTrace *get_trace_instance(FLTInfo *flt);
void flush_first_span(ObTrace *trace);
void flush_trace(ObTrace *trace);

DEFINE_FLT_SERIALIZE_FUNC(appinfo);         // FLT_APP_INFO
DEFINE_FLT_SERIALIZE_FUNC(queryinfo);       // FLT_QUERY_INFO
DEFINE_FLT_SERIALIZE_FUNC(controlinfo);     // FLT_CONTROL_INFO
DEFINE_FLT_SERIALIZE_FUNC(spaninfo);        // FLT_SPAN_INFO
DEFINE_FLT_SERIALIZE_FUNC(driverspaninfo);  // FLT_DRIVER_SPAN_INFO
DEFINE_FLT_SERIALIZE_FUNC(showtracespan);   // FLT_TYPE_SHOW_TRACE_SPAN
DEFINE_FLT_SERIALIZE_FUNC(nosupport);       // FLT_EXTRA_INFO_TYPE_END

my_bool flt_is_vaild(FLTInfo *flt);

int serialize_UUID(char *buf, const int64_t buf_len, int64_t* pos, UUID *uuid);
int deserialize_UUID(const char *buf, const int64_t buf_len, int64_t *pos, UUID *uuid);

int uuid4_init(uint64_t *seed, size_t seed_size);
UUID uuid4_generate(uint64_t *seed);

int flt_init(FLTInfo *flt);
void flt_end(FLTInfo *flt);
int flt_build_request(MYSQL *mysql, FLTInfo *flt);
void flt_set_send_trans_flag(FLTInfo *flt, my_bool send_flag);

int flt_deserialize_extra_info(const char *buf, const int64_t len, int64_t *pos, FullLinkTraceExtraInfoType type, void *flt_info);
int flt_serialize_extra_info(char *buf, const int64_t len, int64_t *pos, void *flt_info);
int flt_get_serialize_size_extra_info(int32_t *size, void *flt_info);

// for encode
int flt_store_type_and_len(char *buf, int64_t len, int64_t *pos, int16_t type, int32_t v_len);
int flt_store_str(char *buf, int64_t len, int64_t *pos, const char *str, const uint64_t str_len, int16_t type);
      //@{Serialize integer data, save the data in v to the position of buf+pos, and update pos
int flt_store_int1(char *buf, int64_t len, int64_t *pos, int8_t v, int16_t type);
int flt_store_int2(char *buf, int64_t len, int64_t *pos, int16_t v, int16_t type);
int flt_store_int3(char *buf, int64_t len, int64_t *pos, int32_t v, int16_t type);
int flt_store_int4(char *buf, int64_t len, int64_t *pos, int32_t v, int16_t type);
int flt_store_int5(char *buf, int64_t len, int64_t *pos, int64_t v, int16_t type);
int flt_store_int6(char *buf, int64_t len, int64_t *pos, int64_t v, int16_t type);
int flt_store_int8(char *buf, int64_t len, int64_t *pos, int64_t v, int16_t type);
int flt_store_double(char *buf, const int64_t len, int64_t *pos, double val, int16_t type);
int flt_store_float(char *buf, const int64_t len, int64_t *pos, float val, int16_t type);

int flt_get_store_str_size(const uint64_t str_len);
int flt_get_store_int1_size();
int flt_get_store_int2_size();
int flt_get_store_int3_size();
int flt_get_store_int4_size();
int flt_get_store_int5_size();
int flt_get_store_int6_size();
int flt_get_store_int8_size();
int flt_get_store_double_size();
int flt_get_store_float_size();

// for decode
int flt_resolve_type_and_len(const char *buf, int64_t len, int64_t *pos, int16_t *type, int32_t *v_len);
int flt_get_str(const char *buf, int64_t len, int64_t *pos, int64_t str_len, char **str);
//@{ Signed integer in reverse sequence, write the result to v, and update pos
int flt_get_int1(const char *buf, int64_t len, int64_t *pos, int64_t v_len, int8_t *val);
int flt_get_int2(const char *buf, int64_t len, int64_t *pos, int64_t v_len, int16_t *val);
int flt_get_int3(const char *buf, int64_t len, int64_t *pos, int64_t v_len, int32_t *val);
int flt_get_int4(const char *buf, int64_t len, int64_t *pos, int64_t v_len, int32_t *val);
int flt_get_int8(const char *buf, int64_t len, int64_t *pos, int64_t v_len, int64_t *val);
int flt_get_double(const char *buf, int64_t len, int64_t *pos, int64_t v_len, double *val);
int flt_get_float(const char *buf, int64_t len, int64_t *pos, int64_t v_len, float *val);
#endif