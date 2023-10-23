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
#ifndef _OB_PROTOCOL20_H
#define _OB_PROTOCOL20_H

#include <ma_global.h>
#include <mysql.h>
#include <mariadb_ctype.h>
#include <mariadb_dyncol.h>
#include <stdint.h>
#include <ma_list.h>
#include <ob_full_link_trace.h>


#define DEFINE_OB20_EXTRAINFO_SERIALIZE_FUNC(type)                                                                    \
  int extrainfo_serialize_##type(char *buf, const int64_t len, int64_t *pos, void *data);                             \
  int extrainfo_deserialize_##type(const char *buf, const int64_t len, int64_t *pos, void *data, const int64_t v_len);\
  int64_t extrainfo_get_serialize_size_##type(void *data)

#define OB20_EXTRAINFO_SERIALIZE_FUNC(type) \
  {                                         \
    extrainfo_serialize_##type,             \
    extrainfo_deserialize_##type,           \
    extrainfo_get_serialize_size_##type     \
  }

typedef union st_ob20protocol_flags
{
  uint32_t flags;
  struct Protocol20Flags
  {
    uint32_t OB_EXTRA_INFO_EXIST:                       1;
    uint32_t OB_IS_LAST_PACKET:                         1;
    uint32_t OB_IS_PROXY_REROUTE:                       1;
    uint32_t OB_IS_NEW_EXTRA_INFO:                      1;
    uint32_t OB_FLAG_RESERVED_NOT_USE:                 28;
  } st_flags;
} Ob20ProtocolFlags;

typedef struct st_ob20protocol_header
{
  uint16_t magic_num;
  uint16_t version;
  uint32_t connection_id;
  uint32_t request_id;
  uint8_t pkt_seq;
  uint32_t payload_len;
  Ob20ProtocolFlags flag;
  uint16_t reserved;
  uint16_t header_checksum;
} Ob20ProtocolHeader;

typedef struct st_ob20protocol_extra_info_list
{
  LIST *list,
       *current;
} Ob20ProtocolExtraInfoList;

/**
0 ～ 999用于ob driver 私有
1001～1999用于ob proxy私有
2001～65535用于server, driver, proxy共有
 */
typedef enum em_extrainfokeytype {
  OB20_DRIVER_END = 1000,
  OB20_PROXY_END = 2000,
  TRACE_INFO = 2001,
  SESS_INFO = 2002,
  FULL_TRC = 2003,
  OB20_SVR_END,
} ExtraInfoKeyType;

typedef struct st_ob20protocol_extra_info
{
  ExtraInfoKeyType key;
  void *value;
} Ob20ProtocolExtraInfo;

typedef struct st_ob20protocol
{
  Ob20ProtocolHeader header;
  Ob20ProtocolExtraInfoList extra_info_list;
  unsigned int checksum;

  // ma_net write buffer
  uchar *real_write_buffer;
  uint32_t real_write_buffer_length;
  
  // extra info
  FLTInfo *flt;    // full link trace
} Ob20Protocol;

typedef int (*extrainfo_serialize_func)(char *buf, const int64_t len, int64_t *pos, void *data);
typedef int (*extrainfo_deserialize_func)(const char *buf, const int64_t len, int64_t *pos, void *data, const int64_t v_len);
typedef int64_t (*extrainfo_get_serialize_size_func)(void *data);

typedef struct st_extrainfo_serialize_func
{
  extrainfo_serialize_func serialize_func;
  extrainfo_deserialize_func deserialize_func;
  extrainfo_get_serialize_size_func get_serialize_size_func; 
} ExtraInfoSerializeFunc;

DEFINE_OB20_EXTRAINFO_SERIALIZE_FUNC(flt);    // full link trace

void update_request_id(uint32_t *request_id);

int ob20_init(Ob20Protocol *ob20protocol, unsigned long conid, my_bool use_flt);
void ob20_end(Ob20Protocol *ob20protocol);

void clear_extra_info(Ob20Protocol *ob20protocol);

size_t get_protocol20_extra_info_length(Ob20Protocol *ob20protocol);

uchar *fill_protocol20_extra_info(Ob20Protocol *ob20protocol, uchar *buffer, size_t buffer_len);

void init_protocol20_header(Ob20Protocol *ob20protocol);

void update_protocol20_header(Ob20Protocol *ob20protocol);

uchar *fill_protocol20_header(Ob20Protocol *ob20protocol, size_t len, size_t pkt_nr, size_t complen,uchar *buffer);

int decode_protocol20_header(Ob20Protocol *ob20protocol, uchar *buffer, uint32_t pkt_len, uint32_t pkt_nr, uint32_t complen);

int decode_protocol20_extra_info(Ob20Protocol *ob20protocol, uchar *buffer);

int ob20_set_extra_info(MYSQL *mysql, ExtraInfoKeyType key, void *value);

int flt_set_module(MYSQL *mysql, const char *module_name);
int flt_set_action(MYSQL *mysql, const char *action_name);
int flt_set_client_info(MYSQL *mysql, const char *client_info);
int flt_set_identifier(MYSQL *mysql, const char *identifier);

int flt_get_control_level(MYSQL *mysql, int *level);
int flt_get_control_sample_pct(MYSQL *mysql, double *sample_pct);
int flt_get_control_record_policy(MYSQL *mysql, int *rp);
int flt_get_control_print_spct(MYSQL *mysql, double *sample_pct);
int flt_get_control_slow_threshold(MYSQL *mysql, long int *slow_threshold);
#endif