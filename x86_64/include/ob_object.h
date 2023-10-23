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

#ifndef _ob_object_h
#define _ob_object_h

#include <mysql.h>
#include <stdint.h>

typedef enum enum_obobjtype
{
  ObNullType = 0,   // 空类型

  ObTinyIntType=1,                // int8, aka mysql boolean type
  ObSmallIntType=2,               // int16
  ObMediumIntType=3,              // int24
  ObInt32Type=4,                 // int32
  ObIntType=5,                    // int64, aka bigint

  ObUTinyIntType=6,                // uint8
  ObUSmallIntType=7,               // uint16
  ObUMediumIntType=8,              // uint24
  ObUInt32Type=9,                    // uint32
  ObUInt64Type=10,                 // uint64

  ObFloatType=11,                  // single-precision floating point
  ObDoubleType=12,                 // double-precision floating point

  ObUFloatType=13,            // unsigned single-precision floating point
  ObUDoubleType=14,           // unsigned double-precision floating point

  ObNumberType=15, // aka decimal/numeric
  ObUNumberType=16,

  ObDateTimeType=17,
  ObTimestampType=18,
  ObDateType=19,
  ObTimeType=20,
  ObYearType=21,

  ObVarcharType=22,  // charset: utf8mb4 or binary
  ObCharType=23,     // charset: utf8mb4 or binary

  ObHexStringType   = 24, // hexadecimal literal, e.g. X'42', 0x42, b'1001', 0b1001

  ObExtendType      = 25, // Min, Max, NOP etc.
  ObUnknownType     = 26, // For question mark(?) in prepared statement, no need to serialize
  // @note future new types to be defined here !!!

  ObTinyTextType    = 27,
  ObTextType        = 28,
  ObMediumTextType  = 29,
  ObLongTextType    = 30,

  ObBitType         = 31,
  ObEnumType        = 32,
  ObSetType         = 33,
  ObEnumInnerType   = 34,
  ObSetInnerType    = 35,

  ObTimestampTZType   = 36, // timestamp with time zone for oracle
  ObTimestampLTZType  = 37, // timestamp with local time zone for oracle
  ObTimestampNanoType = 38, // timestamp nanosecond for oracle
  ObRawType           = 39, // raw type for oracle
  ObIntervalYMType    = 40, // interval year to month
  ObIntervalDSType    = 41, // interval day to second
  ObNumberFloatType   = 42, // oracle float, subtype of NUMBER
  ObNVarchar2Type     = 43, // nvarchar2
  ObNCharType         = 44, // nchar
  ObURowIDType        = 45, // UROWID
  ObLobType           = 46, // Oracle Lob
  ObMaxType                 // invalid type, or count of obj type
} ObObjType;

enum enum_obcollationtype
{
  CS_TYPE_INVALID = 0,
  CS_TYPE_GBK_CHINESE_CI = 28,
  CS_TYPE_UTF8MB4_GENERAL_CI = 45,
  CS_TYPE_UTF8MB4_BIN = 46,
  CS_TYPE_UTF16_GENERAL_CI = 54,
  CS_TYPE_UTF16_BIN = 55,
  CS_TYPE_BINARY = 63,
  CS_TYPE_GBK_BIN = 87,
  CS_TYPE_UTF16_UNICODE_CI = 101,
  CS_TYPE_UTF8MB4_UNICODE_CI = 224,
  CS_TYPE_GB18030_CHINESE_CI = 248,
  CS_TYPE_GB18030_BIN = 249,
  CS_TYPE_MAX,
} ObCollationType;

enum enum_obcollationlevel
{
  CS_LEVEL_EXPLICIT = 0,
  CS_LEVEL_NONE = 1,
  CS_LEVEL_IMPLICIT = 2,
  CS_LEVEL_SYSCONST = 3,
  CS_LEVEL_COERCIBLE = 4,
  CS_LEVEL_NUMERIC = 5,
  CS_LEVEL_IGNORABLE = 6,
  CS_LEVEL_INVALID,   // here we didn't define CS_LEVEL_INVALID as 0,
                      // since 0 is a valid value for CS_LEVEL_EXPLICIT in mysql 5.6.
                      // fortunately we didn't need to use it to define array like charset_arr,
                      // and we didn't persist it on storage.
} ObCollationLevel;

typedef struct st_obmysqltypemap
{
  /* oceanbase::common::ObObjType ob_type; */
  enum_field_types mysql_type;
  uint16_t flags;         /* flags if Field */
  uint64_t length;        /* other than varchar type */
} ObMySQLTypeMap;

static const ObMySQLTypeMap type_maps_[ObMaxType] =
{
  /* ObMinType */
  {MYSQL_TYPE_NULL,      BINARY_FLAG, 0},                        /* ObNullType */
  {MYSQL_TYPE_TINY,      BINARY_FLAG, 0},                        /* ObTinyIntType */
  {MYSQL_TYPE_SHORT,     BINARY_FLAG, 0},                        /* ObSmallIntType */
  {MYSQL_TYPE_INT24,     BINARY_FLAG, 0},                        /* ObMediumIntType */
  {MYSQL_TYPE_LONG,      BINARY_FLAG, 0},                        /* ObInt32Type */
  {MYSQL_TYPE_LONGLONG,  BINARY_FLAG, 0},                        /* ObIntType */
  {MYSQL_TYPE_TINY,      BINARY_FLAG | UNSIGNED_FLAG, 0},        /* ObUTinyIntType */
  {MYSQL_TYPE_SHORT,     BINARY_FLAG | UNSIGNED_FLAG, 0},        /* ObUSmallIntType */
  {MYSQL_TYPE_INT24,     BINARY_FLAG | UNSIGNED_FLAG, 0},        /* ObUMediumIntType */
  {MYSQL_TYPE_LONG,      BINARY_FLAG | UNSIGNED_FLAG, 0},        /* ObUInt32Type */
  {MYSQL_TYPE_LONGLONG,  BINARY_FLAG | UNSIGNED_FLAG, 0},        /* ObUInt64Type */
  {MYSQL_TYPE_FLOAT,     BINARY_FLAG, 0},                        /* ObFloatType */
  {MYSQL_TYPE_DOUBLE,    BINARY_FLAG, 0},                        /* ObDoubleType */
  {MYSQL_TYPE_FLOAT,     BINARY_FLAG | UNSIGNED_FLAG, 0},        /* ObUFloatType */
  {MYSQL_TYPE_DOUBLE,    BINARY_FLAG | UNSIGNED_FLAG, 0},        /* ObUDoubleType */
  {MYSQL_TYPE_NEWDECIMAL,BINARY_FLAG, 0},                        /* ObNumberType */
  {MYSQL_TYPE_NEWDECIMAL,BINARY_FLAG | UNSIGNED_FLAG, 0},        /* ObUNumberType */
  {MYSQL_TYPE_DATETIME,  BINARY_FLAG, 0},                        /* ObDateTimeType */
  {MYSQL_TYPE_TIMESTAMP, BINARY_FLAG | TIMESTAMP_FLAG, 0},       /* ObTimestampType */
  {MYSQL_TYPE_DATE,   BINARY_FLAG, 0},                        /* ObDateType */
  {MYSQL_TYPE_TIME,      BINARY_FLAG, 0},                        /* ObTimeType */
  {MYSQL_TYPE_YEAR,      UNSIGNED_FLAG | ZEROFILL_FLAG, 0},      /* ObYearType */
  {MYSQL_TYPE_VAR_STRING, 0, 0},              /* ObVarcharType */
  {MYSQL_TYPE_STRING, 0, 0},                  /* ObCharType */
  {MYSQL_TYPE_VAR_STRING,BINARY_FLAG, 0},     /* ObHexStringType */
  {MAX_NO_FIELD_TYPES, 0, 0},             /* ObExtendType */
  {MAX_NO_FIELD_TYPES, 0, 0},             /* ObUnknownType */
  /* ObMaxType */
};

int get_mysql_type(ObObjType ob_type, enum_field_types *mysql_type);

int get_ob_type(ObObjType *ob_type, enum_field_types mysql_type);

typedef struct st_obobjmeta
{
  uint8_t type_;
  uint8_t cs_level_;    // collation level
  uint8_t cs_type_;     // collation type
  int8_t scale_;        // scale
} ObObjMeta;

typedef union un_obobjvalue
{
  int64_t int64_;
  uint64_t uint64_;

  float float_;
  double double_;

  const char *string_;

  uint32_t *nmb_digits_;

  int64_t datetime_;
  int32_t date_;
  int64_t time_;
  uint8_t year_;

  int64_t ext_;
  int64_t unknown_;
} ObObjValue;

typedef struct st_obobj
{
  ObObjMeta meta_;  // sizeof = 4
  // union
  // {
  //   int32_t val_len_;
  //   number::ObNumber::Desc nmb_desc_;
  //   ObOTimestampData::UnionTZCtx time_ctx_;
  // };
  int32_t val_len_; // sizeof = 4
  ObObjValue v_;  // sizeof = 8
 
} ObObj;

/* obj serialize/deserialize function */
typedef int (*ob_obj_value_serialize)(const ObObj *obj, char *buf, const int64_t buf_len, int64_t *pos);
typedef int (*ob_obj_value_deserialize)(ObObj *obj, const char *buf, const int64_t data_len, int64_t *pos);
typedef int64_t (*ob_obj_value_get_serialize_size)(const ObObj *obj);

typedef struct obobjtypefuncs
{
  ob_obj_value_serialize serialize;
  ob_obj_value_deserialize deserialize;
  ob_obj_value_get_serialize_size get_serialize_size;
} ObObjTypeFuncs;


#define DECLAR_SERIALIZE_FUNCS(OBJTYPE)                                                                 \
  int obj_val_serialize_##OBJTYPE(const ObObj *obj, char *buf, const int64_t buf_len, int64_t *pos);    \
  int obj_val_deserialize_##OBJTYPE(ObObj *obj, const char *buf, const int64_t data_len, int64_t *pos); \
  int64_t obj_val_get_serialize_size_##OBJTYPE(const ObObj *obj);                                       \

DECLAR_SERIALIZE_FUNCS(ObNullType);
// ObTinyIntType=1,                // int8, aka mysql boolean type
DECLAR_SERIALIZE_FUNCS(ObTinyIntType);
// ObSmallIntType=2,               // int16
DECLAR_SERIALIZE_FUNCS(ObSmallIntType);
// ObMediumIntType=3,              // int24
DECLAR_SERIALIZE_FUNCS(ObMediumIntType);
// ObInt32Type=4,                 // int32
DECLAR_SERIALIZE_FUNCS(ObInt32Type);
// ObIntType=5,                    // int64, aka bigint
DECLAR_SERIALIZE_FUNCS(ObIntType);
// ObUTinyIntType=6,                // uint8
DECLAR_SERIALIZE_FUNCS(ObUTinyIntType);
// ObUSmallIntType=7,               // uint16
DECLAR_SERIALIZE_FUNCS(ObUSmallIntType);
// ObUMediumIntType=8,              // uint24
DECLAR_SERIALIZE_FUNCS(ObUMediumIntType);
// ObUInt32Type=9,                    // uint32
DECLAR_SERIALIZE_FUNCS(ObUInt32Type);
// ObUInt64Type=10,                 // uint64
DECLAR_SERIALIZE_FUNCS(ObUInt64Type);
// ObFloatType=11,                  // single-precision floating point
DECLAR_SERIALIZE_FUNCS(ObFloatType);
// ObDoubleType=12,                 // double-precision floating point
DECLAR_SERIALIZE_FUNCS(ObDoubleType);
// ObUFloatType=13,            // unsigned single-precision floating point
DECLAR_SERIALIZE_FUNCS(ObUFloatType);
// ObUDoubleType=14,           // unsigned double-precision floating point
DECLAR_SERIALIZE_FUNCS(ObUDoubleType);
// ObVarcharType=22            // charset: utf8mb4 or binary
DECLAR_SERIALIZE_FUNCS(ObVarcharType)

void set_tinyint(ObObj *obj, const int8_t value);
void set_tinyint_value(ObObj *obj, const int8_t value);
void set_smallint(ObObj *obj, const int16_t value);
void set_smallint_value(ObObj *obj, const int16_t value);
void set_mediumint(ObObj *obj, const int32_t value);
void set_int32(ObObj *obj, const int32_t value);
void set_int32_value(ObObj *obj, const int32_t value);
void set_int(ObObj *obj, const int64_t value);
void set_int_value(ObObj *obj, const int64_t value);
void set_utinyint(ObObj *obj, const uint8_t value);
void set_usmallint(ObObj *obj, const uint16_t value);
void set_umediumint(ObObj *obj, const uint32_t value);
void set_uint32(ObObj *obj, const uint32_t value);
void set_uint64(ObObj *obj, const uint64_t value);
void set_float(ObObj *obj, const float value);
void set_float_value(ObObj *obj, const float value);
void set_ufloat(ObObj *obj, const float value);
void set_double(ObObj *obj, const double value);
void set_double_value(ObObj *obj, const double value);
void set_udouble(ObObj *obj, const double value);
void set_varchar(ObObj *obj, const char *ptr, int32_t size);

int serialize_ObObj(const ObObj *obj, char *buf, const int64_t buf_len, int64_t *pos);
int deserialize_ObObj(ObObj *obj, const char *buf, const int64_t data_len, int64_t *pos);
int64_t get_serialize_size_ObObj(const ObObj *obj);
#endif