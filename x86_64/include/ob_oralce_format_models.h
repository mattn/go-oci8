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
#include "mysql.h"
#include <stdint.h>

#define DATE_PART_CNT   3
#define TIME_PART_CNT   4
#define OTHER_PART_CNT  4
#define DATETIME_PART_CNT     (DATE_PART_CNT + TIME_PART_CNT)
#define ORACLE_DATE_PART_CNT  (DATE_PART_CNT + TIME_PART_CNT - 1)
#define TOTAL_PART_CNT        (DATETIME_PART_CNT + OTHER_PART_CNT)
#define DT_YEAR   0
#define DT_MON    1
#define DT_MDAY   2

#define DT_HOUR   3
#define DT_MIN    4
#define DT_SEC    5
#define DT_USEC   6

#define DT_DATE   7
#define DT_YDAY   8
#define DT_WDAY   9
#define DT_OFFSET_MIN     10

#define OB_MAX_TZ_ABBR_LEN 32
#define OB_MAX_TZ_NAME_LEN 64

//18446744073709551615
#define MAX_UINT64_STR_LEN 20
#define MAX_DIGITS10_STR_SIZE MAX_UINT64_STR_LEN + 3

#define DT_TYPE_DATE        (1UL << 0)
#define DT_TYPE_TIME        (1UL << 1)
#define DT_WEEK_SUN_BEGIN   (1UL << 5)  // sunday is the first day of week, otherwise monday.
#define DT_WEEK_ZERO_BEGIN  (1UL << 6)  // week num will begin with 0, otherwise 1.
#define DT_WEEK_GE_4_BEGIN  (1UL << 7)  // week which has 4 or more days is week 1, otherwise has
#define DT_TYPE_ORACLE      (1UL << 8)  //oracle timestamp to nanosecond (nano, tz, ltz)
                                        // the first sunday of monday.
#define DT_TYPE_TIMEZONE    (1UL << 10) //oracle timestamp with time zone (tz)

#define DT_TYPE_DATETIME          (DT_TYPE_DATE | DT_TYPE_TIME)

#define IS_SUN_BEGIN(mode)      ((DT_WEEK_SUN_BEGIN & (mode)) ? 1 : 0)
#define IS_ZERO_BEGIN(mode)     ((DT_WEEK_ZERO_BEGIN & (mode)) ? 1 : 0)
#define IS_GE_4_BEGIN(mode)     ((DT_WEEK_GE_4_BEGIN & (mode)) ? 1 : 0)
#define HAS_TYPE_ORACLE(mode)   ((DT_TYPE_ORACLE & (mode)) ? 1 : 0)
#define HAS_TYPE_TIMEZONE(mode) ((DT_TYPE_TIMEZONE & (mode)) ? 1 : 0)

#define DAYS_PER_WEEK 7
#define MONS_PER_YEAR 12
#define SECS_PER_MIN  60
#define MINS_PER_HOUR 60

typedef uint64_t ObDTMode;

enum ElementFlag
{
  INVALID_FLAG = -1,
  AD = 0,
  AD2,  //A.D.
  BC,
  BC2,  //B.C.
  CC,
  SCC,
  D,
  DAY,
  DD,
  DDD,
  DY,
  FF1,
  FF2,
  FF3,
  FF4,
  FF5,
  FF6,
  FF7,
  FF8,
  FF9,
  FF,
  HH,
  HH24,
  HH12,
  IW,
  I,
  IY,
  IYY,
  IYYY,
  MI,
  MM,
  MONTH,
  MON,
  AM,
  AM2,  //A.M.
  PM,
  PM2,  //P.M.
  Q,
  RR,
  RRRR,
  SS,
  SSSSS,
  WW,
  W,
  YGYYY,
  YEAR,
  SYEAR,
  YYYY,
  SYYYY,
  YYY,
  YY,
  Y,
  DS,
  DL,
  TZH,
  TZM,
  TZD,
  TZR,
  X,
  J,
  ///<<< !!!add any flag before this line!!!
  MAX_FLAG_NUMBER
};

enum ElementGroup
{
  RUNTIME_CONFLICT_SOLVE_GROUP = -2,
  NON_CONFLICT_GROUP = -1,
  ///<<< conflict in group, before this line, will be ignored
  NEVER_APPEAR_GROUP = 0,   //the element should never appear
  YEAR_GROUP,               //include : SYYYY YYYY YYY YY Y YGYYY RR RRRR
  MERIDIAN_INDICATOR_GROUP, //include : AM PM
  WEEK_OF_DAY_GROUP,        //include : D Day Dy
  ERA_GROUP,                //include : AD BC
  HOUR_GROUP,               //include : HH HH12 HH24
  MONTH_GROUP,              //include : MONTH MON MM
  DAY_OF_YEAR_GROUP,           //include : DDD, J
  ///<<< !!!add any flag before this line!!!
  MAX_CONFLICT_GROUP_NUMBER
};

struct ObTimeConstStr {
  const char *ptr_;
  int32_t len_;
};

struct ObDFMParseCtx
{
  const char* fmt_str_;
  const char* cur_ch_;
  int64_t remain_len_;

  //the following values are only used in function str_to_ob_time_oracle_dfm
  int64_t expected_elem_flag_;
  my_bool is_matching_by_expected_len_;  //only used for match_int_value
};

enum UpperCaseMode {
  NON_CHARACTER,
  ONLY_FIRST_CHARACTER,
  ALL_CHARACTER
};

struct ObDFMElem
{
  int64_t elem_flag_;             //flag from enum ObDFMFlag
  int64_t offset_;                //offset in origin format string
  my_bool is_single_dot_before_;   //for the dot before FF
  enum UpperCaseMode upper_case_mode_;
};

struct ObTime
{
  uint64_t  mode_;
  int32_t   parts_[TOTAL_PART_CNT];
  // year:    [1000, 9999].
  // month:   [1, 12].
  // day:     [1, 31].
  // hour:    [0, 23] or [0, 838] if it is a time.
  // minute:  [0, 59].
  // second:  [0, 59].
  // usecond: [0, 1000000], 1000000 can only valid after str_to_ob_time, for round.
  // nanosecond: [0, 1000000000], when HAS_TYPE_ORACLE(mode_)
  // date: date value, day count since 1970-1-1.
  // year day: [1, 366].
  // week day: [1, 7], 1 means monday, 7 means sunday.
  // offset minute:  [-12*60, 14*60].

  char tz_name_[OB_MAX_TZ_NAME_LEN];
  char tzd_abbr_[OB_MAX_TZ_ABBR_LEN];//the abbr of time zone region with Daylight Saving Time
  int32_t time_zone_id_;
  uint8_t nano_scale_;
  my_bool is_tz_name_valid_;
};

struct ObFastFormatInt
{
  char buf_[MAX_DIGITS10_STR_SIZE];
  char *ptr_;
  int64_t len_;
};

struct ObOracleTimeLimiter
{
  int32_t MIN;
  int32_t MAX;
  int ERROR_CODE;
};
longlong strtoll10(const char *nptr, char **endptr, int *error);
int calculate_str_oracle_dfm_length(const struct ObTime *ob_time,
                                    const char *fmt_str, const int64_t fmt_len,
                                    int16_t scale, int64_t *len);

int ob_time_to_str_oracle_dfm(const struct ObTime *ob_time,
                              const char *fmt_str, const int64_t fmt_len,
                              int16_t scale,
                              char *buf, int64_t buf_len,
                              int64_t *pos);

int32_t ob_time_to_date(struct ObTime *ob_time);

int str_to_ob_time_oracle_dfm(const char *str, const int64_t str_len,
                              const char *fmt_str, const int64_t fmt_len,
                              struct ObTime *ob_time,
                              int16_t scale);
