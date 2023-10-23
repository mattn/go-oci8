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

#ifndef _OB_COMPLEX_H
#define _OB_COMPLEX_H

#include "mysql.h"
#include "ma_hash.h"
#include "ob_rwlock.h"

typedef struct st_complex_hash {
  unsigned char *hash_key;
  OB_HASH *hash;
  ob_rw_lock_t rwlock;
} COMPLEX_HASH;

typedef enum enum_types {
  TYPE_NUMBER,
  TYPE_VARCHAR2,
  TYPE_CHAR,
  TYPE_DATE,
  TYPE_OBJECT,
  TYPE_COLLECTION,
  TYPE_RAW,
  TYPE_LONG,
  TYPE_LONGLONG,
  TYPE_TINY,
  TYPE_SHORT,
  TYPE_FLOAT,
  TYPE_DOUBLE,
  TYPE_UNKNOW,
  TYPE_MAX
} enum_types;

typedef struct st_complex_type {
  enum_types type;
  unsigned char owner_name[128];
  unsigned char type_name[128];
  unsigned int version;
  my_bool is_valid;
} COMPLEX_TYPE;

typedef struct st_child_type {
  enum_types type;
  COMPLEX_TYPE *object;
} CHILD_TYPE;

typedef struct st_complex_type_object {
  COMPLEX_TYPE header;
  unsigned int attr_no;
  unsigned int init_attr_no;
  CHILD_TYPE *child;
} COMPLEX_TYPE_OBJECT;

typedef struct st_complex_type_collection {
  COMPLEX_TYPE header;
  CHILD_TYPE child;
} COMPLEX_TYPE_COLLECTION;

COMPLEX_TYPE* STDCALL get_complex_type(MYSQL *mysql, unsigned char *owner_name, unsigned char *type_name);
COMPLEX_TYPE* STDCALL get_complex_type_with_local(MYSQL *mysql, unsigned char *type_name);

#endif /* _OB_COMPLEX_H */
