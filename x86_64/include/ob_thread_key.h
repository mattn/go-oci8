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

#ifndef _OB_THREAD_KEY_H
#define _OB_THREAD_KEY_H

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include <windows.h>
typedef DWORD ob_thread_key_t;
#else
#include <pthread.h>
typedef pthread_key_t ob_thread_key_t;
#endif

int ob_create_thread_key(ob_thread_key_t *ob_key, void (*ob_destructor)(void *));
int ob_delete_thread_key(ob_thread_key_t ob_key);
void *ob_get_thread_key(ob_thread_key_t ob_key);
int ob_set_thread_key(ob_thread_key_t ob_key, void *value);

#ifdef __cplusplus
}
#endif

#endif