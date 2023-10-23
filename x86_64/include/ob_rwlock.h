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

#ifndef _OB_RWLOCK_H
#define _OB_RWLOCK_H

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include <windows.h>

typedef CRITICAL_SECTION ob_mutex_t;
typedef int ob_mutexattr_t;
typedef struct st_my_rw_lock_t
{
  SRWLOCK ob_srwlock;
  BOOL is_exclusive;
} ob_rw_lock_t;
#define OB_RW_INITIALIZER {SRWLOCK_INIT, FALSE}
#else
#include <pthread.h>
typedef pthread_mutexattr_t ob_mutexattr_t;
typedef pthread_mutex_t ob_mutex_t;
typedef pthread_rwlock_t ob_rw_lock_t;
#define OB_RW_INITIALIZER PTHREAD_RWLOCK_INITIALIZER
#endif
typedef ob_mutex_t native_mutex_t;
typedef ob_mutexattr_t native_mutexattr_t; 
typedef ob_rw_lock_t native_rw_lock_t; 

#ifndef NATIVE_RW_INITIALIZER 
#define NATIVE_RW_INITIALIZER OB_RW_INITIALIZER
#endif
/* Compatible with previous functions */
#define native_mutex_init     ob_mutex_init
#define native_mutex_lock     ob_mutex_lock
#define native_mutex_trylock  ob_mutex_trylock
#define native_mutex_unlock   ob_mutex_unlock
#define native_mutex_destroy  ob_mutex_destroy
#define native_rw_init        ob_rw_init
#define native_rw_rdlock      ob_rw_rdlock
#define native_rw_tryrdlock   ob_rw_tryrdlock
#define native_rw_wrlock      ob_rw_wrlock
#define native_rw_trywrlock   ob_rw_trywrlock
#define native_rw_destroy     ob_rw_destroy
#define native_rw_unlock      ob_rw_unlock

int ob_mutex_init(ob_mutex_t *mutex, const ob_mutexattr_t *attr);
int ob_mutex_lock(ob_mutex_t *mutex);
int ob_mutex_trylock(ob_mutex_t *mutex);
int ob_mutex_unlock(ob_mutex_t *mutex);
int ob_mutex_destroy(ob_mutex_t *mutex);
int ob_rw_init(ob_rw_lock_t *rwp);
int ob_rw_destroy(ob_rw_lock_t *rwp);
int ob_rw_rdlock(ob_rw_lock_t *rwp);
int ob_rw_tryrdlock(ob_rw_lock_t *rwp);
int ob_rw_wrlock(ob_rw_lock_t *rwp);
int ob_rw_trywrlock(ob_rw_lock_t *rwp);
int ob_rw_unlock(ob_rw_lock_t *rwp);

#ifdef __cplusplus
}
#endif

#endif
