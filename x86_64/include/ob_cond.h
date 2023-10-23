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

#ifndef _OB_COND_H
#define _OB_COND_H

#include "ob_thread.h"
#include "ob_rwlock.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
typedef CONDITION_VARIABLE ob_cond_t;
#else
typedef pthread_cond_t ob_cond_t;
#endif

int ob_cond_init(ob_cond_t *cond);
int ob_cond_destroy(ob_cond_t *cond);
int ob_cond_timedwait(ob_cond_t *cond, ob_mutex_t *mutex, const struct timespec *abstime);
int ob_cond_wait(ob_cond_t *cond, ob_mutex_t *mutex);
int ob_cond_signal(ob_cond_t *cond);
int ob_cond_broadcast(ob_cond_t *cond);

#ifdef __cplusplus
}
#endif

#endif /* _OB_COND_H */
