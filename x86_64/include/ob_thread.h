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

#ifndef _OB_THREAD_H
#define _OB_THREAD_H

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#define NATIVE_RW_INITIALIZER {SRWLOCK_INIT, FALSE}
#include <windows.h>
//#include <WinSock2.h>
//#include <shlwapi.h>
#else
#include <pthread.h>
#ifndef NATIVE_RW_INITIALIZER 
#define NATIVE_RW_INITIALIZER PTHREAD_RWLOCK_INITIALIZER
#endif
#endif

/* Compatible with previous thread defines*/
#define MY_THREAD_CREATE_JOINABLE   OB_THREAD_CREATE_JOINABLE
#define MY_THREAD_CREATE_DETACHED   OB_THREAD_CREATE_DETACHED
#define MY_THREAD_ONCE_INIT         OB_THREAD_ONCE_INIT
#define MY_THREAD_ONCE_INPREGRESS   OB_THREAD_ONCE_INPROGRESS 
#define MY_THREAD_ONCE_DONE         OB_THREAD_ONCE_DONE 
#define my_thread_self              ob_thread_self

#ifdef _WIN32
typedef DWORD            ob_thread_t;
typedef volatile LONG    ob_thread_once_t;
typedef struct thread_attr
{
  DWORD ob_thread_stack_size;
  int ob_thead_state;
} ob_thread_attr_t;

typedef void * (__cdecl *ob_start_routine)(void *);

struct ob_thread_start_param
{
  ob_start_routine func;
  void *arg;
};

unsigned int __stdcall ob_win_thread_start(void *p);
#define OB_THREAD_CREATE_JOINABLE 0
#define OB_THREAD_CREATE_DETACHED 1
#define OB_THREAD_ONCE_INIT       0
#define OB_THREAD_ONCE_INPROGRESS 1
#define OB_THREAD_ONCE_DONE       2
#else
typedef pthread_once_t   ob_thread_once_t;
typedef pthread_t        ob_thread_t;
typedef pthread_attr_t   ob_thread_attr_t;
#define OB_THREAD_CREATE_JOINABLE PTHREAD_CREATE_JOINABLE
#define OB_THREAD_CREATE_DETACHED PTHREAD_CREATE_DETACHED
typedef void *(*ob_start_routine)(void *);
#define OB_THREAD_ONCE_INIT       PTHREAD_ONCE_INIT
#endif

typedef ob_thread_once_t  my_thread_once_t;
typedef ob_thread_t       my_thread_t;
typedef ob_thread_attr_t  my_thread_attr_t;

typedef struct st_ob_thread_handle
{
#ifdef _WIN32
  HANDLE handle;
#endif
  ob_thread_t thread;
} ob_thread_handle;

ob_thread_t ob_thread_self();

int ob_thread_equal(ob_thread_t ob_thread1, ob_thread_t ob_thread2);
int ob_thread_attr_init(ob_thread_attr_t *ob_thread_attr);
int ob_thread_attr_destroy(ob_thread_attr_t *ob_thread_attr);
int ob_thread_attr_setstacksize(ob_thread_attr_t *ob_thread_attr, size_t ob_stacksize);
int ob_thread_attr_setdetachstate(ob_thread_attr_t *ob_thread_attr, int ob_detachstate);
int ob_thread_attr_getstacksize(ob_thread_attr_t *ob_thread_attr, size_t *ob_stacksize);
void ob_thread_yield();

int ob_thread_once(ob_thread_once_t *ob_once_control, void (*ob_routine)(void));
int ob_thread_create(ob_thread_handle *ob_thread, const ob_thread_attr_t *ob_thread_attr, ob_start_routine ob_thread_func, void *ob_thread_arg);
int ob_thread_join(ob_thread_handle *ob_thread, void **ob_join_ptr);
int ob_thread_cancel(ob_thread_handle *ob_thread);
void ob_thread_exit(void *ob_thread_exit_ptr);

#ifdef __cplusplus
}
#endif
#endif