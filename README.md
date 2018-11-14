# go-oci8

[![GoDoc Reference](https://godoc.org/github.com/mattn/go-oci8?status.svg)](http://godoc.org/github.com/mattn/go-oci8)
[![Build Status](https://travis-ci.org/mattn/go-oci8.svg)](https://travis-ci.org/mattn/go-oci8)

## Description

Golang Oracle database driver conforming to the Go database/sql interface

## Installation

Install Oracle full client or Instant Client:

https://www.oracle.com/technetwork/database/database-technologies/instant-client/downloads/index.html

Install a C/C++ compiler

Install pkg-config, edit your package config file oci8.pc (examples below), then set environment variable PKG_CONFIG_PATH to oci8.pc file location
(Or can use Go tag noPkgConfig then setup environment variables CGO_CFLAGS and CGO_LDFLAGS)

Go get with Go version 1.9 or higher

```
go get github.com/mattn/go-oci8
```

Try the simple select example:

https://godoc.org/github.com/mattn/go-oci8#example-package--SqlSelect

If you have a build error it is normaly because of a misconfiguration, make sure to search close issues for help


## oci8.pc Examples

### Windows

```
prefix=/devel/target/XXXXXXXXXXXXXXXXXXXXXXXXXX
exec_prefix=${prefix}
libdir=c:/oraclexe/app/oracle/product/11.2.0/server/oci/lib/msvc
includedir=c:/oraclexe/app/oracle/product/11.2.0/server/oci/include/include

glib_genmarshal=glib-genmarshal
gobject_query=gobject-query
glib_mkenums=glib-mkenums

Name: oci8
Description: oci8 library
Libs: -L${libdir} -loci
Cflags: -I${includedir}
Version: 11.2
```

### Linux

```
prefix=/devel/target/XXXXXXXXXXXXXXXXXXXXXXXXXX
exec_prefix=${prefix}
libdir=/usr/lib/oracle/11.2/client64/lib
includedir=/usr/include/oracle/11.2/client64

glib_genmarshal=glib-genmarshal
gobject_query=gobject-query
glib_mkenums=glib-mkenums

Name: oci8
Description: oci8 library
Libs: -L${libdir} -lclntsh
Cflags: -I${includedir}
Version: 11.2
```

### MacOs

Please install `pkg-config` with [`brew`](https://brew.sh/) if not already present.
Download the instant client and the sdk and unpack it e.g. in your
`Downloads` folder and create therein a file names `oci8.pc`.
Please replace `<username>` with your actual username.

```
prefixdir=/Users/<username>/Downloads/instantclient_12_2/
libdir=${prefixdir}
includedir=${prefixdir}/sdk/include

Name: OCI
Description: Oracle database driver
Version: 12.2
Libs: -L${libdir} -lclntsh
Cflags: -I${includedir}
```

You also have to set these environment variables
(e.g. permanently by adding them to your `.bashrc`)

```
export LD_LIBRARY_PATH=/Users/<username>/Downloads/instantclient_12_2
export PKG_CONFIG_PATH=/Users/<username>/Downloads/instantclient_12_2
```

## SQL Examples

SQL examples can be found in the GoDoc reference:

https://godoc.org/github.com/mattn/go-oci8

And in _example:

https://github.com/mattn/go-oci8/tree/master/_example

## Author

Yasuhiro Matsumoto (a.k.a mattn)

## Special Thanks

Jamil Djadala
