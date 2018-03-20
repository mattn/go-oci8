go-oci8
=======

[![Build Status](https://travis-ci.org/mattn/go-oci8.svg)](https://travis-ci.org/mattn/go-oci8)

Description
-----------

Oracle driver conforming to the built-in database/sql interface

Installation
------------

This package can be installed with the go get command:

    go get github.com/mattn/go-oci8

You need to put `oci8.pc` like into your `$PKG_CONFIG_PATH`. `oci8.pc` should be like below.

### Example for Windows

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

### Example for Linux

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

### Example for MacOs

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

Documentation
-------------

API documentation can be found here: http://godoc.org/github.com/mattn/go-oci8

Examples can be found under the `./_example` directory

License
-------

MIT: http://mattn.mit-license.org/2014

ToDo
----

* LastInserted is not int64
* Fetch number is more improvable

Author
------

Yasuhiro Matsumoto (a.k.a mattn)

Special Thanks
--------------

Jamil Djadala
