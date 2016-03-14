go-oci8
=======

[![Build Status](https://travis-ci.org/mattn/go-oci8.svg)](https://travis-ci.org/mattn/go-oci8)

Description
-----------

oracle driver conforming to the built-in database/sql interface

Installation
------------

This package can be installed with the go get command:

    go get github.com/mattn/go-oci8

You need to put `oci8.pc` like into your `$PKG_CONFIG_PATH`. `oci8.pc` should be like below. This is an example for windows.
PKG-CONFIG is required for compilation, either compiled from [source] (https://pkg-config.freedesktop.org/releases/) locally using the MingGW/MSYS toolchain or as binary as described on https://stackoverflow.com/questions/1710922/how-to-install-pkg-config-in-windows
```
prefix=./target/
exec_prefix=${prefix}
libdir=C:/Oracle/instantclient_12_1/oci.dll
includedir=C:/Oracle/instantclient_12_1/sdk/include

glib_genmarshal=glib-genmarshal
gobject_query=gobject-query
glib_mkenums=glib-mkenums

Name: oci8_win
Version: 12.1
Description: oci8 library
Libs: -L${libdir}
Cflags: -I${includedir}
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
