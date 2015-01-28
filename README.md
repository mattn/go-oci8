go-oci8
=======

Description
-----------

oracle driver conforming to the built-in database/sql interface

Installation
------------

This package can be installed with the go get command:

    go get github.com/mattn/go-oci8

You need to put `oci8.pc` like into your `$PKG_CONFIG_PATH`. `oci8.pc` should be like below. This is an example for windows.

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
