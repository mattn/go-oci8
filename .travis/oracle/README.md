[![Build](https://travis-ci.org/cbandy/travis-oracle.svg?branch=master)](https://travis-ci.org/cbandy/travis-oracle)

Use [Oracle Database Express Edition][] in your builds on [Travis CI][].

[Oracle Database Express Edition]: http://www.oracle.com/technetwork/database/database-technologies/express-edition/overview/index.html
[Travis CI]: https://travis-ci.org/


Usage
-----

To use this tool, you must have an Oracle account with which you have accepted
the current license agreement for [Oracle Database Express Edition][].

1. Add your Oracle username and password to your build [environment variables][],
   either as hidden repository settings or encrypted variables:

   | Variable Name         | Value         |
   | --------------------- | ------------- |
   | `ORACLE_LOGIN_userid` | your username |
   | `ORACLE_LOGIN_pass`   | your password |

2. Add the version information to your build environment variables:

   ```yaml
   - ORACLE_COOKIE=sqldev
   - ORACLE_FILE=oracle11g/xe/oracle-xe-11.2.0-1.0.x86_64.rpm.zip
   - ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe
   - ORACLE_SID=XE
   ```

3. Download and extract the [latest release][] into your project. For example,

   ```shell
   wget 'https://github.com/cbandy/travis-oracle/archive/v2.0.3.tar.gz'
   mkdir -p .travis/oracle
   tar x -C .travis/oracle --strip-components=1 -f v2.0.3.tar.gz
   ```

4. Enable [`sudo`](https://docs.travis-ci.com/user/reference/overview/):

   ```yaml
   sudo: required
   ```

5. Finally, execute the extracted scripts as part of your build, usually
   during [`before_install`](https://docs.travis-ci.com/user/customizing-the-build/#The-Build-Lifecycle):

   ```yaml
   - .travis/oracle/download.sh
   - .travis/oracle/install.sh
   ```

[SQL\*Plus][] is installed to `$ORACLE_HOME/bin/sqlplus`, and the current user
has both normal and DBA access without a password, i.e. `/` and `/ AS SYSDBA`.

[OCI][] and [OCCI][] libraries and header files are in `$ORACLE_HOME/lib` and
`$ORACLE_HOME/rdbms/public`, respectively.

[environment variables]: https://docs.travis-ci.com/user/environment-variables/
[latest release]: https://github.com/cbandy/travis-oracle/releases/latest
[OCCI]: http://www.oracle.com/pls/topic/lookup?ctx=xe112&id=LNCPP
[OCI]: http://www.oracle.com/pls/topic/lookup?ctx=xe112&id=LNOCI
[SQL\*Plus]: http://www.oracle.com/pls/topic/lookup?ctx=xe112&id=SQPUG
