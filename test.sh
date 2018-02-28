set -e

echo installing build tools
apt-get -qq -y update 2>&1 > /dev/null
apt-get -qq -y install git pkg-config gcc 2>&1 > /dev/null
echo installing go1.10
wget -q https://dl.google.com/go/go1.10.linux-amd64.tar.gz -O /tmp/go1.10.linux-amd64.tar.gz
mkdir -p /usr/local
tar xf /tmp/go1.10.linux-amd64.tar.gz -C /usr/local

export PATH=/usr/local/go/bin:$PATH
export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe
export LD_LIBRARY_PATH=/u01/app/oracle/product/11.2.0/xe/lib
export ORACLE_SID=XE

DOCKER_IP=$(ip route | awk 'NR==1 {print $3}')

echo DSN: system/oracle@${DOCKER_IP}:1521/xe

${ORACLE_HOME}/bin/tnsping ${DOCKER_IP}

${ORACLE_HOME}/bin/sqlplus -L -S system/oracle@${DOCKER_IP}:1521/xe <<SQL
CREATE USER scott IDENTIFIED BY tiger DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;
GRANT connect, resource, create view, create synonym TO scott;
SQL

echo go get -d github.com/mattn/go-oci8
go get -d github.com/mattn/go-oci8
cp -r ${TESTDIR} -T .

echo building
cd $(go env GOPATH)/src/github.com/mattn/go-oci8

cat > oci8.pc <<PKGCONFIG
Name: oci8
Description: Oracle Call Interface
Version: 11.1
Cflags: -I${ORACLE_HOME}/rdbms/public
Libs: -L${ORACLE_HOME}/lib -Wl,-rpath,${ORACLE_HOME}/lib -lclntsh
PKGCONFIG

export PKG_CONFIG_PATH=.
export DSN="scott/tiger@${DOCKER_IP}:1521/xe"
go test -x
