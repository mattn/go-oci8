set -e

echo "installing build tools"
apt-get -qq -y update 2>&1 > /dev/null
apt-get -qq -y install git pkg-config gcc 2>&1 > /dev/null


echo "installing go1.10.4"
wget -q https://dl.google.com/go/go1.10.4.linux-amd64.tar.gz -O /tmp/go1.10.4.linux-amd64.tar.gz
mkdir -p /usr/local
tar xf /tmp/go1.10.4.linux-amd64.tar.gz -C /usr/local
export PATH=/usr/local/go/bin:$PATH
export GOROOT=/usr/local/go
mkdir -p /usr/local/goFiles
export GOPATH=/usr/local/goFiles

echo "setting up Oracle"
export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe
export LD_LIBRARY_PATH=/u01/app/oracle/product/11.2.0/xe/lib
export ORACLE_SID=XE

DOCKER_IP=$(ip route | awk 'NR==1 {print $3}')

${ORACLE_HOME}/bin/tnsping ${DOCKER_IP}

${ORACLE_HOME}/bin/sqlplus -L -S system/oracle@${DOCKER_IP}:1521/xe <<SQL
CREATE USER scott IDENTIFIED BY tiger DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;
GRANT connect, resource, create view, create synonym TO scott;
SQL


echo "copy go-oci8"
mkdir -p ${GOPATH}/src/github.com/mattn/go-oci8
cp -r ${TESTDIR}/* ${GOPATH}/src/github.com/mattn/go-oci8/


echo "creating oci8.pc"
cd ${GOPATH}
export PKG_CONFIG_PATH=${GOPATH}
cat > oci8.pc <<PKGCONFIG
Name: oci8
Description: Oracle Call Interface
Version: 11.1
Cflags: -I${ORACLE_HOME}/rdbms/public
Libs: -L${ORACLE_HOME}/lib -Wl,-rpath,${ORACLE_HOME}/lib -lclntsh
PKGCONFIG


echo "testing go-oci8"
go test -v github.com/mattn/go-oci8 -args -disableDatabase=false -hostValid ${DOCKER_IP} -username scott -password tiger
