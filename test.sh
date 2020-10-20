
set -e

echo "installing build tools"
apt-get -qq -y update  2>&1 > /dev/null
apt-get -qq -y install git pkg-config gcc wget  2>&1 > /dev/null


echo "installing go"
cd /tmp/
wget -nv https://dl.google.com/go/go1.13.8.linux-amd64.tar.gz
wget -nv https://dl.google.com/go/go1.12.17.linux-amd64.tar.gz
wget -nv https://dl.google.com/go/go1.11.13.linux-amd64.tar.gz
wget -nv https://dl.google.com/go/go1.10.8.linux-amd64.tar.gz
wget -nv https://dl.google.com/go/go1.9.7.linux-amd64.tar.gz

mkdir -p /usr/local/goFiles1.13.x
tar -xf /tmp/go1.13.8.linux-amd64.tar.gz
mv /tmp/go /usr/local/go1.13.x

mkdir -p /usr/local/goFiles1.12.x
tar -xf /tmp/go1.12.17.linux-amd64.tar.gz
mv /tmp/go /usr/local/go1.12.x

mkdir -p /usr/local/goFiles1.11.x
tar -xf /tmp/go1.11.13.linux-amd64.tar.gz
mv /tmp/go /usr/local/go1.11.x

mkdir -p /usr/local/goFiles1.10.x
tar -xf /tmp/go1.10.8.linux-amd64.tar.gz
mv /tmp/go /usr/local/go1.10.x

mkdir -p /usr/local/goFiles1.9.x
tar -xf /tmp/go1.9.7.linux-amd64.tar.gz
mv /tmp/go /usr/local/go1.9.x


echo "starting Oracle"
/usr/sbin/startup.sh


echo "setting up Oracle"
export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe
export PATH=$ORACLE_HOME/bin:$PATH
export ORACLE_SID=XE
export LD_LIBRARY_PATH=/u01/app/oracle/product/11.2.0/xe/lib

DOCKER_IP=$(ifconfig eth0 | awk '/inet / { printf $2; exit }')

tnsping ${DOCKER_IP}

sqlplus -L -S "sys/oracle@${DOCKER_IP}:1521 as sysdba" <<SQL
CREATE USER scott IDENTIFIED BY tiger DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;
GRANT connect, resource, create view, create synonym TO scott;
GRANT execute ON SYS.DBMS_LOCK TO scott;
alter system set processes=300 scope=spfile;
shutdown immediate

SQL

echo "starting Oracle"
/usr/sbin/startup.sh

echo "creating oci8.pc"
mkdir -p /usr/local/pkg_config
cd /usr/local/pkg_config
export PKG_CONFIG_PATH=/usr/local/pkg_config
cat > oci8.pc <<PKGCONFIG
Name: oci8
Description: Oracle Call Interface
Version: 11.1
Cflags: -I${ORACLE_HOME}/rdbms/public
Libs: -L${ORACLE_HOME}/lib -Wl,-rpath,${ORACLE_HOME}/lib -lclntsh
PKGCONFIG


export PATH_SAVE=${PATH}


echo "testing go-oci8 Go 1.13.x"
export PATH=/usr/local/go1.13.x/bin:${PATH_SAVE}
export GOROOT=/usr/local/go1.13.x
export GOPATH=/usr/local/goFiles1.13.x
mkdir -p ${GOPATH}/src/github.com/mattn/go-oci8
cp -r ${TESTDIR}/* ${GOPATH}/src/github.com/mattn/go-oci8/

go test -v github.com/mattn/go-oci8 -args -disableDatabase=false -hostValid ${DOCKER_IP} -username scott -password tiger


echo "testing go-oci8 Go 1.12.x"
export PATH=/usr/local/go1.12.x/bin:${PATH_SAVE}
export GOROOT=/usr/local/go1.12.x
export GOPATH=/usr/local/goFiles1.12.x
mkdir -p ${GOPATH}/src/github.com/mattn/go-oci8
cp -r ${TESTDIR}/* ${GOPATH}/src/github.com/mattn/go-oci8/

go test -v github.com/mattn/go-oci8 -args -disableDatabase=false -hostValid ${DOCKER_IP} -username scott -password tiger


echo "testing go-oci8 Go 1.11.x"
export PATH=/usr/local/go1.11.x/bin:${PATH_SAVE}
export GOROOT=/usr/local/go1.11.x
export GOPATH=/usr/local/goFiles1.11.x
mkdir -p ${GOPATH}/src/github.com/mattn/go-oci8
cp -r ${TESTDIR}/* ${GOPATH}/src/github.com/mattn/go-oci8/

go test -v github.com/mattn/go-oci8 -args -disableDatabase=false -hostValid ${DOCKER_IP} -username scott -password tiger


echo "testing go-oci8 Go 1.10.x"
export PATH=/usr/local/go1.10.x/bin:${PATH_SAVE}
export GOROOT=/usr/local/go1.10.x
export GOPATH=/usr/local/goFiles1.10.x
mkdir -p ${GOPATH}/src/github.com/mattn/go-oci8
cp -r ${TESTDIR}/* ${GOPATH}/src/github.com/mattn/go-oci8/

go test -v github.com/mattn/go-oci8 -args -disableDatabase=false -hostValid ${DOCKER_IP} -username scott -password tiger


echo "testing go-oci8 Go 1.9.x"
export PATH=/usr/local/go1.9.x/bin:${PATH_SAVE}
export GOROOT=/usr/local/go1.9.x
export GOPATH=/usr/local/goFiles1.9.x
mkdir -p ${GOPATH}/src/github.com/mattn/go-oci8
cp -r ${TESTDIR}/* ${GOPATH}/src/github.com/mattn/go-oci8/

go test -v github.com/mattn/go-oci8 -args -disableDatabase=false -hostValid ${DOCKER_IP} -username scott -password tiger
