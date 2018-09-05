
set -e

echo "installing build tools"
apt-get -qq -y update 2>&1 > /dev/null
apt-get -qq -y install git pkg-config gcc 2>&1 > /dev/null


echo "installing go"
export PATH_SAVE=${PATH}
cd /tmp/
wget -nv https://dl.google.com/go/go1.11.linux-amd64.tar.gz
wget -nv https://dl.google.com/go/go1.10.4.linux-amd64.tar.gz
wget -nv https://dl.google.com/go/go1.9.7.linux-amd64.tar.gz
wget -nv https://dl.google.com/go/go1.8.7.linux-amd64.tar.gz

mkdir -p /usr/local/goFiles1.11.x
tar -xf /tmp/go1.11.linux-amd64.tar.gz
mv /tmp/go /usr/local/go1.11.x

mkdir -p /usr/local/goFiles1.10.x
tar -xf /tmp/go1.10.4.linux-amd64.tar.gz
mv /tmp/go /usr/local/go1.10.x

mkdir -p /usr/local/goFiles1.9.x
tar -xf /tmp/go1.9.7.linux-amd64.tar.gz
mv /tmp/go /usr/local/go1.9.x

mkdir -p /usr/local/goFiles1.8.x
tar -xf /tmp/go1.8.7.linux-amd64.tar.gz
mv /tmp/go /usr/local/go1.8.x


echo "setting up Oracle"
export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe
export LD_LIBRARY_PATH=/u01/app/oracle/product/11.2.0/xe/lib
export ORACLE_SID=XE

DOCKER_IP=$(ip route | awk 'NR==1 {print $3}')

${ORACLE_HOME}/bin/tnsping ${DOCKER_IP}

${ORACLE_HOME}/bin/sqlplus -L -S "sys/oracle@${DOCKER_IP}:1521 as sysdba" <<SQL
CREATE USER scott IDENTIFIED BY tiger DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;
GRANT connect, resource, create view, create synonym TO scott;
GRANT execute ON SYS.DBMS_LOCK TO scott;
create or replace function SCOTT.SLEEP_SECONDS (p_seconds number) return integer is
begin
  dbms_lock.sleep(p_seconds);
  return 1;
end SLEEP_SECONDS;
/

SQL

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


# oci8 does not work with 1.8.x at this time
# ../goFiles1.8.x/src/github.com/mattn/go-oci8/oci8_go18.go:77: undefined: sql.Out

# echo "testing go-oci8 Go 1.8.x"
# export PATH=/usr/local/go1.8.x/bin:${PATH_SAVE}
# export GOROOT=/usr/local/go1.8.x
# export GOPATH=/usr/local/goFiles1.8.x
# mkdir -p ${GOPATH}/src/github.com/mattn/go-oci8
# cp -r ${TESTDIR}/* ${GOPATH}/src/github.com/mattn/go-oci8/

# go test -v github.com/mattn/go-oci8 -args -disableDatabase=false -hostValid ${DOCKER_IP} -username scott -password tiger
