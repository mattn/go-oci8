set -e

setup_go_dist () {
    VERSION_MAJOR_MINOR=$1
    VERSION_RELEASE=$2
    VERSION_FULL="${1}.${2}"

    FILE=go${VERSION_FULL}.linux-amd64.tar.gz
    [ ! -e "${FILE}" ] && wget -nv https://dl.google.com/go/${FILE}

    mkdir -p /usr/local/goFiles${VERSION_MAJOR_MINOR}.x
    tar -xf /tmp/${FILE}
    mv /tmp/go /usr/local/go${VERSION_MAJOR_MINOR}.x
}

test_go_dist () {
    VERSION_MAJOR_MINOR=$1

    echo "testing go-oci8 Go ${VERSION_MAJOR_MINOR}.x"
    export PATH=/usr/local/go${VERSION_MAJOR_MINOR}.x/bin:${PATH_SAVE}
    export GOROOT=/usr/local/go${VERSION_MAJOR_MINOR}.x
    export GOPATH=/usr/local/goFiles${VERSION_MAJOR_MINOR}.x
    mkdir -p ${GOPATH}/src/github.com/mattn/go-oci8
    cp -r ${TESTDIR}/* ${GOPATH}/src/github.com/mattn/go-oci8/

    go test -v github.com/mattn/go-oci8 -args -disableDatabase=false -hostValid ${DOCKER_IP} -username scott -password tiger
}

echo "installing build tools"
apt-get -qq -y update  2>&1 > /dev/null
apt-get -qq -y install git pkg-config gcc wget  2>&1 > /dev/null

echo "installing go"
cd /tmp/

setup_go_dist "1.15" "6"
setup_go_dist "1.14" "13"
setup_go_dist "1.13" "15"
setup_go_dist "1.12" "17"
setup_go_dist "1.11" "13"
setup_go_dist "1.10" "8"
setup_go_dist "1.9" "7"

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

test_go_dist "1.15"
test_go_dist "1.14"
test_go_dist "1.13"
test_go_dist "1.12"
test_go_dist "1.11"
test_go_dist "1.10"
test_go_dist "1.9"
