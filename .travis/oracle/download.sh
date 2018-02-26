#!/bin/sh -e
# vim: set et sw=2 ts=2:

[ -n "$ORACLE_COOKIE" ] || { echo "Missing ORACLE_COOKIE environment variable!"; exit 1; }
[ -n "$ORACLE_FILE" ] || { echo "Missing ORACLE_FILE environment variable!"; exit 1; }

ORACLE_DOWNLOAD_FILE="$(basename "$ORACLE_FILE")"

if [ -n "$ORACLE_DOWNLOAD_DIR" ]; then
  mkdir -p "$ORACLE_DOWNLOAD_DIR"
  ORACLE_DOWNLOAD_FILE="$(readlink -f "$ORACLE_DOWNLOAD_DIR")/$ORACLE_DOWNLOAD_FILE"
fi

if [ "${*#*--unless-exists}" != "$*" ] && [ -f "$ORACLE_DOWNLOAD_FILE" ]; then
  exit 0
fi

cd "$(dirname "$(readlink -f "$0")")"

echo "PhantomJS version $(phantomjs --version)"
npm install bluebird node-phantom-simple

export ORACLE_DOWNLOAD_FILE
export COOKIES='cookies.txt'
export USER_AGENT='Mozilla/5.0'

echo > "$COOKIES"
chmod 600 "$COOKIES"

exec node download.js
