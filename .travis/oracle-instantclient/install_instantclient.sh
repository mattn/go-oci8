#!/bin/sh -e
sudo apt-get -qq update
sudo apt-get --no-install-recommends -qq install alien bc libaio1 unzip
sudo dpkg --install `sudo alien --scripts --to-deb "$ORACLE_INSTANTCLIENT_FILE" | cut -d' ' -f1`
sudo dpkg --install `sudo alien --scripts --to-deb "$ORACLE_INSTANTCLIENT_SDK_FILE" | cut -d' ' -f1`
