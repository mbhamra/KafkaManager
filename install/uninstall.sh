#!/bin/bash

DAEMON=/usr/local/bin/kafka-manager
INIT_D=/etc/init.d/kafka-manager
INSTALL_DIR=/usr/local/share/kafka-manager
CONFIG_DIR=/etc/kafka-manager
WORKER_DIR=${CONFIG_DIR}/workers

# we're going to be mucking about, so we need to be root/sudo'd
if [ "$(id -u)" != "0" ]; then
   echo "This script must be run as root" 1>&2
   exit 1
fi

# where are we now?
WORKING_DIR=$(dirname $(readlink -f $0))

# create and populate installation folder
#remove sybolyc link
\unlink  ${DAEMON}
echo "UnInstalling executable to ${DAEMON}"

# copy environment specific config
echo "UnInstalling configs to ${CONFIG_DIR}"
\rm -rf ${CONFIG_DIR}
\rm -rf ${CONFIG_DIR}/config.ini

# uninstall init script
\rm ${INIT_D}
echo "UnInstalling init script to ${INIT_D}"

echo
echo "UnInstall ok!"
