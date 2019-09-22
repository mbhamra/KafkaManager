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

# determine if & which (supported) distro we're running
echo "Detecting linux distro as redhat- or debian-compatible"
if [ -f /etc/redhat-release ]; then
    DISTRO="rhel"
elif [ -f /etc/debian_version ]; then
    DISTRO="deb"
else
    echo "Only Redhat Enterprise (RHEL) or Debian systems currently supported"
    exit 1
fi

# ensure php executable is in the path
PHPBIN=$(type -P php 2>/dev/null)
if [ -z "${PHPBIN}" ]; then
  while true
  do
    echo "Where is your php executable? (usually /usr/bin)"
    read -re PHPPATH
    # has to be a valid directory
    if [ ! -d ${PHPPATH} ]; then
      echo "Not a directory: ${PHPPATH}"
    # has to have a 'php' executable
    elif [ ! -x "${PHPPATH}/php" ]; then
      echo "No 'php' executable found in ${PHPPATH}"
    # presumably all good
    else
      PHPPATH=$(dirname ${PHPPATH}/php)
      sed "s:##PATH##:PATH=\$PATH\:${PHPPATH}:" <${WORKING_DIR}/${DISTRO}.sh >${WORKING_DIR}/${DISTRO}.build.sh
      break
    fi
  done
else
  cp ${WORKING_DIR}/${DISTRO}.sh ${WORKING_DIR}/${DISTRO}.build.sh
fi

# create and populate installation folder
mkdir -p ${INSTALL_DIR}
cp -r ${WORKING_DIR}/../* ${INSTALL_DIR}/
echo "Installing to ${INSTALL_DIR}"
ln -fs ${INSTALL_DIR}/bin/manager.php ${DAEMON}
echo "Installing executable to ${DAEMON}"

# ask the environment
# echo "Which is the environment?"
#  select APPENV in "development" "testing" "production"; do
#    break
#  done

# copy environment specific config
echo "Installing configs to ${CONFIG_DIR}"
mkdir -p ${CONFIG_DIR}
# cp ${WORKING_DIR}/${APPENV}_config.ini ${CONFIG_DIR}/config.ini
cp ${WORKING_DIR}/config-advanced.ini ${CONFIG_DIR}/config.ini

# install init script
mv ${WORKING_DIR}/${DISTRO}.build.sh ${INIT_D}
chmod +x ${INIT_D}
systemctl daemon-reload
echo "Installing init script to ${INIT_D}"

echo
echo "Install ok!  Run ${INIT_D} to start and stop"
echo "Worker scripts copied to ${WORKER_DIR}, configuration can be edited in ${CONFIG_DIR}/config.ini"
