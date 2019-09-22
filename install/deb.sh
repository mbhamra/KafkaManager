#!/bin/sh

# Kafka worker manager

### BEGIN INIT INFO
# Provides:          kafka-manager
# Required-Start:    $network $remote_fs $syslog
# Required-Stop:     $network $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start daemon at boot time
# Description:       Enable kafka manager daemon
### END INIT INFO

##PATH##
DAEMON=/usr/local/bin/kafka-manager
PIDDIR=/var/run/kafka
PIDFILE=${PIDDIR}/manager.pid
LOGFILE=/var/log/kafka-manager.log
CONFIGDIR=/etc/kafka-manager
KAFKAUSER="kafka"
PARAMS="-c ${CONFIGDIR}/config.ini -vv"

test -x ${DAEMON} || exit 0

. /lib/lsb/init-functions

start()
{
  log_daemon_msg "Starting Kafka Manager"
  if ! test -d ${PIDDIR}
  then
    mkdir ${PIDDIR}
    chown ${KAFKAUSER} ${PIDDIR}
  fi
  if start-stop-daemon \
    --start \
    --startas $DAEMON \
    --pidfile $PIDFILE \
    -- -P $PIDFILE \
       -l $LOGFILE \
       -u $KAFKAUSER \
       -d \
       $PARAMS 
  then
    log_end_msg 0
  else
    log_end_msg 1
    log_warning_msg "Please take a look at the syslog"
    exit 1
  fi
}

stop()
{
  log_daemon_msg "Stopping Kafka Manager"
  if start-stop-daemon \
    --stop \
    --oknodo \
    --retry 20 \
    --pidfile $PIDFILE
  then
    log_end_msg 0
  else
    log_end_msg 1
    exit 1
  fi
}

case "$1" in

  start)
    start
  ;;

  stop)
    stop
  ;;

  restart|force-reload)
    stop
    start
  ;;

  status)
    status_of_proc -p $PIDFILE $DAEMON "Kafka Manager"
  ;;

  *)
    echo "Usage: $0 {start|stop|restart|force-reload|status|help}"
  ;;

esac
