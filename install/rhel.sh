#!/bin/bash

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

# Source function library.
. /etc/rc.d/init.d/functions

##PATH##
DAEMON=/usr/local/bin/kafka-manager
PIDDIR=/var/run/kafka
PIDFILE=${PIDDIR}/manager.pid
LOGFILE=/var/log/kafka-manager.log
CONFIGDIR=/etc/kafka-manager
KAFKAUSER="kafka"
PARAMS="-c ${CONFIGDIR}/config.ini -vv"

RETVAL=0

start() {
        echo -n $"Starting kafka-manager: "
        if ! test -d ${PIDDIR}
        then
          mkdir ${PIDDIR}
          chown ${KAFKAUSER} ${PIDDIR}
        fi
        daemon --pidfile=$PIDFILE $DAEMON \
            -P $PIDFILE \
            -l $LOGFILE \
            -u $KAFKAUSER \
            -d \
            $PARAMS
        RETVAL=$?
        echo
        return $RETVAL
}

stop() {
        echo -n $"Stopping kafka-manager: "
        killproc -p $PIDFILE -TERM $DAEMON
        RETVAL=$?
        echo
}

# See how we were called.
case "$1" in
  start)
        start
        ;;
  stop)
        stop
        ;;
  status)
        status -p $PIDFILE $DAEMON
        RETVAL=$?
        ;;
  restart|reload)
        stop
        start
        ;;
  condrestart|try-restart)
        if status -p $PIDFILE $DAEMON >&/dev/null; then
                stop
                start
        fi
        ;;
  *)
        echo $"Usage: $prog {start|stop|restart|reload|condrestart|status|help}"
        RETVAL=3
esac

exit $RETVAL
