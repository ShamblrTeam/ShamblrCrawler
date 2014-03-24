#!/bin/bash
#
# the root of the project/process directory
PROCESS_DIR=`pwd`/$2

# module name used for pid file etc.
MODULE_NAME=$3

# pid file for starting/stopping
PIDFILE=${PROCESS_DIR}/logs/${MODULE_NAME}.pid

# python path
PYTHON=/usr/bin/python3.3

# tools for process discovery etc.
PGREP=/usr/bin/pgrep
PKILL=/usr/bin/pkill
PSTREE="/usr/bin/pstree -p"

findpids() {
PSINFO=`${PGREP} -f "${MODULE_NAME}"`
test "${PSINFO}"
}

start() {
if findpids 
then
echo "${MODULE_NAME} already running...not starting"
else
echo "Starting ${MODULE_NAME}"
cd ${PROCESS_DIR}
${PYTHON} ${MODULE_NAME} >> ${PROCESS_DIR}/logs/output.log 2>&1 &
echo $! > ${PIDFILE}
echo "${MODULE_NAME} ."
fi
}

stop() {
if findpids
then
echo "Stopping ${MODULE_NAME}."
kill `cat ${PIDFILE}`
rm ${PIDFILE}
echo "${MODULE_NAME} stopped."
else
echo "${MODULE_NAME} not running"
fi
}

tree() {
if [ -a  ${PIDFILE} ] ; then
PID=`cat ${PIDFILE}`
if [ -d /proc/${PID} ] ; then
${PSTREE} ${PID}
else
echo "invalid PID in pidfile, or server not running"
fi
else
echo "pidfile not found"
fi
}

kill_running() {
if findpids; then
echo "trying to kill lingering processes"
${PKILL} -f "${MODULE_NAME}"
sleep 5

if findpids; then
echo "trying really hard to kill lingering processes"
${PKILL} -9 -f "${MODULE_NAME}"
sleep 5
fi
fi
}

status() {
${PGREP} -f "${MODULE_NAME} "
}

status_restart() {
if ps aux | ${PGREP} -f ${MODULE_NAME}  > /dev/null
then
   echo "Running"
else
   echo "Starting!"
   start
   echo "Run started!"
fi
}

usage() {
echo "executable jar startup script"
echo " "
echo "accepts the following commands:"
echo " "
echo "  start   - start the process if it isn't running"
echo "  stop    - stop the process if it is running"
echo "  restart - restart the process if it is running"
echo "  status  - list PIDs matching the process (from pgrep)"
echo "  tree    - list PIDs in tree format (from pidfile)"
echo "  kill    - forcefully stop the process"
echo "  help    - this usage information"
echo " "
}

case "$1" in
'start')
start
;;
'stop')
stop
;;
'restart')
stop
sleep 5
kill_running
start
;;
'kill')
stop
kill_running
;;
'status')
status
;;
'status_restart')
status_restart
;;
'tree')
tree
;;
'help')
usage
;;
*)
echo "Usage: $0 [start|stop|restart|kill|status|tree|help]"
;;
esac
