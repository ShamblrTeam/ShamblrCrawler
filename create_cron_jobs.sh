#!/bin/bash
promptyn () {
    while true; do
        read -p "$1 " yn
        case $yn in
        [Yy]* ) return 0;;
        [Nn]* ) return 1;;
        * ) echo "Please answer yes or no.";;
        esac
    done
}

DIR=`pwd`
CRON_FILE="cron_jobs.txt"
CRON_LOG="$DIR/cron.log"

# create cron jobs
if promptyn "Just the APIFetch monitor? Answer 'yes' to get cronjobs for APIFetch. Answer 'no' to get cronjobs for Frontier and DbWrapper."; then
    echo "* * * * * /bin/sh $DIR/monitor.sh status_restart APIfetch tumblr_test.py >> $CRON_LOG 2>&1" > $CRON_FILE 
else
    echo "* * * * * /bin/sh $DIR/monitor.sh status_restart Frontier test.py >> $CRON_LOG 2>&1" > $CRON_FILE 
    echo "* * * * * /bin/sh $DIR/monitor.sh status_restart DbWrapper dp_handler.py >> $CRON_LOG 2>&1" >> $CRON_FILE 
fi

# begin the cron job
crontab cron_jobs.txt
