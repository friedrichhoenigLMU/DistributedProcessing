#!/bin/bash

echo "  *******************************  importing task table  *******************************"

startDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-2hour")
endDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-1hour")

echo "start date: ${startDate}"
echo "end date: ${endDate}"

echo "Removing previous data in HDFS"
hdfs dfs -rm -R -f -skipTrash hdfs://analytix/atlas/analytics/tasks_temp

echo "Starting sqoop"
./TaskSqoopToAnalytix.sh "${startDate}" "${endDate}"
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc
fi
echo "Sqooping DONE."

echo "Starting indexing"
pig -4 log4j.properties -f TasksToESuc.pig
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with task pig indexer. Exiting."
    exit $rc
fi
echo "Indexing in UC DONE."
