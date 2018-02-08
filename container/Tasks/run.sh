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
echo "Sqooping DONE."

echo "Starting indexing"
pig -4 log4j.properties -f TasksToESuc.pig
echo "Indexing in UC DONE."
