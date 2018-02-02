#!/bin/bash

echo "  *******************************  importing task table  *******************************"

startDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-2hour")
endDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-1hour")
fileName=$(date -u '+%Y-%m-%d_%H' -d "-2hour")
ind=$(date -u '+%Y-%m' -d "-2hour")
echo "start date: ${startDate}"
echo "end date: ${endDate}"
echo "file name: ${fileName}"
echo "index : ${ind}"
./TaskSqoopToAnalytix.sh "${startDate}" "${endDate}" "${fileName}"
echo "Sqooping DONE."

pig -4 log4j.properties -f TasksToESuc.pig -param INPD=${fileName} -param ININD=${ind}
echo "Indexing in UC DONE."
