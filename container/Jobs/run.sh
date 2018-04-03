#!/bin/bash

echo "  *******************************  importing jobs table  *******************************"

startDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-2hour")
endDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-1hour")
fileName=$(date -u '+%Y-%m-%d_%H' -d "-2hour")
ind=$(date -u '+%Y-%m-%d' -d "-2hour")
echo "start date: ${startDate}"
echo "end date: ${endDate}"
echo "index : ${ind}"

echo "Removing previous data in HDFS"
hdfs dfs -rm -R -f -skipTrash hdfs://analytix/atlas/analytics/jobs_temp

./JobSqoopToAnalytix.sh "${startDate}" "${endDate}" 
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc
fi

echo "Sqooping DONE."

pig -4 log4j.properties -f JobsToESuc.pig -param ININD=${ind}
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with pig indexer. Exiting."
    exit $rc
fi

echo "Indexing UC DONE."