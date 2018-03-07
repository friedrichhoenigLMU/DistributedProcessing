#!/bin/bash

echo "  *******************************  importing job status table  *******************************"

startDate=$(date +%Y-%m-%d -d "-4day")
endDate=$(date +%Y-%m-%d  -d "-3day")

echo "start date: ${startDate}"
echo "end date: ${endDate}"

hdfs dfs -rm -R -f -skipTrash /atlas/analytics/job_states/${startDate}

./SqoopToAnalytix.sh ${startDate} ${endDate}

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc; 
fi

echo "Sqooping DONE."

pig -4 log4j.properties -f resumming.pig -param date=${startDate}

echo "DONE"

