#!/bin/bash

# this script is called once a day
# it scoops jobs parent info and stores it in hdfs
# fetches is from there and copies it to uct2-collectd for processing and indexing.

echo "  *******************************  importing parent/child table  *******************************"

startDate=$(date -u '+%Y-%m-%d' -d "-48hour")
endDate=$(date -u '+%Y-%m-%d' -d "-24hour")

echo "start date: ${startDate}"
echo "end date: ${endDate}"

hdfs dfs -rm -R -f -skipTrash /atlas/analytics/job_parents_temp

./SqoopToAnalytix.sh "${startDate}" "${endDate}"

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc; 
fi

echo "Sqooping DONE."

echo "Merge data into file in temp. Will index it from there."
rm -f /tmp/job_parents_temp.csv
hdfs dfs -getmerge /atlas/analytics/job_parents_temp /tmp/job_parents_temp.csv

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with getmerge. Exiting."
    exit $rc; 
fi

echo "Running updater"
python3.6 updater.py

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with updater. Exiting."
    exit $rc; 
fi

echo "DONE."