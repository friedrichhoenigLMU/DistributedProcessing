#!/bin/bash

# this script is called once a day
# it scoops jobs parent info and stores it in hdfs
# fetches is from there and copies it to uct2-collectd for processing and indexing.

echo "  *******************************  importing parent/child table  *******************************"

startDate=$(date -u '+%Y-%m-%d' -d "-48hour")
endDate=$(date -u '+%Y-%m-%d' -d "-24hour")

echo "start date: ${startDate}"
echo "end date: ${endDate}"

hdfs dfs -rm -R -f -skipTrash /atlas/analytics/job_parents/${startDate} 

./SqoopToAnalytix.sh "${startDate}" "${endDate}"
echo "Sqooping DONE."

echo "copy file to UC. Will index it from there."
hdfs dfs -getmerge /atlas/analytics/job_parents/${startDate} /tmp/${startDate}.update
scp /tmp/${startDate}.update uct2-collectd.mwt2.org:/tmp/.
rm /tmp/${startDate}.update

echo "DONE."