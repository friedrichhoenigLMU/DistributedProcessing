#!/bin/bash

# this script is called once a day
# it scoops jobs parent info and stores it in hdfs
# fetches is from there and copies it to uct2-collectd for processing and indexing.

echo "  *******************************  importing parent/child table  *******************************"


export SQOOP_HOME=/usr/local/sqoop
export PATH=$PATH:$SQOOP_HOME/bin
export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom
kinit analyticssvc@CERN.CH -k -t /tmp/keytab/analyticssvc.keytab

startDate=2018-01-07
endDate=2018-02-01

echo "start date: ${startDate}"
echo "end date: ${endDate}"

hdfs dfs -rm -R -f -skipTrash /atlas/analytics/job_parents_temp

./SqoopToAnalytix.sh "${startDate}" "${endDate}"
echo "Sqooping DONE."

echo "Merge data into file in temp. Will index it from there."
rm -f /tmp/job_parents_temp.csv
hdfs dfs -getmerge /atlas/analytics/job_parents_temp /tmp/job_parents_temp.csv

echo "Running updater"
python3.6 updater.py

echo "DONE."
