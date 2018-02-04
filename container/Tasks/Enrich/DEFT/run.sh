#!/bin/bash

echo "  *******************************  Updating task table with DEFT info  *******************************"

startDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-2hour")
ind=$(date -u '+%Y-%m' -d "-2hour")
echo "start date: ${startDate}"
echo "index : ${ind}"
./SqoopToAnalytix.sh "${startDate}"
echo "Sqooping DONE."


hdfs dfs -getmerge /atlas/analytics/DEFT_temp /tmp/DEFT.update
python3.5 updater.py /tmp/DEFT.update

echo "Updating UC DONE."

hdfs dfs -rm -R -f /atlas/analytics/DEFT_temp
rm /tmp/DEFT.update

echo "Clean up finished."