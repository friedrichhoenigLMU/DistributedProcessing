#!/bin/bash

echo "  *******************************  Updating task table with DEFT info  *******************************"

hdfs dfs -rm -R -f -skipTrash /atlas/analytics/DEFT_temp
rm /tmp/DEFT.update
echo "Clean up finished."

startDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-2hour")
echo "start date: ${startDate}"
./SqoopToAnalytix.sh "${startDate}"
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc
fi
echo "Sqooping DONE."


hdfs dfs -getmerge /atlas/analytics/DEFT_temp /tmp/DEFT.update
python3.6 updater.py /tmp/DEFT.update
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with deft indexer. Exiting."
    exit $rc
fi
echo "Updating UC DONE."