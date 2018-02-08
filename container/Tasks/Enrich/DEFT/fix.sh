#!/bin/bash

echo "  *******************************  FULL update of task table with DEFT info  *******************************"

export SQOOP_HOME=/usr/local/sqoop
export PATH=$PATH:$SQOOP_HOME/bin
export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom
kinit analyticssvc@CERN.CH -k -t /tmp/keytab/analyticssvc.keytab

hdfs dfs -rm -R -f -skipTrash /atlas/analytics/DEFT_temp
rm /tmp/DEFT.update
echo "Clean up finished."

startDate="2013-12-01 00:00:00"
echo "start date: ${startDate}"
./SqoopToAnalytix.sh "${startDate}"
echo "Sqooping DONE."

hdfs dfs -getmerge /atlas/analytics/DEFT_temp /tmp/DEFT.update
python3.6 updater.py /tmp/DEFT.update

echo "Updating UC DONE."
