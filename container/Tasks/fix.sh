#!/bin/bash
# accepts only one parameter: date that has to be fixed. 
# eg. fix.sh 2018-01-01
echo "  *******************************  fixing tasks   *******************************"

export SQOOP_HOME=/usr/local/sqoop
export PATH=$PATH:$SQOOP_HOME/bin
export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom
kinit analyticssvc@CERN.CH -k -t /tmp/keytab/analyticssvc.keytab


startDate="$1 00:00:00"
endDate="$1 23:59:59"

echo "start date: ${startDate}"
echo "end date: ${endDate}"

echo "Removing previous data in HDFS"
hdfs dfs -rm -R -f -skipTrash hdfs://analytix/atlas/analytics/tasks_temp

./TaskSqoopToAnalytix.sh "${startDate}" "${endDate}" 
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc
fi
echo "Sqooping DONE."

pig -4 log4j.properties -f TasksToESuc.pig
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with task indexer. Exiting."
    exit $rc
fi
echo "Indexing UC DONE."