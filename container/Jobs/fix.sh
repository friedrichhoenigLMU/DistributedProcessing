#!/bin/bash
# accepts only one parameter: date that has to be fixed. 
# eg. fix.sh 2018-01-01
echo "  *******************************  fixing jobs   *******************************"

export SQOOP_HOME=/usr/local/sqoop
export PATH=$PATH:$SQOOP_HOME/bin
export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom
kinit analyticssvc@CERN.CH -k -t /tmp/keytab/analyticssvc.keytab

startDate="$1 00:00:00"
endDate="$1 23:59:59"
fileName="$1"
ind="$1"
echo "start date: ${startDate}"
echo "end date: ${endDate}"
echo "file name: ${fileName}"
echo "index : ${ind}"

echo "Removing previous data in HDFS"
hdfs dfs -rm -R -f -skipTrash  hdfs://analytix/atlas/analytics/jobs/$1_*

./JobSqoopToAnalytix.sh "${startDate}" "${endDate}" "${fileName}"
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc
fi

echo "Sqooping DONE."

pig -4 log4j.properties -f JobsToESuc.pig -param INPD=${fileName} -param ININD=${ind}

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with pig indexer. Exiting."
    exit $rc
fi

echo "Sqooping DONE. Starting resumming."

pig -4 log4j.properties -f resumming.pig -param date=${startDate}

echo "Done resumming. Starting updates."


echo "DONE"