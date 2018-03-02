#!/bin/bash

echo "  *******************************  importing job status table  *******************************"

export SQOOP_HOME=/usr/local/sqoop
export PATH=$PATH:$SQOOP_HOME/bin
export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom
kinit analyticssvc@CERN.CH -k -t /tmp/keytab/analyticssvc.keytab

startDate=2018-02-16
endDate=2018-02-17

echo "start date: ${startDate}"
echo "end date: ${endDate}"

hdfs dfs -rm -R -f -skipTrash /atlas/analytics/job_states/${startDate}

./SqoopToAnalytix.sh ${startDate} ${endDate}

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc; 
fi

echo "Sqooping DONE."

# pig -4 log4j.properties -f StatusToESuc.pig -param INPD=${fileName} -param ININD=${ind}

echo "DONE"

