#!/bin/bash

echo "  *******************************  importing job status table  *******************************"

export SQOOP_HOME=/usr/local/sqoop
export PATH=$PATH:$SQOOP_HOME/bin
export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom
kinit analyticssvc@CERN.CH -k -t /tmp/keytab/analyticssvc.keytab

startDate=2018-03-02
endDate=2018-03-02

echo "start date: ${startDate}"
echo "end date: ${endDate}"

hdfs dfs -rm -R -f -skipTrash /atlas/analytics/job_states/${startDate}

./SqoopToAnalytix.sh ${startDate} ${endDate}

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc; 
fi

echo "Sqooping DONE. Starting resumming."

pig -4 log4j.properties -f resumming.pig -param date=${startDate}

echo "Done resumming. Starting updates."


echo "DONE"
