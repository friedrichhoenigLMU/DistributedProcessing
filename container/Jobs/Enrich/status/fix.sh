#!/bin/bash

echo "  *******************************  importing job status table  *******************************"

export SQOOP_HOME=/usr/local/sqoop
export PATH=$PATH:$SQOOP_HOME/bin
export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom
kinit analyticssvc@CERN.CH -k -t /tmp/keytab/analyticssvc.keytab

startDate=2018-03-02
endDate=2018-03-03

echo "start date: ${startDate}"
echo "end date: ${endDate}"

hdfs dfs -rm -R -f -skipTrash /atlas/analytics/job_states/${startDate}

./SqoopToAnalytix.sh ${startDate} ${endDate}

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc; 
fi

echo "Done resumming. "


echo "Merge data into file in temp. Will index it from there."
rm -f /tmp/job_status_temp.csv
hdfs dfs -getmerge /atlas/analytics/temp/job_state_data /tmp/job_status_temp.csv


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