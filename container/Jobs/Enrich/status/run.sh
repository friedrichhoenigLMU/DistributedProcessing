#!/bin/bash


# export JAVA_HOME=/etc/alternatives/java_sdk_1.9.0_openjdk/jre/

echo "  *******************************  importing job status table  *******************************"

startDate=$(date +%Y-%m-%d -d "-4day")
endDate=$(date +%Y-%m-%d  -d "-3day")

echo "start date: ${startDate}"
echo "end date: ${endDate}"

hdfs dfs -rm -R -f -skipTrash /atlas/analytics/job_states/${startDate}

./SqoopToAnalytix.sh ${startDate} ${endDate}

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc; 
fi

echo "Sqooping DONE. Starting resumming."

hdfs dfs -rm -R -f -skipTrash temp/job_state_data
pig -4 log4j.properties -f resumming.pig -param date=${startDate}


rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with pig resumming. Exiting."
    exit $rc; 
fi

echo "Done resumming. "


echo "Merge data into file in temp. Will index it from there."
rm -f /tmp/job_status_temp.csv
hdfs dfs -getmerge temp/job_state_data /tmp/job_status_temp.csv


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