#!/bin/bash

echo "  *******************************  importing job status table  *******************************"
echo " between $1 and $2 "

sqoop import \
    --connect $JOB_ORACLE_CONNECTION_STRING \
    --username $JOB_ORACLE_USER --password $JOB_ORACLE_PASS \
    --table ATLAS_PANDA.JOBS_STATUSLOG \
    -m 4 --split-by PANDAID --as-avrodatafile \
    --target-dir /atlas/analytics/job_states/$1 \
    --columns PANDAID,MODIFICATIONTIME,JOBSTATUS,PRODSOURCELABEL,CLOUD,COMPUTINGSITE,MODIFTIME_EXTENDED  \
    --where "MODIFTIME_EXTENDED between TO_DATE('$1','YYYY-MM-DD') and TO_DATE('$2','YYYY-MM-DD') " \
    --map-column-java PANDAID=Long,MODIFICATIONTIME=String,MODIFTIME_EXTENDED=String

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc; 
fi

echo "DONE"