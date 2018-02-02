#!/bin/bash

echo "  *******************************  Sqooping DEFT table  *******************************"
echo " between $1 and $2 "


sqoop import \
    --direct \
    --connect $JOB_ORACLE_CONNECTION_STRING \
    --table ATLAS_DEFT.T_PRODUCTION_TASK \
    --where "UPDATE_TIME between TO_DATE('$1','YYYY-MM-DD HH24:MI:SS') and TO_DATE('$2','YYYY-MM-DD HH24:MI:SS') " \
    --username $JOB_ORACLE_USER --password $JOB_ORACLE_PASS \
    -m 1 --as-avrodatafile --target-dir /atlas/analytics/DEFT_temp \
    --columns TASKID,OUTPUT_FORMATS \
    --map-column-java TASKID=Long,OUTPUT_FORMATS=String

echo "DONE"