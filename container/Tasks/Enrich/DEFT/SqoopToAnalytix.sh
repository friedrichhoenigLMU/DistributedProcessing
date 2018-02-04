#!/bin/bash

echo "  *******************************  Sqooping DEFT table  *******************************"
echo " after $1 "


sqoop import \
    -D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
    --direct \
    --connect $JOB_ORACLE_CONNECTION_STRING \
    --table ATLAS_DEFT.T_PRODUCTION_TASK \
    # --query "select TASKID, OUTPUT_FORMATS from ATLAS_DEFT.T_PRODUCTION_TASK  WHERE TIMESTAMP > TO_DATE('$1','YYYY-MM-DD HH24:MI:SS')  AND \$CONDITIONS " \
    --where "TIMESTAMP > TO_DATE('$1','YYYY-MM-DD HH24:MI:SS') " \
    --username $JOB_ORACLE_USER --password $JOB_ORACLE_PASS \
    -m 2 --target-dir /atlas/analytics/DEFT_temp \
    --columns TASKID,OUTPUT_FORMATS \
    --map-column-java TASKID=Long,OUTPUT_FORMATS=String

echo "DONE"