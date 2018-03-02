#!/bin/bash

echo "  *******************************  Sqooping DEFT table  *******************************"
echo " after $1 "


sqoop import \
    -D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
    --direct \
    --connect $JOB_ORACLE_CONNECTION_STRING \
    --query "select TASKID, OUTPUT_FORMATS from ATLAS_DEFT.T_PRODUCTION_TASK  WHERE TIMESTAMP > TO_DATE('$1','YYYY-MM-DD HH24:MI:SS') AND OUTPUT_FORMATS IS NOT NULL AND \$CONDITIONS " \
    --username $JOB_ORACLE_USER --password $JOB_ORACLE_PASS \
    -m 1 --target-dir /atlas/analytics/DEFT_temp \
    --map-column-java TASKID=Long,OUTPUT_FORMATS=String

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc
fi

echo "DONE"