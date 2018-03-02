#!/bin/bash

echo "  *******************************  sqooping JEDI_JOB_RETRY_HISTORY table  *******************************"
echo " between $1 and $2 "

sqoop import --direct \
    --connect $JOB_ORACLE_ADG_CONNECTION_STRING \
    --username $JOB_ORACLE_USER --password $JOB_ORACLE_PASS \
    --query "select OLDPANDAID, NEWPANDAID, RELATIONTYPE from ATLAS_PANDA.JEDI_JOB_RETRY_HISTORY WHERE INS_UTC_TSTAMP between TO_DATE('$1 00:00:00','YYYY-MM-DD HH24:MI:SS') and TO_DATE('$2 00:00:00','YYYY-MM-DD HH24:MI:SS') AND \$CONDITIONS " \
    -m 1  --target-dir /atlas/analytics/job_parents_temp

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with sqoop. Exiting."
    exit $rc; 
fi

echo "DONE"