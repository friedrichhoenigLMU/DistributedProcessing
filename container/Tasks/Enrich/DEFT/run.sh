#!/bin/bash

echo "  *******************************  Updating task table with DEFT info  *******************************"

startDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-2hour")
ind=$(date -u '+%Y-%m' -d "-2hour")
echo "start date: ${startDate}"
echo "index : ${ind}"
./SqoopToAnalytix.sh "${startDate}"
echo "Sqooping DONE."

# pig -4 log4j.properties -f JobsToESuc.pig -param -param ININD=${ind}
echo "Updating UC DONE."