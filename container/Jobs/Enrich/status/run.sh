#!/bin/bash

echo "  *******************************  importing job status table  *******************************"

startDate=$(date +%Y-%m-%d -d "-4day")
endDate=$(date +%Y-%m-%d  -d "-3day")

echo "start date: ${startDate}"
echo "end date: ${endDate}"

./SqoopToAnalytix.sh ${startDate} ${endDate}

echo "Sqooping DONE."

# pig -4 log4j.properties -f StatusToESuc.pig -param INPD=${fileName} -param ININD=${ind}

echo "DONE"

