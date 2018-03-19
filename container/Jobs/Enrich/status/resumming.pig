REGISTER 'merge.py'  USING jython as myfuncs;
REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';

REGISTER '/elasticsearch-hadoop/elasticsearch-hadoop-pig.jar';

job_states_new_all= LOAD '/atlas/analytics/job_states/$date' USING AvroStorage() AS (PANDAID: long,MODIFICATIONTIME: chararray,JOBSTATUS: chararray,PRODSOURCELABEL: chararray,CLOUD: chararray,COMPUTINGSITE: chararray, MODIFTIME_EXTENDED:chararray ); 

job_states_new = FOREACH job_states_new_all GENERATE PANDAID, MODIFTIME_EXTENDED, JOBSTATUS;


g = GROUP job_states_new BY PANDAID;
g_sorted = FOREACH g {job_states_sorted = ORDER job_states_new BY MODIFTIME_EXTENDED ASC; GENERATE group, job_states_sorted;  };

out = FOREACH g_sorted GENERATE FLATTEN(myfuncs.make_times_path(job_states_sorted)) AS (PANDAID:long, jobstatus_start:chararray, jobstatus_end:chararray, path:chararray, time_start:chararray, time_end:chararray, failed:float, defined:float, holding:float, merging:float, pending:float, running:float, activated:float, cancelled:float, transferring:float, sent:float, closed:float, assigned:float, finished:float, starting:float, waiting:float) ;

out_date = FOREACH out GENERATE PANDAID, jobstatus_start, jobstatus_end, path, ToDate(time_start, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS first_state_time:datetime, ToDate(time_end, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS last_state_time:datetime, failed, defined, holding, merging, pending, running, activated, cancelled, transferring, sent, closed, assigned, finished, starting, waiting ;

-- l = limit out_date 10;
-- dump l;

STORE out_date INTO 'temp/job_state_data' USING PigStorage(',');
