REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER '/elasticsearch-hadoop/elasticsearch-hadoop-pig.jar';

SET default_parallel 5;
SET pig.noSplitCombination TRUE;

define EsStorage org.elasticsearch.hadoop.pig.EsStorage(
    'es.nodes=http://es-head01.mwt2.org,http://es-head02.mwt2.org,http://es-head03.mwt2.org',
    'es.port=9200',
    'es.mapping.id=jeditaskid',
    'es.http.timeout=5m',
    'es.batch.size.entries=1000'
    );


PAN = LOAD '/atlas/analytics/tasks_temp' USING AvroStorage();
--DESCRIBE PAN;2016-04-28_20

--L = LIMIT PAN 1000; --dump L;

REC = FOREACH PAN GENERATE  
    JEDITASKID as jeditaskid, 
    TASKNAME as taskname, 
    STATUS as status, 
    USERNAME as username, 
    REQID as reqid, OLDSTATUS as oldstatus, CLOUD as cloud, SITE as site, 
    PRODSOURCELABEL as prodsourcelabel, WORKINGGROUP as workinggroup, VO as vo, 
    CORECOUNT as corecount, TASKTYPE as tasktype, PROCESSINGTYPE as processingtype, 
    TASKPRIORITY as taskpriority, CURRENTPRIORITY as currentpriority, 
    ARCHITECTURE as architecture, TRANSUSES as transuses, TRANSHOME as transhome, TRANSPATH as transpath, 
    LOCKEDBY as lockedby, TERMCONDITION as termcondition, SPLITRULE as splitrule, 
    WALLTIME as walltime, WALLTIMEUNIT as walltimeunit, 
    CPUTIME as cputime, CPUTIMEUNIT as cputimeunit, 
    OUTDISKCOUNT as outdiskcount, OUTDISKUNIT as outdiskunit, 
    WORKDISKCOUNT as workdiskcount, WORKDISKUNIT as workdiskunit, 
    RAMCOUNT as ramcount, RAMUNIT as ramunit, 
    IOINTENSITY as iointensity, IOINTENSITYUNIT as iointensityunit, 
    WORKQUEUE_ID as workqueue_id, PROGRESS as progress, FAILURERATE as failurerate, ERRORDIALOG as errordialog, 
    COUNTRYGROUP as countrygroup, PARENT_TID as parent_tid, EVENTSERVICE as eventservice, 
    TICKETID as ticketid, TICKETSYSTEMTYPE as ticketsystemtype, 
    SUPERSTATUS as superstatus, CAMPAIGN as campaign, 
    MERGERAMCOUNT as mergeramcount, MERGERAMUNIT as mergeramunit, 
    MERGEWALLTIME as mergewalltime, MERGEWALLTIMEUNIT as mergewalltimeunit, 
    MERGECORECOUNT as mergecorecount, 
    GOAL as goal, NUMTHROTTLED as numthrottled, 
    CPUEFFICIENCY as cpuefficiency, BASEWALLTIME as basewalltime, AMIFLAG_OLD as amiflag_old, 
    AMIFLAG as amiflag, NUCLEUS as nucleus, BASERAMCOUNT as baseramcount, 
    LOCKEDTIME as lockedtime, 
    STATECHANGETIME as statechangetime, 
    THROTTLEDTIME as throttledtime, 
    ASSESSMENTTIME as assessmenttime, 
    CREATIONDATE as creationdate, 
    MODIFICATIONTIME as modificationtime, 
    STARTTIME as starttime, 
    ENDTIME as endtime, 
    FROZENTIME as frozentime, 
    RESCUETIME as rescuetime, 
    TTCPREDICTIONDATE as ttcpredictiondate, 
    TTCREQUESTED as ttcrequested, TTCPREDICTED as ttcpredicted, 
    REQUESTTYPE as requesttype, GSHARE as gshare, USEJUMBO as usejumbo, 
    RESOURCE_TYPE as resource_type, 
    DISKIO as diskio, DISKIOUNIT as diskiounit,
    ToString(ToDate(CREATIONDATE),'yyyy') as indexName;

STORE REC INTO 'tasks_archive_{indexName}/task_data' USING EsStorage();
