#!/usr/bin/env python3

# this code updates child_ids info on all jobs that have been retried

from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import scan, bulk
import glob
import pandas as pd

es = Elasticsearch([{'host': 'localhost', 'port': 9200}], timeout=60)
#es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200}], timeout=60)


INDEX = 'job_states_*'
CH_SIZE = 250000


def exec_update(jobs, new):
    old = pd.DataFrame(jobs, columns=['ind', 'PANDAID'] + fields).set_index('PANDAID')
    new["ind"] = old["ind"]  # get index before dropping empty entries
    old = old[old.jobstatus_start.notnull()].fillna(0.0)  # filter out entries that have never been updated
    old['path'] = old['path'].astype('str')  # stupid woraround
    old['jobstatus_end'] = old['jobstatus_end'].astype('str')  # stupid woraround

    new = new.fillna(0.0)
    old.to_pickle("old.pickle")
    new.to_pickle("new.pickle")

    dur_fields = ['failed', 'defined', 'holding',
                  'merging', 'pending', 'running', 'activated',
                  'cancelled', 'transferring', 'sent', 'closed',
                  'assigned', 'finished', 'starting', 'waiting']

    # add durations
    for field_name in dur_fields:
        new[field_name] = new[field_name].add(old[field_name], fill_value=0.0)

    # calculate time between records
    for field_name in dur_fields:
        field_filter = old.jobstatus_end == field_name
        delta = new.first_state_time.astype('datetime64[ns]') - old[field_filter].last_state_time.astype('datetime64[ns]')
        delta = delta.dt.total_seconds().dropna()
        new[field_name] = new[field_name].add(delta, fill_value=0.0)

    new['jobstatus_start'].update(old["jobstatus_start"])
    new['first_state_time'].update(old["first_state_time"])
    new['path'] = old["path"].add(new["path"], fill_value='')

    #new["ind"] = INDEX
    # new["ind"].update(old["ind"])
    #new['ind'] = old['ind']
    # print(new.head())

    data = []
    for PANDAID, row in new.iterrows():
        data.append({
            '_op_type': 'update',
            '_index': row['ind'],
            '_type': 'job_state_data',
            '_id': int(PANDAID),
            'doc': {field: row[field] for field in fields if row[field]}
        })

    res = bulk(client=es, actions=data, stats_only=True, timeout="5m")
    print("updated:", res[0], "  issues:", res[1])
    return


df = pd.read_csv('/tmp/job_status_temp.csv', header=None, names=['PANDAID', 'jobstatus_start', 'jobstatus_end', 'path', 'first_state_time', 'last_state_time', 'failed', 'defined', 'holding',
                                                                 'merging', 'pending', 'running', 'activated', 'cancelled', 'transferring', 'sent', 'closed', 'assigned', 'finished', 'starting', 'waiting'])


print('jobs found in the file:', df.PANDAID.count())


# # leave only retries
# df = df[df.relation_type == 'retry']
# del df['relation_type']

# print('jobs to be updated:', df.old_pid.count())

# sort according to raising old_pid.
df.sort_values(by='PANDAID', inplace=True)

gl_min = df.PANDAID.min()
gl_max = df.PANDAID.max()
print("gl_min: {}, gl_max: {}".format(gl_min, gl_max))
count = 0

fields = ['jobstatus_start', 'jobstatus_end', 'path', 'first_state_time', 'last_state_time', 'failed', 'defined', 'holding',
          'merging', 'pending', 'running', 'activated', 'cancelled', 'transferring', 'sent', 'closed', 'assigned', 'finished', 'starting', 'waiting']

for i in range(gl_min, gl_max + 1, CH_SIZE):

    loc_min = i
    loc_max = min(gl_max, loc_min + CH_SIZE)
    print('chunk:', loc_min, '-', loc_max)

    ch = df[(df['PANDAID'] >= loc_min) & (df['PANDAID'] <= loc_max)]
    if ch.shape[0] == 0:
        print('skipping chunk')
        continue

    job_query = {
        "size": 0,
        "_source": ["_id"] + fields,
        'query': {
            'bool': {
                'must': [{
                    "range": {
                        "PANDAID": {"gte": int(ch.PANDAID.min()), "lte": int(ch.PANDAID.max())}
                    }
                }]
                # ,
                # 'must_not': [{"term": {"jobstatus": "finished"}}]
            }
        }
    }

    ch.set_index("PANDAID", inplace=True)
    print("Starting to scroll")
    jobs = []
    scroll = scan(client=es, index=INDEX, query=job_query, scroll='5m', timeout="1m", size=100)

    # looping over all jobs in all these indices

    for res in scroll:
        count += 1
        if int(res["_id"]) in ch.index:
            jobs.append(dict({"PANDAID": int(res['_id']), "ind": res['_index']}, **res["_source"]))
        if count % 10000 == 0:
            print('scanned:', count)

    if len(jobs) > 0:
        exec_update(jobs, ch)
        jobs = []
    else:
        print('PROBLEM ... should have seen at least', ch.shape[0], 'jobs')


print("All done.")
