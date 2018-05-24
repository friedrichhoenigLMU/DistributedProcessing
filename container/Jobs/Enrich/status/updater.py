#!/usr/bin/env python3

# this code updates child_ids info on all jobs that have been retried

from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import scan, bulk
import glob
import pandas as pd

#es = Elasticsearch([{'host': 'localhost', 'port': 9200}], timeout=60)
es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200}], timeout=60)


INDEX = 'jobs_archive_*'
CH_SIZE = 250000


def exec_update(jobs, new):
    old = pd.DataFrame(jobs, columns=['ind', 'PANDAID'] + fields).set_index('PANDAID')
    old = old[~old.index.duplicated(keep='last')] # drop duplicate jobs in jobs_archive
    new["ind"] = old["ind"]  # get index before dropping empty entries
    old = old[old.js_start.notnull()].fillna(0.0)  # filter out entries that have never been updated
    old['js_path'] = old['js_path'].astype('str')  # stupid woraround
    old['js_end'] = old['js_end'].astype('str')  # stupid woraround

    new = new.fillna(0.0)
    #old.to_pickle("old.pickle")
    #new.to_pickle("new.pickle")

    dur_fields = ['js_failed', 'js_defined', 'js_holding',
                  'js_merging', 'js_pending', 'js_running', 'js_activated',
                  'js_cancelled', 'js_transferring', 'js_sent', 'js_closed',
                  'js_assigned', 'js_finished', 'js_starting', 'js_waiting']

    # add durations
    for field_name in dur_fields:
        new[field_name] = new[field_name].add(old[field_name], fill_value=0.0)

    # calculate time between records
    for field_name in dur_fields:
        field_filter = old.js_end == field_name
        delta = new.js_first_state_time.astype('datetime64[ns]') - old[field_filter].js_last_state_time.astype('datetime64[ns]')
        delta = delta.dt.total_seconds().dropna()
        new[field_name] = new[field_name].add(delta, fill_value=0.0)

    new['js_start'].update(old["js_start"])
    new['js_first_state_time'].update(old["js_first_state_time"])
    new['js_path'] = old["js_path"].add(new["js_path"], fill_value='')

    #new["ind"] = INDEX
    # new["ind"].update(old["ind"])
    #new['ind'] = old['ind']
    # print(new.head())

    data = []
    for PANDAID, row in new.iterrows():
        if not row['ind']:  # if jobs not yet in ES
            continue        # skip job
        data.append({
            '_op_type': 'update',
            '_index': row['ind'],
            '_type': 'jobs_data',
            '_id': int(PANDAID),
            'doc': {field: row[field] for field in fields if row[field]}
        })
        

    res = bulk(client=es, actions=data, stats_only=True, timeout="5m")
    print("updated:", res[0], "  issues:", res[1])
    return


df = pd.read_csv('/tmp/job_status_temp.csv', header=None, names=['PANDAID', 'js_start', 'js_end', 'js_path', 'js_first_state_time', 'js_last_state_time', 'js_failed', 'js_defined', 'js_holding',
                                                                 'js_merging', 'js_pending', 'js_running', 'js_activated', 'js_cancelled', 'js_transferring', 'js_sent', 'js_closed', 'js_assigned', 'js_finished', 'js_starting', 'js_waiting'])


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

fields = ['js_start', 'js_end', 'js_path', 'js_first_state_time', 'js_last_state_time', 'js_failed', 'js_defined', 'js_holding',
          'js_merging', 'js_pending', 'js_running', 'js_activated', 'js_cancelled', 'js_transferring', 'js_sent', 'js_closed', 'js_assigned', 'js_finished', 'js_starting', 'js_waiting']

for i in range(gl_min, gl_max + 1, CH_SIZE):

    loc_min = i
    loc_max = min(gl_max + 1, loc_min + CH_SIZE)
    print('chunk:', loc_min, '-', loc_max)

    ch = df[(df['PANDAID'] >= loc_min) & (df['PANDAID'] < loc_max)]
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
                        "pandaid": {"gte": int(ch.PANDAID.min()), "lte": int(ch.PANDAID.max())}
                    }
                }]
                # ,
                # 'must_not': [{"term": {"jobstatus": "js_finished"}}]
            }
        }
    }

    ch.set_index("PANDAID", inplace=True)
    print("Starting to scroll")
    jobs = []
    scroll = scan(client=es, index=INDEX, query=job_query, scroll='5m', timeout="5m", size=10000)

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
