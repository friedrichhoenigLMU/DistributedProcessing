#!/usr/bin/env python3

# this code updates child_ids info on all jobs that have been retried
import sys
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import scan, bulk

import pandas as pd

es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200}], timeout=60)

INDEX = 'jobs'
CH_SIZE = 250000


def exec_update(jobs, df):
    jdf = pd.DataFrame(jobs)
    jdf.set_index('pid', inplace=True)
    # print(jdf.head())
    jc = jdf.join(df).dropna()
    print(' jobs:', jc.shape[0])

    # jcg = jc.groupby(jc.index)
    # cnts = jcg.count()
    # print("multiples:", cnts[cnts.new_pid>1])

    ma = {}
    for old_pid, row in jc.iterrows():
        ind = row['ind']
        child_id = row['new_pid']
        # print(ind,child_id)
        if old_pid not in ma:
            ma[old_pid] = ['', []]
        ma[old_pid][0] = ind
        ma[old_pid][1].append(int(child_id))

    data = []
    for k, v in ma.items():
        data.append({
            '_op_type': 'update',
            '_index': v[0],
            '_type': 'jobs_data',
            '_id': int(k),
            'doc': {'child_ids': v[1]}
        })

    res = bulk(client=es, actions=data, stats_only=True, timeout="5m")
    print("updated:", res[0], "  issues:", res[1])
    return


df = pd.read_csv('/tmp/job_parents_temp.csv', header=None, names=['old_pid', 'new_pid', 'relation_type'])
print('jobs found in the file:', df.old_pid.count())

# leave only retries
df = df[df.relation_type == 'retry']
del df['relation_type']

print('jobs to be updated:', df.old_pid.count())
if df.old_pid.count()==0:
    print('Since there is nothing to update, will exit now.')
    sys.exit(0)

# sort according to raising old_pid.
df.sort_values(by='old_pid', inplace=True)

gl_min = df.old_pid.min()
gl_max = df.old_pid.max()

count = 0

for i in range(gl_min, gl_max, CH_SIZE):

    loc_min = i
    loc_max = min(gl_max, loc_min + CH_SIZE)
    print('chunk:', loc_min, '-', loc_max)

    ch = df[(df['old_pid'] >= loc_min) & (df['old_pid'] <= loc_max)]
    if ch.shape[0] == 0:
        print('skipping chunk')
        continue

    job_query = {
        "size": 0,
        "_source": ["_id"],
        'query': {
            'bool': {
                'must': [{
                    "range": {
                        "pandaid": {"gte": int(ch.old_pid.min()), "lte": int(ch.old_pid.max())}
                    }
                }],
                'must_not': [{"term": {"jobstatus": "finished"}}]
            }
        }
    }

    # make index to be old_pid
    ch.set_index("old_pid", inplace=True)

    jobs = []
    scroll = scan(client=es, index=INDEX, query=job_query, scroll='5m', timeout="5m", size=10000)

    # looping over all jobs in all these indices

    for res in scroll:
        count += 1
        jobs.append({"pid": int(res['_id']), "ind": res['_index']})
        if count % 1000000 == 0:
            print('scanned:', count)

    if len(jobs) > 0:
        exec_update(jobs, ch)
        jobs = []
    else:
        print('PROBLEM ... should have seen at least', ch.shape[0], 'jobs')


print("All done.")
