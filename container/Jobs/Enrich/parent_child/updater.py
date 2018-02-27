#!/usr/bin/env python3

# this code updates child_ids info on all jobs that have been retried

from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import scan, bulk

import pandas as pd

es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200}], timeout=60)

INDEX = 'jobs'
CH_SIZE = 100000


def getIndices(min_pid, max_pid):
    # find oldest and newest index that should be scanned

    min_limit_q = {
        "size": 1,
        'query': {
            "range": {
                "pandaid": {
                    "lte": int(min_pid),
                    "gt": 0
                }
            }
        },
        "sort": [{"pandaid": {"order": "desc"}}]
    }

    r_min = es.search(index=INDEX, body=min_limit_q)
    min_index = r_min['hits']['hits'][0]['_index']

    max_limit_q = {
        "size": 1,
        'query': {
            "range": {
                "pandaid": {
                    "lte": int(max_pid + 10E6),
                    "gt": int(max_pid)
                }
            }
        },
        "sort": [{"pandaid": {"order": "asc"}}]
    }

    r_min = es.search(index=INDEX, body=max_limit_q)
    max_index = r_min['hits']['hits'][0]['_index']

    print("limit indices: ", min_index, max_index)

    # get relevant job indices
    indices = es.cat.indices(index=INDEX, h="index").split('\n')
    indices = sorted(indices)
    indices = [x for x in indices if x != '']

    selected_indices = []
    acc = False
    for i in indices:
        if i == min_index:
            acc = True
        if i == max_index:
            acc = False
        if i == min_index or i == max_index or acc == True:
            selected_indices.append(i)

    job_indices = ''
    job_indices = ','.join(selected_indices)
    print(job_indices)
    return job_indices


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

# sort according to raising old_pid.
df.sort_values(by='old_pid', inplace=True)

# make large chunks
chunks = []
df_size = df.shape[0]
for i in range(0, df_size, CH_SIZE):
    chunks.append(df[i:min(i + CH_SIZE, df_size)])

print('Total chunks:', len(chunks))

for ch in chunks:
    job_indices = getIndices(ch.old_pid.min(), ch.old_pid.max())

    # make index to be old_pid
    ch.set_index("old_pid", inplace=True)

    job_query = {
        "size": 0,
        "_source": ["_id"],
        'query': {
            'bool': {'must_not': [{"term": {"jobstatus": "finished"}}]}
        }
    }

    jobs = []
    scroll = scan(client=es, index=job_indices, query=job_query, scroll='5m', timeout="5m", size=10000)
    count = 0

    # looping over all jobs in all these indices

    for res in scroll:
        count += 1
        if not count % 100000:
            print('read:', count)
            exec_update(jobs, ch)
            jobs = []
        # print(res)
        jobs.append({"pid": int(res['_id']), "ind": res['_index']})
        # if count%5 == 1: exec_update(jobs)

    exec_update(jobs, ch)
    jobs = []

print("All done.")
