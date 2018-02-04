#!/usr/bin/env python3

# this code updates tasks ES docs with DEFT data

import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk
import pandas as pd

INFILE = sys.argv[1]
print("processing input file", INFILE)

df = pd.read_csv(INFILE, header=None, names=['taskid', 'output_formats'])
print(df.head())

# find min taskid
min_tid = df.taskid.min()

# make index to be old_pid
df.set_index("taskid", inplace=True)

# find oldest and newest index that should be scanned
es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200}], timeout=60)

min_limit_q = {
    "size": 1,
    'query': {
        "range": {
            "jeditaskid": {
                "lte": int(min_tid),
                "gt": 0
            }
        }
    },
    "sort": [{"jeditaskid": {"order": "desc"}}]
}

r_min = es.search(index="tasks_archive_*", body=min_limit_q)
min_index = r_min['hits']['hits'][0]['_index']

print("min index: ", min_index)

# get relevant job indices
indices = es.cat.indices(index="tasks_archive_*", h="index", request_timeout=600).split('\n')
indices = sorted(indices)
indices = [x for x in indices if x != '']

selected_indices = []
acc = False
for i in indices:
    if i == min_index:
        acc = True
    if acc == True:
        selected_indices.append(i)

task_indices = ''
task_indices = ','.join(selected_indices)
print(task_indices)

task_query = {
    "size": 0,
    "_source": ["_id"],
    'query': {
        "match_all": {}
        # maybe loop over over not finished jobs
        # 'bool': {'must_not': [{"term": {"jobstatus": "finished"}}]}
    }

}


def exec_update(tasks):
    global df
    jdf = pd.DataFrame(tasks)
    jdf.set_index('taskid', inplace=True)
    # print(jdf.head())
    return
    jc = jdf.join(df).dropna()
    print('parent:', df.shape[0], '\t jobs:', jdf.shape[0], 'jcleaned:', jc.shape[0])
    jcg = jc.groupby(jc.index)
    cnts = jcg.count()
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
        d = {
            '_op_type': 'update',
            '_index': v[0],
            '_type': 'jobs_data',
            '_id': int(k),
            'doc': {'child_ids': v[1]}
        }
        data.append(d)

    res = bulk(client=es, actions=data, stats_only=True, timeout="5m")
    print(res)


tasks = []
scroll = scan(client=es, index=task_indices, query=task_query, scroll='5m', timeout="5m", size=10000)
count = 0

for res in scroll:
    count += 1
    if not count % 100000:
        #print(' selected:', count)
        exec_update(tasks)
        jobs = []
    # print(res)
    tasks.append({"taskid": int(res['_id']), "ind": res['_index']})
    #if count%5 == 1: exec_update(jobs)

exec_update(tasks)
tasks = []
