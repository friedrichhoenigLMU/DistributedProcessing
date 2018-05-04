from __future__ import division
#from pig_util import outputSchema

from datetime import datetime, timedelta


statuscodes = {
    "failed": 'x',
    "defined": 'd',
    "holding": 'h',
    "merging": 'm',
    "pending": 'p',
    "running": 'r',
    "activated": 'a',
    "cancelled": 'c',
    "transferring": 't',
    "sent": 'e',
    "closed": 'o',
    "assigned": 'i',
                "finished": 'f',
                "starting": 's',
                "waiting": 'w',
}


def total_seconds(td):
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6


def toDatetime(timestr):
    split_time = timestr.split('.')
    date = datetime.strptime(split_time[0], "%Y-%m-%d %H:%M:%S")
    if len(split_time) > 1:
        date += timedelta(seconds=float('.' + split_time[1]))

    return date

# def toDatetime(timestr):
#     try:
#         return datetime.strptime(timestr, "%Y-%m-%d %H:%M:%S.%f")
#     except ValueError:
#         return datetime.strptime(timestr, "%Y-%m-%d %H:%M:%S")


@outputSchema('stats:tuple(PANDAID:long, jobstatus_start:chararray, jobstatus_end:chararray, path:chararray, time_start:chararray, time_end:chararray, failed:float, defined:float, holding:float, merging:float, pending:float, running:float, activated:float, cancelled:float, transferring:float, sent:float, closed:float, assigned:float, finished:float, starting:float, waiting:float)')
def make_times_path(arg):
    res = {
        "failed": 0.,
        "defined": 0.,
        "holding": 0.,
        "merging": 0.,
        "pending": 0.,
        "running": 0.,
        "activated": 0.,
        "cancelled": 0.,
        "transferring": 0.,
        "sent": 0.,
        "closed": 0.,
        "assigned": 0.,
        "finished": 0.,
        "starting": 0.,
        "waiting": 0.,
    }

    pandaid, modificationtime_last, jobstatus_last = arg[0]
    jobstatus_start = jobstatus_last
    time_start = modificationtime_last
    time_end = modificationtime_last

    modificationtime_last = toDatetime(modificationtime_last)
    for event in arg[1:]:
        pandaid, modificationtime, jobstatus = event
        time_end = modificationtime
        modificationtime = toDatetime(modificationtime)
        elapsed_time = total_seconds(modificationtime - modificationtime_last)
        res[jobstatus_last] += elapsed_time
        modificationtime_last, jobstatus_last = modificationtime, jobstatus

    path = ''.join(statuscodes[i[2]] for i in arg)

    if not '.' in time_start:
        time_start += '.000000'
    if not '.' in time_end:
        time_end += '.000000'

    return (
        pandaid,
        jobstatus_start,
        jobstatus_last,
        path,
        time_start,
        time_end,
        res["failed"] or None,
        res["defined"] or None,
        res["holding"] or None,
        res["merging"] or None,
        res["pending"] or None,
        res["running"] or None,
        res["activated"] or None,
        res["cancelled"] or None,
        res["transferring"] or None,
        res["sent"] or None,
        res["closed"] or None,
        res["assigned"] or None,
        res["finished"] or None,
        res["starting"] or None,
        res["waiting"] or None,

    )


@outputSchema('stats:tuple(PANDAID:long, jobstatus_start:chararray, jobstatus_end:chararray, path:chararray, time_start:chararray, time_end:chararray, failed:float, defined:float, holding:float, merging:float, pending:float, running:float, activated:float, cancelled:float, transferring:float, sent:float, closed:float, assigned:float, finished:float, starting:float, waiting:float)')
def mergeSummaries(arg):
    if len(arg) > 2:
        raise Exception("can only merge two records" + repr(arg))
    if len(arg) == 1:
        return arg

    start_time0, start_time1 = toDatetime(arg[0][4]), toDatetime(arg[1][4])
    if start_time0 > start_time1:
        first, second = list(arg[1]), list(arg[0])
    else:
        first, second = list(arg[0]), list(arg[1])

    for x in [first, second]:
        for i, v in enumerate(x):
            x[i] = v or 0.

    pandaid = first[0]
    jobstatus_start = first[1]
    jobstatus_last = second[2]
    path = first[3] + second[3][1:]
    time_start = first[4]
    time_end = second[5]
    failed = first[6] + second[6]
    defined = first[7] + second[7]
    holding = first[8] + second[8]
    merging = first[9] + second[9]
    pending = first[10] + second[10]
    running = first[11] + second[11]
    activated = first[12] + second[12]
    cancelled = first[13] + second[13]
    transferring = first[14] + second[14]
    sent = first[15] + second[15]
    closed = first[16] + second[16]
    assigned = first[17] + second[17]
    finished = first[18] + second[18]
    starting = first[19] + second[19]
    waiting = first[20] + second[20]

    return (
        pandaid,
        jobstatus_start,
        jobstatus_last,
        path,
        time_start,
        time_end,
        failed or None,
        defined or None,
        holding or None,
        merging or None,
        pending or None,
        running or None,
        activated or None,
        cancelled or None,
        transferring or None,
        sent or None,
        closed or None,
        assigned or None,
        finished or None,
        starting or None,
        waiting or None,
    )
