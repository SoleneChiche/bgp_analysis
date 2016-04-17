#!/usr/bin/env python
import json
from _pybgpstream import BGPStream, BGPRecord

# Create a new bgpstream instance and a reusable bgprecord instance
stream = BGPStream()
rec = BGPRecord()
start = 1438416000
stop = 1438417000
# Consider RIPE RRC 10 only
stream.add_filter('collector', 'rrc10')

# Consider this time interval:
# Sat Aug  1 08:20:11 UTC 2015
stream.add_interval_filter(start, stop)

# Start the stream
stream.start()

# Get next record
records = {}
graph_points = {}
window = 100
count_a = 'Count of A updates'
count_a_window = 'Count of A last ' + str(window) + ' s'
count_w = 'Count of W updates'
count_w_window = 'Count of W last ' + str(window) + ' s'

def countAlast10s(index, records, peer_address):
    cntA = 0
    for i in range(index-window, index):
        if i in records[peer_address].keys():
            cntA += records[peer_address][i][count_a]
        else:
            pass
    return cntA

def countWlast10s(index, records, peer_address):
    cntW = 0
    for i in range(index - window, index):
        if i in records[peer_address].keys():
            cntW += records[peer_address][i][count_w]
        else:
            pass
    return cntW

while stream.get_next_record(rec):
    # Print the record information only if it is not a valid record
    if rec.status != "valid":
        print rec.project, rec.collector, rec.type, rec.time, rec.status
    else:
        elem = rec.get_next_elem()
        while elem:
            if elem.peer_address not in records:
                if elem.type == 'A':
                    records[elem.peer_address] = {rec.time: {count_a: 1, count_w: 0}}
                elif elem.type == 'W':
                    records[elem.peer_address] = {rec.time: {count_a: 0, count_w: 1}}
            elif elem.peer_address in records:
                if rec.time not in records[elem.peer_address]:
                    if elem.type == 'A':
                        records[elem.peer_address][rec.time] = {count_a: 1, count_w: 0}
                    elif elem.type == 'W':
                        records[elem.peer_address][rec.time] = {count_a: 0, count_w: 1}
                elif rec.time in records[elem.peer_address]:
                    if elem.type == 'A':
                        records[elem.peer_address][rec.time][count_a] += 1
                    elif elem.type == 'W':
                        records[elem.peer_address][rec.time][count_w] += 1
            elem = rec.get_next_elem()

for key in records:
    graph_points[key] = {}
    for x in range(start, stop):
        if x in records[key]:
            graph_points[key][x] = {count_a_window: countAlast10s(x, records, key),
                                    count_w_window: countWlast10s(x, records, key)}



with open('data.json', 'w') as outfile:
    json.dump(graph_points, outfile, indent=2)
