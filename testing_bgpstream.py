#!/usr/bin/env python
import json
import loadData

start = 1438416000
stop = 1438417000

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

records = loadData.load_data(start, stop)

for key in records:
    graph_points[key] = {}
    for x in range(start, stop):
        if x in records[key]:
            graph_points[key][x] = {count_a_window: countAlast10s(x, records, key),
                                    count_w_window: countWlast10s(x, records, key)}



with open('data.json', 'w') as outfile:
    json.dump(graph_points, outfile, indent=2)
