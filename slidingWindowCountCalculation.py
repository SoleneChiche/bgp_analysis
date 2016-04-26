import csv
import json
import sys

start = sys.argv[1]
stop = sys.argv[2]
window = 120
graph_points = {}
count_a_window = 'Count of A last ' + str(window) + ' s'
count_w_window = 'Count of W last ' + str(window) + ' s'
list_data = []


def count_window(index, points, peer, type):
    cnt = 0
    if type == 'A':
        for i in range(index - window, index):
            if i in points[peer]:
                cnt += points[peer][i]['A']
            else:
                pass
    else:
        for i in range(index - window, index):
            if i in points[peer]:
                cnt += points[peer][i]['W']
            else:
                pass
    return cnt


with open('result.csv', 'rb') as csv_file:
    data = csv.reader(csv_file, delimiter=',')
    for row in data:
        for value in list(data):
            peer = value[0]
            timestamp = int(value[1])
            a = int(value[2])
            w = int(value[3])
            if peer not in graph_points:
                graph_points[peer] = {}
            if timestamp not in graph_points[peer]:
                graph_points[peer][timestamp] = {'A': 0, 'W': 0}

            graph_points[peer][timestamp]['A'] += a
            graph_points[peer][timestamp]['W'] += w
result = {}
for peer in graph_points:
    result[peer] = {}
    for timestamp in graph_points[peer]:
        val_a = count_window(timestamp, graph_points, peer, 'A')
        val_w = count_window(timestamp, graph_points, peer, 'W')
        if val_a > 500 or val_w > 500:
            result[peer][timestamp] = {count_a_window: val_a, count_w_window: val_w}
        else:
            pass

with open('result.json', 'w') as outfile:
    json.dump(result, outfile, indent=2)
