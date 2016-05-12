import csv
import json
import sys

start = sys.argv[1]
stop = sys.argv[2]
window = 60
graph_points = {}
count_a_window = 'Count of A last ' + str(window) + ' s'
count_w_window = 'Count of W last ' + str(window) + ' s'
peer_names_list = []


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

with open('result-rrc00-rrc01-rrc02.csv', 'rb') as csv_file:
    data = csv.reader(csv_file, delimiter=',')
    for row in data:
        if row[0] == 'peer':
            continue
        if row[0] != 'prefixA' and row[0] != 'prefixW':
            peer = row[0]
            timestamp = int(row[1])
            a = int(row[2])
            w = int(row[3])
            if peer not in graph_points:
                graph_points[peer] = {}
            if timestamp not in graph_points[peer]:
                graph_points[peer][timestamp] = {'A': 0, 'W': 0, 'prefixA': [], 'prefixW': []}

            graph_points[peer][timestamp]['A'] += a
            graph_points[peer][timestamp]['W'] += w
        elif row[0] == 'prefixA':
            graph_points[peer][timestamp]['prefixA'].extend(row)
            graph_points[peer][timestamp]['prefixA'].remove('prefixA')
        elif row[0] == 'prefixW':
            graph_points[peer][timestamp]['prefixW'].extend(row)
            graph_points[peer][timestamp]['prefixW'].remove('prefixW')
        else:
            print 'An error occurred'

result = {}
for peer in graph_points:
    result[peer] = {}
    for timestamp in graph_points[peer]:
        val_a = count_window(timestamp, graph_points, peer, 'A')
        val_w = count_window(timestamp, graph_points, peer, 'W')
        if val_a > 1000 or val_w > 1000:
            result[peer][timestamp] = {count_a_window: val_a, count_w_window: val_w}
            if val_a > 5000 or val_w > 5000:
                burst_file = open('burst-'+peer+'-'+str(timestamp)+'.csv', 'w')
                for i in range(timestamp - window, timestamp + window):
                    if i in graph_points[peer]:
                        burst_file.write(peer + ',' + str(i) + ',' + str(graph_points[peer][i]['A']) + ',' +
                                         str(graph_points[peer][i]['W']) + '\n')
                        burst_file.write('prefixA' + ',' + ','.join(graph_points[peer][i]['prefixA']) + '\n')
                        burst_file.write('prefixW' + ',' + ','.join(graph_points[peer][i]['prefixW']) + '\n')
                    else:
                        pass
        else:
            pass
    if result[peer] == {}:
        del result[peer]
    else:
        with open(peer.replace(':', '.')+'.json', 'w') as outfile:
            json.dump(result[peer], outfile, indent=2)
            peer_names_list.append(peer.replace(':', '.')+'.json')

with open('json_file_names.json', 'w') as outfile:
    json.dump(peer_names_list, outfile, indent=2)

