import os
import csv
import json
import math

jsonBurstNames = []
dirListing = os.listdir(os.getcwd())
lastTime = 0
prop = {}

for peer in dirListing:
    if peer[0].isdigit():
        fileDir = os.listdir(peer)
        for burst in fileDir:
            if not burst.endswith('.json'):
                f = open(peer + '/' + burst, 'rb')  # open in binary mode (see the doc)
                reader = csv.reader(f, delimiter=',')
                burst_size = sum(1 for row in reader)
                f.seek(0)
                perc5 = math.ceil((50.0 / 100) * burst_size)
                perc10 = math.ceil((10.0 / 100) * burst_size)
                perc90 = math.ceil((90.0 / 100) * burst_size)
                perc95 = math.ceil((95.0 / 100) * burst_size)
                tmpP5 = 0.0
                tmpP10 = 0.0
                tmpP90 = 0.0
                tmpP95 = 0.0
                graphPoints = {}
                lastTime = 0
                for row in reader:
                    if not row[0].isdigit():
                        continue
                    time = int(row[0])
                    if time not in graphPoints:
                        if lastTime == 0:
                            graphPoints[time] = 0
                        else:
                            graphPoints[time] = graphPoints[lastTime]
                        lastTime = time
                    graphPoints[time] += 1
                    if graphPoints[time] >= perc5 and tmpP5 == 0:
                        tmpP5 = time
                    if graphPoints[time] >= perc10 and tmpP10 == 0:
                        tmpP10 = time
                    if graphPoints[time] >= perc90 and tmpP90 == 0:
                        tmpP90 = time
                    if graphPoints[time] >= perc95 and tmpP95 == 0:
                        tmpP95 = time

                if burst not in prop:
                    prop[burst] = {}
                prop[burst]['5-95'] = tmpP95 - tmpP5
                prop[burst]['10-90'] = tmpP90 - tmpP10
                prop[burst]['5-90'] = tmpP90 - tmpP5
                prop[burst]['10-95'] = tmpP95 - tmpP10

                with open(peer + '/' + burst + '-cumulative-graph.json', 'w') as outfile:
                    jsonBurstNames.append(peer + '/' + burst + '-cumulative-graph.json')
                    json.dump(graphPoints, outfile, indent=2)

jsondata = json.dumps(jsonBurstNames, indent=2)
fd = open('json_burst_cumulative_names.json', 'w')
fd.write(jsondata)
fd.close()

with open('propagation_times.json', 'w') as outfile:
    json.dump(prop, outfile, indent=2)
