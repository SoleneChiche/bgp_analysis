import os
import csv
import json

jsonBurstNames = []
dirListing = os.listdir(os.getcwd())

for file in dirListing:
    if file[0].isdigit():
        fileDir = os.listdir(file)
        for burst in fileDir:
            if not burst.endswith('.json'):
                f = open(file+'/'+burst, 'rb')  # open in binary mode (see the doc)
                reader = csv.reader(f, delimiter=',')
                graphpoints = {}
                for row in reader:
                    time = row[0]
                    if time not in graphpoints:
                        graphpoints[time] = 0
                    graphpoints[time] += 1
                with open(file + '/' + burst + '-graph.json', 'w') as outfile:
                    jsonBurstNames.append(file+'/'+burst+'-graph.json')
                    json.dump(graphpoints, outfile, indent=2)

jsondata = json.dumps(jsonBurstNames, indent=2)
fd = open('json_burst_names.json', 'w')
fd.write(jsondata)
fd.close()
