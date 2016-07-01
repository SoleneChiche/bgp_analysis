# 1: loop through all bursts bursts
# 2: create table, each prefix:
# - size of the bursts containing it
# - number of burst A/W in which it appears

# 3: take one peer
# -> compare each burst to the other bursts for that peer and calculate in percentage their similarities
# return table with the percentages {peer: {burstx:{bursty: 10%, burstz = 20%, etc...}}
import os
import csv
import json

prefixTable = {}
allPrefixes = []
dirListing = os.listdir(os.getcwd())

for peer in dirListing:
    if peer[0].isdigit():
        fileDir = os.listdir(peer)
        for burst in fileDir:
            if not burst.endswith('.json'):
                f = open(peer + '/' + burst, 'rb')  # open in binary mode (see the doc)
                reader = csv.reader(f, delimiter=',')
                burst_size = sum(1 for row in reader)
                f.seek(0)
                for row in reader:
                    if len(row) > 1:
                        prefix = row[1]
                        if prefix not in prefixTable:
                            allPrefixes.append(prefix)
                            prefixTable[prefix] = {'numberOfABursts': 0, 'numberOfWBursts': 0}
                        if 'burstA' in burst:
                            prefixTable[prefix]['numberOfABursts'] += 1
                        elif 'burstW' in burst:
                            prefixTable[prefix]['numberOfWBursts'] += 1
                f.close()

print len(allPrefixes)
writer = csv.writer(open('allPrefixes.csv', 'a'),
                    delimiter=',', lineterminator='\n')
for x in allPrefixes:
    writer.writerow([x])

with open('results-similarities.json', 'w') as outfile:
    json.dump(prefixTable, outfile, indent=2)
