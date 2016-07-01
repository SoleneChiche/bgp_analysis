import csv
import os

reference = {}
# Biggest W burst with 548592 different prefixes.
# Indexing is in term of time, so if we pick the first 1000, we will predict the next 100 for example.
i = 0
with open('allPrefixes.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        reference[row[0]] = i
        i += 1

prefixes = []

with open('176.12.110.8/176.12.110.8-1463677673-burstW.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        prefixes.append(row[1])


def ToHotOne(prefixes):
    hotOne = [0] * len(reference)
    for prefix in prefixes:
        index = reference.get(prefix)
        if index is not None:
            hotOne[index] = 1
        else:
            print "Can't resolve %s" % prefix
    return hotOne


def writeHotOne(path, hot_one):
    with open(path, 'w') as csvfile:
        wtr = csv.writer(csvfile, delimiter=',', lineterminator='\n')
        wtr.writerow(['value'])
        for x in hot_one:
          wtr.writerow([x])

if not os.path.exists('176.12.110.8/burstInput'):
    os.makedirs('176.12.110.8/burstInput')

if not os.path.exists('176.12.110.8/burstLabel'):
    os.makedirs('176.12.110.8/burstLabel')

i = 0
in_size = 200
out_size = 10

while i < len(prefixes) - in_size - out_size:
    start = i
    mid = i + in_size
    stop = mid + out_size

    input = ToHotOne(prefixes[start:mid])
    output = ToHotOne(prefixes[mid:stop])

    i = stop

    print "%d / %d" % (i, len(prefixes))

    writeHotOne('176.12.110.8/burstInput/inputVector' + str(start) + '-' + str(mid) + '.csv', input)
    writeHotOne('176.12.110.8/burstLabel/labelVector' + str(mid) + '-' + str(stop) + '.csv', output)
