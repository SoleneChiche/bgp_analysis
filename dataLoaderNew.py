from _pybgpstream import BGPStream, BGPRecord
import sys
import ast
import time
import json
import os
import csv

burst2writeA = {}
burst2writeW = {}
graph_points = {}
THSLD = 5000


def main():
    start = int(sys.argv[1])
    stop = int(sys.argv[2])
    print 'Interval: ' + str(stop-start)
    collectors = ast.literal_eval(sys.argv[3])
    window = int(sys.argv[4])
    s = time.time()
    load_data(start, stop, collectors, window)

    e = time.time()
    print 'Total time: ', (e - s)


def saveGraphPoint(queue, updateType, peer, timestamp, collectors):
    if len(queue) > THSLD:
        if peer not in graph_points:
            fd = open('csv_peernames-'+'-'.join(collectors)+'.csv', 'a')
            fd.write(peer.replace(':', '_')+'/'+peer.replace(':', '_') + '-graph.json' + '\n')
            fd.close()
            graph_points[peer] = {}
        if timestamp not in graph_points[peer]:
            graph_points[peer][timestamp] = {'A': 0, 'W': 0}

        graph_points[peer][timestamp][updateType] = len(queue)


def cleanQueue(queue, timestamp, window):
    if queue and timestamp > queue[-1]['tst']:
        while queue:
            if queue[0]['tst'] < timestamp - window:
                del queue[0]
            else:
                break

def currentBurstTime(burstqueue, peer, timestamp, window):
    if peer not in burstqueue or not burstqueue[peer]:
        return None
    else:
        lasttst = max(burstqueue[peer].keys())
        if timestamp - window > lasttst:
            return None
        return lasttst


def writeBurst(peer, burstqueue, updatetype, timestamp):
    peer_file_name = peer.replace(':', '_')
    if not os.path.exists(peer_file_name):
        os.makedirs(peer_file_name)

    if updatetype == 'A':
        with open(peer_file_name+'/'+peer_file_name+'-'+str(timestamp)+'-burstA.csv', 'a') as f:
            for elem in burstqueue[peer][timestamp]:
                f.write(str(elem['tst']) + ',' + elem['prefix'] + '\n')
    else:
        with open(peer_file_name+'/'+peer_file_name+'-'+str(timestamp)+'-burstW.csv', 'a') as f:
            for elem in burstqueue[peer][timestamp]:
                f.write(str(elem['tst']) + ',' + elem['prefix'] + '\n')


def handleUpdate(queue, burstqueue, update, peer, updatetype, timestamp, window):

    cleanQueue(queue, timestamp, window)
    queue.append(update)
    length = len(queue)

    lasttst = currentBurstTime(burstqueue, peer, timestamp, window)
    # not recording any burst and we are detecting a burst
    # we record the end of the burst
    if lasttst:
        burstqueue[peer][lasttst].append(update)
    # not recording any burst and we are detecting a burst
    elif length > THSLD:
        if peer not in burstqueue:
            burstqueue[peer] = {}
        burstqueue[peer][timestamp] = []
        for elem in queue:
            burstqueue[peer][timestamp].append(elem)
        if len(burstqueue[peer]) > 1:
            writeBurst(peer, burstqueue, updatetype, min(burstqueue[peer]))
            del burstqueue[peer][min(burstqueue[peer])]


def load_data(start, stop, collectors, window):
    peers = {}

    # collectors is a list of the collectors we want to include
    # Start and stop define the interval we are looking in the data

    # Create a new bgpstream instance and a reusable bgprecord instance
    stream = BGPStream()
    rec = BGPRecord()

    # Add filter for each collector.
    # If no collector is mentioned, it will consider all of them
    if collectors:
        for collector in collectors:
            print collector
            stream.add_filter('collector', collector)
    else:
        for i in range(0, 10):
            stream.add_filter('collector', 'rrc0' + str(i))
        for i in range(10, 15):
            stream.add_filter('collector', 'rrc' + str(i))

    # Consider the interval from "start" to "stop" in seconds since epoch
    stream.add_interval_filter(start, stop)

    # Start the stream
    stream.start()

    # For each record (one record = one second, can have multiple same second)
    while stream.get_next_record(rec):
        timestamp = rec.time
        if rec.status != "valid":
            print rec.project, rec.collector, rec.type, timestamp, rec.status
        else:
            elem = rec.get_next_elem()
            while elem:
                if elem.type not in ['A', 'W']:
                    elem = rec.get_next_elem()
                    continue

                peer = elem.peer_address
                updatetype = elem.type
                prefix = elem.fields['prefix']
                if peer not in peers:
                    peers[peer] = {
                        'queueA': [],
                        'queueW': []
                    }
                update = {'tst': timestamp, 'prefix': prefix}
                if updatetype == 'A':
                    handleUpdate(peers[peer]['queueA'], burst2writeA, update, peer, updatetype, timestamp, window)
                    saveGraphPoint(peers[peer]['queueA'], updatetype, peer, timestamp, collectors)
                else:
                    handleUpdate(peers[peer]['queueW'], burst2writeW, update, peer, updatetype, timestamp, window)
                    saveGraphPoint(peers[peer]['queueW'], updatetype, peer, timestamp, collectors)
                elem = rec.get_next_elem()

    for peer in graph_points:
        peer_file_name = peer.replace(':', '_')
        if not os.path.exists(peer_file_name):
            os.makedirs(peer_file_name)
        with open(peer_file_name+'/'+peer_file_name + '-graph.json', 'w') as outfile:
            json.dump(graph_points[peer], outfile, indent=2)

    # print last bursts
    if burst2writeA:
        for peer in burst2writeA:
            if burst2writeA[peer]:
                for timestamp in burst2writeA[peer]:
                    writeBurst(peer, burst2writeA, 'A', timestamp)

    if burst2writeW:
        for peer in burst2writeW:
            if burst2writeW[peer]:
                for timestamp in burst2writeW[peer]:
                    writeBurst(peer, burst2writeW, 'W', timestamp)

    # transform csv names in json file to use getJSON in plotGrap
    # step to CSV is used to avoid appending to the end of a json file directly as appending overwrite the whole file
    jsonlist = []
    with open('csv_peernames-'+'-'.join(collectors)+'.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            jsonlist.append(row[0])


    jsondata = json.dumps(jsonlist, indent=2)
    fd = open('json_file_names-' + '-'.join(collectors) + '.json', 'w')
    fd.write(jsondata)
    fd.close()


if __name__ == '__main__':
    main()
