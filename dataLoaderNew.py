from _pybgpstream import BGPStream, BGPRecord
import sys
import ast
import time
import json

burst2writeA = {}
burst2writeW = {}
graph_points= {}
peer_names_list = []
THSLD = 100

def main():
    start = int(sys.argv[1])
    stop = int(sys.argv[2])
    print 'Interval: ' + str(stop-start)
    collectors = ast.literal_eval(sys.argv[3])
    window = int(sys.argv[4])
    print collectors
    s = time.time()
    load_data(start, stop, collectors, window)
    e = time.time()
    print 'Total time: ', (e - s)

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
                if peer.replace(':', '_')+'-graph.json' not in peer_names_list:
                    peer_names_list.append(peer.replace(':', '_')+'-graph.json')

                updatetype = elem.type
                prefix = elem.fields['prefix']
                if peer not in peers:
                    peers[peer] = {
                        'queueA': [],
                        'queueW': []
                    }
                update = {'tst': timestamp, 'prefix': prefix}
                if updatetype == 'A':
                    handleUpdate(peers[peer]['queueA'], burst2writeA, update, peer, timestamp, window)
                    saveGraphPoint(peers[peer]['queueA'], updatetype, peer, timestamp)
                else:
                    handleUpdate(peers[peer]['queueW'], burst2writeW, update, peer, timestamp, window)
                    saveGraphPoint(peers[peer]['queueW'], updatetype, peer, timestamp)
                elem = rec.get_next_elem()
    """
    print len(burst2writeA)
    print burst2writeA.keys()
    print len(burst2writeW)
    print burst2writeW.keys()
    for peerName in burst2writeA:
        peer = burst2writeA[peerName]
        for burstTime in peer:
            burst = peer[burstTime]
            print burst[-1]['tst']- burst[0]['tst']
    print json.dumps(burst2writeA, indent=2)
    """

    for peer in burst2writeA:
        with open(peer.replace(':', '_')+'-burstA.json', 'w') as outfile:
            json.dump(burst2writeA[peer], outfile, indent=2)

    for peer in burst2writeW:
        with open(peer.replace(':', '_') + '-burstW.json', 'w') as outfile:
            json.dump(burst2writeW[peer], outfile, indent=2)

    for peer in graph_points:
        with open(peer.replace(':', '_') + '-graph.json', 'w') as outfile:
            json.dump(graph_points[peer], outfile, indent=2)

    with open('json_file_names.json', 'w') as outfile:
         json.dump(peer_names_list, outfile, indent=2)


def saveGraphPoint(queue, updateType, peer, timestamp):
    if len(queue) > THSLD:
        if peer not in graph_points:
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
        # if queue:
        #     print queue[-1]['tst'] - queue[0]['tst']


def currentBurstTime(burstqueue, peer, timestamp, window):
    if peer not in burstqueue or not burstqueue[peer]:
        return None
    else:
        lasttst = max(burstqueue[peer].keys())
        if timestamp - window > lasttst:
            return None
        return lasttst


def handleUpdate(queue, burstqueue, update, peer, timestamp, window):

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
        burstqueue[peer][timestamp] = list(queue)

if __name__ == '__main__':
    main()
