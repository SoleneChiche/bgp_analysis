from _pybgpstream import BGPStream, BGPRecord
import sys
import ast
import time


def main():
    start = int(sys.argv[1])
    stop = int(sys.argv[2])
    collectors = ast.literal_eval(sys.argv[3])
    print collectors
    s = time.time()
    load_data(start, stop, collectors)
    e = time.time()
    print e - s


def load_data(start, stop, collectors):
    # collectors is a list of the collectors we want to include
    # Start and stop define the interval we are looking in the data

    # Create a new bgpstream instance and a reusable bgprecord instance
    stream = BGPStream()
    rec = BGPRecord()

    # Consider RIPE RRC 10 only
    print
    if collectors:
        for collector in collectors:
            print collector
            stream.add_filter('collector', collector)
    else:
        for i in range(0, 10):
            stream.add_filter('collector', 'rrc0'+str(i))
        for i in range(10, 15):
            stream.add_filter('collector', 'rrc'+str(i))

    # Consider this time interval:
    # Sat Aug  1 08:20:11 UTC 2015

    stream.add_interval_filter(start, stop)

    # Start the stream
    stream.start()
    result = open('result-'+'-'.join(collectors)+'.csv', 'w')
    result.write("peer, timestamp, countA, countW \n")
    current_time = 0
    peers = {}
    while stream.get_next_record(rec):
        timestamp = rec.time
        if timestamp != current_time:
            flush_peers(peers, current_time, result)
            peers = {}

        current_time = timestamp
        # Print the record information only if it is not a valid record

        if rec.status != "valid":
            print rec.project, rec.collector, rec.type, timestamp, rec.status
        else:
            elem = rec.get_next_elem()
            while elem:
                if elem.peer_address not in peers:
                    peers[elem.peer_address] = {'a': 0, 'w': 0, 'prefix_a': [], 'prefix_w': []}
                if elem.type == 'A':
                    peers[elem.peer_address]['a'] += 1
                    peers[elem.peer_address]['prefix_a'].append(elem.fields['prefix'])
                elif elem.type == 'W':
                    peers[elem.peer_address]['w'] += 1
                    peers[elem.peer_address]['prefix_w'].append(elem.fields['prefix'])
                elem = rec.get_next_elem()

    result.close()


def flush_peers(peers, current_time, result):
    for peer in peers:
        val_a = peers[peer]['a']
        val_w = peers[peer]['w']
        result.write(peer + ',' + str(current_time) + ',' + str(val_a) + ',' + str(val_w) + '\n')
        result.write('prefixA' + ',' + ','.join(peers[peer]['prefix_a']) + '\n')
        result.write('prefixW' + ',' + ','.join(peers[peer]['prefix_w']) + '\n')
if __name__ == '__main__':
    main()
