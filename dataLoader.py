from _pybgpstream import BGPStream, BGPRecord
import sys
import time

def main():
    start = int(sys.argv[1])
    stop = int(sys.argv[2])
    # collector = sys.argv[3]
    s = time.time()
    load_data(start, stop, '')
    e = time.time()
    print e - s

def load_data(start, stop, collectors):
    # collectors is a list of the collectors we want to include
    # Start and stop define the interval we are looking in the data

    # Create a new bgpstream instance and a reusable bgprecord instance
    stream = BGPStream()
    rec = BGPRecord()

    # Consider RIPE RRC 10 only
    if collectors:
        for elem in enumerate(collectors):
            stream.add_filter('collector', elem)
    else:
        stream.add_filter('collector', 'rrc10')

    # Consider this time interval:
    # Sat Aug  1 08:20:11 UTC 2015

    stream.add_interval_filter(start, stop)

    # Start the stream
    stream.start()

    result = open('result.csv', 'w')
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
                    peers[elem.peer_address] = {'a': 0, 'w': 0}
                if elem.type == 'A':
                    peers[elem.peer_address]['a'] += 1
                elif elem.type == 'W':
                    peers[elem.peer_address]['w'] += 1
                elem = rec.get_next_elem()

    result.close()


def flush_peers(peers, current_time, result):
    for peer in peers:
        val_a = peers[peer]['a']
        val_w = peers[peer]['w']
        if val_a > 10 or val_w > 10:
            result.write(peer + ',' + str(current_time) + ',' + str(val_a) + ',' + str(val_w) + '\n')


if __name__ == '__main__':
    main()

