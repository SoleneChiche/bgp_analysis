from _pybgpstream import BGPStream, BGPRecord
import sys
import json


def main():
    start = int(sys.argv[1])
    stop = int(sys.argv[2])
    # collector = sys.argv[3]
    length_interval = stop - start
    records_time_interval = 1000
    count = length_interval // records_time_interval
    rest = length_interval % records_time_interval
    if records_time_interval > length_interval:
        load_data(start, stop, '')
    else:
        for i in range (1, count):
            stop = start + records_time_interval
            load_data(start, stop, '')
            start = stop
        load_data(start, start+rest, '')


def load_data(start, stop, collectors):
    count_a = 'A updates'
    count_w = 'W updates'
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

    records = {}
    while stream.get_next_record(rec):
        # Print the record information only if it is not a valid record
        if rec.status != "valid":
            print rec.project, rec.collector, rec.type, rec.time, rec.status
        else:
            elem = rec.get_next_elem()
            while elem:
                if elem.peer_address not in records:
                    if elem.type == 'A':
                        records[elem.peer_address] = {rec.time: {count_a: 1, count_w: 0}}
                    elif elem.type == 'W':
                        records[elem.peer_address] = {rec.time: {count_a: 0, count_w: 1}}
                elif elem.peer_address in records:
                    if rec.time not in records[elem.peer_address]:
                        if elem.type == 'A':
                            records[elem.peer_address][rec.time] = {count_a: 1, count_w: 0}
                        elif elem.type == 'W':
                            records[elem.peer_address][rec.time] = {count_a: 0, count_w: 1}
                    elif rec.time in records[elem.peer_address]:
                        if elem.type == 'A':
                            records[elem.peer_address][rec.time][count_a] += 1
                        elif elem.type == 'W':
                            records[elem.peer_address][rec.time][count_w] += 1
                elem = rec.get_next_elem()

    with open('result-'+str(start)+'-'+str(stop)+'.json', 'w') as fp:
        json.dump(records, fp, indent=2)

if __name__ == '__main__':
    main()

