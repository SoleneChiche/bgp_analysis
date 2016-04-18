from _pybgpstream import BGPStream, BGPRecord

# Create a new bgpstream instance and a reusable bgprecord instance
stream = BGPStream()
rec = BGPRecord()
records = {}
COUNT_A = 'Count of A updates'
COUNT_W = 'Count of W updates'

def load_data(start, stop):
    # Consider RIPE RRC 10 only
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
                        records[elem.peer_address] = {rec.time: {COUNT_A: 1, COUNT_W: 0}}
                    elif elem.type == 'W':
                        records[elem.peer_address] = {rec.time: {COUNT_A: 0, COUNT_W: 1}}
                elif elem.peer_address in records:
                    if rec.time not in records[elem.peer_address]:
                        if elem.type == 'A':
                            records[elem.peer_address][rec.time] = {COUNT_A: 1, COUNT_W: 0}
                        elif elem.type == 'W':
                            records[elem.peer_address][rec.time] = {COUNT_A: 0, COUNT_W: 1}
                    elif rec.time in records[elem.peer_address]:
                        if elem.type == 'A':
                            records[elem.peer_address][rec.time][COUNT_A] += 1
                        elif elem.type == 'W':
                            records[elem.peer_address][rec.time][COUNT_W] += 1
                elem = rec.get_next_elem()

    return records
