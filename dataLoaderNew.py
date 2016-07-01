from _pybgpstream import BGPStream, BGPRecord
import sys
import ast
import time
import json
import os
import csv

burst2writeA = {}   # Dictionary recording the A updates
burst2writeW = {}   # Dictionary recording the W updates
graph_points = {}   # Dictionary with all peers and their burst counts, used to plot the graphs of the bursts


def main():
    # Register the starting point in seconds, from epoch time
    start = int(sys.argv[1])
    # Register the stopping point in seconds, from epoch time
    stop = int(sys.argv[2])
    print 'Interval: ' + str(stop-start)
    # Register the array of collectors we want to take the updates from
    collectors = ast.literal_eval(sys.argv[3])
    # Register the window where once we get a burst, we record the last [window] seconds of updates
    window = int(sys.argv[4])
    # Threshold in number of updates when the burst is recorded
    threshold = int(sys.argv[5])
    # Time when we start the loading process
    s = time.time()
    # Launch the loading process
    load_data(start, stop, collectors, window, threshold)
    # Time when we stop the loading process
    e = time.time()
    # Total time it took to collect and process all updates
    print 'Total time: ', (e - s)


# Function to save in json file the point we display in plotGraph.js
def saveGraphPoint(queue, updateType, peer, timestamp, collectors, threshold):
    # We register the burst points only if the threshold is reached
    if len(queue) >= threshold:
        # If the peer is not graph_points, we open a csv file to append its name in it
        # We use the intermediate csv file as a json file can't be appended as easily
        if peer not in graph_points:
            fd = open('csv_peernames-'+'-'.join(collectors)+'.csv', 'a')
            # We append in the csv file the json file name used in plotGraph
            fd.write(peer.replace(':', '_')+'/'+peer.replace(':', '_') + '-graph.json' + '\n')
            fd.close()
            # We create a space in graph_points to append the burst updates
            graph_points[peer] = {}
        # If the update timestamp is not yet recorded, we create the space for it in the dictionary
        if timestamp not in graph_points[peer]:
            graph_points[peer][timestamp] = {'A': 0, 'W': 0}
        # We finally append the size of the burst at the timestamp moment
        graph_points[peer][timestamp][updateType] = len(queue)


# Function to flush properly a burst
# The burst is flushed only if the updates arrival exceeds the size of the window
def cleanQueue(queue, timestamp, window):
    # If the current timestamp observed is bigger than the last timestamp recorded
    if queue and timestamp > queue[-1]['tst']:
        while queue:
            # If the timestamp observed is out of the window, we delete these elements of the queue
            if queue[0]['tst'] + window < timestamp:
                del queue[0]
            else:
                break


# Return the last timestamp observed for the peer and return None if the window as been exceeded
# If the window is exceeded it means we'll have to record the current timestamp
def currentBurstTime(burstqueue, peer, timestamp, window):
    if peer not in burstqueue or not burstqueue[peer]:
        return None
    else:
        lasttst = max(burstqueue[peer].keys())
        # We want to record twice the size of the window
        if timestamp > lasttst + window:
            return None
        return lasttst


# Write in csv file the raw update bursts
def writeBurst(peer, burstqueue, updatetype, timestamp):
    # Replace ':' by '_' for file naming recognition
    peer_file_name = peer.replace(':', '_')
    # Create a directory for the peer
    if not os.path.exists(peer_file_name):
        os.makedirs(peer_file_name)
    # Create the csv file containing the timestamps and prefixes in the burst
    if updatetype == 'A':
        with open(peer_file_name+'/'+peer_file_name+'-'+str(timestamp)+'-burstA.csv', 'a') as f:
            for elem in burstqueue[peer][timestamp]:
                f.write(str(elem['tst']) + ',' + elem['prefix'] + '\n')
    else:
        with open(peer_file_name+'/'+peer_file_name+'-'+str(timestamp)+'-burstW.csv', 'a') as f:
            for elem in burstqueue[peer][timestamp]:
                f.write(str(elem['tst']) + ',' + elem['prefix'] + '\n')


# Handle a new update collected by the loader
def handleUpdate(queue, burstQueue, update, peer, updatetype, timestamp, window, threshold):

    # Clean the queue before handling the update
    cleanQueue(queue, timestamp, window)
    queue.append(update)
    length = len(queue)

    lasttst = currentBurstTime(burstQueue, peer, timestamp, window)

    # we record the end of the burst if a timestamp is still in a burst window
    if lasttst:
        burstQueue[peer][lasttst].append(update)
    # not recording any burst and we are detecting a burst
    elif length > threshold:
        if peer not in burstQueue:
            burstQueue[peer] = {}
        burstQueue[peer][timestamp] = []
        # Record what we already have in the queue in burstqueue
        for elem in queue:
            burstQueue[peer][timestamp].append(elem)
        # Write in a csv file the first window seconds of the burst
        if len(burstQueue[peer]) > 1:
            writeBurst(peer, burstQueue, updatetype, min(burstQueue[peer]))
            del burstQueue[peer][min(burstQueue[peer])]


# Main function to load the data and process it
def load_data(start, stop, collectors, window, threshold):
    peers = {}

    # collectors is a list of the collectors we want to include
    # Start and stop define the interval we are looking in the data

    # Create a new BGPStream instance and a reusable BGPRecord instance
    stream = BGPStream()
    rec = BGPRecord()

    # Add filter for each collector.
    # If no collector is mentioned, it will consider 16 of them
    if collectors:
        for collector in collectors:
            print collector
            stream.add_filter('collector', collector)
    else:
        for i in range(0, 10):
            stream.add_filter('collector', 'rrc0' + str(i))
        for i in range(10, 16):
            stream.add_filter('collector', 'rrc' + str(i))

    stream.add_filter('record-type', 'updates')

    # Consider the interval from "start" to "stop" in seconds since epoch
    stream.add_interval_filter(start, stop)

    # Start the stream
    stream.start()

    # For each record (one record = one second, can have multiple elements for the same second) we handle its updates
    while stream.get_next_record(rec):
        timestamp = rec.time
        if rec.status != "valid":
            print rec.project, rec.collector, rec.type, timestamp, rec.status
        else:
            # Go through all elements of the record
            elem = rec.get_next_elem()
            while elem:
                # Consider only the A and W updates
                if elem.type not in ['A', 'W']:
                    elem = rec.get_next_elem()
                    continue

                peer = elem.peer_address
                updatetype = elem.type
                prefix = elem.fields['prefix']
                if peer not in peers:
                    peers[peer] = {
                        'A': [],
                        'W': []
                    }
                update = {'tst': timestamp, 'prefix': prefix}
                if updatetype == 'A':
                    handleUpdate(peers[peer]['A'], burst2writeA, update, peer, updatetype, timestamp, window, threshold)
                    saveGraphPoint(peers[peer]['A'], updatetype, peer, timestamp, collectors, threshold)
                else:
                    handleUpdate(peers[peer]['W'], burst2writeW, update, peer, updatetype, timestamp, window, threshold)
                    saveGraphPoint(peers[peer]['W'], updatetype, peer, timestamp, collectors, threshold)
                elem = rec.get_next_elem()

    # After processing all records, we write the graph json files with the graph points recorded for each peer
    for peer in graph_points:
        peer_file_name = peer.replace(':', '_')
        if not os.path.exists(peer_file_name):
            os.makedirs(peer_file_name)
        with open(peer_file_name+'/'+peer_file_name + '-graph.json', 'w') as outfile:
            json.dump(graph_points[peer], outfile, indent=2)

    # Write the last burst of A updates if there is one left
    if burst2writeA:
        for peer in burst2writeA:
            if burst2writeA[peer]:
                for timestamp in burst2writeA[peer]:
                    writeBurst(peer, burst2writeA, 'A', timestamp)

    # Write the last burst of W updates if there is one left
    if burst2writeW:
        for peer in burst2writeW:
            if burst2writeW[peer]:
                for timestamp in burst2writeW[peer]:
                    writeBurst(peer, burst2writeW, 'W', timestamp)

    # transform csv names in json file to use getJSON in plotGrap
    # step to CSV is used to avoid appending to the end of a json file directly as appending
    # to a json file overwrite the whole file
    jsonlist = []
    with open('csv_peernames-'+'-'.join(collectors)+'.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            jsonlist.append(row[0])

    jsondata = json.dumps(jsonlist, indent=2)
    fd = open('json_file_names-' + '-'.join(collectors) + '.json', 'w')
    fd.write(jsondata)
    fd.close()

# Launch the program
if __name__ == '__main__':
    main()
