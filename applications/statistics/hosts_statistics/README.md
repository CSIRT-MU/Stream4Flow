## Host statistics

###Description
An application for computing statistics for all hosts in network. Computed statistics for each host each window are following:
- **Basic Characteristics**: sum of flows, packets and bytes
- **Port Statistics**: number of distinct destination ports
- **Communication Peers**: number of distinct communication peers
- **Average Flow Duration**: average duration of flows in a given window

###Usage:
- General 
`  detection_ddos.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oh <output-hostname>:<output-port> -net <regex for network range>`

- Stream4Flow example (using network range 10.10.0.0/16)
`/home/spark/applications/run-application.sh /home/spark/applications/host_statistics/host_statistics.py -iz producer:2181 -it ipfix.entry -oh consumer:20101 -net "10\.10\..+"`