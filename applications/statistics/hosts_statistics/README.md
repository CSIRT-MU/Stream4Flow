## Host statistics

### Description
An application for computing statistics for all hosts in network. Computed statistics for each host in each window are following:
- **Basic Characteristics**: sum of flows, packets and bytes
- **Port Statistics**: number of distinct destination ports
- **Communication Peers**: number of distinct communication peers
- **Average Flow Duration**: average duration of flows
- **TCP Flags Distribution**: number of each individual TCP flags 

### Usage:
- General 
`host_stats.py --iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -net <CIDR network range>`

- Stream4Flow example (using network range 10.10.0.0/16)
`./run-application.sh ./host_statistics/spark/host_statistics.py -iz producer:2181 -it ipfix.entry -oz producer:9092 -ot results.output-net "10.0.0.0/24"`
