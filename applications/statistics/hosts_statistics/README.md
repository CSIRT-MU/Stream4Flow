## Host statistics

### Description
An application for computing statistics for all hosts in network. Computed statistics for each host in each window are following:
- **Basic Characteristics**: sum of flows, packets and bytes
- **Port Statistics**: number of distinct destination ports
- **Communication Peers**: number of distinct communication peers
- **Average Flow Duration**: average duration of flows
- **TCP Flags Distribution**: number of each individual TCP flags 

### Usage
- General:
 
`  host_stats.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>  -oz <output-hostname>:<output-port> -ot <output-topic> -net <network range>`

- Stream4Flow example (using network range 10.10.0.0/16):

`/home/spark/applications/run-application.sh /home/spark/applications/host_statistics/host_stats.py -iz producer:2181 -it ipfix.entry -oz producer:9092 -ot host.stats -net "10.10.0.0/16"`

## Top N Host statistics

### Description
An application for collecting the top N characteristics for all hosts, particularly:

- **Top N destination ports**: ports of the highest number of flows on given ports from each source IP
- **Destination IPs**: destination IP addresses with the highest number of flows from each source IP
- **HTTP Hosts**: destination HTTP addresses with the highest number of flows for each source IP

## Usage
- General:

`  top_n_host_stats.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oz <output-hostname>:<output-port> -ot <output-topic> -n <number of Top results> -net <network range>`

- Stream4Flow example (using network range 10.10.0.0/16):

`/home/spark/applications/run-application.sh /home/spark/applications/host_statistics/top_n_host_stats.py -iz producer:2181 -it ipfix.entry -oz producer:9092 -ot topn.stats -n 10 -net "10.10.0.0/16"`
