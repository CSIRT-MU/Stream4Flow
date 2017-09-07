## DNS statistics

### Description

Counts the following statistics from defined input topic every 20 seconds:
- Record types
- Response codes
- Queried domains by domain name
- Queried domains that do not exist
- Queried external dns servers from local network
- Queried dns servers on local network from the outside network
- Domains with hosts which queried them

### Usage:
- General
` dns_statistics.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -lc <local-network>/<subnet-mask>`

- To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. You also need a kafka topic to which output will be sent.
Then you can run the example
`/home/spark/applications/run-application.sh /home/spark/applications/statistics/dns_statistics/dns_statistics.py -iz producer:2181 -it ipfix.entry -oz producer:9092 -ot results.output -lc 10.10.0.0/16`


