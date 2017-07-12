## Protocol statistics

### Description

Counts number of flows, packets, and bytes for TCP, UDP, and other flows received from Kafka every 10 seconds. Template application for a application developers.

### Usage:
- General 
` protocols_statistics.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic>`

- To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then
 you can run the example
`/home/spark/applications/run-application.sh /home/spark/applications/examples/protocols_statistics.py -iz producer:2181 -it ipfix.entry -oz producer:9092 -ot results.output`


