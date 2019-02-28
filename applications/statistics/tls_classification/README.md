## TLS Classification Statistics

### Description

Clyssify TLS clients and counts the following statistics from defined input topic every 30 seconds:
- Operating system
- Browser type
- Device type and application

### Usage:
- General
` tls_classification.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -d <dictionary CSV>`

- To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. You also need a kafka topic to which output will be sent.
Then you can run the example
`/home/spark/applications/run-application.sh /home/spark/applications/statistics/dns_statistics/tls_classification.py -iz producer:2181 -it ipfix.entry -oz producer:9092 -ot results.output -d /home/spark/applications/statistics/dns_statistics/tls_classification_dictionary.csv`
