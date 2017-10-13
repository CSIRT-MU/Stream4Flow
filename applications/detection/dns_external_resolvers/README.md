## External DNS Servers Usage Detection

### Description
A method for a detection of external dns resolvers usage in the specified local network.

### Usage:
- General
`dns_external_resolvers.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -ln <local-network>/<subnet-mask>`

- Stream4Flow example
`/home/spark/applications/run-application.sh ./detection/dns_external_resolvers/spark/dns_external_resolvers.py -iz producer:2181 -it ipfix.entry -oz producer:9092 -ot results.output -ln 10.10.0.0/16`