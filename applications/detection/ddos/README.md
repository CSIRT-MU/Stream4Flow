## DDOS detection

### Description
A method for detection of DoS/DDoS attacks based on an evaluation of the incoming/outgoing packet volume ratio and its variance to the long-time ratio.

### Usage:
- General 
`detection_ddos.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -nf <regex for network range>`

- Stream4Flow example (using network range 10.10.0.0/16)
`/home/spark/applications/run-application.sh /home/spark/applications/detection/ddos/detection_ddos.py -iz producer:2181 -it ipfix.entry -oz producer:9092 -ot results.output -nf "10\.10\..+"`