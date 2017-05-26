## Horizontal and Vertical TCP Ports Scan Detection

### Description
A method for a detection of horizontal and vertical TCP ports scans on the network using adjustable threshold for number of
flows. Default values are: window size = 60, min amount of flows = 20.

### Usage:
- General 
`ports_scan.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oh <output-hostname>:<output-port>`

- Stream4Flow example
`/home/spark/applications/run-application.sh  /home/spark/applications/detection/ports_scan/spark/ports_scan.py
-iz producer:2181 -it ipfix.entry -oh consumer:20101`