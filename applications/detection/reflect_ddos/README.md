## DDOS detection

###Description
A method for detection of reflected DoS/DDoS attacks based on an evaluation of the incoming/outgoing bytes volume ratio. he detection is aimed to protect DNS known servers in local network infrastructure (only DNS traffic on UDP is considered)

###Usage:
- General 
`detection_reflectddos.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oh
    <output-hostname>:<output-port> -dns <comma separated list of DNS servers IP addresses>`

- Stream4Flow example (using network range 10.10.0.0/16)
`/home/spark/applications/run-application.sh  /home/spark/applications/detection/reflected_ddos/spark/detection_reflectddos.py -iz producer:2181 -it ipfix.entry -oh consumer:20101 -dns "10.10.0.1,10.10.0.2`