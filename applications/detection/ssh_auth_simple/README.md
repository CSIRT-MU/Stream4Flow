## SSH Authentication Attack Detection

### Description
A method for a detection of attacks on SSH authentication (brute-force or dictionary) based
on simple threshold values of SSH connections.

### Usage:
- General 
`ssh_auth_simple.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oh
    <output-hostname>:<output-port>`

- Stream4Flow example
`/home/spark/applications/run-application.sh  /home/spark/applications/detection/ssh_auth_simple/spark/ssh_auth_simple.py -iz producer:2181 -it ipfix.entry -oh consumer:20101`