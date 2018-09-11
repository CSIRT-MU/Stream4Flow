## Detection of Patterns in IP Flow Data

### Description
A highly flexible, easily extensible and modular application, capable of analyzing IP flow data, and comparing known patterns with real measurements in real time.

### Application Usage:
- General 
`./pattern_finder.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -c <configuration-file>`

- Stream4Flow example
`/home/spark/applications/run-application.sh  /home/spark/applications/detection/pattern_finder/spark/pattern_finder.py -iz producer:2181 -it ipfix.entry -oz producer:9092
      -ot app.pattern-finder -c configuration.yml`