<img src="https://stream4flow.ics.muni.cz/images/logo_text-small.png" style="padding-bottom: 20px"/>

A framework for the real-time IP flow data analysis built on Apache Spark Streaming, a modern distributed stream processing system.

## About Stream4Flow

The basis of the Stream4Flow framework is formed by the IPFIXCol collector, Kafka messaging system, Apache Spark, and Elastic Stack. IPFIXCol enables incoming IP flow records to be transformed into the JSON format provided to the Kafka messaging system. The selection of Kafka was based on its scalability and partitioning possibilities, which provide sufficient data throughput. Apache Spark was selected as the data stream processing framework for its quick IP flow data throughput, available programming languages (Scala, Java, or Python) and MapReduce programming model. The analysis results are stored in Elastic Stack containing Logstash, Elasticsearch, and Kibana, which enable storage, querying, and visualizing the results. The Stream4Flow framework also contains the additional web interface in order to make administration easier and visualize complex results of the analysis.

![architecture](https://stream4flow.ics.muni.cz/images/architecture.png)

### Use-cases
- **Stream-Based Network Monitoring**: Thanks to the scalability of the framework, it is fitted for deployment in a wide range of networks from small company network to large-scale, high-speed networks of ISPs. It is compatible with common network probes for IP flow monitoring and export.
- **Real-time Attack Detection**: The stream-based approach enables to detect attacks with only a few seconds delay. An instant attack detection provides time to set up a defense and lowers harms caused by an attack. A sample detections are provided, and you can easily create custom detection method.
- **Host Profiling**: Apart from monitoring of the whole network, the monitoring can be focused on individual hosts. No host agents are needed, and the monitoring is transparent. You can get a long-term profile for each host connected to your network and explore its behavior.
- **Situational Awareness**: Data from network monitoring, attack detection, and host profiling can be gathered together to provide complex situational awareness over your network. The advanced analysis of the collected data can reveal information important both from security and business perspective.

## Getting started
We have it all prepared for you. Everything is preconfigured. You have to only choose the deployment variant.

### Deployment

#### Default machine configuration
- **Producer** - a machine for receiving data from network and probes, and providing data for Spark Cluster via Apache Kafka.
    - producer default IP address - 192.168.0.2
- **Consumer** - a machine receives results from Spark Cluster, stores the results, and runs the web server with framework frontend.
    - consumer default IP address - 192.168.0.3
- **Spark Cluster**- cluster of machines for data stream processing. **Spark Master** machine manages the Spark Cluster and provides a control interface for the cluster. **Spark Slaves** serves mainly for data processing. The number of Spark Slaves can be changed in configuration files.
    - Spark Master default IP address - 192.168.0.100
    - Spark Slave default IP address - 192.168.0.101

#### Default login credentials
- user: **spark**
- login: **Stream4Flow**

#### Requirements
- [Vagrant](https://www.vagrantup.com/) >= 1.8.0
- [ansible](https://www.ansible.com/) >= 2.1.0
- Internet connection

### We support two types of deployment:
- **Standalone deployment:** Stream4Flow will be deployed into virtual machines on your physical machine using [Vagrant](https://www.vagrantup.com/)
- **Cluster deployment:** you can deploy Stream4Flow on your own cluster using [ansible](https://www.ansible.com/)
    - requirement: Debian-based OS

### Standalone deployment

1. download repository
2. go to folder **provisioning/**
3. run vagrant provisioning: `vagrant up`

The minimum hardware requirements for standalone Stream4Flow cluster
- 14GB of RAM 


### Cluster deployment

_Note:  machnies in cluster must run Debian OS_

1. download repository
2. go to folder **provisioning/ansible**
3. supply your inventory file with you cluster deployment according to file inventory.ini.example
4. run ansible `ansible-playbook -i <your inventory file> site.yml --user <username> --ask-pass` (consult ansible docs for further information)

### Usage

| Usage |  Description | Usage information |
|---|---|---|
| Input data  | Input point for network monitoring data in **IPFIX/Netflow**  format | <ul><li> producer IP addres</li> <li>default IP is 192.168.0.2</li> <li> port **UDP/4739** </li></ul>  |
| Stream4Flow Web Interface | Web interface for application for viewing data |<ul><li> consumer IP address</li> <li>default IP address is http://192.168.0.3/ </li><li>default login:**stream4flow**</li><li>default password:**stream4flow**</li></ul>|
| Spark Web Interface | Apache Spark streaming interface for application control | <ul><li> Spark master IP address:8080</li> <li>default IP address is http://192.168.0.100:8080/ </li></ul>|
| Kibana Web Interface | Elastic Kibana web interface for Elasticsearch data | <ul><li>index name: **spark-*** </li><li> consumer IP address:5601</li> <li>default IP address is http://192.168.0.3:5601/ </li></ul>|

#### Run an expample application protocols_statistics

1. login to Spark Master machine via ssh
`ssh spark@192.168.0.2`
2. go to application directory
`cd /home/spark/applications/`
3. run example application
`./run-application.sh ./protocols-statistics/protocols_statistics.py -iz producer:2181 -it ipfix.entry -oh consumer:20101`
 

#### Send data to Stream4Flow

Stream4Flow is compatible with any Netflow v5/9 or IPFIX network probe. To measure your first data for Stream4Flow, you can use [softflowd](https://code.google.com/archive/p/softflowd/) - Flow-based network traffic analyser

- Install softflowd
`sudo apt-get install softflowd`

- Start data export
    - Standalone deployment
      `softflowd -i <your interface> -n 192.168.0.2:4739`
    - Cluster deployment 
      `softflowd -i <your interface> -n <IP address of producer>:4739`
    - for more softflowd options see [man pages](http://manpages.ubuntu.com/manpages/precise/man8/softflowd.8.html)