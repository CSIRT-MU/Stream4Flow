<img src="https://raw.githubusercontent.com/CSIRT-MU/Stream4Flow/master/images/logo-text-small.png" style="padding-bottom: 20px"/>

A framework for the real-time IP flow data analysis built on Apache Spark Streaming, a modern distributed stream processing system.

### *This project is no longer maintained*

⚠ Project **Stream4Flow** is no longer maintained as the used frameworks are constantly evolving, and it is not in our capacity to continually update the installation scripts. If you're interested in other network data processing tools and our current research, check out [CSIRT-MU](https://github.com/CSIRT-MU) repositories.


## About Stream4Flow

The basis of the Stream4Flow framework is formed by the IPFIXCol collector, Kafka messaging system, Apache Spark, and Elastic Stack. IPFIXCol is able to receive IP flows from a majority of network Netflow/IPFIX probes (e.g., Flowmon Probe, softflowd, etc.). IPFIXCol enables incoming IP flow records to be transformed into the JSON format provided to the Kafka messaging system. The selection of Kafka was based on its scalability and partitioning possibilities, which provide sufficient data throughput. Apache Spark was selected as the data stream processing framework for its quick IP flow data throughput, available programming languages (Scala, Java, or Python) and MapReduce programming model. The analysis results are stored in Elastic Stack containing Logstash, Elasticsearch, and Kibana, which enable storage, querying, and visualizing the results. The Stream4Flow framework also contains the additional web interface to make administration easier and visualize complex results of the analysis.

![architecture](https://raw.githubusercontent.com/CSIRT-MU/Stream4Flow/master/images/architecture.png)



### Framework Features
- **Full Stack Solution**: The framework provides full stack solution for IP flow analysis prototyping. It is possible to connect to the majority of IP flow network probes. The framework integrates tools for data collection, data processing, manipulation, storage, and presentation. It is compatible with common network probes for IP flow monitoring and export.
- **Easy Deployment**: The deployment of the framework is fully automated for cloud deployment using cutting-edge technologies for software orchestration. The deployment comes with example prototype applications and initial tests to further ease the prototype development.
- **High Performance**: Thanks to the scalability of the framework, it is fitted for processing network traffic in a wide range of networks from small company network to large-scale, high-speed networks of ISPs. Its distributed nature enables computationally intensive analyses.
- **Real-time Analysis**: The stream-based approach provides results of IP flow analysis prototype with only a few seconds delay. The results can be explored in various ways in a user interface in real time. IP analysis prototype can be immediately improved according to provided results.

### Use-cases
- **Stream-Based Network Monitoring**: The framework enables to run analyses in data streams. It is suitable for various data pre-processing, continuous queries.
- **Real-time Attack Detection**: The stream-based approach enables to detect attacks with only a few seconds delay. An instant attack detection provides time to set up a defense and lowers harms caused by an attack. Sample detections are provided, and you can easily create custom detection method.
- **Host Profiling**: Apart from monitoring of the whole network, the monitoring can be focused on individual hosts. No host agents are needed, and the monitoring is transparent. You can get a long-term profile for each host connected to your network and explore its behavior.
- **Situational Awareness**: Data from network monitoring, attack detection, and host profiling can be gathered together to provide complex situational awareness over your network. The advanced analysis of the collected data can reveal information important both from security and business perspective.

**More on stream-based IP flow analysis is described in our paper titled [Toward Stream-Based IP Flow Analysis](https://doi.org/10.1109/MCOM.2017.1600972).**

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
- [Vagrant](https://www.vagrantup.com/) >= 1.9.7
- [Ansible](https://www.ansible.com/) >= 2.1.0
- python 2.7
- Internet connection

### We support two types of deployment:
- **Standalone deployment:** Stream4Flow will be deployed into virtual machines on your physical machine using [Vagrant](https://www.vagrantup.com/)
- **Cluster deployment:** you can deploy Stream4Flow on your cluster using [Ansible](https://www.ansible.com/)
    - requirement: Debian-based OS

### Standalone deployment

_Note: The minimum hardware requirement is 12GB of RAM_

1. download repository
2. go to folder **provisioning/**
3. (optional) update guests configuration in **configuration.yml**
3. run vagrant provisioning: `vagrant up` or start guests separately `vagrant up <guest-name>`
4. upload your SSH key to guests or allow password based SSH login (use `vagrant ssh <guest-name>`)

See [provision/README.md](./provisioning/README.md) for additional information about provisioning and Vagrant usage.


### Cluster deployment

_Note:  machines in the cluster must run Debian OS with systemd_

1. download repository
2. go to folder **provisioning/ansible**
3. supply your inventory file with you cluster deployment according to file inventory.ini.example
4. run ansible `ansible-playbook -i <your inventory file> site.yml --user <username> --ask-pass` (consult ansible docs for further information)

### Usage

| Usage |  Description | Usage information |
|---|---|---|
| Input data  | Input point for network monitoring data in **IPFIX/Netflow**  format | <ul><li> producer IP address</li> <li>default IP is 192.168.0.2</li> <li> port **UDP/4739** </li></ul>  |
| Stream4Flow Web Interface | Web interface for application for viewing data |<ul><li> consumer IP address</li> <li>default IP address is http://192.168.0.3/ </li><li>default login:**Stream4Flow**</li><li>default password:**Stream4Flow**</li></ul>|
| Spark Web Interface | Apache Spark streaming interface for application control | <ul><li> Spark master IP address:8080</li> <li>default IP address is http://192.168.0.100:8080/ </li></ul>|
| Kibana Web Interface | Elastic Kibana web interface for Elasticsearch data | <ul><li>index name: **spark-*** </li><li> consumer IP address:5601</li> <li>default IP address is http://192.168.0.3:5601/ </li></ul>|

#### Run an example application protocols_statistics

1. login to Spark Master machine via ssh `ssh spark@192.168.0.100`
2. go to application directory
`cd /home/spark/applications/`
3. run example application
`./run-application.sh ./statistics/protocols_statistics/spark/protocols_statistics.py -iz producer:2181 -it ipfix.entry -oz producer:9092 -ot results.output`
 

#### Send data to Stream4Flow

Stream4Flow is compatible with any Netflow v5/9 or IPFIX network probe. To measure your first data for Stream4Flow, you can use either commercial solution such as [Flowmon Probe](https://www.flowmon.com/en/products/flowmon/probe)  or an open-source alternative [softflowd](https://code.google.com/archive/p/softflowd/) 

- Install softflowd
`sudo apt-get install softflowd`

- Start data export
    - Standalone deployment
      `softflowd -i <your interface> -D -n 192.168.0.2:4739`
    - Cluster deployment 
      `softflowd -i <your interface> -D -n <IP address of producer>:4739`
    - for more softflowd options see [man pages](http://manpages.ubuntu.com/manpages/precise/man8/softflowd.8.html)
    

# How to reference

**Bibtex**

````bibtex
@ARTICLE{jirsik-2017-toward, 
  author={Jirsik, Tomas and Cermak, Milan and Tovarnak, Daniel and Celeda, Pavel}, 
  journal={IEEE Communications Magazine}, 
  title={Toward Stream-Based IP Flow Analysis}, 
  year={2017}, 
  volume={55}, 
  number={7}, 
  pages={70-76}, 
  doi={10.1109/MCOM.2017.1600972}, 
  ISSN={0163-6804},
}
````

**Plain text**
```
T. Jirsik, M. Cermak, D. Tovarnak and P. Celeda, "Toward Stream-Based IP Flow Analysis," in IEEE Communications Magazine, vol. 55, no. 7, pp. 70-76, 2017.
doi: 10.1109/MCOM.2017.1600972
```

**Related Publications**

- [Toward Stream-Based IP Flow Analysis](https://doi.org/10.1109/MCOM.2017.1600972)
- [A Performance Benchmark for NetFlow Data Analysis on Distributed Stream Processing Systems](https://doi.org/10.1109/NOMS.2016.7502926)
- [Real-time analysis of NetFlow data for generating network traffic statistics using Apache Spark](https://doi.org/10.1109/NOMS.2016.7502952)

# Acknowledgement
   
The SecurityCloud project is supported by the [Technology Agency of the Czech Republic](https://www.tacr.cz/) under No. TA04010062 Technology for processing and analysis of network data in big data concept.

#### Project partners

[CESNET, z. s. p. o.](https://www.cesnet.cz/?lang=en)

[Flowmon Networks, a.s.](https://www.flowmon.com/)
