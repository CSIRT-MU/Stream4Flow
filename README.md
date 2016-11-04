# Stream4Flow
A framework for the real-time IP flow data analysis built on Apache Spark Streaming, a modern distributed stream processing system.

## About Stream4Flow



## Getting started
We have it all preparied for you. Everything is preconfigured. You have to only choose the deployment variant.

### Deployment
We support two types of deployment:
- **Standalone deployment:** Stream4Flow will be deployed into virtual maniches on your physical machine using [Vagrant](https://www.vagrantup.com/)
- **Cluster deployment:** you can deploy Stream4Flow on your own cluster using [ansible](https://www.ansible.com/)

#### Requirements
- latest version of [Vagrant](https://www.vagrantup.com/)
- latest version of [ansible](https://www.ansible.com/)
- Internet connection

#### Standalone deployment

1. clone repository
2. go to folder **provisioning/**
3. run vagrant provisioning: `vagrant up`

The minimum hardware requirements for standalone Stream4Flow cluster
- 14GB of RAM 
- 80GB hard drive space 

Default machine configuration
- producer IP address - 192.168.0.2
- consumer IP address - 192.168.0.3
- Spark Master IP address - 192.168.0.100
- Spark Slave IP address - 192.168.0.101

Default login credentials
- user: **spark**
- login: **Stream4Flow**

#### Cluster deployment
1. clone repository
2. go to folder **provisioning/ansible**
3. supply your inventory file with you cluster deployment according to file inventory.ini.example
4. run ansible `ansible-playbook -i <your inventory file> site.yml`

### Usage

| Usage |  Description | Usage information |
|---|---|---|
| Input data  | Input point for network monitoring data in **IPFIX/Netflow**  format | <ul><li> producer IP addres (default IP is 192.168.0.2)</li> <li> port **UDP/4739** </li></ul>  |
| Stream4Flow Web Interface | Web interface for application for viewing data |<ul><li> consumer IP address</li> <li>default IP address is http://192.168.0.3/ </li></ul>|
| Spark Web Interface | Apache Spark streaming interface for application control | <ul><li> consumer IP address:8080</li> <li>default IP address is http://192.168.0.100:8080/ </li></ul>| 

#### Run an expample application protocols_statistics

1. login to Spark Master machine via ssh
`ssh spark@192.168.0.2`
2. go to application directory
`cd /home/spark/applications/`
3. run example application
`./run-application.sh ./protocols-statistics/protocols_statistics.py -iz producer:2181 -it ipfix.entry -oh consumer:20101`
 

