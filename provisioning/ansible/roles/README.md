# Ansible Roles

## Common

Common tasks done for all manchines in a cluster

- install java
- add users
- set a host file

## Elk

Install Elastic cluster on the consumer machine

**Templates:**
- elasticsearch_elasticsearch.yml.j2 - elasticsearch base configuration
- elasticsearch_logging.yml.j2 - elasticsearch logging configuration
- kibana.yml.j2 - Kibana configuration
- logstash_kafka-to-elastic.conf.j2 - Logstash configuration file
- logstash_templates_spark-elasticsearch-template.json.j2 - Logstash template file

## Example-application

Install necessary prerequisites on the spark cluster and deploy the example application on the spark master. 

## IPFICcol

Install IPFIXcol on the producer machine. 

**Variables:**
- build - set of commands to build IPfixcol binary
- build_fastbit_compile - set to 'true' if you want to compile fastbit from source (should be necessary only if downloaded binary version doesn't work)
- script_path - path to the location of ipfixcol scripts
- script_filename - filename of ipfixcol script to run. Allowed values are: startup.xml.tcp and startup.xml.udp. If you want to change the script later after deployment, set the IPFIXCOL_SCRIPT environment variable accordingly in /etc/default/ipfixcol
- packages.apt.yml: List of required apt packages to download.

**Templates:**
- ipfixcol.conf.j2: Ubuntu upstart file for ipfixcol service
- ipfixcol.j2: defaults file for ipfixcol service

## Kafka

Installs Aoache Kafka on the producer machine

**Templates:**
- kafka-broker.service.j2: Ubuntu upstart file for kafka service
- kafka-broker.j2: Defaults file for kafka service
- kafka-server-start.sh.j2: Start script for Kafka server

## Spark

Installs Apache Spark on the spark cluster

**Templates**
- run-application.sh.j2 - startup script for a spark application


**Variables:**
- user: under which user Spark should be installed
- slave{x}IP: IP address of Spark Slave, where {x} is a number from 1.. If adding more slaves, add their IPs here as other entries like slave2IP: <IP_addr2> slave3IP: <IP_addr3> ...
- download mirrors: URLs from where to download Apache Spark and Kafka Assembly
- spark_inflated_dir_name: Name of Spark directory when unarchived
- spark test settings: Settings for Apache Spark

## Ubuntu-systemd-normalizer

**Templates**
- tcpnormalizer@.service.j2: systemd service script

**Variables**
- normalizer_user: set user for normalizer

## Web

Install web interface on the consumer machine

**Variables:**
- cert_subj: SSL certificate subject - set according to your needs
- phantomjs_url: PhantomJS download URL
- web2py_passwd: Password for web2py administration through web interface

**Templates**:
- chgpasswd.py.j2: Script to change default web2py password to value set in web2py_passwd (see above).
- web2py.conf.j2: Web2py configuration file

**Files**
- routes.py: Default application settings
