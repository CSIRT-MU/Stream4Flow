# Stream4Flow

## Vagrant VM configuration - Vagrantfile
- network: private network with internet access, IPs sets in # IP CONFIGURATION section
- virtual machine properties: in section # VM PROPERTIES CONFIGURATION, can be set memory in MB a number of CPUs.
- number of Spark slaves: set in # NUMBER OF SLAVES section, Spark Slave IP address starts at sparkSlave_prefix.101 and increments by one.

## Vagrant commands:
- vagrant up [<VM_name>]  : Brings up virtual machine(s)
- vagrant halt [<VM_name>]: Shutdown virtual machine(s)
- vagrant destroy [<VM_name>]: Completely delete virtual machine(s) and their associated resources (virtual hard drives, ...)

## Ansible roles

All configurable variables are stored in ansible/roles/<name_of_role>/vars/*
Templates of configuration files are stored in ansible/roles/<name_of_role>/templates/*

## Spark vars:
- user: under which user Spark should be installed
- slave{x}IP: IP address of Spark Slave, where {x} is a number from 1.. If adding more slaves, add their IPs here as other entries like slave2IP: <IP_addr2> slave3IP: <IP_addr3> ...
- work directories - for Apache Spark
- download mirrors - URLs from where to download Apache Spark and Kafka Assembly
- spark_inflated_dir_name: name of Spark directory when unarchived
- spark test settings - Settings for Apache Spark

## ELK Stack templates:
- elasticsearch_elasticsearch.yml.j2 - elasticsearch base configuration
- elasticsearch_logging.yml.j2 - elasticsearch logging configuration
- kibana.yml.j2 - Kibana configuration
- logstash_spark-to-elastic.conf.j2 - Logstash configuration file
- logstash_templates_spark-elasticsearch-template.json.j2 - Logstash template file

## IPfixcol vars:
- build - set of commands to build IPfixcol binary
- build_fastbit_compile - set to 'true' if you want to compile fastbit from source (should be necessary only if downloaded binary version doesn't work)
- script_path - path to the location of ipfixcol scripts
- script_filename - filename of ipfixcol script to run. Allowed values are: startup.xml.tcp and startup.xml.udp. If you want to change the script later after deployment, set the IPFIXCOL_SCRIPT environment variable accordingly in /etc/default/ipfixcol

## IPfixcol vars 
- packages.apt.yml: List of required apt packages to download.

## IPfixcol templates:
- ipfixcol.conf.j2: Ubuntu upstart file for ipfixcol service
- ipfixcol.j2: defaults file for ipfixcol service
- startup.xml.j2: ipfixcol configuration script to run. Check the comments in the file for more info.

## kafka vars:
- kafka_dir: Kafka home directory
- kafka_download_url: Download location of Kafka
- kafka_filename: Name of Kafka directory when unarchived
- retention: Retention setting for the Kafka topic
- kafka_maximum_heap_space: Maximum java heap space for kafka in MB (Default 0.5 of total RAM)
- kafka_minimum_heap_space: Minimum java heap space for kafka in MB (Default 0.25 of total RAM)

## kafka templates:
- kafka-broker.service.j2: Ubuntu upstart file for kafka service
- kafka-broker.j2: defaults file for kafka service
- kafka-server-start.sh.j2: Start script for Kafka server

## web vars:
- cert_subj: SSL certificate subject - set according to your needs
- phantomjs_url: PhantomJS download URL
- web2py_passwd: Password for web2py administration through web interface

## web templates:
- chgpasswd.py.j2: Script to change default web2py password to value set in web2py_passwd (see above).
- iptables.conf.j2: Firewall rules for all services to work correctly
- iptables.j2: Script to automatically load iptables rules on boot
- web2py.conf.j2: Web2py configuration file

