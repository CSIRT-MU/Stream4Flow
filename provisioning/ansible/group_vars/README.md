# Ansible variables
This directory stores all configurable variables. Divided by roles that are using them.

## Variables for all:
Contains variables for user configuration.
- **user**: User for Spark installation
- **user_passwd**: User password
- **maven_proxy**: Enable proxy support {False/True}
- **proxy_id**:The unique identifier for this proxy. This is used to differentiate between proxy elements.
- **proxy_user,proxy_pass**:These elements appear as a pair denoting the login and password required to authenticate to this proxy server.
- **proxy_host,proxy_port,proxy_protocol**: The protocol://host:port of the proxy, seperated into discrete elements.
- **non_proxy_hosts**:This is a list of hosts which should not be proxied. The delimiter of the list is the expected type of the proxy server; the example above is pipe delimited - comma delimited is also common.

## Consumer variables
Contains variables for web configuration.
- **cert_subj**: SSL certificate subject - set according to your needs
- **web2py_passwd**: Password for web2py administration through web interface
- **repository_url**: Stream4Flow repository url for installing web

## Producer variables
Contains variables for Apache Kafka and Ipfixcol configuration.

### Kafka variables
- **kafka_dir**: Kafka home directory
- **kafka_download_url**: Download location of Kafka
- **kafka_filename**: Name of Kafka directory when unarchived
- **retention**: Retention setting for the Kafka topic
- **kafka_maximum_heap_space**: Maximum java heap space for kafka in MB (Default 0.5 of total RAM)
- **kafka_minimum_heap_space**: Minimum java heap space for kafka in MB (Default 0.25 of total RAM)

### Ipfixcol variables
- **script_path**: Path to the location of ipfixcol scripts
- **script_filename**: Filename of ipfixcol script to run. Allowed values are: startup.xml.tcp and startup.xml.udp. If you want to change the script later after deployment, set the IPFIXCOL_SCRIPT environment variable accordingly in /etc/default/ipfixcol

## SparkMaster and sparkSlave variables
Contains variables for Apache Spark configuration.
- **download mirrors** - URLs from where to download Apache Spark and Kafka Assembly
- **spark_inflated_dir_name**: Name of Spark directory when unarchived
- **spark_batch_size**: size of Spark's Batch
- **spark_worker_cores**: number of Spark Worker's CPU

