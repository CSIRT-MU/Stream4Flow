# Ansible provisioning

## Playbook structure

### Files
- site.yml - default playbook to built Stream4Flow cluster
- all.yml - deploys general configuration for all machines in a cluster
- producer.yml - deploys producer machine
- consumer.yml - deploys consumer machine
- sparkMaster.yml - deploys Spark Master machine
- spakrSlave.yml - deploys Spark Slave machine
- inventory.ini.example - example inventory file for ansible, which can be used in [cluster deployment](https://github.com/CSIRT-MU/Stream4Flow#cluster-deployment).

### Directories
- group_wars - contains configurable settings for Stream4Flow
- roles - contains individual roles