# Stream4Flow
A framework for the real-time IP flow data analysis built on Apache Spark Streaming, a modern distributed stream processing system.


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
2. go to folder **provision/**
3. run vagrant provision: `vagrant up`

#### Cluster deployment
1. clone repository
2. go to folder **provision/ansible**
3. supply your inventory file with you cluster deployment according to file inventory.ini.example
4. run ansible `ansible-playbook -i <your inventory file> site.yml`