# Stream4Flow provisioning

## Vagrant Provisioning

Configuration of vagrant provisioning is in Vagrantfile.

Configurable options:
- IP CONFIGURATION: This section configures IP addresses of virtual machines
- NUMBER OF SLAVES: You can choose number of slaves to built (max. 155), Spark Slave IP address starts at sparkSlave_prefix.101 and increments by one.
- VM PROPERTIES CONFIGURATION: Options for virtual machines (can be set memory in MB a number of CPUs.)

### Vagrant commands:
- vagrant up [<VM_name>]  : Brings up virtual machine(s)
- vagrant halt [<VM_name>]: Shutdown virtual machine(s)
- vagrant destroy [<VM_name>]: Completely delete virtual machine(s) and their associated resources (virtual hard drives, ...)

## Ansible Provisioning


All configurable variables are stored in ansible/roles/globas_vars/*
Templates of configuration files are stored in ansible/roles/<name_of_role>/templates/*


