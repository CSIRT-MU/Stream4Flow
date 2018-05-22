# Stream4Flow provisioning

## Vagrant Provisioning

All configuration of guest deployed by vagrant provisioning is in [configuration.yml](./configuration.yml).

**Configurable options:**
- *common* – Settings common for all guests
    - *box, box_url* – used Vagrant boxes (using different boxes can cause malfunction of the provisioning)
    - *provision_on_guest* – true value allows to provision guest separately, false will provision all at once but faster
- *producer* – Producer guest settings
    - *ip* – used address of the guest
    - *memory* – reserved memory (decrease can cause malfunction of the framework)
    - *cpu* – a number of virtual CPUs (decrease can cause malfunction of the framework)
- *sparkMaster* – Spark master guest settings
    - *ip* – used address of the guest
    - *memory* – reserved memory (decrease can cause malfunction of the framework)
    - *cpu* – a number of virtual CPUs (decrease can cause malfunction of the framework)
- *sparkSlave* – Slave guests settings (each slave will have the same configuration)
    - *count* – a number of slaves that will be provisioned (max. 155)
    - *ip_prefix* – IP address prefix of slave guests (suffix starts at 101)
    - *memory* – reserved memory (decrease can cause malfunction of the framework)
    - *cpu* – a number of virtual CPUs (decrease can cause malfunction of the framework)
- *consumer* – Consumer guest settings
    - *ip* – used address of the guest
    - *memory* – reserved memory (decrease can cause malfunction of the framework)
    - *cpu* – a number of virtual CPUs (decrease can cause malfunction of the framework)

### Vagrant commands:
- `vagrant up` – Brings up the whole framework
- `vagrant up <guest_name>` – Brings up the guest
- `vagrant halt <guest_name>` – Shutdown the guest
- `vagrant destroy <guest_name>`– Completely delete given guest and its associated resources (virtual hard drives, ...)
- `vagrant provision <guest_name>` – Run Ansible provisioning on the guest
- `vagrant ssh <guest_name>` – Connect to the guest via SSH

Available guest names: *producer*, *sparkMaster*, *sparkSlave101* ... *sparkSlave156*, *consumer*.

## Ansible Provisioning

Stream4Flow framework confogiration and variables are available in [ansible/roles/globas_vars/*](./ansible/roles/globas_vars/).
- Templates of configuration files are stored in *ansible/roles/<name_of_role>/templates/**
