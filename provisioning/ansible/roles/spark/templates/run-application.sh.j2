#!/bin/bash

if [ -z "$1" ] 
then
    echo "You must specify the application with arguments"
    exit 1;
fi
APPLICATION=$@
MODULESDIR=$(dirname "$1")/modules

# Output colors
GREEN='\033[0;32m'
NC='\033[0m' # No Color
PYFILES=''

if [ -d $MODULESDIR ]
then
    echo -e "${GREEN}[info] Creating zip with all modules${NC}"
    zip -r $MODULESDIR{.zip,}
    PYFILES="--py-files $MODULESDIR.zip"
fi

echo -e "${GREEN}[info] Running application $APPLICATION...${NC}"

export SPARK_MASTER_HOST={{ masterIP }}
export SPARK_LOCAL_HOST={{ masterIP }}

{{ dir_spark }}/spark-bin/bin/spark-submit --total-executor-cores {{ groups['sparkSlave'] | length if (groups['sparkSlave'] | length) >= 2 else 2}} --executor-memory {{ spark_worker_memory | string + 'M' if spark_worker_memory < 2048 else '2G' }} $PYFILES $APPLICATION
