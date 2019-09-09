#!/bin/bash

SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"

source ${SCRIPT_PATH}/tools/set_path.sh
echo $(python ${SCRIPT_PATH}/generate_cron_yaml.py)
