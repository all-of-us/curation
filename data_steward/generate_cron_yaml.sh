#!/bin/bash
hpos=( $(cut -d ',' -f1 ./spec/_data/hpo.csv) )
hpos=("${hpos[@]:1}")

if [ -z "$1"] || [-z "$2"]
then
    echo "supply both args <env_name>  <file_name>"
    exit 1
fi

env=$1
filename=$2

echo "cron:"

echo "- description: validate all hpos"
echo "  url: /data_steward/v1/ValidateHpoFiles/all"
echo "  schedule: every 240 hours" 


echo "- description: website generation"
echo "  url: /tasks/sitegen"
echo "  schedule: every 60 minutes"
for i in "${hpos[@]}"
do
  temp="${i%\"}"
  temp="${temp#\"}"
  echo "- description: validate hpo $temp"
  echo "  url: /data_steward/v1/ValidateHpoFiles/$temp"
  echo "  schedule: every 24 hours" 
done
