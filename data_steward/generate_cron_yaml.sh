#!/bin/bash
hpos=( $(cut -d ',' -f1 ./resources/hpo.csv) )
hpos=("${hpos[@]:1}")

if [ -z "$1" ] || [ -z "$2" ]
then
    echo "supply both args <env_name>  <file_name>"
    exit 1
fi

env=$1
filename=$2

# TODO move this logic to python
echo "cron:" > $filename

echo "- description: validate all hpos" >> $filename
echo "  url: /data_steward/v1/ValidateAllHpoFiles" >> $filename
echo "  schedule: every 3 hours" >> $filename

for i in "${hpos[@]}"
do
  temp="${i%\"}"
  temp="${temp#\"}"
  echo "- description: validate hpo $temp">> $filename
  echo "  url: /data_steward/v1/ValidateHpoFiles/$temp">> $filename
  echo "  schedule: 4th saturday of dec 03:00">> $filename
done

for i in "${hpos[@]}"
do
  temp="${i%\"}"
  temp="${temp#\"}"
  echo "- description: copy hpo $temp files">> $filename
  echo "  url: /data_steward/v1/CopyFiles/$temp">> $filename
  echo "  schedule: every 24 hours">> $filename 
done
