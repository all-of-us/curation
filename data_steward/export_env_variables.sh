#!/bin/bash
hpos=( $(cut -d ',' -f1 ./spec/_data/hpo.csv) )
hpos=("${hpos[@]:1}")
env=$1
appid="aou-res-curation-$env"
export APPLICATION_ID="aou-res-curation-$env"
export BIGQUERY_DATASET_ID="${env}_fake"
export BUCKET_NAME_FAKE="$APPLICATION_ID-fake"
for i in "${hpos[@]}"
do
  temp="${i%\"}"
  temp="${temp#\"}"
  echo "  BUCKET_NAME_${temp^^}=\"$APPLICATION_ID-$temp\""
  export BUCKET_NAME_${temp^^}="$APPLICATION_ID-$temp"
done


for i in "${hpos[@]}"
do
  temp="${i%\"}"
  temp="${temp#\"}"
  echo "  BUCKET_NAME_${temp^^}:\"$APPLICATION_ID-$temp\""
done
