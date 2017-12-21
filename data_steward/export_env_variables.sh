#!/bin/bash
hpos=( $(cut -d ',' -f1 ./spec/_data/hpo.csv) )
hpos=("${hpos[@]:1}")

if [ $1 = "mouse=a" ]
then
    echo "using default env name test"
    env="test"
else
    echo "using env name $1"
    env=$1
fi
appid="aou-res-curation-$env"
export APPLICATION_ID="aou-res-curation-$env"
export BIGQUERY_DATASET_ID="${env}_temp"
export BUCKET_NAME_FAKE="$APPLICATION_ID-fake"
export DRC_BUCKET_NAME="$APPLICATION_ID-drc-spec"
export BUCKET_NAME_NYC="$APPLICATION_ID-nyc"
for i in "${hpos[@]}"
do
  temp="${i%\"}"
  temp="${temp#\"}"
  # echo "  BUCKET_NAME_${temp^^}=\"$APPLICATION_ID-$temp\""
  export BUCKET_NAME_${temp^^}="$APPLICATION_ID-$temp"
done


#for i in "${hpos[@]}"
#do
#  temp="${i%\"}"
#  temp="${temp#\"}"
#  echo "  BUCKET_NAME_${temp^^}:\"$APPLICATION_ID-$temp\""
#done
