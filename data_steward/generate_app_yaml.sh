#!/bin/bash
# generates app.yaml from app_base.yaml and environment name ($1)
hpos=( $(cut -d ',' -f1 ./resources/hpo.csv) )
hpos=("${hpos[@]:1}")

if [ -z "$1" ]
then
    echo "using default env name test"
    env="test"
else
    echo "using env name $1"
    env=$1
fi

cp app_base.yaml app.yaml
APPLICATION_ID="aou-res-curation-$env"
echo "" >> app.yaml
echo "env_variables:">>app.yaml
echo  "  BIGQUERY_DATASET_ID: \"${env}_fake\"" >> app.yaml
echo  "  BUCKET_NAME_FAKE: \"$APPLICATION_ID-fake\"">> app.yaml
echo  "  BUCKET_NAME_NYC: \"$APPLICATION_ID-nyc\"">> app.yaml
echo  "  DRC_BUCKET_NAME: \"$APPLICATION_ID-drc-spec\"">> app.yaml
echo  "  BUCKET_NAME_UNIONED_EHR: \"$APPLICATION_ID-drc-spec\"">> app.yaml
for i in "${hpos[@]}"
do
  temp="${i%\"}"
  temp="${temp#\"}"
  echo "  BUCKET_NAME_${temp^^}: \"$APPLICATION_ID-$temp\"" >> app.yaml
done


#for i in "${hpos[@]}"
#do
#  temp="${i%\"}"
#  temp="${temp#\"}"
#  echo "  BUCKET_NAME_${temp^^}:\"$APPLICATION_ID-$temp\""
#done
