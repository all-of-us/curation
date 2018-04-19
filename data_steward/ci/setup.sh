#!/bin/bash

# Create buckets
gsutil mb -c nearline -p ${APPLICATION_ID} gs://${DRC_BUCKET_NAME}
gsutil mb -c nearline -p ${APPLICATION_ID} gs://${BUCKET_NAME_FAKE}
gsutil mb -c nearline -p ${APPLICATION_ID} gs://${BUCKET_NAME_NYC}
gsutil mb -c nearline -p ${APPLICATION_ID} gs://${BUCKET_NAME_PITT}

# Create dataset
bq mk --dataset --description "Test dataset for ${USERNAME}" ${APPLICATION_ID}:${BIGQUERY_DATASET_ID}

# Create vocabulary tables if they do not already exist
VOCABULARY_DATASET="${APPLICATION_ID}:aou_full_vocabulary_2018_01_04"
DEST_PREFIX="${APPLICATION_ID}:${BIGQUERY_DATASET_ID}"
for t in `bq ls ${VOCABULARY_DATASET} | grep TABLE | awk '{print $1}'`
do
  CLONE_CMD="bq cp -n ${VOCABULARY_DATASET}.${t} ${DEST_PREFIX}.${t}"
  echo `${CLONE_CMD}`
done
