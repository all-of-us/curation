#!/bin/bash

gcloud config set project ${APPLICATION_ID}

# Create buckets
create_bucket.sh ${DRC_BUCKET_NAME}
create_bucket.sh ${BUCKET_NAME_FAKE}
create_bucket.sh ${BUCKET_NAME_NYC}
create_bucket.sh ${BUCKET_NAME_PITT}
create_bucket.sh ${BUCKET_NAME_CHS}
create_bucket.sh ${BUCKET_NAME_UNIONED_EHR}

# Delete datasets if existing
bq rm -r -d -f ${APPLICATION_ID}:${RDR_DATASET_ID}
bq rm -r -d -f ${APPLICATION_ID}:${EHR_RDR_DATASET_ID}
bq rm -r -d -f ${APPLICATION_ID}:${BIGQUERY_DATASET_ID}
bq rm -r -d -f ${APPLICATION_ID}:${UNIONED_DATASET_ID}

# Create datasets
bq mk --dataset --description "Test RDR dataset for ${USERNAME}" ${APPLICATION_ID}:${RDR_DATASET_ID}
bq mk --dataset --description "Test EHR-RDR dataset for ${USERNAME}" ${APPLICATION_ID}:${EHR_RDR_DATASET_ID}
bq mk --dataset --description "Test EHR dataset for ${USERNAME}" ${APPLICATION_ID}:${BIGQUERY_DATASET_ID}
bq mk --dataset --description "Test EHR union dataset for ${USERNAME}" ${APPLICATION_ID}:${UNIONED_DATASET_ID}

# Create vocabulary tables if they do not already exist
VOCABULARY_DATASET="${APPLICATION_ID}:vocabulary20190423"
DEST_PREFIX="${APPLICATION_ID}:${BIGQUERY_DATASET_ID}"
for t in $(bq ls ${VOCABULARY_DATASET} | grep TABLE | awk '{print $1}')
do
  CLONE_CMD="bq cp -n ${VOCABULARY_DATASET}.${t} ${DEST_PREFIX}.${t}"
  echo $(${CLONE_CMD})
done
