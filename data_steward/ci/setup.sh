#!/bin/bash

# Create buckets
for b in ${DRC_BUCKET_NAME} ${BUCKET_NAME_FAKE} ${BUCKET_NAME_NYC} ${BUCKET_NAME_PITT} ${BUCKET_NAME_CHS} ${BUCKET_NAME_UNIONED_EHR}; do
  $(git rev-parse --show-toplevel)/data_steward/ci/create_bucket.sh ${b}
done

# Delete datasets if existing
bq rm -r -d -f ${APPLICATION_ID}:${RDR_DATASET_ID}
bq rm -r -d -f ${APPLICATION_ID}:${COMBINED_DATASET_ID}
bq rm -r -d -f ${APPLICATION_ID}:${BIGQUERY_DATASET_ID}
bq rm -r -d -f ${APPLICATION_ID}:${UNIONED_DATASET_ID}
bq rm -r -d -f ${APPLICATION_ID}:${FITBIT_DATASET_ID}

# Create datasets
bq mk --dataset --description "Test RDR dataset for ${USERNAME}" ${APPLICATION_ID}:${RDR_DATASET_ID}
bq mk --dataset --description "Test COMBINED dataset for ${USERNAME}" ${APPLICATION_ID}:${COMBINED_DATASET_ID}
bq mk --dataset --description "Test EHR dataset for ${USERNAME}" ${APPLICATION_ID}:${BIGQUERY_DATASET_ID}
bq mk --dataset --description "Test EHR union dataset for ${USERNAME}" ${APPLICATION_ID}:${UNIONED_DATASET_ID}
bq mk --dataset --description "Test FITBIT dataset for ${USERNAME}" ${APPLICATION_ID}:${FITBIT_DATASET_ID}

# Create vocabulary tables if they do not already exist
VOCABULARY="${APPLICATION_ID}:${VOCABULARY_DATASET}"
DEST_PREFIX="${APPLICATION_ID}:${BIGQUERY_DATASET_ID}"
for t in $(bq ls "${VOCABULARY}" | grep TABLE | awk '{print $1}')
do
  CLONE_CMD="bq cp --project_id=${APPLICATION_ID} -n ${VOCABULARY}.${t} ${DEST_PREFIX}.${t}"
  echo $(${CLONE_CMD})
done
