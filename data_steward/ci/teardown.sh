# Delete buckets
for b in ${DRC_BUCKET_NAME} ${BUCKET_NAME_FAKE} ${BUCKET_NAME_NYC} ${BUCKET_NAME_PITT} ${BUCKET_NAME_CHS} ${BUCKET_NAME_UNIONED_EHR}; do
  $(git rev-parse --show-toplevel)/data_steward/ci/delete_bucket.sh ${b}
done

# Delete datasets if existing
for d in ${RDR_DATASET_ID} ${RDR_DATASET_ID} ${COMBINED_DATASET_ID} ${BIGQUERY_DATASET_ID} ${UNIONED_DATASET_ID}; do
  echo "removing ${APPLICATION_ID}:${d}"
  bq rm -r -d -f ${APPLICATION_ID}:${d}
done
