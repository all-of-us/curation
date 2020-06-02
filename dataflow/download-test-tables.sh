#!/bin/bash

set -e

TEST_DATA_DIR="test_data"

CDR_PROJECT="aou-res-curation-test"
CDR_PREFIX="${CDR_PROJECT}.synthea_ehr_ops_20200513.unioned_ehr"



for tbl in care_site condition_occurrence death device_exposure drug_exposure fact_relationship location measurement note observation person procedure_occurrence provider specimen visit_occurrence; do
  cond=""
  if grep -q "person_id" "fields/${tbl}.json"; then
    cond="WHERE MOD(person_id, 2500) = 0"
  elif grep -q "${tbl}_id" "fields/${tbl}.json"; then
    cond="WHERE MOD(${tbl}_id, 2500) = 0"
  fi
  bq \
    --format=json \
    --project_id "${CDR_PROJECT}" \
    query \
    --nouse_legacy_sql \
    "SELECT * FROM \`${CDR_PREFIX}_${tbl}\` ${cond}" \
    | jq -c ".[]" \
    > "test_data/${tbl}.json"
done

