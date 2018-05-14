#!/bin/bash -e

# Imports RDR ETL results from GCS into a dataset in BigQuery.
# Assumes you have already activated a service account that is able to
# access the files in GCS.

USAGE="tools/import_rdr_omop.sh --project <PROJECT> --account <ACCOUNT> --directory <DIRECTORY> --data_set <DATA SET>"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --directory) DIRECTORY=$2; shift 2;;
    --data_set) DATA_SET=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${ACCOUNT}" ] || [ -z "${PROJECT}" ] || [ -z "${DIRECTORY}" ] \
  || [ -z "${DATA_SET}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi

bq mk -f ${DATA_SET}
for file in $(gsutil ls gs://${PROJECT}-cdm/${DIRECTORY})
do
  filename=$(basename ${file})
  table_name="${filename%.*}"
  echo "Importing ${DATA_SET}.${table_name}..."
  bq rm -f ${DATA_SET}.${table_name}
  schema_name="${table_name}"
  bq load --allow_quoted_newlines --skip_leading_rows=1 ${DATA_SET}.${table_name} $file resources/fields/${schema_name}.json
done

echo "Done."
