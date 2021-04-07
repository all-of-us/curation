#!/usr/bin/env bash
set -ex

# Imports RDR ETL results from GCS into a dataset in BigQuery.
# Assumes you have already activated a service account that is able to
# access the files in GCS.

USAGE="tools/import_rdr_omop.sh
    --rdr_project <PROJECT where rdr files are dropped>
    --rdr_directory <DIRECTORY, not including the gs:// bucket name>
    --key_file <path to key file>
    --rdr_upload_date <Date that the RDR ETL has run - format yyyymmdd>
    --vocab_dataset <vocabulary dataset>"

while true; do
  case "$1" in
  --rdr_project)
    RDR_PROJECT=$2
    shift 2
    ;;
  --rdr_directory)
    RDR_DIRECTORY=$2
    shift 2
    ;;
  --output_dataset)
    OUTPUT_DATASET=$2
    shift 2
    ;;
  --key_file)
    KEY_FILE=$2
    shift 2
    ;;
  --rdr_upload_date)
    RDR_UPLOAD_DATE=$2
    shift 2
    ;;
  --vocab_dataset)
    VOCAB_DATASET=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${RDR_PROJECT}" ]] || [[ -z "${RDR_DIRECTORY}" ]] || [[ -z "${KEY_FILE}" ]] || [[ -z "${VOCAB_DATASET}" ]] || [[ -z "${RDR_UPLOAD_DATE}" ]]; then
  echo "Usage: $USAGE"
  exit 1
fi

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
FIELDS_DIR="${DATA_STEWARD_DIR}/resource_files/schemas"
RDR_FIELDS_DIR="${FIELDS_DIR}/rdr"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"

app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${KEY_FILE}")

export GOOGLE_APPLICATION_CREDENTIALS="${KEY_FILE}"
export GOOGLE_CLOUD_PROJECT="${app_id}"
export BIGQUERY_DATASET_ID="${OUTPUT_DATASET}"


RDR_DATASET="rdr${RDR_UPLOAD_DATE}"

source "${TOOLS_DIR}/set_path.sh"

bq mk -f --description "RDR DUMP loaded from ${RDR_DIRECTORY} dated ${RDR_UPLOAD_DATE}" "${GOOGLE_CLOUD_PROJECT}:${RDR_DATASET}"

python "${DATA_STEWARD_DIR}/cdm.py" "${RDR_DATASET}"
# accommodate new file "pid_rid_mapping" by RDR
bq mk --table "${GOOGLE_CLOUD_PROJECT}:${RDR_DATASET}.pid_rid_mapping" "${RDR_FIELDS_DIR}/pid_rid_mapping.json"
bq mk --table "${GOOGLE_CLOUD_PROJECT}:${RDR_DATASET}.cope_survey_semantic_version_map" "${RDR_FIELDS_DIR}/cope_survey_semantic_version_map.json"

cdm_files=$(gsutil ls gs://${RDR_PROJECT}-cdm/${RDR_DIRECTORY})
if [[ $? -ne 0 ]]; then
  echo "failed to read CDM files from RDR, verify your --rdr_directory exists with gsutil ls gs://${RDR_PROJECT}-cdm"
  exit 1
fi
for file in $cdm_files; do
  filename=$(basename ${file})
  table_name=${filename%.*}
  # Locate the schema file (ensuring one and only one is found)
  schema_file=$(find "${FIELDS_DIR}" -type f -iname "${table_name}.json")
  schema_count=$(echo "$schema_file" | wc -l);
  if [[ 1 -ne schema_count ]]; then
    echo "Import failed: ${schema_count} schemas were found for table ${table_name}."
    exit 1
  fi
  echo "Importing ${RDR_DATASET}.${table_name}..."
  CLUSTERING_ARGS=""
  # Will not add clustering to pid_rid_mapping table
  if grep -q person_id "${schema_file}" | [[ "${filename}" -ne "pid_rid_mapping.csv" ]]; then
    CLUSTERING_ARGS="--time_partitioning_type=DAY --clustering_fields person_id"
  fi
  JAGGED_ROWS=""
  if [[ "${filename}" == "observation_period.csv" ]]; then
    JAGGED_ROWS="--allow_jagged_rows"
  fi
  bq load --project_id ${GOOGLE_CLOUD_PROJECT} --replace --allow_quoted_newlines ${JAGGED_ROWS} ${CLUSTERING_ARGS} --skip_leading_rows=1 ${GOOGLE_CLOUD_PROJECT}:${RDR_DATASET}.${table_name} $file $schema_file
done

echo "Copying vocabulary"
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${VOCAB_DATASET} --target_dataset ${RDR_DATASET}

unset PYTHONPATH

set +ex
