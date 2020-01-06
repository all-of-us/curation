#!/bin/bash -ex

# Imports RDR ETL results from GCS into a dataset in BigQuery.
# Assumes you have already activated a service account that is able to
# access the files in GCS.

USAGE="tools/import_rdr_omop.sh
    --rdr_project <PROJECT where rdr files are dropped>
    --rdr_directory <DIRECTORY, not including the gs:// bucket name>
    --output_dataset <DATA SET>
    --key_file <path to key file>
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

if [[ -z "${RDR_PROJECT}" ]] || [[ -z "${RDR_DIRECTORY}" ]] || [[ -z "${OUTPUT_DATASET}" ]] ||
  [[ -z "${KEY_FILE}" ]] || [[ -z "${VOCAB_DATASET}" ]]; then
  echo "Usage: $USAGE"
  exit 1
fi

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"

app_id=$(cat "${key_file}" | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);')

export GOOGLE_APPLICATION_CREDENTIALS="${KEY_FILE}"
export GOOGLE_CLOUD_PROJECT="${app_id}"
export BIGQUERY_DATASET_ID="${OUTPUT_DATASET}"

today=$(date '+%Y%m%d')
export BACKUP_DATASET="${OUTPUT_DATASET}_backup"

#---------Create curation virtual environment----------
# create a new environment in directory curation_venv
virtualenv -p "$(which python3.7)" "${DATA_STEWARD_DIR}/curation_venv"

# activate it
source "${DATA_STEWARD_DIR}/curation_venv/bin/activate"

# install the requirements in the virtualenv
pip install -r "${DATA_STEWARD_DIR}/requirements.txt"

source "${TOOLS_DIR}/set_path.sh"

bq mk -f --description "RDR DUMP loaded from ${RDR_DIRECTORY} on ${today}" "${GOOGLE_CLOUD_PROJECT}:${OUTPUT_DATASET}"

python "${DATA_STEWARD_DIR}/cdm.py" "${OUTPUT_DATASET}"

cdm_files=$(gsutil ls gs://${RDR_PROJECT}-cdm/${RDR_DIRECTORY})
if [[ $? -ne 0 ]]; then
  echo "failed to read CDM files from RDR, verify your --rdr_directory exists with gsutil ls gs://${RDR_PROJECT}-cdm"
  exit 1
fi
for file in $cdm_files; do
  filename=$(basename ${file})
  table_name=${filename%.*}
  echo "Importing ${OUTPUT_DATASET}.${table_name}..."
  CLUSTERING_ARGS=""
  if grep -q person_id resources/fields/"${table_name}".json; then
    CLUSTERING_ARGS="--time_partitioning_type=DAY --clustering_fields person_id"
  fi
  JAGGED_ROWS=""
  if [[ "${filename}" == "observation_period.csv" ]]; then
    JAGGED_ROWS="--allow_jagged_rows"
  fi
  bq load --project_id ${GOOGLE_CLOUD_PROJECT} --replace --allow_quoted_newlines ${JAGGED_ROWS} ${CLUSTERING_ARGS} --skip_leading_rows=1 ${GOOGLE_CLOUD_PROJECT}:${OUTPUT_DATASET}.${table_name} $file resources/fields/${table_name}.json
done

echo "Copying vocabulary"
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${VOCAB_DATASET} --target_dataset ${OUTPUT_DATASET}

echo "Creating a RDR back-up"
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${OUTPUT_DATASET} --target_dataset ${BACKUP_DATASET}

#set BIGQUERY_DATASET_ID variable to dataset name where the vocabulary exists
export BIGQUERY_DATASET_ID="${VOCAB_DATASET}"
export RDR_DATASET_ID="${OUTPUT_DATASET}"
echo "Cleaning the RDR data"
python "${CLEANER_DIR}/clean_cdr.py" -d rdr -s

echo "Done."

unset PYTHONPATH
deactivate
