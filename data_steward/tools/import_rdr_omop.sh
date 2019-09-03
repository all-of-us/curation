#!/bin/bash -e

# Imports RDR ETL results from GCS into a dataset in BigQuery.
# Assumes you have already activated a service account that is able to
# access the files in GCS.

USAGE="tools/import_rdr_omop.sh
    --project <PROJECT where rdr files are dropped>
    --directory <DIRECTORY>
    --dataset <DATA SET>
    --key_file <path to key file>
    --app_id <application id>
    --vocab_dataset <vocabulary dataset>"

while true; do
  case "$1" in
  --project)
    PROJECT=$2
    shift 2
    ;;
  --directory)
    DIRECTORY=$2
    shift 2
    ;;
  --dataset)
    DATASET=$2
    shift 2
    ;;
  --key_file)
    KEY_FILE=$2
    shift 2
    ;;
  --app_id)
    APP_ID=$2
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

if [[ -z "${PROJECT}" ]] || [[ -z "${DIRECTORY}" ]] || [[ -z "${DATASET}" ]] ||
  [[ -z "${KEY_FILE}" ]] || [[ -z "${APP_ID}" ]] || [[ -z "${VOCAB_DATASET}" ]]; then
  echo "Usage: $USAGE"
  exit 1
fi

export GOOGLE_APPLICATION_CREDENTIALS="${KEY_FILE}"
export APPLICATION_ID="${APP_ID}"
export BIGQUERY_DATASET_ID="${DATASET}"

today=$(date '+%Y%m%d')
export BACKUP_DATASET="${DATASET}_backup"

#---------Create curation virtual environment----------
# create a new environment in directory curation_env
virtualenv -p $(which python2.7) curation_env

# activate it
source curation_env/bin/activate

# install the requirements in the virtualenv
pip install -t lib -r requirements.txt

source tools/set_path.sh

bq mk -f --description "RDR DUMP loaded from ${DIRECTORY} on ${today}" ${DATASET}

echo "python cdm.py ${DATASET}"
python cdm.py ${DATASET}

for file in $(gsutil ls gs://${PROJECT}-cdm/${DIRECTORY}); do
  filename=$(basename ${file})
  table_name="${filename%.*}"
  echo "Importing ${DATASET}.${table_name}..."
  CLUSTERING_ARGS=
  if grep -q person_id resources/fields/${table_name}.json; then
    CLUSTERING_ARGS="--time_partitioning_type=DAY --clustering_fields person_id "
  fi
  JAGGED_ROWS=
  if [[ "${filename}" == "observation_period.csv" ]]; then
    JAGGED_ROWS="--allow_jagged_rows "
  fi
  bq load --replace --allow_quoted_newlines ${JAGGED_ROWS}${CLUSTERING_ARGS}--skip_leading_rows=1 ${DATASET}.${table_name} $file resources/fields/${table_name}.json
done

echo "Creating a RDR back-up"
./tools/table_copy.sh --source_app_id ${APP_ID} --target_app_id ${APP_ID} --source_dataset ${DATASET} --target_dataset ${BACKUP_DATASET}

#set BIGQUERY_DATASET_ID variable to dataset name where the vocabulary exists
export BIGQUERY_DATASET_ID="${VOCAB_DATASET}"
echo "Fixing the PMI_Skip and the PPI_Vocabulary using command - fix_rdr_data.py -p ${APP_ID} -d ${DATASET}"
python tools/fix_rdr_data.py -p ${APP_ID} -d ${DATASET}

echo "Done."

unset PYTHONPATH
deactivate
