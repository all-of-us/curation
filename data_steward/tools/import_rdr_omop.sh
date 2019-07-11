#!/bin/bash -e

# Imports RDR ETL results from GCS into a dataset in BigQuery.
# Assumes you have already activated a service account that is able to
# access the files in GCS.

USAGE="tools/import_rdr_omop.sh
    --project <PROJECT where rdr files are dropped>
    --account <ACCOUNT >
    --directory <DIRECTORY>
    --data_set <DATA SET>
    --key_file <path to key file>
    --app_id <application id>"

while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --directory) DIRECTORY=$2; shift 2;;
    --data_set) DATA_SET=$2; shift 2;;
    --key_file) KEY_FILE=$2; shift 2;;
    --app_id) APP_ID=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [[ -z "${ACCOUNT}" ]] || [[ -z "${PROJECT}" ]] || [[ -z "${DIRECTORY}" ]] \
  || [[ -z "${DATA_SET}" ]] || [[ -z "${KEY_FILE}" ]] || [[ -z "${APP_ID}" ]]
then
  echo "Usage: $USAGE"
  exit 1
fi

export GOOGLE_APPLICATION_CREDENTIALS="${KEY_FILE}"
export APPLICATION_ID="${APP_ID}"

today=$(date '+%Y%m%d')

#---------Create curation virtual environment----------
set -e
# create a new environment in directory curation_env
virtualenv  -p $(which python2.7) curation_env

# activate it
source curation_env/bin/activate

# install the requirements in the virtualenv
pip install -t lib -r requirements.txt

source tools/set_path.sh

bq mk -f --description "RDR DUMP loaded from ${DIRECTORY} on ${today}" ${DATA_SET}

echo "python cdm.py ${DATA_SET}"
python cdm.py ${DATA_SET}

for file in $(gsutil ls gs://${PROJECT}-cdm/${DIRECTORY})
do
  filename=$(basename ${file})
  table_name="${filename%.*}"
  echo "Importing ${DATA_SET}.${table_name}..."
  bq rm -f ${DATA_SET}.${table_name}
  CLUSTERING_ARGS=
  if grep -q person_id resources/fields/${table_name}.json
  then
    CLUSTERING_ARGS="--time_partitioning_type=DAY --clustering_fields person_id "
  fi
  JAGGED_ROWS=
  if [[ "${filename}" = "observation_period.csv" ]]
  then
    JAGGED_ROWS="--allow_jagged_rows "
  fi
  bq load --replace --allow_quoted_newlines ${JAGGED_ROWS}${CLUSTERING_ARGS}--skip_leading_rows=1 ${DATA_SET}.${table_name} $file resources/fields/${table_name}.json
done

echo "Done."

unset PYTHONPATH
deactivate
