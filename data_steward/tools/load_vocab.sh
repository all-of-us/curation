#!/usr/bin/env bash

# Given a path to vocabulary csv files downloaded from Athena and specified by --in_dir:
# 1. Add the local vocabulary AoU_General
# 2. Transform the vocabulary files to a format BigQuery can load
# 3. Upload the transformed files to the GCS path specified by --gcs_path
# 4. Load the vocabulary in the dataset specified by --dataset

USAGE="
tools/load_vocab.sh --app_id app_id --in_dir in_dir [--gcs_path gcs_path --dataset dataset]
 --app_id   app_id   GCP project associated with the dataset
 --in_dir   in_dir   directory where vocabulary files are located
 --gcs_path gcs_path full GCS path to save transformed files
 --dataset  dataset  name of BigQuery dataset to create and load vocabulary
"
SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"
BASE_DIR="$( cd ${SCRIPT_PATH} && cd .. && pwd )"
AOU_GENERAL_PATH="${BASE_DIR}/resources/aou_general"
AOU_GENERAL_VOCABULARY_CONCEPT_ID="2000000000"
AOU_GENERAL_VOCABULARY_REFERENCE="https://docs.google.com/document/d/10Gji9VW5-RTysM-yAbRa77rXqVfDfO2li2U4LxUQH9g"
OMOP_VOCABULARY_CONCEPT_ID="44819096"
while true; do
  case "$1" in
    --app_id) APP_ID=$2; shift 2;;
    --in_dir) IN_DIR=$2; shift 2;;
    --dataset) DATASET=$2; shift 2;;
    --gcs_path) GCS_PATH=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [[ -z "${APP_ID}" ]] || [[ -z "${IN_DIR}" ]]
then
  echo "Usage: $USAGE"
  exit 1
fi

source ${SCRIPT_PATH}/set_path.sh

# Determine the version of the OMOP vocabulary
OMOP_VOCABULARY_VERSION=$(cat ${IN_DIR}/VOCABULARY.csv | grep ${OMOP_VOCABULARY_CONCEPT_ID} | cut -f4)
echo "Version of OMOP Standard vocabulary is ${OMOP_VOCABULARY_VERSION}"

# Move vocabulary files to backup folder
BACKUP_DIR="${IN_DIR}-backup"
echo "Creating backup in ${BACKUP_DIR}..."
mkdir ${BACKUP_DIR}
cp ${IN_DIR}/* ${BACKUP_DIR}

# Format dates, standardize line endings
echo "Transforming the files in ${IN_DIR}..."
python ${BASE_DIR}/vocabulary.py transform_files --in_dir ${BACKUP_DIR} --out_dir ${IN_DIR}

TEMP_DIR=$(mktemp -d)
echo "Created temp dir ${TEMP_DIR}"

# Append AoU_General vocabulary and concept records
echo "Adding AoU_General to vocabulary in ${IN_DIR}..."
cp ${IN_DIR}/* ${TEMP_DIR}
python ${BASE_DIR}/vocabulary.py add_aou_general --in_dir ${TEMP_DIR} --out_dir ${IN_DIR}

rm -rf ${TEMP_DIR}

# Upload to bucket
if [[ -z "${GCS_PATH}" ]]
then
  echo "GCS path not specified, skipping GCS upload and BigQuery load"
  exit 0
fi
echo "Uploading ${IN_DIR}/* to ${GCS_PATH}..."
gsutil -m cp ${IN_DIR}/* ${GCS_PATH}

# Load in BigQuery dataset
if [[ -z "${DATASET}" ]]
then
  echo "Dataset not specified, skipping BigQuery load"
  exit 0
fi
echo "Creating and loading dataset ${DATASET}..."
bq mk --project_id ${APP_ID} --dataset_id ${DATASET} --description "Vocabulary ${OMOP_VOCABULARY_VERSION} loaded from ${GCS_PATH}"
for file in $(gsutil ls ${GCS_PATH})
do
 filename=$(basename ${file,,})
 table_name="${filename%.*}"
 gsutil cp ${file} .
 echo "Loading ${DATASET}.${table_name}..."
 bq load --project_id ${APP_ID} --source_format CSV --quote "" --field_delimiter "\t" --max_bad_records 500 --skip_leading_rows 1 ${DATASET}.${table_name} ${file} resources/fields/${table_name}.json
done
