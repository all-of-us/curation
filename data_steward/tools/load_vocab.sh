#!/usr/bin/env bash
set -ex
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
 [--venv_dir venv_dir directory where virtual environment should be created]
"
SCRIPT_PATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"
BASE_DIR="$(cd ${SCRIPT_PATH} && cd .. && pwd)"
VENV_DIR="curation_venv"
OMOP_VOCABULARY_CONCEPT_ID="44819096"
while true; do
  case "$1" in
  --app_id)
    APP_ID=$2
    shift 2
    ;;
  --in_dir)
    IN_DIR=$2
    shift 2
    ;;
  --dataset)
    DATASET=$2
    shift 2
    ;;
  --gcs_path)
    GCS_PATH=$2
    shift 2
    ;;
  --venv_dir)
    VENV_DIR=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${APP_ID}" ]] || [[ -z "${IN_DIR}" ]]; then
  echo "Usage: $USAGE"
  exit 1
fi

source ${SCRIPT_PATH}/set_path.sh

# Determine the version of the OMOP vocabulary
OMOP_VOCABULARY_VERSION="$(grep ${OMOP_VOCABULARY_CONCEPT_ID} ${IN_DIR}/VOCABULARY.csv | cut -f4)"
echo "Version of OMOP Standard vocabulary is ${OMOP_VOCABULARY_VERSION}"

# Move vocabulary files to backup folder
BACKUP_DIR="${IN_DIR}-backup"
echo "Creating backup in ${BACKUP_DIR}..."
mkdir ${BACKUP_DIR}
cp -a ${IN_DIR}/* ${BACKUP_DIR}

# create a new environment in directory curation_venv
virtualenv -p $(which python3.7) ${VENV_DIR}
# activate it
source ${VENV_DIR}/bin/activate
# install the requirements in the virtualenv
pip install -r requirements.txt

# Format dates, standardize line endings
echo "Transforming the files in ${IN_DIR}..."
python ${BASE_DIR}/vocabulary.py transform_files --in_dir ${BACKUP_DIR} --out_dir ${IN_DIR}

TEMP_DIR=$(mktemp -d)
echo "Created temp dir ${TEMP_DIR}"

# Append vocabulary and concept records
echo "Adding AoU_General and AoU_Custom to vocabulary in ${IN_DIR}..."
cp -a ${IN_DIR}/* ${TEMP_DIR}

python ${BASE_DIR}/vocabulary.py add_aou_vocabs --in_dir ${TEMP_DIR} --out_dir ${IN_DIR}

rm -rf ${TEMP_DIR}

# Upload to bucket
if [[ -z "${GCS_PATH}" ]]; then
  echo "GCS path not specified, skipping GCS upload and BigQuery load"
  exit 0
fi
echo "Uploading ${IN_DIR}/* to ${GCS_PATH}..."
gsutil -m cp ${IN_DIR}/* ${GCS_PATH}

# Load in BigQuery dataset
if [[ -z "${DATASET}" ]]; then
  echo "Dataset not specified, skipping BigQuery load"
  exit 0
fi
echo "Creating and loading dataset ${DATASET}..."
bq mk --project_id ${APP_ID} --dataset_id ${DATASET} --description "Vocabulary ${OMOP_VOCABULARY_VERSION} loaded from ${GCS_PATH}"
for file in $(gsutil ls ${GCS_PATH}); do
  filename=$(basename ${file})
  # bash3 friendly lowercase
  filename_lower=$(echo ${filename} | tr '[:upper:]' '[:lower:]')
  table_name="${filename_lower%.*}"
  echo "Loading ${DATASET}.${table_name}..."
  bq load --replace=true --project_id ${APP_ID} --source_format CSV --quote "" --field_delimiter "\t" --max_bad_records 500 --skip_leading_rows 1 ${DATASET}.${table_name} ${file} ${BASE_DIR}/resources/fields/${table_name}.json
done
