#!/usr/bin/env bash
set -ex

USAGE="
Usage: run_cleaning_rules.sh
  --key_file <path to key file>
  --vocab_dataset <vocab dataset>
  --dataset <dataset name to apply cleaning rules>
  --snapshot_dataset <Dataset name to copy result dataset>
  --data_stage <Dataset stage>
"

while true; do
  case "$1" in
  --app_id)
    app_id=$2
    shift 2
    ;;
  --dataset)
    dataset=$2
    shift 2
    ;;
  --vocab_dataset)
    vocab_dataset=$2
    shift 2
    ;;
  --key_file)
    key_file=$2
    shift 2
    ;;
  --snapshot_dataset)
    snapshot_dataset=$2
    shift 2
    ;;
  --data_stage)
    data_stage=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${app_id}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${dataset}" ]] || [[ -z "${snapshot_dataset}" ]] || [[ -z "${snapshot_dataset}" ]]; then
  echo "$USAGE"
  exit 1
fi

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"

app_id=$(cat "${key_file}" | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);')

today=$(date '+%Y%m%d')

echo "today --> ${today}"
echo "dataset --> ${dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "snapshot_dataset --> ${snapshot_dataset}"
echo "Data Stage --> ${data_stage}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

#---------Create curation virtual environment----------
# create a new environment in directory curation_venv
virtualenv -p "$(which python3.7)" "${DATA_STEWARD_DIR}/curation_venv"

# activate it
source "${DATA_STEWARD_DIR}/curation_venv/bin/activate"

# install the requirements in the virtualenv
pip install -r "${DATA_STEWARD_DIR}/requirements.txt"

source "${TOOLS_DIR}/set_path.sh"

#--------------------------------------------------------
export COMBINED_DATASET_ID="${dataset}"
export BIGQUERY_DATASET_ID="${dataset}"


# run cleaning_rules on a dataset
python "${CLEANER_DIR}/clean_cdr.py" -d ${data_stage} -s 2>&1 | tee cleaning_rules_log.txt

# Create a snapshot dataset with the result
python "${TOOLS_DIR}/snapshot_by_query.py" -p "${app_id}" -d "${dataset}" -n "${snapshot_dataset}"

unset PYTHOPATH
deactivate
