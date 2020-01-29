#!/usr/bin/env bash
set -ex

USAGE="
Usage: run_achilles_report.sh
  --key_file <path to key file>
  --vocab_dataset <vocab dataset>
  --dataset <Dataset ID>
  --result_bucket <Internal bucket>
"

while true; do
  case "$1" in
  --key_file)
    key_file=$2
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
  --result_bucket)
    result_bucket=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${dataset}" ]] || [[ -z "${result_bucket}" ]]; then
  echo "$USAGE"
  exit 1
fi

app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"

echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "dataset --> ${dataset}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "result_bucket --> ${result_bucket}"

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
#------------------------------------------------------

export BIGQUERY_DATASET_ID="${dataset}"
export BUCKET_NAME_NYC="test-bucket"

# copy vocabulary tables to the rdr dataset to run the achilles analysis.
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${dataset}

# Run Achilles analysis
python "${TOOLS_DIR}/run_achilles_and_export.py" --bucket=${result_bucket} --folder=${dataset}

# Deactivate venv and unset PYTHONPATH
unset PYTHONPATH
deactivate

set +ex
