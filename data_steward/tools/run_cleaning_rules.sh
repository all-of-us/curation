#!/usr/bin/env bash
set -ex

USAGE="
Usage: run_cleaning_rules.sh
  --key_file <path to key file>
  --vocab_dataset <vocab dataset>
  --dataset <dataset name to apply cleaning rules>
  --snapshot_dataset <Dataset name to copy result dataset>
  --data_stage <Dataset stage>
  --pre_post_deid <Dataset stage choose b/w pre_deid or post_died>
"

while true; do
  case "$1" in
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
  --pre_post_deid)
    pre_post_deid=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${dataset}" ]] || [[ -z "${snapshot_dataset}" ]] || [[ -z "${data_stage}" ]] || [[ -z "${pre_post_deid}" ]]; then
  echo "$USAGE"
  exit 1
fi

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"

app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")

today=$(date '+%Y%m%d')

echo "today --> ${today}"
echo "dataset --> ${dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "snapshot_dataset --> ${snapshot_dataset}"
echo "Data Stage --> ${data_stage}"
echo "pre_post_deid --> ${pre_post_deid}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

source "${TOOLS_DIR}/set_path.sh"

#--------------------------------------------------------
export COMBINED_DATASET_ID="${dataset}"
export BIGQUERY_DATASET_ID="${dataset}"


# run cleaning_rules on a dataset
python "${CLEANER_DIR}/clean_cdr.py" -d ${data_stage} -s 2>&1 | tee cleaning_rules_log.txt

# Create a snapshot dataset with the result
python "${TOOLS_DIR}/snapshot_by_query.py" -p "${app_id}" -d "${dataset}" -n "${snapshot_dataset}" -s "${pre_post_deid}"

unset PYTHOPATH
