#!/usr/bin/env bash

USAGE="
Usage: run_cleaning_rules.sh
  --key_file <path to key file>
  --app_id <application id>
  --vocab_dataset <vocab dataset>
  --dataset <dataset name to apply cleaning rules>
  --snapshot_dataset <Dataset name to copy result dataset>
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
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${app_id}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${dataset}" ]] || [[ -z "${snapshot_dataset}" ]]; then
  echo "$USAGE"
  exit 1
fi

today=$(date '+%Y%m%d')

echo "today --> ${today}"
echo "dataset --> ${dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "snapshot_dataset --> ${snapshot_dataset}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export APPLICATION_ID="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

#---------Create curation virtual environment----------
set -e
# create a new environment in directory curation_env
virtualenv -p $(which python2.7) curation_env

# activate it
source curation_env/bin/activate

# install the requirements in the virtualenv
pip install -t ../lib -r ../requirements.txt

source set_path.sh

#--------------------------------------------------------
export EHR_RDR_DATASET_ID="${dataset}"
export BIGQUERY_DATASET_ID="${dataset}"

cd ../cdr_cleaner/

# run cleaning_rules on a dataset
python clean_cdr.py -s 2>&1 | tee cleaning_rules_log.txt

cd ../tools/

# Create a snapshot dataset with the result
python create_snapshot_dataset_with_schemas.py -p "${app_id}" -d "${dataset}" -n "${snapshot_dataset}"

unset PYTHOPATH
deactivate
