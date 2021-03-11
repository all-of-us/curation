#!/usr/bin/env bash
set -ex
# This Script automates the process of generating the rdr_snapshot and apply rdr cleaning rules

USAGE="
Usage: create_rdr_snapshot.sh
  --key_file <path to key file>
  --rdr_dataset <RDR dataset ID>
  --dataset_release_tag <release tag for the CDR>
  --truncation_date date to truncate the RDR data to. The cleaning rules defaults to the current date if unset.
"

while true; do
  case "$1" in
  --key_file)
    key_file=$2
    shift 2
    ;;
  --rdr_dataset)
    rdr_dataset=$2
    shift 2
    ;;
  --dataset_release_tag)
    dataset_release_tag=$2
    shift 2
    ;;
  --truncation_date)
    truncation_date=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] ||  [[ -z "${rdr_dataset}" ]] || [[ -z "${dataset_release_tag}" ]] ; then
  echo "${USAGE}"
  exit 1
fi

# specific check on truncation_date. It should not cause a failure if it is not set.
if [[ -z "${truncation_date}" ]] ; then
  echo "truncation_date is unset.  Will default to the current date in the cleaning rule."
fi

app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")

echo "rdr_dataset --> ${rdr_dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "dataset_release_tag --> ${dataset_release_tag}"
echo "rdr truncation_date --> .  ${truncation_date}  ."

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
#CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

# shellcheck source=src/set_path.sh
source "${TOOLS_DIR}/set_path.sh"
#------------------------------------------------------
tag=$(git describe --abbrev=0 --tags)
version=${tag}

echo "--------------------------> Snapshotting  and cleaning RDR Dataset"
rdr_clean="${dataset_release_tag}_rdr"
rdr_clean_staging="${rdr_clean}_staging"
rdr_clean_sandbox="${rdr_clean}_sandbox"
rdr_clean_staging_sandbox="${rdr_clean_staging}_sandbox"

# create empty staging dataset
bq mk --dataset --description "Intermediary dataset to apply cleaning rules on ${rdr_dataset}" --label "phase:staging" --label "release_tag:${dataset_release_tag}" --label "de_identified:false"  "${app_id}":"${rdr_clean_staging}"

# create empty sandbox dataset
bq mk --dataset --description "Sandbox created for storing records affected by the cleaning rules applied to ${rdr_clean_staging}" --label "phase:sandbox" --label "release_tag:${dataset_release_tag}" --label "de_identified:false"  "${app_id}":"${rdr_clean_staging_sandbox}"

#copy tables
"${TOOLS_DIR}/table_copy.sh" --source_app_id "${app_id}" --target_app_id "${app_id}" --source_dataset "${rdr_dataset}" --target_dataset "${rdr_clean_staging}" --sync false

#set BIGQUERY_DATASET_ID variable to dataset name where the vocabulary exists
export BIGQUERY_DATASET_ID="${rdr_clean_staging}"
export RDR_DATASET_ID="${rdr_clean_staging}"
echo "Cleaning the RDR data"
data_stage="rdr"

echo "--------------------------> applying cleaning rules on staging"
python "${CLEANER_DIR}/clean_cdr.py"  --project_id "${app_id}" --dataset_id "${rdr_clean_staging}" --sandbox_dataset_id "${rdr_clean_staging_sandbox}" --data_stage ${data_stage} --truncation_date "${truncation_date}" -s 2>&1 | tee rdr_cleaning_log_"${rdr_clean}".txt

# Create a snapshot dataset with the result
python "${TOOLS_DIR}/snapshot_by_query.py" --project_id "${app_id}" --dataset_id "${rdr_clean_staging}" --snapshot_dataset_id "${rdr_clean}"

bq update --description "${version} clean version of ${rdr_dataset}" --set_label "phase:clean" --set_label "release_tag:${dataset_release_tag}" --set_label "de_identified:false" ${app_id}:${rdr_clean}

#copy sandbox dataset
"${TOOLS_DIR}/table_copy.sh" --source_app_id "${app_id}" --target_app_id "${app_id}" --source_dataset "${rdr_clean_staging_sandbox}" --target_dataset "${rdr_clean_sandbox}"

# Update sandbox description
bq update --description "Sandbox created for storing records affected by the cleaning rules applied to ${rdr_clean}" --set_label "phase:sandbox" --set_label "release_tag:${dataset_release_tag}" --set_label "de_identified:false" "${app_id}":"${rdr_clean_sandbox}"

bq rm -r -d "${rdr_clean_staging_sandbox}"
bq rm -r -d "${rdr_clean_staging}"

echo "Done."

unset PYTHONPATH

set +ex