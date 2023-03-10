#!/usr/bin/env bash
set -ex
# This Script automates the process of generating the ehr_snapshot

USAGE="
Usage: create_ehr_snapshot.sh
  --key_file <path to key file>
  --ehr_dataset <EHR dataset ID>
  --dataset_release_tag <release tag for the CDR>
"

while true; do
  case "$1" in
  --key_file)
    key_file=$2
    shift 2
    ;;
  --ehr_dataset)
    ehr_dataset=$2
    shift 2
    ;;
  --dataset_release_tag)
    dataset_release_tag=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${ehr_dataset}" ]] || [[ -z "${dataset_release_tag}" ]] ; then
  echo "${USAGE}"
  exit 1
fi

app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")

echo "ehr_dataset --> ${ehr_dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "dataset_release_tag --> ${dataset_release_tag}"

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

# shellcheck source=src/set_path.sh
source "${TOOLS_DIR}/set_path.sh"

echo "-------------------------->Snapshotting EHR Dataset"
ehr_snapshot="${dataset_release_tag}_ehr"
echo "ehr_snapshot --> ${ehr_snapshot}"

bq mk --dataset --description "snapshot of latest EHR dataset ${ehr_dataset} ran on $(date +'%Y-%m-%d')" --label "owner:curation" --label "release_tag:${dataset_release_tag}" --label "de_identified:false" ${app_id}:${ehr_snapshot}

#copy tables
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_dataset} --target_dataset ${ehr_snapshot} --sync false

echo "Done."

unset PYTHONPATH

set +ex
