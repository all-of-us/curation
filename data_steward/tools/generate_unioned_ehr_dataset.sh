#!/usr/bin/env bash
set -ex

# Generate unioned_ehr dataset
# Loads vocabulary from specified dataset and unioned EHR tables from specified dataset


USAGE="
Usage: generate_unioned_ehr_dataset.sh
  --key_file <path to key file>
  --run_as <service account email for impersonation>
  --pmi_email <pmi-ops account email>
  --vocab_dataset <vocab dataset>
  --ehr_snapshot <EHR dataset>
  --dataset_release_tag <release tag for the CDR>
  --ehr_cutoff_date <ehr_cut_off date format yyyy-mm-dd>
"

echo
while true; do
  case "$1" in
  --key_file)
    key_file=$2
    shift 2
    ;;
  --run_as)
    run_as=$2
    shift 2
    ;;
  --pmi_email)
    pmi_email=$2
    shift 2
    ;;
  --vocab_dataset)
    vocab_dataset=$2
    shift 2
    ;;
  --ehr_snapshot)
    ehr_snapshot=$2
    shift 2
    ;;
  --dataset_release_tag)
    dataset_release_tag=$2
    shift 2
    ;;
  --ehr_cutoff_date)
    ehr_cutoff_date=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${run_as}" ]] || [[ -z "${pmi_email}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${ehr_snapshot}" ]] ||
  [[ -z "${dataset_release_tag}" ]]  || [[ -z "${ehr_cutoff_date}" ]]; then
  echo "${USAGE}"
  exit 1
fi

app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")

tag=$(git describe --abbrev=0 --tags)
version=${tag}

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"

echo "ehr_snapshot --> ${ehr_snapshot}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "dataset_release_tag --> ${dataset_release_tag}"
echo "ehr_cutoff_date --> ${ehr_cutoff_date}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

source "${TOOLS_DIR}/set_path.sh"

echo "-------------------------->Take a Snapshot of Unioned EHR Submissions (step 5)"
source_prefix="unioned_ehr_"
unioned_ehr_dataset="${dataset_release_tag}_unioned_ehr"
unioned_ehr_dataset_sandbox="${unioned_ehr_dataset}_sandbox"
unioned_ehr_dataset_backup="${unioned_ehr_dataset}_backup"
unioned_ehr_dataset_staging="${unioned_ehr_dataset}_staging"

#---------------------------------------------------------------------
# Step 1 Create an empty dataset
bq mk --dataset --description "copy unioned_ehr tables from ${ehr_snapshot}" --label "owner:curation" --label "phase:backup" --label "release_tag:${dataset_release_tag}" --label "de_identified:false" ${app_id}:${unioned_ehr_dataset_backup}

#----------------------------------------------------------------------
# Step 2 Create the clinical tables for unioned EHR data set
python "${DATA_STEWARD_DIR}/cdm.py" ${unioned_ehr_dataset_backup}

#----------------------------------------------------------------------
# Step 3 Copy OMOP vocabulary to unioned EHR data set
python "${DATA_STEWARD_DIR}/cdm.py" --component vocabulary ${unioned_ehr_dataset_backup}

"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${unioned_ehr_dataset_backup} --sync false

#----------------------------------------------------------------------
# Step 4 copy unioned ehr clinical tables tables
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snapshot} --source_prefix ${source_prefix} --target_dataset ${unioned_ehr_dataset_backup} --sync false

#----------------------------------------------------------------------
# Step 5 copy mapping tables tables
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snapshot} --source_prefix _mapping_ --target_dataset ${unioned_ehr_dataset_backup} --target_prefix _mapping_

echo "removing tables copies unintentionally"
bq rm -f ${unioned_ehr_dataset_backup}._mapping_ipmc_nu_condition_occurrence
bq rm -f ${unioned_ehr_dataset_backup}._mapping_ipmc_nu_device_exposure
bq rm -f ${unioned_ehr_dataset_backup}._mapping_ipmc_nu_drug_exposure
bq rm -f ${unioned_ehr_dataset_backup}._mapping_ipmc_nu_fact_relationship
bq rm -f ${unioned_ehr_dataset_backup}._mapping_ipmc_nu_measurement
bq rm -f ${unioned_ehr_dataset_backup}._mapping_ipmc_nu_note
bq rm -f ${unioned_ehr_dataset_backup}._mapping_ipmc_nu_observation
bq rm -f ${unioned_ehr_dataset_backup}._mapping_ipmc_nu_procedure_occurrence
bq rm -f ${unioned_ehr_dataset_backup}._mapping_ipmc_nu_specimen
bq rm -f ${unioned_ehr_dataset_backup}._mapping_ipmc_nu_visit_occurrence

# Run cleaning rules on unioned_ehr_dataset
# create an intermediary table to apply cleaning rules on
bq mk --dataset --description "intermediary dataset to apply cleaning rules on ${unioned_ehr_dataset_backup}" --label "owner:curation" --label "phase:staging" --label "release_tag:${dataset_release_tag}" --label "de_identified:false" ${app_id}:${unioned_ehr_dataset_staging}

"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${unioned_ehr_dataset_backup} --target_dataset ${unioned_ehr_dataset_staging}

export UNIONED_DATASET_ID="${unioned_ehr_dataset_staging}"
export BIGQUERY_DATASET_ID="${unioned_ehr_dataset_staging}"
export VOCABULARY_DATASET="${vocab_dataset}" # required by populate_route_ids cleaning rule
data_stage='unioned'

# create sandbox dataset
bq mk --dataset --description "Sandbox created for storing records affected by the cleaning rules applied to ${unioned_ehr_dataset}" --label "owner:curation" --label "phase:sandbox" --label "release_tag:${dataset_release_tag}" --label "de_identified:false" ${app_id}:${unioned_ehr_dataset_sandbox}

unset GOOGLE_APPLICATION_CREDENTIALS
gcloud config set account "${pmi_email}"

# run cleaning_rules on a dataset
python "${CLEANER_DIR}/clean_cdr.py" --project_id "${app_id}" --run_as "${run_as}" --dataset_id "${unioned_ehr_dataset_staging}" --sandbox_dataset_id "${unioned_ehr_dataset_sandbox}" --data_stage "${data_stage}" -s --cutoff_date "${ehr_cutoff_date}" 2>&1 | tee unioned_cleaning_log_"${unioned_ehr_dataset_staging}".txt

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
gcloud auth activate-service-account --key-file=${key_file}

# Create a snapshot dataset with the result
python "${TOOLS_DIR}/snapshot_by_query.py" --project_id "${app_id}" --dataset_id "${unioned_ehr_dataset_staging}" --snapshot_dataset_id "${unioned_ehr_dataset}"

bq update --description "${version} clean version of ${unioned_ehr_dataset_backup}" --set_label "owner:curation" --set_label "phase:clean" --set_label "release_tag:${dataset_release_tag}" --set_label "de_identified:false" ${app_id}:${unioned_ehr_dataset}

# Update sandbox description
bq update --description "Sandbox created for storing records affected by the cleaning rules applied to ${unioned_ehr_dataset}" --set_label "owner:curation" --set_label "phase:sandbox" --set_label "release_tag:${dataset_release_tag}" --set_label "de_identified:false" ${app_id}:${unioned_ehr_dataset_sandbox}


unset PYTHONPATH

set +ex
