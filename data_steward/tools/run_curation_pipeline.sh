#!/usr/bin/env bash
set -ex

# This script automates the curation playbook (https://docs.google.com/document/d/1QnwUhJmrVc9JNt64goeRw-K7490gbaF7DsYhTTt9tUo/edit#heading=h.k24j7tgoprtn)
# It assuming steps 1-3 are already done via a daily cron job. This script automates 4-7.

USAGE="
Usage: run_curation_pipeline.sh
  --key_file <path to key file>
  --vocab_dataset <vocab dataset>
  --ehr_dataset <EHR dataset ID>
  --rdr_dataset <RDR dataset ID>
  --result_bucket <Internal bucket name>
  --dataset_release_tag <release tag for the CDR>
  --ehr_cutoff <ehr_cut_off date format yyyy-mm-dd>
  --rdr_export_date <date RDR export is run format yyyy-mm-dd>
"

while true; do
  case "$1" in
  --key_file)
    key_file=$2
    shift 2
    ;;
  --vocab_dataset)
    vocab_dataset=$2
    shift 2
    ;;
  --ehr_dataset)
    ehr_dataset=$2
    shift 2
    ;;
  --rdr_dataset)
    rdr_dataset=$2
    shift 2
    ;;
  --result_bucket)
    result_bucket=$2
    shift 2
    ;;
  --dataset_release_tag)
    dataset_release_tag=$2
    shift 2
    ;;
    --ehr_cutoff)
    ehr_cutoff=$2
    shift 2
    ;;
  --rdr_export_date)
    rdr_export_date=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${ehr_dataset}" ]] || [[ -z "${rdr_dataset}" ]] ||
 [[ -z "${result_bucket}" ]] || [[ -z "${dataset_release_tag}" ]] || [[ -z "${ehr_cutoff}" ]] || [[ -z "${rdr_export_date}" ]]; then
  echo "Specify the key file location, vocabulary and dataset release tag. $USAGE"
  exit 1
fi

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"

app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")

echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "ehr_dataset --> ${ehr_dataset}"
echo "rdr_dataset --> ${rdr_dataset}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "result_bucket --> ${result_bucket}"
echo "dataset_release_tag --> ${dataset_release_tag}"
echo "ehr_cutoff_date --> ${ehr_cutoff}"
echo "rdr_export_date --> ${rdr_export_date}"

#---------------------------------------------------------
# Step 1 create EHR and RDR snapshot
echo "-------------------------->Take a Snapshot of EHR Dataset (step 1)"
"${TOOLS_DIR}/create_ehr_and_rdr_snapshot.sh" --key_file ${key_file} --ehr_dataset ${ehr_dataset} --rdr_dataset ${rdr_dataset} --dataset_release_tag ${dataset_release_tag}

#---------------------------------------------------------
# Step 2 Generate Unioned ehr dataset
echo "-------------------------->Generate Unioned ehr dataset (step 2)"
ehr_snapshot="${dataset_release_tag}_ehr"
echo "ehr_snapshot ----> ${ehr_snapshot}"
"${TOOLS_DIR}/generate_unioned_ehr_dataset.sh" --key_file ${key_file} --ehr_snapshot ${ehr_snapshot} --vocab_dataset ${vocab_dataset} --dataset_release_tag ${dataset_release_tag}

#---------------------------------------------------------
# Step 3 Generate combined dataset
echo "-------------------------->Generate combined ehr rdr dataset (step 3)"
unioned_ehr_dataset="${dataset_release_tag}_unioned_ehr"
echo "unioned_ehr_dataset --> $unioned_ehr_dataset"
"${TOOLS_DIR}/generate_combined_dataset.sh" --key_file ${key_file} --vocab_dataset ${vocab_dataset} --unioned_ehr_dataset ${unioned_ehr_dataset} \
--rdr_dataset ${rdr_dataset} --dataset_release_tag ${dataset_release_tag} --ehr_cutoff_date ${ehr_cutoff_date} --rdr_export_date ${rdr_export_date}

#-------------------------------------------------------
# Step 4 Run achilles on combined dataset
echo "-------------------------->Run achilles on identified CDR"
combined_dataset="${dataset_release_tag}_combined"
combined_backup="${combined_dataset}_backup"
export BUCKET_NAME_NYC="test-bucket"

# Running Achilles analysis on Combined dataset
export BIGQUERY_DATASET_ID="${combined_backup}"
"${TOOLS_DIR}/run_achilles_report.sh" --dataset ${combined_backup} --key_file ${key_file} --result_bucket ${result_bucket} --vocab_dataset ${vocab_dataset}

# Running Achilles analysis on combined clean datased
export BIGQUERY_DATASET_ID="${combined_dataset}"
"${TOOLS_DIR}/run_achilles_report.sh" --dataset ${combined_dataset} --key_file ${key_file} --result_bucket ${result_bucket} --vocab_dataset ${vocab_dataset}

#--------------------------------------------------------
# Step 5 Run deid on cdr
echo "-------------------------->Run de identification on the identified CDR"
cdr_deid="${combined_dataset}_deid"
echo "cdr_deid --> ${cdr_deid}"
"${TOOLS_DIR}/deid_runner.sh" --key_file ${key_file} --cdr_id ${combined_dataset} --vocab_dataset ${vocab_dataset} --dataset_release_tag ${dataset_release_tag}

#-------------------------------------------------------
# Step 6 Run achilles on de-identified dataset
echo "-------------------------->Run achilles on de-identified CDR"
cdr_deid_base="${cdr_deid}_base"
cdr_deid_clean="${cdr_deid}_clean"
export BUCKET_NAME_NYC="test-bucket"

# Running Achilles analysis on deid_base dataset
export BIGQUERY_DATASET_ID="${cdr_deid_base}"
"${TOOLS_DIR}/run_achilles_report.sh" --dataset ${cdr_deid_base} --key_file ${key_file} --result_bucket ${result_bucket} --vocab_dataset ${vocab_dataset}

# Running Achilles analysis on deid_clean dataset
export BIGQUERY_DATASET_ID="${cdr_deid_clean}"
"${TOOLS_DIR}/run_achilles_report.sh" --dataset ${cdr_deid_clean} --key_file ${key_file} --result_bucket ${result_bucket} --vocab_dataset ${vocab_dataset}

#---------------------------------------------------------
# Step 7 Snapshot result datasets for cohort builder team to use
output_base="${dataset_release_tag}_deid_output_base"
output="${dataset_release_tag}_deid_output"
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${cdr_deid_base} --target_dataset ${output_base} --sync false

"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${cdr_deid_clean} --target_dataset ${output} --sync false

set +ex