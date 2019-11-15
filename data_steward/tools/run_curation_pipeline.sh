#!/usr/bin/env bash

# This script automates the curation playbook (https://docs.google.com/document/d/1QnwUhJmrVc9JNt64goeRw-K7490gbaF7DsYhTTt9tUo/edit#heading=h.k24j7tgoprtn)
# It assuming steps 1-3 are already done via a daily cron job. This script automates 4-7.

ehr_dataset="auto_pipeline_input"
rdr_dataset="test_rdr"
result_bucket="drc_curation_internal_test"

USAGE="
Usage: run_curation_pipeline.sh
  --key_file <path to key file>
  --vocab_dataset <vocab dataset>
  --deid_config <path to deid config json file>
  [--ehr_dataset <EHR dataset: default is ${ehr_dataset}>]
  [--rdr_dataset <RDR dataset: default is ${rdr_dataset}>]
  [--result_bucket <Internal bucket: default is ${result_bucket}>]
  --identifier <version identifier>
"

while true; do
  case "$1" in
  --ehr_dataset)
    ehr_dataset=$2
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
  --rdr_dataset)
    rdr_dataset=$2
    shift 2
    ;;
  --result_bucket)
    result_bucket=$2
    shift 2
    ;;
  --deid_config)
    deid_config=$2
    shift 2
    ;;
  --identifier)
    identifier=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${app_id}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${deid_config}" ]] || [[ -z "${identifier}" ]]; then
  echo "Specify the key file location, application ID and deid config file. $USAGE"
  exit 1
fi

today=$(date '+%Y%m%d')
current_dir=$(pwd)
app_id=$(cat "${key_file}" | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);')

echo "today --> ${today}"
echo "ehr_dataset --> ${ehr_dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "rdr_dataset --> ${rdr_dataset}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "result_bucket --> ${result_bucket}"
echo "deid_config --> ${deid_config}"
echo "current_dir --> ${current_dir}"
echo "identifier --> ${identifier}"

#---------------------------------------------------------
# Step 1 create EHR snapshot
echo "-------------------------->Take a Snapshot of EHR Dataset (step 1)"
ehr_snapshot="${identifier}_ehr"
rdr_snapshot="${identifier}_rdr"
echo "ehr_snapshot --> ${ehr_snapshot}"
echo "rdr_snapshot= ${rdr_snapshot}"
./create_ehr_and_rdr_snapshot.sh --key_file ${key_file} --app_id ${app_id} --ehr_dataset ${ehr_dataset} --rdr ${rdr_dataset} --identifier ${identifier}

#---------------------------------------------------------
# Step 2 Generate Unioned ehr dataset
echo "-------------------------->Generate Unioned ehr dataset (step 2)"
source_prefix="unioned_ehr_"
unioned_ehr_dataset="${identifier}_unioned_ehr"
echo "unioned_ehr_dataset --> $unioned_ehr_dataset"
./generate_unioned_ehr_dataset.sh --key_file ${key_file} --app_id ${app_id} --ehr_snapshot ${ehr_snapshot} --vocab_dataset ${vocab_dataset} --identifier ${identifier}

#---------------------------------------------------------
# Step 3 Generate combined dataset
echo "-------------------------->Generate combined ehr rdr dataset (step 3)"
combined_backup="${identifier}_combined_backup"
combined="${identifier}_combined"
tag=$(git describe --abbrev=0 --tags)
version=${tag}

./generate_combined_dataset.sh --key_file ${key_file} --app_id ${app_id} --vocab_dataset ${vocab_dataset} --unioned_ehr_dataset ${unioned_ehr_dataset} --rdr_dataset ${rdr_dataset} --identifier ${identifier}

#-------------------------------------------------------
# Step 4 Run achilles on combined dataset
echo "-------------------------->Run achilles on identified CDR"
export BIGQUERY_DATASET_ID="${combined_backup}"
export BUCKET_NAME_NYC="test-bucket"
./run_achilles_report.sh --dataset ${combined_backup} --key_file ${key_file} --app_id ${app_id} --result_bucket ${result_bucket}

echo "-------------------------->Run achilles on identified CDR clean"
export BIGQUERY_DATASET_ID="${combined}"
export BUCKET_NAME_NYC="test-bucket"
./run_achilles_report.sh --dataset ${combined} --key_file ${key_file} --app_id ${app_id} --result_bucket ${result_bucket}

#--------------------------------------------------------
# Step 5 Run deid on cdr
echo "-------------------------->Run de identification on the identified CDR"
cdr_deid="${combined}_deid"
cdr_deid_base="${identifier}_deid_base"
cdr_deid_clean="${identifier}_deid_clean"
echo "cdr_deid --> ${cdr_deid}"
./deid_runner.sh --key_file ${key_file} --cdr_id ${combined} --vocab_dataset ${vocab_dataset} --identifier ${identifier}

cd tools || exit

#-------------------------------------------------------
# Step 6 Run achilles on de-identified dataset
echo "-------------------------->Run achilles on de-identified CDR backup"
echo "cdr_deid --> ${cdr_deid}"
./run_achilles_report.sh --dataset ${cdr_deid} --key_file ${key_file} --app_id ${app_id} --result_bucket ${result_bucket}

echo "-------------------------->Run achilles on de-identified CDR deid base"
echo "cdr_deid_base --> ${cdr_deid_base}"
./run_achilles_report.sh --dataset ${cdr_deid_base} --key_file ${key_file} --app_id ${app_id} --result_bucket ${result_bucket}

echo "-------------------------->Run achilles on de-identified CDR deid clean"
echo "cdr_deid_clean --> ${cdr_deid_clean}"
./run_achilles_report.sh --dataset ${cdr_deid_clean} --key_file ${key_file} --app_id ${app_id} --result_bucket ${result_bucket}
