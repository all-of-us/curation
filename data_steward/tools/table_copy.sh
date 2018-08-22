#!/usr/bin/env bash

# Copy prefixed tables from one dataset to another, removing (or replacing) prefix
# Note: Attempts to create the target dataset before copying
#
# # Example Usages
#
# ## Copy `dataset1.prefix_tableA` to `dataset2.tableA`
# > tools/table_copy.sh --source_dataset dataset1 --source_prefix prefix_ --target_dataset dataset2
#
# ## Copy `dataset1.tableA` to `dataset1.prefix_tableA`
# > tools/table_copy.sh --source_dataset dataset1 --target_prefix prefix_ --target_dataset dataset1

USAGE="tools/table_copy.sh --source_dataset <SOURCE_DATASET> --source_prefix <SOURCE_PREFIX> --target_dataset <TARGET_DATASET> [--target_prefix <TARGET_PREFIX:''>]"

while true; do
  case "$1" in
    --source_dataset) SOURCE_DATASET=$2; shift 2;;
    --target_dataset) TARGET_DATASET=$2; shift 2;;
    --source_prefix) SOURCE_PREFIX=$2; shift 2;;
    --target_prefix) TARGET_PREFIX=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

bq mk --dataset ${TARGET_DATASET}

if [ -z "${TARGET_PREFIX}" ]
then
  TARGET_PREFIX=""
fi

if [ -z "${SOURCE_DATASET}" ] || [ -z "${TARGET_DATASET}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi

# Copy the tables
# Tables beginning with underscore "_" are skipped
for t in $(bq ls -n 2000 ${SOURCE_DATASET} |
           grep TABLE |
           awk '{print $1}' |
           grep -v ^\_ |
           ( [[ "${SOURCE_PREFIX}" ]] && grep ${SOURCE_PREFIX} || cat ))
do
  TARGET_TABLE=${t//${SOURCE_PREFIX}/}
  CP_CMD="bq cp -f ${SOURCE_DATASET}.${t} ${TARGET_DATASET}.${TARGET_PREFIX}${TARGET_TABLE}"
  echo $(${CP_CMD})
done
