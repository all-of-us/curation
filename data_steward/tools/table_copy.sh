#!/usr/bin/env bash

# Copy prefixed tables from one dataset to another, removing (or replacing) prefix
# Note: Attempts to create the target dataset before copying

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

if [ -z "${SOURCE_DATASET}" ] || [ -z "${SOURCE_PREFIX}" ] || [ -z "${TARGET_DATASET}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi

for t in $(bq ls ${SOURCE_DATASET} | grep TABLE | awk '{print $1}' | grep ${SOURCE_PREFIX})
do
  TARGET_TABLE=${t//${SOURCE_PREFIX}/}
  CP_CMD="bq cp ${SOURCE_DATASET}.${t} ${TARGET_DATASET}.${TARGET_PREFIX}${TARGET_TABLE}"
  echo $(${CP_CMD})
done
