#!/bin/bash

# Default configuration is test
CONFIG="test"
if [ $# -ne 0 ]
then
    CONFIG=$1
fi

# Require username in GH_USERNAME or CIRCLE_USERNAME
export APPLICATION_ID="aou-res-curation-$CONFIG"
export USERNAME="${GH_USERNAME:-${CIRCLE_USERNAME:-}}"

if [ -z "${USERNAME}" ]
then
    echo "Please set environment variable GH_USERNAME or CIRCLE_USERNAME";
    exit 1;
fi

# Dataset IDs must be alphanumeric (plus underscores) and <= 1024 characters
# See https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#datasetReference.datasetId
PROJECT_PREFIX="${APPLICATION_ID//-/_}"
USERNAME_PREFIX="${USERNAME//-/_}"
# Determine branch name either by CIRCLE_BRANCH var or using git
CURRENT_BRANCH="${CIRCLE_BRANCH:-$(git rev-parse --abbrev-ref HEAD)}"
# Replace dashes, slashes with underscore
CURRENT_BRANCH="${CURRENT_BRANCH//-/_}"
CURRENT_BRANCH="${CURRENT_BRANCH//\//_}"
BUCKET_PREFIX="${PROJECT_PREFIX}_${CURRENT_BRANCH}_${USERNAME_PREFIX}"

# GCS buckets are globally unique, so we prefix with project id and username
export DRC_BUCKET_NAME="${BUCKET_PREFIX}_drc"
export BUCKET_NAME_FAKE="${BUCKET_PREFIX}_hpo_fake"
export BUCKET_NAME_NYC="${BUCKET_PREFIX}_hpo_nyc"
export BUCKET_NAME_PITT="${BUCKET_PREFIX}_hpo_pitt"
export BUCKET_NAME_CHS="${BUCKET_PREFIX}_hpo_chs"
export BUCKET_NAME_UNIONED_EHR="${BUCKET_PREFIX}_drc"

# Datasets can be scoped by project so we prefix with username
# Note: Dataset IDs must be alphanumeric (plus underscores) and <= 1024 characters
# See https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#datasetReference.datasetId
DATASET_PREFIX="${CURRENT_BRANCH}_${USERNAME_PREFIX}"
export BIGQUERY_DATASET_ID="${DATASET_PREFIX}_ehr"
export RDR_DATASET_ID="${DATASET_PREFIX}_rdr"

# .circlerc is sourced before each test and deploy command
# See https://www.compose.com/articles/experience-with-circleci/#dontcommitcredentials
if [ -n "CIRCLECI" ]
then
  echo "export APPLICATION_ID=${APPLICATION_ID}" >> $HOME/.circlerc
  echo "export USERNAME=${USERNAME}" >> $HOME/.circlerc
  echo "export DRC_BUCKET_NAME=${DRC_BUCKET_NAME}" >> $HOME/.circlerc
  echo "export BUCKET_NAME_FAKE=${BUCKET_NAME_FAKE}" >> $HOME/.circlerc
  echo "export BUCKET_NAME_NYC=${BUCKET_NAME_NYC}" >> $HOME/.circlerc
  echo "export BUCKET_NAME_PITT=${BUCKET_NAME_PITT}" >> $HOME/.circlerc
  echo "export BUCKET_NAME_CHS=${BUCKET_NAME_CHS}" >> $HOME/.circlerc
  echo "export BIGQUERY_DATASET_ID=${BIGQUERY_DATASET_ID}" >> $HOME/.circlerc
  echo "export RDR_DATASET_ID=${RDR_DATASET_ID}" >> $HOME/.circlerc
  echo "export BUCKET_NAME_UNIONED_EHR=${BUCKET_NAME_UNIONED_EHR}" >> $HOME/.circlerc
fi
