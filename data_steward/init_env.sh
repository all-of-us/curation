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

# GCS buckets are globally unique, so we prefix with project id and username
export DRC_BUCKET_NAME="${PROJECT_PREFIX}_${USERNAME_PREFIX}_drc"
export BUCKET_NAME_FAKE="${PROJECT_PREFIX}_${USERNAME_PREFIX}_hpo_fake"
export BUCKET_NAME_NYC="${PROJECT_PREFIX}_${USERNAME_PREFIX}_hpo_nyc"
export BUCKET_NAME_PITT="${PROJECT_PREFIX}_${USERNAME_PREFIX}_hpo_pitt"
export BUCKET_NAME_CHS="${PROJECT_PREFIX}_${USERNAME_PREFIX}_hpo_chs"

# Datasets can be scoped by project so we prefix with username
# Note: Dataset IDs must be alphanumeric (plus underscores) and <= 1024 characters
# See https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#datasetReference.datasetId
export BIGQUERY_DATASET_ID="${USERNAME_PREFIX}_ehr"

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
fi