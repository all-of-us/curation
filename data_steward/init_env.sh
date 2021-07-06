#!/bin/bash

# Default configuration is test
CONFIG="test"
if [ $# -ne 0 ]; then
  CONFIG=$1
fi

# TODO: Move away from this global state; none of the following is sticky since
# there is no expectation of rerunning this script in new shells or on machine
# restarts.
export GOOGLE_APPLICATION_CREDENTIALS=${HOME}/gcloud-credentials-key.json
export APPLICATION_ID=aou-res-curation-test
export GOOGLE_CLOUD_PROJECT=aou-res-curation-test
export VOCABULARY_DATASET=vocabulary20210601

# Require username in GH_USERNAME or CIRCLE_USERNAME
export USERNAME=$(echo "${GH_USERNAME:-${CIRCLE_USERNAME:-}}" | tr '[:upper:]' '[:lower:]')

if [ -z "${USERNAME}" ]; then
  if [ -n "${CIRCLECI}" ]; then
    # If we're on circle without a CIRCLE_NAME, fallback to a default username.
    # This can happen if a Circle job is triggered outside the context of a
    # specific user interaction, e.g. during automated nightly cron runs.
    export USERNAME="circleci"
  else
    echo "Please set environment variable GH_USERNAME, CIRCLE_USERNAME, or CIRCLECI"
    exit 1
  fi
fi

# Dataset IDs must be alphanumeric (plus underscores) and <= 1024 characters
# See https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#datasetReference.datasetId
PROJECT_PREFIX="${APPLICATION_ID//-/_}"
USERNAME_PREFIX="${USERNAME//-/_}"
PROJECT_USERNAME="${CIRCLE_PROJECT_USERNAME//-/_}"
# Determine branch name either by CIRCLE_BRANCH var or using git
CURRENT_BRANCH="${CIRCLE_BRANCH:-$(git rev-parse --abbrev-ref HEAD)}"
# Replace dashes, slashes with underscore
CURRENT_BRANCH="${CURRENT_BRANCH//-/_}"
CURRENT_BRANCH="${CURRENT_BRANCH//\//_}"
BUCKET_PREFIX="${PROJECT_USERNAME}_${USERNAME_PREFIX}_${CURRENT_BRANCH}"

# GCS buckets are globally unique, so we prefix with project id and username
export DRC_BUCKET_NAME="${BUCKET_PREFIX}_drc"
export BUCKET_NAME_FAKE="${BUCKET_PREFIX}_fake"
export BUCKET_NAME_NYC="${BUCKET_PREFIX}_nyc"
export BUCKET_NAME_PITT="${BUCKET_PREFIX}_pitt"
export BUCKET_NAME_CHS="${BUCKET_PREFIX}_chs"
export BUCKET_NAME_UNIONED_EHR="${BUCKET_PREFIX}_drc"

# Datasets can be scoped by project so we prefix with username
# Note: Dataset IDs must be alphanumeric (plus underscores) and <= 1024 characters
# See https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#datasetReference.datasetId
DATASET_PREFIX="${PROJECT_USERNAME}_${USERNAME_PREFIX}_${CURRENT_BRANCH}"
export BIGQUERY_DATASET_ID="${DATASET_PREFIX}_ehr"
export RDR_DATASET_ID="${DATASET_PREFIX}_rdr"
export COMBINED_DATASET_ID="${DATASET_PREFIX}_combined"
export UNIONED_DATASET_ID="${DATASET_PREFIX}_unioned"
export COMBINED_DEID_DATASET_ID="${DATASET_PREFIX}_deid"

# .circlerc is sourced before each test and deploy command
# See https://www.compose.com/articles/experience-with-circleci/#dontcommitcredentials
if [ -n "${CIRCLECI}" ]; then
  echo "export GOOGLE_APPLICATION_CREDENTIALS=${HOME}/gcloud-credentials-key.json" >>"${BASH_ENV}"
  echo "export APPLICATION_ID=${APPLICATION_ID}" >>"${BASH_ENV}"
  echo "export GOOGLE_CLOUD_PROJECT=${GOOGLE_CLOUD_PROJECT}" >>"${BASH_ENV}"
  echo "export USERNAME=${USERNAME}" >>"${BASH_ENV}"
  echo "export DRC_BUCKET_NAME=${DRC_BUCKET_NAME}" >>"${BASH_ENV}"
  echo "export BUCKET_NAME_FAKE=${BUCKET_NAME_FAKE}" >>"${BASH_ENV}"
  echo "export BUCKET_NAME_NYC=${BUCKET_NAME_NYC}" >>"${BASH_ENV}"
  echo "export BUCKET_NAME_PITT=${BUCKET_NAME_PITT}" >>"${BASH_ENV}"
  echo "export BUCKET_NAME_CHS=${BUCKET_NAME_CHS}" >>"${BASH_ENV}"
  echo "export BIGQUERY_DATASET_ID=${BIGQUERY_DATASET_ID}" >>"${BASH_ENV}"
  echo "export RDR_DATASET_ID=${RDR_DATASET_ID}" >>"${BASH_ENV}"
  echo "export COMBINED_DATASET_ID=${COMBINED_DATASET_ID}" >>"${BASH_ENV}"
  echo "export COMBINED_DEID_DATASET_ID=${COMBINED_DEID_DATASET_ID}" >>"${BASH_ENV}"
  echo "export UNIONED_DATASET_ID=${UNIONED_DATASET_ID}" >>"${BASH_ENV}"
  echo "export BUCKET_NAME_UNIONED_EHR=${BUCKET_NAME_UNIONED_EHR}" >>"${BASH_ENV}"
  echo "export VOCABULARY_DATASET=${VOCABULARY_DATASET}" >>"${BASH_ENV}"
  echo "export PATH=${PATH}:${CIRCLE_WORKING_DIRECTORY}/ci" >>"${BASH_ENV}"
fi
