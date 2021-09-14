#!/usr/bin/env bash

set -e

# load funcs
source "${CIRCLE_SCRIPTS_DIR}"/funcs.sh

echo "Activating Google Cloud Credentials..."

# this must be defined whether we're running in CircleCI or
if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]];
then
  echo "Environment variable GOOGLE_APPLICATION_CREDENTIALS is missing or empty"
  exit 1
fi

# test if we're running within CircleCI
if in_ci;
then
  if [[ -z "${GCLOUD_CREDENTIALS_KEY}" ]] || [[ -z "${GCLOUD_CREDENTIALS}" ]];
  then
    echo "Environment variables GCLOUD_CREDENTIALS and/or GCLOUD_CREDENTIALS_KEY are missing or empty"
    exit 1
  fi

  echo "Writing decoded CircleCI GCP key to ${GOOGLE_APPLICATION_CREDENTIALS}"

  echo "${GCLOUD_CREDENTIALS}" | \
    openssl enc -d -aes-256-cbc -base64 -A -md md5 -k "${GCLOUD_CREDENTIALS_KEY}" \
    -out "${GOOGLE_APPLICATION_CREDENTIALS}"
fi

gcloud auth activate-service-account --key-file "${GOOGLE_APPLICATION_CREDENTIALS}" ;