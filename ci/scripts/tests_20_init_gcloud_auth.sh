#!/usr/bin/env bash

set -e

echo "Activating Google Cloud Credentials..."

if [[ -n "${CIRCLECI}" ]];
then
  echo "Writing decoded CircleCI GCP key to ${GOOGLE_APPLICATION_CREDENTIALS}"

  openssl enc -d -aes-256-cbc -base64 -A -md md5 -k "${GCLOUD_CREDENTIALS_KEY}" -in <(echo "{$GCLOUD_CREDENTIALS}") -out "${GOOGLE_APPLICATION_CREDENTIALS}"
fi

gcloud auth activate-service-account --key-file "${GOOGLE_APPLICATION_CREDENTIALS}" ;