#!/bin/bash -e
USAGE="delete_bucket.sh <bucket-name>"
BUCKET_NAME="$1"
if [ -z "${APPLICATION_ID}" ]
then
  PROJECT="$(gcloud config get-value project)"
else
  PROJECT="${APPLICATION_ID}"
fi
if [ -z "${BUCKET_NAME}" ] || [ -z "${PROJECT}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi
gsutil rb gs://${BUCKET_NAME}