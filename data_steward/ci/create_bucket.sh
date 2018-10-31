#!/bin/bash -e
USAGE="create_bucket.sh <bucket-name>"
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
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
gsutil mb gs://${BUCKET_NAME}
gsutil lifecycle set ${SCRIPTPATH}/${PROJECT}-bucket-lifecycle.json gs://${BUCKET_NAME}
gsutil iam set ${SCRIPTPATH}/${PROJECT}-bucket-iam-policy.txt gs://${BUCKET_NAME}
