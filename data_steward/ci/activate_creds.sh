#!/bin/bash -e

# Creates credentials file $1 from two environment variables (see
# below) which combine to decrypt the keys for a service account.
# Does gcloud auth using the result.

echo "1 ---------------------"
echo -n $GCLOUD_CREDENTIALS | tail -c 3
echo "2 ---------------------"
echo -n $GCLOUD_CREDENTIALS_KEY | tail -c 3
echo "3 ---------------------"

echo $GCLOUD_CREDENTIALS | \
     openssl enc -d -aes-256-cbc -base64 -A -md md5 -k $GCLOUD_CREDENTIALS_KEY \
     > $1

gcloud auth activate-service-account --key-file $1

