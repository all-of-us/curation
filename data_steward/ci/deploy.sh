#!/bin/bash

# Deploys the application to the project $1 and installs configuration $2 using
# service account credentials file $3.

# Assumes 'gcloud auth' has been done with an appropriate deploy account.

# This script is executed by the circle.yml config when the conditions
# specified there are met, and is also used by the production admin.

# The AppEngine Admin API must be activated for the project being deployed
# to (it is not activated by default).

# The gcloud deployment account must have permissions as described here:
# https://cloud.google.com/appengine/docs/python/access-control

# 1. In particular it requires project editor permission: this is currently
# required to push queues.yaml, cron.yaml, and index.yaml.

# 2. It must also have write access to the two Cloud Storage buckets used
# for staging the deployment - this comes automatically with project editor
# status.

# In the future it will likely be possible to grant weaker permissions
# (such as the AppEngine Deployer role).

set -e

# When this script exits, kill any child jobs.
trap '[[ "$(jobs -p)" ]] && kill $(jobs -p)' EXIT

PROJECT_ID=$1
if [ -n "${CIRCLE_TAG}" ]
then
  # For a tagged commit, deploy a version matching that tag. We tag some commits
  # on master to tell CircleCI to deploy them as named versions.
  VERSION=${CIRCLE_TAG}
else
  # On test, CircleCI automatically deploys all commits to master, for testing.
  # These versions need not persist, so give them all the same name.
  VERSION=circle-ci-test
fi
echo "Deploying $VERSION to: $PROJECT_ID"

export CLOUDSDK_CORE_DISABLE_PROMPTS=1
# cp cron_default.yaml cron.yaml

echo "Deploying curation to ${PROJECT_ID}"
# chmod 700 ./generate_app.yaml.sh
./generate_app_yaml.sh test
# cat app_base.yaml app_nonprod.yaml > app.yaml
# - gcloud app deploy app.yaml --project=all-of-us-ehr-dev --version=${VERSION}
# gcloud app deploy app.yaml cron.yaml index.yaml offline.yaml queue.yaml --project=${PROJECT_ID} --version=${VERSION}
gcloud app deploy app.yaml --project=${PROJECT_ID} --version=${VERSION}
# rm app.yaml cron.yaml
