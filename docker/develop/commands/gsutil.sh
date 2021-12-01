#!/usr/bin/env bash

set -e
set +x

source "${CURATION_SCRIPTS_DIR}/funcs.sh"

CMD_NAME="gsutil"

activate_gcloud

cd "${CIRCLE_WORKING_DIRECTORY}" && exec "${CMD_NAME}" "$@"