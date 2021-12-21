#!/usr/bin/env bash

set -e

source "${CURATION_SCRIPTS_DIR}"/funcs.sh

activate_gcloud

python3 ./data_steward/tools/delete_stale_test_buckets.py --first_n=200 