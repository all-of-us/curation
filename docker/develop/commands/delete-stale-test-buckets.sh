#!/usr/bin/env bash

set -e

source "${CURATION_SCRIPTS_DIR}"/funcs.sh

echo "Initializing envvars..."
require_ok "run-tests/00_init_env.sh"
activate_gcloud

python3 ./data_steward/tools/delete_stale_test_buckets.py --first_n=200 