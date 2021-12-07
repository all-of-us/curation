#!/usr/bin/env bash

set -e

CMD_NAME="delete-stale-test-datasets"

source "${CURATION_SCRIPTS_DIR}"/funcs.sh


echo "Initializing envvars..."
require_ok "run-tests/00_init_env.sh"
require_ok "run-tests/10_prep_output_paths.sh"
activate_gcloud

python3 ./data_steward/tools/delete_stale_test_datasets.py