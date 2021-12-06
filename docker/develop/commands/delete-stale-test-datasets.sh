#!/usr/bin/env bash

set -e

CMD_NAME="delete-stale-test-datasets"

python3 /home/circleci/project/data_steward/tools/delete_stale_test_datasets.py