#!/usr/bin/env bash

set -e

python "${CIRCLE_WORKING_DIRECTORY}/tests/runner.py" \
    --test-path "${CIRCLE_WORKING_DIRECTORY}/tests/unit_tests" \
    --coverage-file "${CIRCLE_WORKING_DIRECTORY}/.coveragerc_unit"
