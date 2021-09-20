#!/usr/bin/env bash

set -e

cd "${CIRCLE_WORKING_DIRECTORY}" &&
  python tests/runner.py \
    --test-path "${CIRCLE_WORKING_DIRECTORY}/tests/unit_tests" \
    --coverage-file ".coveragerc_unit"
