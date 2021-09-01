#!/usr/bin/env bash

set -e

cd "${CIRCLE_WORKING_DIRECTORY}" \
  && tests/run_tests.sh -s unit