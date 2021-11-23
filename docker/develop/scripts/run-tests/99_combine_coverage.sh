#!/usr/bin/env bash

set -e

"${CIRCLE_WORKING_DIRECTORY}"/tests/combine_coverage.sh
