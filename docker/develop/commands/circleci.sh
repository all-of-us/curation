#!/usr/bin/env bash

set -e

CMD_NAME="circleci"

cd "${CIRCLE_WORKING_DIRECTORY}" && exec "${CMD_NAME}" "$@"