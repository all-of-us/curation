#!/usr/bin/env bash

set -e
set +x

source "${CURATION_SCRIPTS_DIR}/funcs.sh"

DEFINED=$(declare -F | sed -e 's/declare -f //g' -e 's/^.*/  &/g')

HELP_TEXT=$(
  cat <<EOT
${CMD_NAME} command

  This command allows arbitrary execution of anything within the container.

Functions file:

 "${CURATION_SCRIPTS_DIR}/funcs.sh"

Working directory:

  "${CIRCLE_WORKING_DIRECTORY}"

Declared functions:

${DEFINED}

Usage:

  ./curation.sh execute underscore_me "not-underscored"

  # the above will result in this output:

Executing:
  Function: underscore_me
  Args: not-underscored

Execution results:
  Code: 0
  Output:
-- output start
not_underscored
-- output end
EOT
)

run_args=("${@}")

for i in "${run_args[@]}"; do
  case $i in
  --help)
    echo "${HELP_TEXT}"
    exit 0
    ;;
  -help)
    echo "${HELP_TEXT}"
    exit 0
    ;;
  help)
    echo "${HELP_TEXT}"
    exit 0
    ;;
  *) ;;

  esac
done

cd "${CIRCLE_WORKING_DIRECTORY}" && capture_execution "${run_args[@]}"
