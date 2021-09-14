#!/usr/bin/env bash

set -e

source "${CURATION_SCRIPTS_DIR}"/funcs.sh

HELP_TEXT=$(cat << EOT
Must provide at least one of "lint", "unit", and / or "integration" as argument to execution
EOT
)

RUN_UNIT=0
RUN_INTEGRATION=0
RUN_LINT=0

for i in "$@"; do
  case $i in
    unit)
      RUN_UNIT=1
      shift
      ;;
    integration)
      RUN_INTEGRATION=1
      shift
      ;;
    lint)
      RUN_LINT=1
      shift
      ;;
    *)
      echo "Unknown option ${i}"
      echo "${HELP_TEXT}"
      exit 1
      ;;
  esac
done

if [ "${RUN_LINT}" -ne 1 ] && [ "${RUN_UNIT}" -ne 1 ] && [ "${RUN_INTEGRATION}" -ne 1 ];
then
  echo "${HELP_TEXT}"
  exit 0
fi

echo "Initializing envvars..."
require_ok "00_init_env.sh"

if [ "${RUN_LINT}" -eq 1 ];
then
  echo "Running linting checks..."

  # if we're running inside CI, execute some additional lint checks
  if in_ci;
  then
    require_ok "lint_00_validate_commit_message.sh"
    require_ok "lint_10_validate_pr_title.sh"
  fi

  # always execute yapf & pylint checks
  require_ok "lint_20_yapf.sh"
  require_ok "lint_30_pylint.sh"
fi

if [ "${RUN_UNIT}" -eq 1 ] || [ "${RUN_INTEGRATION}" -eq 1 ];
then
  echo "Running test setup script(s)..."

  require_ok "tests_00_prep_output_paths.sh"
  require_ok "tests_10_init_gcloud_auth.sh"
fi

if [ "${RUN_UNIT}" -eq 1 ];
then
  require_ok "tests_unit_00_execute.sh"
fi

if [ "${RUN_INTEGRATION}" -eq 1 ];
then
  if ! in_ci ;
  then
      export FORCE_RUN_INTEGRATION=1
  fi
  require_ok "tests_integration_00_execute.sh"
  require_ok "tests_integration_99_teardown.sh"
fi

if [ "${RUN_UNIT}" -eq 1 ] || [ "${RUN_INTEGRATION}" -eq 1 ];
then
  require_ok "tests_50_combine_coverage.sh"
fi

echo "We've reached the end."