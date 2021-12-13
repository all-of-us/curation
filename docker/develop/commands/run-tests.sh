#!/usr/bin/env bash

set -e

CMD_NAME="run-tests"

source "${CURATION_SCRIPTS_DIR}"/funcs.sh

UNIT_NAME="unit"
INTEGRATION_NAME="integration"
LINT_NAME="lint"

HELP_TEXT=$(
  cat <<EOT
${CMD_NAME} command

  This command allows you to run any / all of the defined tests within the Curation project.

Available subcommands:
  "${LINT_NAME}" - Execute all "linter" tests, including yapf and pylint
    Example:
      Linux & macOS:
        ./curation.sh ${CMD_NAME} lint
      Windows:
        .\curation.ps1 ${CMD_NAME} lint
    Flags:
      None

  "${UNIT_NAME}" - Execute all "unit" tests.
    Example:
      Linux & macOS:
        ./curation.sh ${CMD_NAME} unit
      Windows:
        .\curation.ps1 ${CMD_NAME} unit
    Flags:
      --test-pattern    provide name of file to limit run to
          Example: ./curation.sh ${CMD_NAME} '--' unit --test-pattern "resources_test.py"

    "${INTEGRATION_NAME}" - Execute all "integration" tests
      Example:
        Linux & macOS:
          ./curation.sh ${CMD_NAME} integration
        Windows:
          .\curation.ps1 ${CMD_NAME} integration
      Flags:
        --test-pattern    provide name of file to limit run to
            Example: ./curation.sh ${CMD_NAME} '--' integration --test-pattern "bq_utils_test.py"

EOT
)

# these will be flipped to 1 when that command's name is seeen
run_unit=0
run_integration=0
run_lint=0

# this is used to track which command we're currently building flags for
current_cmd=""

run_args=("${@}")
unit_args=()
integration_args=()

for i in "${run_args[@]}"; do
  case $i in
  help)
    echo "${HELP_TEXT}"
    exit 0
    ;;
  unit)
    if [ $run_unit -eq 1 ]; then
      echo "\"${UNIT_NAME}\" already specified once"
      exit 1
    fi
    run_unit=1
    current_cmd="${UNIT_NAME}"
    shift
    ;;
  integration)
    if [ $run_integration -eq 1 ]; then
      echo "\"${INTEGRATION_NAME}\" already specified once"
      exit 1
    fi
    run_integration=1
    current_cmd="${INTEGRATION_NAME}"
    shift
    ;;
  lint)
    if [ $run_lint -eq 1 ]; then
      echo "\"${LINT_NAME}\" already specified once"
      exit 1
    fi
    run_lint=1
    current_cmd="${LINT_NAME}"
    shift
    ;;
  *)
    if [[ "${current_cmd}" == "${UNIT_NAME}" ]]; then
      unit_args+=("${i}")
    elif [[ "${current_cmd}" == "${INTEGRATION_NAME}" ]]; then
      integration_args+=("${i}")
    else
      echo "${HELP_TEXT}"
      echo "Unknown option ${i}"
      echo "run_args=${run_args[*]}"
      exit 1
    fi
    ;;
  esac
done

if [ "${run_lint}" -ne 1 ] && [ "${run_unit}" -ne 1 ] && [ "${run_integration}" -ne 1 ]; then
  echo "${HELP_TEXT}"
  exit 0
fi

if [ "${run_lint}" -eq 1 ]; then
  echo "Running linting checks..."

  # if we're running inside CI, execute some additional lint checks
  if in_ci; then
    require_ok "run-tests/lint_00_validate_commit_message.sh"
    require_ok "run-tests/lint_10_validate_pr_title.sh"
  fi

  # always execute yapf & pylint checks
  require_ok "run-tests/lint_20_yapf.sh"
  require_ok "run-tests/lint_30_pylint.sh"
fi

if [ "${run_unit}" -eq 1 ] || [ "${run_integration}" -eq 1 ]; then
  echo "Running test setup script(s)..."

# determine if env var is set containing test filepaths
if [[ -n "${CURATION_TESTS_FILEPATH}" ]]; then
  echo "----------------------------------------------------------------------"
  echo "Running the following tests in filepath ${CURATION_TESTS_FILEPATH}:"
  while read -r line; do
    echo "$line"
  done < "${CURATION_TESTS_FILEPATH}"
  echo "----------------------------------------------------------------------"
fi

  require_ok "run-tests/10_prep_output_paths.sh"
fi

if [ "${run_unit}" -eq 1 ]; then
  require_ok "run-tests/unit_00_execute.sh" "${unit_args[@]}"
fi

if [ "${run_integration}" -eq 1 ]; then
  if ! in_ci; then
    export FORCE_RUN_INTEGRATION=1
  fi

  activate_gcloud
  require_ok "run-tests/integration_10_execute.sh" "${integration_args[@]}"
  require_ok "run-tests/integration_99_teardown.sh"
fi

if [ "${run_unit}" -eq 1 ] || [ "${run_integration}" -eq 1 ]; then
  require_ok "run-tests/99_combine_coverage.sh"
fi

echo "We've reached the end."
