#!/usr/bin/env bash

# Public: List available commands
#
# Looks for and lists all available shell scripts under the "commands" directory,
# appending them to a variable provided as $1
#
# $1 - External array variable to add values to
#
# Examples:
#
#   // define variable to store list of commands in
#   available_commands=()
#   // execute "find_available_commands", passing it our global variable
#   find_available_commands available_commands
#
function find_available_commands() {
  local -n cmds="${1}"
  for run_cmd in "${CURATION_COMMANDS_DIR}"/*.sh; do
    cmdname=$(basename "${run_cmd}" | sed 's/.sh$//')
    cmds+=("${cmdname}")
  done
}

# Public: Test whether we're running in a CI environment or not
#
# Determines if $CIRCLECI or $CI envvars are set and == "true"
#
# Returns 0 if we ARE in CI, otherwise returns 1
#
function in_ci() {
  if [[ -n "${CIRCLECI}" ]] && [[ "${CIRCLECI}" == "true" ]]; then
    return 0
  elif [[ -n "${CI}" ]] && [[ "${CI}" == "true" ]]; then
    return 0
  else
    return 1
  fi
}

# Public: Uppercase input string
#
# Uses tr to uppercase all characters in provided string, echoing
# the result to stdout
#
# $1 - Input string
#
# Examples
#
#   uppercase "lower"
#   // outputs: "LOWER"
#
#   uppercase "caMel"
#   // outputs: "CAMEL"
#
function uppercase() {
  echo -n "${1}" | tr '[:lower:]' '[:upper:]'
}

# Public: Lowercase input string
#
# Uses tr to lowercase all characters in provided string, echoing
# the result to stdout
#
# $1 - Input string
#
# Examples:
#
#   lowercase "UPPER"
#   // outputs: "upper"
#
#   lowercase "CAmEL"
#   // outputs: "camel"
#
function lowercase() {
  echo -n "${1}" | tr '[:upper:]' '[:lower:]'
}

# Public: Replaces non-alphanumeric characters with "_"
#
# Accepts any input string, replacing any character that is not
# alphanumeric (a-z, A-Z, or 0-9) with "_", echoing the result to stdout.
#
# $1 - Input string
# $2 - (Optional) [Default: "_"] Character to replace non-alphanumeric characters with
#
# Examples:
#
#   ensure_only_alphanumeric "this sentence-is/awkward\\"
#   // outputs: "this_sentence_is_awkward_"
#
function ensure_only_alphanumeric() {
  local rep
  rep=${2:-'_'}
  echo -n "${1}" | sed -re "s/[^a-zA-Z0-9_]/${rep}/g"
}

# Public: Escape newline characters
#
# Replaces all "\n" characters in a string with "\\n", echoing the
# result to stdout.  Useful for parsing git comments and the like.
#
# $1 - Input string
#
# Examples
#   escape_newlines "this comment\nhas a newline"
#   // outputs: "this comment\\nhas a newline"
#
function escape_newlines() {
  echo -n "${1}" | sed ':a;N;$!ba;s/\n/\\n/g'
}

# Public: Ensure we have a usable "username" value for the executing user
#
# Attempts to ensure we have a usable username value, regardless of
# environment.  This value is used to create datasets and buckets for our
# various tests, meaning we are limited in the characters we're allowed to
# use, e.g. no spaces or specials.  To that end, this func also executes
# "ensure_only_alphanumeric" as part of its execution.
#
# The output comes from the final `lowercase` call.
#
function define_username() {
  # if USERNAME is already set, use it.
  local out
  if [ -n "${USERNAME}" ]; then
    out="${USERNAME}"
  elif [ -n "${GH_USERNAME}" ]; then
    out="${GH_USERNAME}"
  elif [ -n "${CIRCLE_USERNAME}" ]; then
    out="${CIRCLE_USERNAME}"
  else
    out="${USER:-$(whoami)}"
  fi

  local clean
  clean=$(ensure_only_alphanumeric "${out}")

  lowercase "${clean}"

  return 0
}

# Public: Define environment variables
#
# Defines a given environment variable in both ~/.profile and ~/.bashrc,
# also attempting to export it into the current shell.  By default it will NOT
# override an existing envvar.
#
# $1 - Environment variable name
# $2 - Environment variable value
# $3 - (Optional) If set to 1, will explicitly define $1 to $2, ignoring any
#      existing value.
#
# Examples
#
#   // Set envvar if not already set
#   set_env "GOOGLE_CLOUD_PROJECT" "aou-res-curation-test"
#
#   // Override envvar
#   set_env "GOOGLE_CLOUD_PROJECT" "aou-res-curation-test" 1
#
function set_env() {
  local name=$1
  local default=$2
  local value
  if [[ -n $3 ]] && [[ "${3}" == "1" ]]; then
    value="${default}"
  else
    value="${!name:-$default}"
  fi
  echo export "${name}"=\""${value}"\" | tee -a "${HOME}"/.bashrc "${HOME}"/.profile
  export "${name}"="${value}"
}

# Public: Define envvars used for various commands
# TODO: Investigate ways to have fewer envvars always defined
#
# Defines every envvar necessary for commands in this container.
#
function define_common_envvars() {
  # determine username
  determined_username=$(define_username)
  set_env "USERNAME" "${determined_username}" 1
  set_env "USERNAME_PREFIX" "${USERNAME//-/_}"
  set_env "CIRCLE_PROJECT_USERNAME" "all-of-us"
  set_env "PROJECT_USERNAME" "${CIRCLE_PROJECT_USERNAME//-/_}"

  # remove newlines from last commit message
  cleaned_last_commit=$(escape_newlines "$(git log -1 --pretty=%B)")
  set_env "GIT_LAST_LOG" "${cleaned_last_commit}" 1

  # reformat local branch name
  cleaned_circle_branch=$(ensure_only_alphanumeric "$(git rev-parse --abbrev-ref HEAD)")
  set_env "CIRCLE_BRANCH" "${cleaned_circle_branch}" 1

  set_env "CURRENT_BRANCH" "${CIRCLE_BRANCH}"
  set_env "GOOGLE_APPLICATION_CREDENTIALS" "${HOME}/gcloud-credentials-key.json"
  set_env "APPLICATION_ID" "aou-res-curation-test"
  set_env "GOOGLE_CLOUD_PROJECT" "${APPLICATION_ID}"
  set_env "GOOGLE_CLOUD_PROJECT_ID" "${APPLICATION_ID}"
  set_env "PROJECT_PREFIX" "${APPLICATION_ID//-/_}"

  ## dataset envvars

  set_env "DATASET_PREFIX" "${PROJECT_USERNAME}_${USERNAME_PREFIX}_${CURRENT_BRANCH}"

  set_env "BIGQUERY_DATASET_ID" "${DATASET_PREFIX}"_ehr
  set_env "RDR_DATASET_ID" "${DATASET_PREFIX}"_rdr
  set_env "EHR_RDR_DATASET_ID" "${DATASET_PREFIX}"_ehr_rdr
  set_env "COMBINED_DATASET_ID" "${DATASET_PREFIX}"_combined
  set_env "UNIONED_DATASET_ID" "${DATASET_PREFIX}"_unioned
  set_env "COMBINED_DEID_DATASET_ID" "${DATASET_PREFIX}"_deid
  set_env "FITBIT_DATSET_ID" "${DATASET_PREFIX}"_fitbit
  set_env "VOCABULARY_DATASET" "vocabulary20210601"

  ## bucket envvars

  set_env "BUCKET_PREFIX" "${PROJECT_USERNAME}"_"${USERNAME_PREFIX}"_"${CURRENT_BRANCH}"

  set_env "DRC_BUCKET_NAME" "${BUCKET_PREFIX}"_drc
  set_env "BUCKET_NAME_FAKE" "${BUCKET_PREFIX}"_fake
  set_env "BUCKET_NAME_NYC" "${BUCKET_PREFIX}"_nyc
  set_env "BUCKET_NAME_PITT" "${BUCKET_PREFIX}"_pitt
  set_env "BUCKET_NAME_CHS" "${BUCKET_PREFIX}"_chs
  set_env "BUCKET_NAME_UNIONED_EHR" "${BUCKET_PREFIX}"_unioned_ehr
  set_env "BUCKET_NAME_${BUCKET_PREFIX}_FAKE" "${BUCKET_NAME_FAKE}"
}

# Public: Activate Google sdk's
#
# Attempts to ensure that the Google Cloud SDK's have functional credentials
# and basic configuration applied so they are usable.
#
# TODO: determine way to define more granular gcloud config parameters, potentially via volume mount.
#
function activate_gcloud() {
  echo "Activating Google Cloud Credentials..."

  # this must always be defined, whether we're in ci or not.  it is not only used to _read_ the credentials,
  # but also as the location to _write_ the credentials we decrypt when run in ci.
  if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
    echo "Environment variable GOOGLE_APPLICATION_CREDENTIALS is missing or empty"
    exit 1
  fi

  # if running in ci, we must attempt to decrypt the usable cloud credentials json file based
  # on a few ci-specific envvars.
  if in_ci; then

    # test to ensure required envvars are defined
    if [[ -z "${GCLOUD_CREDENTIALS_KEY}" ]] || [[ -z "${GCLOUD_CREDENTIALS}" ]]; then
      echo "Environment variables GCLOUD_CREDENTIALS and/or GCLOUD_CREDENTIALS_KEY are missing or empty"
      exit 1
    fi

    echo "Writing decoded CircleCI GCP key to ${GOOGLE_APPLICATION_CREDENTIALS}"

    # decrypt credentials, then write the decrypted contents to the filepath
    # specified by $GOOGLE_APPLICATION_CREDENTIALS
    echo "${GCLOUD_CREDENTIALS}" |
      openssl enc -d -aes-256-cbc -base64 -A -md md5 -k "${GCLOUD_CREDENTIALS_KEY}" \
        -out "${GOOGLE_APPLICATION_CREDENTIALS}"
  fi

  # DC-2043: ensure project is set for the runtime config (defaults to "default")
  gcloud config set project "${GOOGLE_CLOUD_PROJECT}"

  # activate service account
  gcloud auth activate-service-account --key-file "${GOOGLE_APPLICATION_CREDENTIALS}"
}

# Public: Execute function, capturing output
#
# Allows for "testing" of any funcs defined in the runtime this is called within
#
# $1 - Name of function to execute, cannot be self
# ${2...} - (Optional) Set of arguments to pass to $1
#
# Examples:
#
#   // execute without args
#   capture_execution printenv
#
#   // execute with args
#   capture_execution ensure_only_alphanumeric "what*is)love"
#
function capture_execution() {
  local func_args=("$@")
  local tfunc_name
  local tfunc_args
  local exec_res
  local exec_res_code

  tfunc_name="${func_args[*]::1}"
  if [[ "${tfunc_name}" == "capture_execution" ]]; then
    echo "Cannot execute myself"
    exit 1
  fi

  tfunc_args=()
  for v in "${func_args[@]:1}"
  do
    tfunc_args+=("${v}")
  done

  if [[ ! $(type -t "${tfunc_name}") == function ]]; then
    echo "No function named ${tfunc_name} is defined"
    echo "Provided args: ${func_args[*]}"
    exit 1
  fi

  echo "Executing:"
  echo "  Function: ${tfunc_name}"

  if [[ "${#tfunc_args[@]}" == "0" ]]; then
    echo "  Args: None"
  else
    echo "  Args: ${tfunc_args[*]}"
  fi

  set +e
  # define alias from stdout to
  exec 3>&1
  exec_res=$("${tfunc_name}" "${tfunc_args[@]}") >&3
  exec_res_code="$?"
  exec 3>&-
  set -e

  echo ""

  echo "Execution results:"
  echo "  Code: ${exec_res_code}"
  echo "  Output:"
  echo "-- output start"

  if [[ -n "${exec_res}" ]]; then
    echo "${exec_res}"
  else
    echo "  Empty / No output"
  fi

  echo "-- output end"
}

# Public: Execute a function, erroring if it exited non-zero
#
# Executes a provided script with args in a new subshell, capturing
# the output and exit codes.  If a non-zero exit code is seen, prints output
# and returns non-zero.
#
# $1 - Path of script to execute, must be under directory defined by $CURATION_SCRIPTS_DIR
# ${2..} - (Optional) Set of args to pass to $1
#
# Examples:
#
#   // execute without args
#   require_ok "run-tests/lint_00_validate_commit_message.sh"
#
#   // execute with args
#  require_ok "run-tests/integration_10_execute.sh" "${integration_args[@]}"
#
function require_ok() {
  local func_args=("$@")
  local script_name
  local script_file
  local script_args
  local script_res
  local script_res_code

  script_name="${func_args[*]::1}"
  script_file="${CURATION_SCRIPTS_DIR}/${script_name}"
  script_args=()
  for v in "${func_args[@]:1}"
  do
    script_args+=("${v}")
  done

  if [[ ! -f "${script_file}" ]]; then
    echo "No script named ${script_name} is available at path ${CURATION_SCRIPTS_DIR}"
    echo "Provided args: ${func_args[*]}"
    exit 1
  elif [[ ! -x "${script_file}" ]]; then
    echo "Script ${script_name} is not executable"
    echo "Provided args: ${func_args[*]}"
    exit 1
  fi

  if [[ "${#script_args[@]}" == "0" ]]; then
    echo "Executing \"${script_file}\" with no args..."
  else
    echo "Executing \"${script_file} ${script_args[*]}\"..."
  fi

  set +e
  # todo: as it stands, this does not quite 100% redirect output, requires further tweaking
  exec 3>&1
  script_res=$(bash --login "${script_file}" "${script_args[@]}" >&3)
  script_res_code="$?"
  exec 3>&-
  set -e

  if [[ "${script_res_code}" -ne "0" ]]; then
    echo "${script_res}"
    echo "Error running script \"${script_file}\": script exited with code ${script_res_code}"
    exit 1
  fi
  return 0
}
