#!/usr/bin/env bash

function find_available_commands() {
  local -n cmds="${1}"
  for run_cmd in "${CURATION_COMMANDS_DIR}"/*.sh; do
    cmdname=$(basename "${run_cmd}" | sed 's/.sh$//')
    cmds+=("${cmdname}")
  done
}

function in_ci() {
  if [[ -n "${CIRCLECI}" ]] && [[ "${CIRCLECI}" == "true" ]]; then
    return 0
  elif [[ -n "${CI}" ]] && [[ "${CI}" == "true" ]]; then
    return 0
  else
    return 1
  fi
}

function uppercase() {
  echo -n "${1}" | tr '[:lower:]' '[:upper:]'
}

function lowercase() {
  echo -n "${1}" | tr '[:upper:]' '[:lower:]'
}

function underscore_me() {
  # matches: [/- ]+
  # replace: "_"
  echo -n "${1}" | sed -e 's#/#_#g; s/-/_/g; s/ /_/g'
}

function escape_newlines() {
  echo -n "${1}" | sed ':a;N;$!ba;s/\n/\\n/g'
}

# attempts to compile a usable 'whoami' value
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
  clean=$(underscore_me "${out}")

  lowercase "${clean}"

  return 0
}

function set_env() {
  local name=$1
  local default=$2
  local value
  if [[ -z $3 ]]; then
    value="${!name:-$default}"
  else
    value="${default}"
  fi
  echo export "${name}"=\""${value}"\" | tee -a "${HOME}"/.bashrc "${HOME}"/.profile
  export "${name}"="${value}"
}

function activate_gcloud() {
  echo "Activating Google Cloud Credentials..."

  # this must be defined whether we're running in CircleCI or
  if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
    echo "Environment variable GOOGLE_APPLICATION_CREDENTIALS is missing or empty"
    exit 1
  fi

  # test if we're running within CircleCI
  if in_ci; then
    if [[ -z "${GCLOUD_CREDENTIALS_KEY}" ]] || [[ -z "${GCLOUD_CREDENTIALS}" ]]; then
      echo "Environment variables GCLOUD_CREDENTIALS and/or GCLOUD_CREDENTIALS_KEY are missing or empty"
      exit 1
    fi

    echo "Writing decoded CircleCI GCP key to ${GOOGLE_APPLICATION_CREDENTIALS}"

    echo "${GCLOUD_CREDENTIALS}" |
      openssl enc -d -aes-256-cbc -base64 -A -md md5 -k "${GCLOUD_CREDENTIALS_KEY}" \
        -out "${GOOGLE_APPLICATION_CREDENTIALS}"
  fi

  gcloud auth activate-service-account --key-file "${GOOGLE_APPLICATION_CREDENTIALS}"
}

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
    echo "No script named ${script_name} is available at path ${CURATION_SCRIPTS_DIR}"\
    echo "Provided args: ${func_args[*]}"
    exit 1
  elif [[ ! -x "${script_file}" ]]; then
    echo "Script ${script_name} is not executable"
    echo "Provided args: ${func_args[*]}"
    exit 1
  fi

  echo "Executing \"${script_file} ${script_args[*]}\"..."

  set +e
  # define alias from stdout to
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
