#!/usr/bin/env bash

function in_ci {
  if [[ -n "${CIRCLECI}" ]] && [[ "${CIRCLECI}" == "true" ]];
  then
    return 0
  elif [[ -n "${CI}" ]] && [[ "${CI}" == "true" ]];
  then
    return 0
  else
    return 1
  fi
}

function uppercase {
  echo -n "${1}" | tr '[:lower:]' '[:upper:]'
}

function lowercase {
  echo -n "${1}" | tr '[:upper:]' '[:lower:]'
}

function underscore_me {
  # matches: [/- ]+
  # replace: "_"
  echo -n "${1}" | sed -e 's#/#_#g; s/-/_/g; s/ /_/g'
}

function escape_newlines {
  echo -n "${1}" | sed ':a;N;$!ba;s/\n/\\n/g'
}

# attempts to compile a usable 'whoami' value
function define_username() {
  # if USERNAME is already set, use it.
  local out
  if [ -n "${USERNAME}" ];
  then
    out="${USERNAME}"
  elif [ -n "${GH_USERNAME}" ];
  then
    out="${GH_USERNAME}"
  elif [ -n "${CIRCLE_USERNAME}" ];
  then
    out="${CIRCLE_USERNAME}"
  else
    out="${USER:-$(whoami)}"
  fi

  local clean
  clean=$(underscore_me "${out}")

  lowercase "${clean}"

  return 0
}

function set_env {
  local name=$1
  local default=$2
  local value
  if [[ -z $3 ]];
  then
    value="${!name:-$default}"
  else
    value="${default}"
  fi
  echo export "${name}"=\""${value}"\" | tee -a "${HOME}"/.bashrc "${HOME}"/.profile
  export "${name}"="${value}"
}

function require_ok {
  # shellcheck source=/dev/null
  if ! . "${CURATION_SCRIPTS_DIR}/${1}" ;
  then
    echo "Error running script \"${CURATION_SCRIPTS_DIR}/${1}\""
    exit 1
  fi
  return 0
}
