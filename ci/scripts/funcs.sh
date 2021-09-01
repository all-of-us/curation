#!/usr/bin/env bash

# attempts to compile a usable 'whoami' value
function define_username() {
  # if USERNAME is already set, use it.
  local out=""
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

  echo -n "${out}" | tr '[:upper:]' '[:lower:]'

  return 0
}

function underscore_me {
    echo -n "${1}" | sed -e 's#/#_#g; s/-/_/g'
}

function escape_newlines {
  echo -n "${1}" | sed ':a;N;$!ba;s/\n/\\n/g'
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
