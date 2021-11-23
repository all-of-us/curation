#!/usr/bin/env bash

source "${CURATION_SCRIPTS_DIR}"/funcs.sh

HELP_TEXT=$(
  cat <<EOT
Curation Development Container

Available commands:
EOT
)

# create var to house command list
available_commands=()

# populate command list
find_available_commands available_commands

for avail_cmd in "${available_commands[@]}"; do
  HELP_TEXT+='
'
  HELP_TEXT+="  - ${avail_cmd}"
done

run_args=("$@")

function exit_error() {
  echo "${HELP_TEXT}"
  echo "Runtime args: ${run_args[*]}"
  for m in "$@"; do
    echo "${m}"
  done
  exit 1
}

function exit_help() {
  echo "${HELP_TEXT}"
  exit 0
}

if [ "$#" -eq 0 ]; then
  exit_error "Error: command not provided"
fi

cmd_name="${run_args[*]::1}"
cmd_args=()
for v in "${run_args[@]:1}"
do
  cmd_args+=("${v}")
done

if [[ -z "${cmd_name}" ]] || [[ "${cmd_name}" =~ -{0,}help ]]; then
  exit_help
fi

# shellcheck disable=SC2076
if [[ -z "${cmd_name}" ]] || [[ ! "${available_commands[*]}" =~ "${cmd_name}" ]]; then
  exit_error "Error: \"${cmd_name}\" is not a valid command"
fi

exec "${CURATION_COMMANDS_DIR}/${cmd_name}.sh" "${cmd_args[@]}"