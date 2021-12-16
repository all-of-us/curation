#!/usr/bin/env bash

set -e

CMD_NAME="format"

source "${CURATION_SCRIPTS_DIR}"/funcs.sh

HELP_TEXT=$(
  cat <<EOT
${CMD_NAME} command

  This command will reformat your code in place using yapf

Available subcommands:
  None

EOT
)

run_args=("${@}")

for i in "${run_args[@]}"; do
  case $i in
  help)
    echo "${HELP_TEXT}"
    exit 0
    ;;
  esac
done

require_ok "format/yapf_00_execute.sh"