#!/usr/bin/env bash

# initialize python venv, including pip install steps

# don't suppress errors
set -e

VENV_PATH="${CIRCLE_WORKING_DIRECTORY}"/"${VENV_NAME}"
VENV_ACTIVATE="${VENV_PATH}"/bin/activate

# shellcheck disable=SC1090
python -m venv "${VENV_PATH}" \
  && source "${VENV_ACTIVATE}" \
  && echo "source ${VENV_ACTIVATE}" | tee -a "${HOME}/.bashrc" "${HOME}/.profile" \
  && echo "export PYTHONPATH=:${CIRCLE_WORKING_DIRECTORY}:${CIRCLE_WORKING_DIRECTORY}/data_steward:${CIRCLE_WORKING_DIRECTORY}/tests:\"\${PYTHONPATH}\"" | tee -a "${HOME}/.bashrc" "${HOME}/.profile" \
  && python -m pip install --upgrade pip setuptools \
  && python -m pip install -r data_steward/requirements.txt
