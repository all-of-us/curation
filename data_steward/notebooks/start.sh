#!/usr/bin/env bash

USAGE="start.sh"

# Determine separator to use for directories and PYTHONPATH
# only tested on Git Bash for Windows
SEP=':'
if [[ "$OSTYPE" == "msys" ]]; then
  SEP=';'
fi

NOTEBOOKS_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
BASE_DIR="$( cd "${NOTEBOOKS_DIR}" && cd .. && pwd )"

export PYTHONPATH="${PYTHONPATH}${SEP}${BASE_DIR}"
echo "Which python: $(which python)"

# The path set to /c/path/to/file gets converted to C:\\c\\path\\to\\file
# instead of C:\\path\\to\\file, requiring the following fix for Git Bash for Windows
if [[ "$OSTYPE" == "msys" ]]; then
  for i in $(echo "${PYTHONPATH//;/ }")
  do
      PATH_VAR="$(echo "${i}" | tail -c +3)"
      export PYTHONPATH="${PYTHONPATH}${SEP}${PATH_VAR}"
      echo "Added path ${PATH_VAR}"
  done
fi

jupyter nbextension enable --py --sys-prefix qgrid
jupyter nbextension enable --py --sys-prefix widgetsnbextension
jupyter notebook --notebook-dir=${BASE_DIR} --config=${NOTEBOOKS_DIR}/jupyter_notebook_config.py
