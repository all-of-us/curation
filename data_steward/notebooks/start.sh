#!/usr/bin/env bash

USAGE="
start.sh
  --pmi_email <PMI-OPS email to set credential account>
  --project_id <Project id to set default project>
"

while true; do
  case "$1" in
    --pmi_email) PMI_EMAIL=$2; shift 2;;
    --project_id) PROJECT_ID=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [[ -z "${PMI_EMAIL}" ]] || [[ -z "${PROJECT_ID}" ]];
then
  echo "Usage: $USAGE"
  exit 1
fi

# Determine separator to use for directories and PYTHONPATH
# only tested on Git Bash for Windows
SEP=':'
if [[ "$OSTYPE" == "msys" ]]; then
  SEP=';'
fi

NOTEBOOKS_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
BASE_DIR="$( cd "${NOTEBOOKS_DIR}" && cd .. && pwd )"
export PYTHONPATH="${PYTHONPATH}${SEP}${BASE_DIR}"
unset GOOGLE_APPLICATION_CREDENTIALS

echo "Setting gcloud config with ${PMI_EMAIL} for application ID ${PROJECT_ID}..."
gcloud config set account "${PMI_EMAIL}"
gcloud config set project "${PROJECT_ID}"

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
