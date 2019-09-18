#!/bin/bash -e

# Note: On Windows cygpath executable must be in current PATH in order to normalize paths

# Determine separator to use for directories and PYTHONPATH
SEP=':'
PATHSEP='/'
if [[ "$OSTYPE" == "msys" ]]; then
  SEP=';'
  PATHSEP='\'
fi

# Function normalizes path for OS
norm_path () {
  if [[ "$OSTYPE" == "msys" ]]; then
    echo "$(cygpath -w "$1")"
  else
    echo "$1"
  fi
}

DATA_STEWARD_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
export DATA_STEWARD_DIR="$(norm_path ${DATA_STEWARD_DIR})"

# libs should appear first in PYTHONPATH so we can override versions from
# the GAE SDK. (Specifically, we need oauth2client >= 4.0.0 and GAE uses 1.x.)
export PYTHONPATH=${PYTHONPATH}${SEP}${DATA_STEWARD_DIR}
