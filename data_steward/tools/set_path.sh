#!/bin/bash -e

# Sets environment variables used to run Python with the AppEngine SDK.
# Note: On Windows cygpath executable must be in current PATH in order to normalize paths

GCLOUD_PATH=$(which gcloud)
CLOUDSDK_ROOT_DIR=${GCLOUD_PATH%/bin/gcloud}
APPENGINE_HOME="${CLOUDSDK_ROOT_DIR}/platform/appengine-java-sdk"
GAE_SDK_ROOT="${CLOUDSDK_ROOT_DIR}/platform/google_appengine"

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

# The next line enables Python libraries for Google Cloud SDK
GAEPATH=$(norm_path ${GAE_SDK_ROOT})

# * OPTIONAL STEP *
# If you wish to import all Python modules, you may iterate in the directory
# tree and import each module.
#
# * WARNING *
# Some modules have two or more versions available (Ex. django), so the loop
# will import always its latest version.
for module in ${GAE_SDK_ROOT}/lib/*; do
  if [ -r ${module} ];
  then
    expr=$(norm_path ${module})
    GAEPATH="${expr}${SEP}${GAEPATH}"
  fi
done
unset module

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
export BASE_DIR="$(norm_path ${BASE_DIR})"
LIB_DIR="${BASE_DIR}${PATHSEP}lib"
ROOT_REPO_DIR="$( cd "${BASE_DIR}" && cd .. && pwd )"
# libs should appear first in PYTHONPATH so we can override versions from
# the GAE SDK. (Specifically, we need oauth2client >= 4.0.0 and GAE uses 1.x.)
export PYTHONPATH=${PYTHONPATH}${SEP}${BASE_DIR}${SEP}${LIB_DIR}${SEP}${GAEPATH}
