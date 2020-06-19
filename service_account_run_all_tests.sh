#!/bin/bash

set -x

#source /app/env/bin/activate
source /app/slim_env/bin/activate
python --version

# update the repo
git fetch
git checkout master
git merge origin/master

# ensure most recent dependencies are installed
pip --proxy http://10.0.0.40:3128 install --upgrade pip
pip --proxy http://10.0.0.40:3128 install wheel pipdeptree 
#pip --proxy http://10.0.0.40:3128 install -r /app/curation/data_steward/requirements.txt
#pip --proxy http://10.0.0.40:3128 install -r /app/curation/data_steward/dev_requirements.txt
#pip --proxy http://10.0.0.40:3128 install -r /app/curation/data_steward/deid/requirements.txt
pip --proxy http://10.0.0.40:3128 install -r /app/curation/slim_requirements.txt

PYTHONPATH=/app/curation:/app/curation/data_steward:$PYTHONPATH

pipdeptree > new_slim_pip_output.txt

# setting required credentials
export GOOGLE_CLOUD_PROJECT=aou-res-curation-test
export APPLICATION_ID=aou-res-curation-test
export GOOGLE_APPLICATION_CREDENTIALS=/app/curation/curapp-aou-res-curation-test.json
gcloud auth activate-service-account --key-file=/app/curation/curapp-aou-res-curation-test.json
gcloud config set project aou-res-curation-test
gcloud config list
echo $GOOGLE_APPLICATION_CREDENTIALS

# environment, bucket, and dataset setup
export GH_USERNAME=$(whoami)
source /app/curation/data_steward/init_env.sh
#source /app/curation/data_steward/ci/setup.sh
# have to reset here because init_env.sh overrides the previous setting
export GOOGLE_APPLICATION_CREDENTIALS=/app/curation/curapp-aou-res-curation-test.json
PYTHONPATH=$PYTHONPATH python data_steward/ci/setup.py

echo "monkey patching to run tests"
sed -i 's/hashlib.md5()/hashlib.md5(usedforsecurity=False)/g' /app/slim_env/lib64/python3.6/site-packages/coverage/misc.py

echo "Running unit tests"
./tests/run_tests.sh -s unit 2>&1 | tee unittest_output.txt

echo "Running integration tests"
./tests/run_tests.sh -s integration 2>&1 | tee integration_test_output.txt

set +x
