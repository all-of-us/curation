#!/bin/bash
# install google-cloud-sdk since it is not yet available via apt-get on ubuntu 20.0.4

# don't suppress errors.
set -e
# TODO: this is done as there is currently (as of 2021.08.19) no 20.04 release in the google repo for...reasons
# TODO: parameterize gsdk version and sha
wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-353.0.0-linux-x86_64.tar.gz
# verify integrity
echo 94fcb77632fed5b6fa61d1bf6c619cbfceb63f3d95911bfe15e7caa401df81c0 \
  google-cloud-sdk-353.0.0-linux-x86_64.tar.gz | sha256sum --check --status && if [[ "$?" -ne 0 ]]; then exit 1; fi
# if we get here, assume things are ok and do the following:
# 1. untar source
# 2. execute installer
# 3. cd back to home
# 4. update components (should help us not have to manually manage sha and wget version)
tar -xzvf google-cloud-sdk-353.0.0-linux-x86_64.tar.gz \
  && cd ./google-cloud-sdk \
  && ./install.sh --quiet \
  && echo "source $(pwd)/path.bash.inc" >> /home/circleci/.profile \
  && cd .. \
  && ./google-cloud-sdk/bin/gcloud components update --quiet
# finally, add to path
set +e
