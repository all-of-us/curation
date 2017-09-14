# Data Steward

Specification document and data curation processes for data submitted to the DRC.

## Development Requirements

 * Python 2.7.* (download from [here](https://www.python.org/downloads/) and install)
 * pip (download [get-pip.py](https://bootstrap.pypa.io/get-pip.py) and run `python get-pip.py`)
 * Google [Cloud SDK](https://cloud.google.com/sdk/downloads#interactive)
    * `google-cloud-sdk-app-engine-python` (follow instructions in Cloud SDK)
 * _Recommended: [virtualenv](https://pypi.python.org/pypi/virtualenv)_

### Local Environment

In general we want to keep unit tests local (see 
[Local Unit Testing for Python](https://cloud.google.com/appengine/docs/standard/python/tools/localunittesting)), 
but some services such as bigquery do not readily support local emulation and thus require access to cloud services on 
the internet. The following environment variables are needed to configure access to these services. 

| name | description |
| ---- | ----------- |
| `GOOGLE_APPLICATION_CREDENTIALS` | Location of service account credentials in JSON format (see [Google Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials#howtheywork)) |
| `APPLICATION_ID` | Google cloud project ID. For development, we use `all-of-us-ehr-dev`. |
| `BIGQUERY_DATASET_ID` | ID of the biquery dataset where CDM data are to be loaded. Must be in the project associated with `APPLICATION_ID`. |
| `DRC_BUCKET_NAME` | Name of the bucket where specification document and report is located. |
| `BUCKET_NAME_<HPO_ID>` | Name of the bucket where CDM files are to be uploaded by HPO site with id `<HPO_ID>`. |

## Installation / Configuration

 * Install requirements by running

        pip install -t lib -r requirements.txt
