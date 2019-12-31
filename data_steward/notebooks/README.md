# CDR Ops

Collaboration-ready Jupyter environment for CDR operations
 
Jupyter notebooks allow analysts to do reporting, visualization and analytics with relative ease, but they typically 
sacrifice many of the benefits of software engineering practices. The environment in this component allows Jupyter
notebooks to represented by plain Python `.py` scripts. This provides the following benefits: 

 * Easy version control (with git)
 * Edit in Jupyter or in your favorite IDE 
 * Notebooks can be used like any other Python module (e.g. imported into other scripts)

## Requirements

 * Python 2.7
 * A service account associated with a data curation environment 

## Getting Started

Run the following command to get started:

```bash
start.sh --key_file /path/to/service-account-key.json
```

This creates a virtual environment located in `${HOME}/cdr_ops_env` and starts Jupyter notebook. The following
directories have notebooks we use:

 * `data_steward/analytics/cdr_ops`
 * `data_steward/cdr_cleaner/manual_cleaning_rules`

## Parameters (optional)

To avoid having to manually provide dataset_id parameters for multiple notebooks, you may create `notebooks/parameters.py` and reference them in your notebooks. This file must **not** be committed to the repository.

```python
# parameters.py
VOCABULARY_DATASET_ID = 'vocabulary_dataset_id'
RDR_DATASET_ID = 'rdr_dataset_id'
EHR_DATASET_ID = 'ehr_dataset_id'
COMBINED_DATASET_ID = 'combined_dataset_id'
DEID_DATASET_ID = 'deid_dataset_id'
DRC_BUCKET_NAME = 'drc_bucket_name'
RDR_PROJECT_ID = 'rdr_project_id'
SANDBOX = 'sandbox_dataset_id'
UNIONED_EHR_DATASET_ID = 'unioned_ehr_dataset_id'
```

## Defaults

The `defaults` notebook module automatically determines the latest available rdr, unioned, combined, and deid dataset_ids.

```python
from defaults import DEFAULT_DATASETS

LATEST_DATASETS_MESSAGE = '''
The most recent datasets are listed below
rdr: {latest.rdr}
unioned: {latest.unioned}
combined: {latest.combined}
deid: {latest.deid}
'''.format(latest=DEFAULT_DATASETS.latest)
``` 

## Contributing

Just add and edit Python (.py) scripts in Jupyter. They will be converted to notebook (.ipynb) files and rendered in Jupyter on the fly. 
