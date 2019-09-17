# CDR Ops

Collaboration-ready Jupyter environment for CDR operations
 
Jupyter notebooks allow analysts to do reporting, visualization and analytics with relative ease, but they typically 
sacrifice many of the benefits of software engineering practices. This environment provides the best of both worlds.

The `.ipynb` file format enables the responsive development workflow and cell-based layout of components we write but 
it also limits their reusability. scripts be used both like Jupyter notebooks or regular modules in Python.
This means we get the best of both worlds.
 * Notebooks are version controlled
 * You may edit scripts using your favorite IDE
 * Modules imported into other scripts    

## Requirements

 * Python 2.7
 * A service account associated with a data curation environment 

## Getting Started

Run the following command to get started:

```bash
start.sh --key_file /path/to/service-account-key.json
```

This creates a virtual environment located in `${HOME}/cdr_ops_env` and starts Jupyter notebook.

## Parameters

Create `parameters.py` for notebooks which require special parameters to run. The file must **not** be committed to the repository.

```python
# parameters.py
VOCABULARY_DATASET_ID = 'vocabulary_dataset_id'
RDR_DATASET_ID = 'rdr_dataset_id'
EHR_DATASET_ID = 'ehr_dataset_id'
COMBINED_DATASET_ID = 'combined_dataset_id'
DEID_DATASET_ID = 'deid_dataset_id'
DRC_BUCKET_NAME = 'drc_bucket_name'
RDR_PROJECT_ID = 'rdr_project_id'
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
'''.format(latest=DEFAULT_DATASETS.latest))
``` 

## Contributing

Just add and edit Python (.py) scripts in Jupyter. They will be converted to notebook (.ipynb) files and rendered in Jupyter on the fly. 
