# Notebooks

Data curation notebooks using [jupytext](https://github.com/mwouts/jupytext) and [Google Cloud Datalab](https://cloud.google.com/datalab/) libraries. 

## Requirements

 * Python 2.7
 * A service account associated with a data curation environment 

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

## Getting Started

This command installs all dependencies in a local virtual environment and starts jupyter notebook.

```bash
start.sh --key_file /path/to/service-account-key.json
```

## Contributing

Just add and edit Python (.py) scripts in Jupyter. They will be converted to notebook (.ipynb) files and rendered in Jupyter on the fly. See `ehr_operations.py` for reference. 
