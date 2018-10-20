# Notebooks

Data curation notebooks using [jupytext](https://github.com/mwouts/jupytext) and [Google Cloud Datalab](https://cloud.google.com/datalab/) libraries. 

## Requirements

 * Python 2.7
 * A service account associated with a data curation environment 

## Getting Started

This command installs all dependencies in a local virtual environment and starts jupyter notebook.

```bash
start.sh --key_file /path/to/service-account-key.json
```

## Contributing

Just add and edit Python (.py) scripts in Jupyter. They will be converted to notebook (.ipynb) files and rendered in Jupyter on the fly. See `ehr_operations.py` for reference. 
