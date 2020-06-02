Setup

- Initial GCP setup:

  - Enable GCP APIs in the project per https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python
  - Create a service account
  - Grant SA "Dataflow Admin" as well as BigQuery Data Editor (for r/w to BigQuery)
  - Configure GCP networks appropriately:

    - For this demo, I created a new VPC network within the project, per https://cloud.google.com/dataflow/docs/guides/specifying-networks
    - I also had to setup a firewall rule as described here: https://cloud.google.com/dataflow/docs/guides/routes-firewall
    - In production, we will need to work with Sysadmins to determine what the correct configuration is
    - The sysadmin team appears to have disabled the normal "default" network creation on new GCP projects,
      which breaks standard Dataflow setup. We will need to specify an existing shared VPC network, or
      figure out the appropriate settings and create a new network for Curation projects

- Install requirements:

```
mkvirtualenv cdr-beam
pip install apache-beam apache-beam[gcp]
```

- Copy schema JSON files into this subdir (.gitignored to avoid clutter, just a hack):

```
cp -r data_steward/resource_files/fields dataflow/fields
```

- Run the pipeline, from the dataflow dir locally, from local JSON files:

```
python main.py --setup_file $PWD/setup.py
```

- Run the pipeline, from the dataflow dir locally, against BigQuery. The downsample
  parameter will approximately downsample the number of participants selected from BQ:

```
python main.py --setup_file $PWD/setup.py --from-bigquery --downsample-inverse-prob 2500
```

- Run the pipeline on Dataflow, from the dataflow dir:

```
python main.py \
  --setup_file $PWD/setup.py \
  --from-bigquery \
  --to-bigquery aou-res-curation-test:calbach_dataflow_testing \
  --runner DataflowRunner
```

## Generating test inputs

The easiest way is to run the pipeline, then copy the outputs into the test_data directory.

```
python main.py --setup_file $PWD/setup.py

for f in out/*-of-*; do cp "${f}" "$(echo "${f}" | sed -e "s/txt-00000-of-00001/json/" -e "s,out/,test_data/,")"; done
```
