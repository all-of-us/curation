"""
"""

from __future__ import absolute_import

import argparse
from datetime import datetime
import json
import logging
import re

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from datasteward_df import common
from datasteward_df import negative_ages


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--from-bigquery',
                        dest='from_bigquery',
                        const=True,
                        default=False,
                        nargs='?',
                        help='Whether to load from BigQuery')
    parser.add_argument('--to-bigquery',
                        dest='to_bigquery',
                        default=None,
                        help='BigQuery dataset to load into, if any')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--project=aou-res-curation-test',
        '--service-account=dataflow-test@aou-res-curation-test.iam.gserviceaccount.com',
        '--staging_location=gs://dataflow-test-dc-864/staging',
        '--temp_location=gs://dataflow-test-dc-864/tmp',
        '--job_name=curation-prototype', '--region=us-central1',
        '--network=dataflow-test-dc-864'
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:

        table_prefix = 'unioned_ehr'

        # Read all of the EHR inputs, into a dictionary of:
        #   table -> PCollection of table rows
        combined_by_domain = {}
        for tbl in common.AOU_REQUIRED:
            if known_args.from_bigquery:
                combined_by_domain[tbl] = (
                    p | f"{tbl}" >> beam.io.Read(
                        beam.io.BigQuerySource(
                            # To downsample:  WHERE MOD(person_id, 2500) = 0; fix for non-person tables
                            query=
                            f"SELECT * FROM `aou-res-curation-test.synthea_ehr_ops_20200513.{table_prefix}_{tbl}`",
                            use_standard_sql=True)))
            else:
                # TODO: FIX!
                combined_by_domain[tbl] = (
                    p | f"read {tbl}" >> ReadFromText(f"test_data/{tbl}.json") |
                    f"{tbl} from JSON" >> beam.Map(json.loads))

        person_by_key = (
            combined_by_domain['person'] |
            'person by key' >> beam.Map(lambda p: (p['person_id'], p)))

        # Cleaning rule construction: build the appropriate inputs and overwrite
        # the domain pcollection.
        for tbl in negative_ages.TABLES:
            if tbl not in combined_by_domain:
                logging.warning(f"table {tbl} missing from input, skipping")
                continue

            by_person = (combined_by_domain[tbl] | f"{tbl} by person id" >>
                         beam.Map(lambda row: (row['person_id'], row)))
            combined_by_domain[tbl] = ({
                tbl: by_person,
                'person': person_by_key
            } | f"{tbl} cogrouped" >> beam.CoGroupByKey() | beam.ParDo(
                negative_ages.DropNegativeAges(tbl)))

        # Write the output.
        for domain, data in combined_by_domain.items():
            if known_args.to_bigquery:
                with open(f"fields/{domain}.json") as schema_file:
                    data | f"output for {domain}" >> beam.io.WriteToBigQuery(
                        f"{known_args.to_bigquery}.{domain}",
                        schema={'fields': json.load(schema_file)},
                        write_disposition=beam.io.BigQueryDisposition.
                        WRITE_TRUNCATE,
                        create_disposition=beam.io.BigQueryDisposition.
                        CREATE_IF_NEEDED)
            else:
                data | f"output for {domain}" >> beam.io.WriteToText(
                    f"out/{domain}.txt")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
