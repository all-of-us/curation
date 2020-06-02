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
from datasteward_df import temporal_consistency


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
    parser.add_argument(
        '--downsample-inverse-prob',
        dest='downsample_inverse_prob',
        default=None,
        type=int,
        help='Downsample by the inverse probability of this value, ' +
        'e.g. a value of 1000 downsamples to ~1/1000 rows')
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
        schema_by_domain = {}
        for tbl in common.AOU_REQUIRED:
            with open(f"fields/{tbl}.json") as schema_file:
                schema_by_domain[tbl] = json.load(schema_file)
            field_names = set(f["name"] for f in schema_by_domain[tbl])
            if known_args.from_bigquery:
                conditional = ''
                if known_args.downsample_inverse_prob:
                    sampled_id = None
                    if "person_id" in field_names:
                        sampled_id = "person_id"
                    elif f"{tbl}_id" in field_names:
                        sampled_id = f"{tbl}_id"

                    if sampled_id:
                        conditional = f"WHERE MOD({sampled_id}, {known_args.downsample_inverse_prob}) = 0"
                combined_by_domain[tbl] = (p | f"{tbl}" >> beam.io.Read(
                    beam.io.BigQuerySource(
                        query=
                        f"SELECT * FROM `aou-res-curation-test.synthea_ehr_ops_20200513.{table_prefix}_{tbl}` {conditional}",
                        use_standard_sql=True)))
            else:
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

        # Aggregate all necessary tables for temporal_consistency.
        # An alternate pattern here could be to move this into a helper within the
        # rule module, e.g.:
        #   temporal_inconsistency_pcol = temporal_consistency.prepare_inputs(combined_by_domain)
        #
        # This hides business logic in the rule module, but also hides heavy processing
        # side-effects. It's likely such a pattern will be needed if we want to move to a
        # more generalized cleaning rule model within Beam.
        by_visit = {}

        def key_by_visit(tbl):
            # Fallback to the primary ID, otherwise we'll cogroup all rows with
            # null visits.
            return lambda row: (row['visit_occurrence_id'] or
                                f"{tbl}/{row[tbl + '_id']}", row)

        for tbl in list(
                temporal_consistency.TABLES) + [common.VISIT_OCCURRENCE]:
            by_visit[tbl] = (
                combined_by_domain[tbl] |
                f"{tbl} by visit occurence" >> beam.Map(key_by_visit(tbl)))

        cogrouped_by_visit = (by_visit |
                              "cogrouped by visit for temporal consistency" >>
                              beam.CoGroupByKey())
        for tbl in temporal_consistency.TABLES:
            combined_by_domain[tbl] = (
                cogrouped_by_visit |
                temporal_consistency.CleanTemporalConsistency(tbl))

        # Write the output.
        for domain, data in combined_by_domain.items():
            if known_args.to_bigquery:
                data | f"output for {domain}" >> beam.io.WriteToBigQuery(
                    f"{known_args.to_bigquery}.{domain}",
                    schema={'fields': schema_by_domain[domain]},
                    write_disposition=beam.io.BigQueryDisposition.
                    WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.
                    CREATE_IF_NEEDED)
            else:
                (data | f"JSON write for {domain}" >> beam.Map(json.dumps) |
                 f"output for {domain}" >>
                 beam.io.WriteToText(f"out/{domain}.txt"))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
