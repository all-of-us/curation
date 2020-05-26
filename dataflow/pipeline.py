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


class ExtractDobAsMeasurement(beam.DoFn):
    """Removes DOB from the person table and emit as a measurement row.

  Note: This is just meant to illustrate moving data from person -> another
  table. More realistically this belongs in the observation table instead, but
  there was some issues in loading test data for the observation table.
  """

    OUTPUT_TAG_MEASUREMENT = 'measurement'
    DOB_CONCEPT_ID = 4083587

    def process(self, person):
        """Receives a person row and produces a modified person row and measurement row.
    Args:
      element: processing element.
    Yields:
      person rows as main output, "measurement" as a tagged output
    """

        if "birth_datetime" in person:
            # Note: here we're generating a new row sans ID. That has interesting
            # implications.
            dob = person["birth_datetime"]
            yield pvalue.TaggedOutput(
                self.OUTPUT_TAG_MEASUREMENT, {
                    "person_id": person["person_id"],
                    "measurement_concept_id": self.DOB_CONCEPT_ID,
                    "measurement_datetime": dob
                })

        for key in [
                "year_of_birth", "month_of_birth", "day_of_birth",
                "birth_datetime"
        ]:
            person[key] = None
        yield person


def clamp_condition_start_datetime(c):
    start = datetime.strptime(c["condition_start_datetime"],
                              "%Y-%m-%d %H:%M:%S.%f %Z")
    if start > datetime.utcnow():
        c["condition_start_datetime"] = str(datetime.utcnow())


def filter_by_id(p, blacklist):
    return p["person_id"] not in blacklist


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--from-bigquery',
                        dest='from_bigquery',
                        const=True,
                        default=False,
                        nargs='?',
                        help='Whether to load from BigQuery')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
        # run your pipeline on the Google Cloud Dataflow Service.
        '--runner=DataflowRunner',
        # CHANGE 3/5: Your project ID is required in order to run your pipeline on
        # the Google Cloud Dataflow Service.
        '--project=aou-res-curation-test',
        # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
        # files.
        '--staging_location=gs://dataflow-test-dc-864/staging',
        '--temp_location=gs://dataflow-test-dc-864/tmp',
        '--job_name=curation-prototype',
        '--region=us-central1',
        '--network=dataflow-test-dc-864'
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:

        # Read all of the EHR inputs, into a dictionary of:
        #   table -> site_name -> PCollection of table rows
        ehr_inputs = {}
        for tbl in ['person', 'measurement', 'condition_occurrence']:
            ehr_inputs[tbl] = {}
            for site in ['nyc', 'pitt']:
                if known_args.from_bigquery:
                    ehr_inputs[tbl][site] = (p | f"{site}_{tbl}" >> beam.io.Read(
                        beam.io.BigQuerySource(
                            query=
                            f"SELECT * FROM `aou-res-curation-test.calbach_prototype.{site}_{tbl}`",
                            use_standard_sql=True)))
                else:
                    ehr_inputs[tbl][site] = (
                        p | f"read {site}_{tbl}" >>
                        ReadFromText(f"../test_data/{site}/{tbl}.json") |
                        f"{site}_{tbl} from JSON" >> beam.Map(json.loads))

        # Merge tables across all sites, resulting in:
        #  table -> PCollection of table rows
        # Question: How should these ID spaces be reconciled?
        combined_by_domain = {}
        for tbl, data_by_site in ehr_inputs.items():
            combined_by_domain[tbl] = data_by_site.values(
            ) | f"ehr merge for {tbl}" >> beam.Flatten()

        # 1. Move data from person table elsewhere.

        # Transform person rows, generate new measurement rows.
        combined_by_domain["person"], extracted_meas_rows = (
            combined_by_domain["person"] |
            beam.ParDo(ExtractDobAsMeasurement()).with_outputs(
                ExtractDobAsMeasurement.OUTPUT_TAG_MEASUREMENT, main='person'))

        # Merge the new measurement rows into the larger collection.
        combined_by_domain["measurement"] = (
            combined_by_domain["measurement"],
            extracted_meas_rows) | beam.Flatten()

        # 2. Perform a row-level table transform
        combined_by_domain["condition_occurrence"] = (
            combined_by_domain["condition_occurrence"] |
            beam.Map(clamp_condition_start_datetime))

        # 3. Retract participants by ID.
        person_id_blacklist = (
            combined_by_domain['person'] | beam.Map(lambda p: p['person_id'])
            # Simulates more complex criteria here, likely involving other tables.
            | "generate the person ID blacklist" >>
            beam.Filter(lambda pid: int(pid) % 2 == 0) |
            beam.Map(lambda pid: (pid, True)) | beam.combiners.ToDict())

        # Drop all data for blacklisted participants from all tables.
        for (domain, data) in combined_by_domain.items():
            combined_by_domain[domain] = (
                data | f"blacklist {domain}" >> beam.Filter(
                    filter_by_id,
                    blacklist=pvalue.AsSingleton(person_id_blacklist)))

        # 4. Group-by-participant transforms, e.g. remove duplicate measurements
        combined_by_domain['measurement'] = (
            combined_by_domain['measurement']
            # Define unique rows as person+measurement concept ID.
            | beam.Map(lambda m:
                       ((m["person_id"], m["measurement_concept_id"]), m))
            # We don't care which one, just compare the row JSON for a deterministic result.
            | beam.combiners.Top.PerKey(1, key=lambda a: str(a)) |
            beam.Values())

        # XXX: Need to figure out how ID generation is meant to work here. That will impact
        # how we go about creating the mapping tables.
        # Initial idea is that we likely attach some payload to the in-flight representation
        # of a row.

        for domain, data in combined_by_domain.items():
            data | f"output for {domain}" >> beam.io.WriteToText(
                f"out/{domain}.txt")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
