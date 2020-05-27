"""
Based on https://github.com/all-of-us/curation/blob/develop/data_steward/cdr_cleaner/cleaning_rules/negative_ages.py
"""

from datetime import datetime
import logging

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from datasteward_df import common

DATE_FIELDS = {
    common.OBSERVATION_PERIOD: 'observation_period_start_date',
    common.VISIT_OCCURRENCE: 'visit_start_date',
    common.CONDITION_OCCURRENCE: 'condition_start_date',
    common.PROCEDURE_OCCURRENCE: 'procedure_date',
    common.DRUG_EXPOSURE: 'drug_exposure_start_date',
    common.OBSERVATION: 'observation_date',
    common.DRUG_ERA: 'drug_era_start_date',
    common.CONDITION_ERA: 'condition_era_start_date',
    common.MEASUREMENT: 'measurement_date',
    common.DEVICE_EXPOSURE: 'device_exposure_start_date'
}

TABLES = DATE_FIELDS.keys()

MAX_AGE = 150


class DropNegativeAges(beam.DoFn):
    """
    Drops invalid ages.

    Requires a co-grouped input of person table and domain table.
    """

    def __init__(self, tbl):
        self.tbl = tbl

    def _parse_time(self, dt):
        return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f %Z")

    def _parse_date(self, date):
        return datetime.strptime(date, "%Y-%m-%d")

    def default_label(self):
        return f"filter negative ages from {self.tbl}"

    def process(self, by_person):
        (person_id, info) = by_person
        if len(info['person']) == 0:
            # Temporary hack - should be a data quality issue
            return
        if len(info['person']) != 1:
            raise ValueError(
                f"found {len(info['person'])} person rows for {person_id}")
        birth = self._parse_time(info['person'][0]["birth_datetime"])
        for row in info[self.tbl]:
            row_time = self._parse_date(row[DATE_FIELDS[self.tbl]])
            if row_time < birth:
                logging.info("dropping negative age event")
                continue
            if row_time.year - birth.year > MAX_AGE:
                logging.info("dropping > 150yo event")
                continue
            yield row
