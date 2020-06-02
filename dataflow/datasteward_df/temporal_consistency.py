"""
Based on https://github.com/all-of-us/curation/blob/develop/data_steward/cdr_cleaner/temporal_consistency.py
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

TABLE_DATES = {
    common.CONDITION_OCCURRENCE: ('condition_start_date', 'condition_end_date'),
    common.DRUG_EXPOSURE:
        ('drug_exposure_start_date', 'drug_exposure_end_date'),
    common.DEVICE_EXPOSURE:
        ('device_exposure_start_date', 'device_exposure_start_date')
}
TABLES = TABLE_DATES.keys()

INPATIENT_VISIT = 9201
OUTPATIENT_VISIT = 9202
ER_VISIT = 9203


class CleanTemporalConsistency(beam.PTransform):

    def __init__(self, tbl):
        self.tbl = tbl

    def default_label(self):
        return f"fix temporal inconsistency in {self.tbl}"

    def _parse_date(self, date):
        return datetime.strptime(date, "%Y-%m-%d")

    def _fmt_date(self, date):
        return datetime.strftime(date, "%Y-%m-%d")

    def replace_invalid_ends(self, by_visit):
        (vo_id, info) = by_visit

        vo = info[common.VISIT_OCCURRENCE]
        visit_concept_id = None
        if len(vo) > 1:
            raise ValueError(
                f"wanted 1 visit occurence, got {len(vo)} for {vo_id}")
        elif len(vo) == 1:
            visit_concept_id = vo[0]['visit_concept_id']

        max_end = None
        for (tbl, (_, end)) in TABLE_DATES.items():
            for row in info[tbl]:
                if not row[end]:
                    continue
                end_date = self._parse_date(row[end])
                if not max_end or end_date > max_end:
                    max_end = end_date

        (start, end) = TABLE_DATES[self.tbl]
        for row in info[self.tbl]:
            if (not row[start] or not row[end] or
                    self._parse_date(row[start]) <= self._parse_date(row[end])):
                # Valid date range, leave unomdified
                pass
            elif (visit_concept_id in [INPATIENT_VISIT] and max_end and
                  self._parse_date(row[start]) <= max_end):
                row[end] = self._fmt_date(max_end)
            else:
                row[end] = row[start]

            yield row

    def expand(self, cogrouped_by_visit):
        """
        The input must contain the following pcollections co-grouped by visit occurrence ID:
        - visit_occurrence
        - condition_occurrence
        - drug_exposure
        - device_exposure

        Inputs should be keyed by table name.

        One of the target cleaning tables is indicated in the constructor as the cleaning target.

        Outputs a pcollection containing all updated/filtered rows of the target table.
        """
        return (cogrouped_by_visit | f"replace null end dates on {self.tbl}" >>
                beam.ParDo(self.replace_invalid_ends))
