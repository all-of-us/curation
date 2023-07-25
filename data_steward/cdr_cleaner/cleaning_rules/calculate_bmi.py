"""
Some participants self-report their height and weight. This CR calculates the BMI for
those self-reported measurements. (Note BMIs are already calculated if the data is from HPO sites.)

Each participant can self-report their height and weight multiple times. BMIs are calculated
for each self-reported hight/weight pair. BMIs are not calculated if either height or weight is missing.

Original Issue: DC-3239
"""
# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.clean_height_weight import CleanHeightAndWeight
from common import JINJA_ENV, MEASUREMENT
from constants.cdr_cleaner.clean_cdr import (CONTROLLED_TIER_DEID_CLEAN, QUERY,
                                             REGISTERED_TIER_DEID_CLEAN)
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

CREATE_SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
WITH self_reported_height AS (
    SELECT
        m.measurement_id, 
        m.person_id, 
        m.visit_occurrence_id, 
        m.value_as_number,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_time,
        e.src_id
    FROM `{{project}}.{{dataset}}.measurement` m
    JOIN `{{project}}.{{dataset}}.measurement_ext` e
    ON m.measurement_id = e.measurement_id
    WHERE e.src_id LIKE 'Participant Portal%'
    AND m.measurement_concept_id = 3036277
), self_reported_weight AS (
    SELECT
        m.measurement_id, 
        m.person_id, 
        m.visit_occurrence_id, 
        m.value_as_number,
        m.measurement_date,
        e.src_id
    FROM `{{project}}.{{dataset}}.measurement` m
    JOIN `{{project}}.{{dataset}}.measurement_ext` e
    ON m.measurement_id = e.measurement_id
    WHERE e.src_id LIKE 'Participant Portal%'
    AND m.measurement_concept_id = 3025315
)
SELECT
    ROW_NUMBER() OVER(
        ORDER BY h.person_id, h.visit_occurrence_id
    ) + (
        SELECT MAX(measurement_id) 
        FROM `{{project}}.{{dataset}}.measurement`
    ) AS measurement_id,
    h.person_id,
    3038553 AS measurement_concept_id,
    h.measurement_date,
    h.measurement_datetime,
    h.measurement_time,
    32865 AS measurement_type_concept_id,
    NULL AS operator_concept_id,
    w.value_as_number / POW(h.value_as_number / 100, 2) AS value_as_number,
    NULL AS value_as_concept_id,
    9531 AS unit_concept_id,
    NULL AS range_low,
    NULL AS range_high,
    NULL AS provider_id,
    h.visit_occurrence_id,
    NULL AS visit_detail_id,
    CAST(NULL AS STRING) AS measurement_source_value,
    NULL AS measurement_source_concept_id,
    'kg/m2' AS unit_source_value,
    CAST(NULL AS STRING) AS value_source_value,
    h.src_id
FROM self_reported_height h
JOIN self_reported_weight w
ON h.person_id = w.person_id 
AND h.visit_occurrence_id = w.visit_occurrence_id
AND h.measurement_date = w.measurement_date
AND h.src_id = w.src_id
)
""")

INSERT_BMI_QUERY = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.measurement`
SELECT
    measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_time,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    value_source_value
FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`
""")

INSERT_EXT_QUERY = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.measurement_ext`
SELECT 
    measurement_id,
    src_id
FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`
""")


class CalculateBmi(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = ("Calculate BMI for self-reported height and weight.")

        super().__init__(issue_numbers=['dc3223'],
                         description=desc,
                         affected_datasets=[
                             REGISTERED_TIER_DEID_CLEAN,
                             CONTROLLED_TIER_DEID_CLEAN
                         ],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[MEASUREMENT],
                         depends_on=[CleanHeightAndWeight],
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.
        """
        queries = [
            {
                QUERY:
                    CREATE_SANDBOX_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(MEASUREMENT))
            },
            {
                QUERY:
                    INSERT_BMI_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(MEASUREMENT))
            },
            {
                QUERY:
                    INSERT_EXT_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(MEASUREMENT))
            },
        ]

        return queries

    def setup_rule(self, client):
        pass

    def get_sandbox_tablenames(self):
        """
        Return a list table names created to backup deleted data.
        """
        return [self.sandbox_table_for(table) for table in self.affected_tables]

    def setup_validation(self, client):
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        raise NotImplementedError("Please fix me.")


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ARGS = parser.default_parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(CalculateBmi,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(CalculateBmi,)])
