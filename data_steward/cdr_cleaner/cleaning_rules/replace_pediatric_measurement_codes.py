"""
Replace the five pediatric growth-measurement placeholder codes with LOINC.

Original Issues: DL-2480

Five pediatric growth measurements carry placeholder codes: 22222-0, 33333-0,
44444-0, 55555-0 and 66666-0. They were minted to unblock enrollment before
annotation to standard terms finished. They are not LOINC codes and they resolve
to nothing, so releasing without replacing them ships five measurement types no
vocabulary can interpret.

The replacement codes are specified outright in the Pediatrics CT+ MVP PRD, so
this is a defined substitution rather than a mapping exercise. All five
replacement concepts are LOINC, domain Measurement, standard_concept 'S', so
each is written into measurement_concept_id directly with no 'Maps to' step.

A placeholder row carries its code in measurement_source_value with both concept
id columns at 0, so replacing the source value alone would leave the row
unresolvable. All three columns are set.

The predicate keys on the placeholder code in measurement_source_value. Which
of the two RDR measurement row shapes the pediatric placeholders arrive in is
not yet confirmed; if they arrive under the PMI code instead, the predicate
keys on that column value rather than this one and nothing else changes.

Pediatric measurement codes outside the replacement map are not touched, and
neither is a row already annotated to a real concept.
"""

# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import JINJA_ENV, MEASUREMENT

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DL2480']

# The five substitutions, from the Pediatrics CT+ MVP PRD. Held here rather than
# in a lookup table because the set is five fixed rows: in the module it stays
# visible in code review and versioned with the rule that reads it. Revisit only
# if the set stops being small and static.
#
# 'field' and 'pmi_code' are documentation: they name the measurement each
# placeholder stands for and the code it would carry on the other RDR row shape.
# Only placeholder_code, loinc_code and concept_id are rendered into SQL.
PLACEHOLDER_REPLACEMENTS = [
    {
        'field': 'Growth Percentile Weight for Age',
        'pmi_code': 'growth-percentile-weight-for-age',
        'placeholder_code': '22222-0',
        'loinc_code': '8336-0',
        'concept_id': 3013131,
    },
    {
        'field': 'Growth Percentile Height for Age',
        'pmi_code': 'growth-percentile-height-for-age',
        'placeholder_code': '33333-0',
        'loinc_code': '8303-0',
        'concept_id': 3036798,
    },
    {
        'field': 'Growth Percentile Weight for Length',
        'pmi_code': 'growth-percentile-weight-for-length',
        'placeholder_code': '44444-0',
        'loinc_code': '77606-2',
        'concept_id': 46236327,
    },
    {
        'field': 'Growth Percentile Head Circumference for Age',
        'pmi_code': 'growth-percentile-head-circumference-for-age',
        'placeholder_code': '55555-0',
        'loinc_code': '8289-1',
        'concept_id': 3035763,
    },
    {
        'field': 'Growth Percentile BMI for Age',
        'pmi_code': 'growth-percentile-bmi-for-age',
        'placeholder_code': '66666-0',
        'loinc_code': '59576-9',
        'concept_id': 40762638,
    },
]

# The sandbox carries the pre-update measurement columns plus the three
# replacement values, so the rows changed per placeholder stay queryable after
# the run. The update is driven off this table rather than re-deriving the join,
# so exactly the sandboxed rows are the rows changed.
SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
WITH replacements AS (
  SELECT * FROM UNNEST([
  {% for r in replacements %}
    STRUCT(
      '{{r.placeholder_code}}' AS placeholder_code,
      '{{r.loinc_code}}' AS loinc_code,
      {{r.concept_id}} AS concept_id
    ){{ "," if not loop.last }}
  {% endfor %}
  ])
)
SELECT
  m.*,
  r.loinc_code AS new_measurement_source_value,
  r.concept_id AS new_measurement_source_concept_id,
  r.concept_id AS new_measurement_concept_id
FROM `{{project}}.{{dataset}}.measurement` m
JOIN replacements r
ON m.measurement_source_value = r.placeholder_code
)
""")

UPDATE_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project}}.{{dataset}}.measurement` m
SET
  m.measurement_source_value = s.new_measurement_source_value,
  m.measurement_source_concept_id = s.new_measurement_source_concept_id,
  m.measurement_concept_id = s.new_measurement_concept_id
FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` s
WHERE m.measurement_id = s.measurement_id
""")


class ReplacePediatricMeasurementCodes(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id=None,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Replace the five pediatric growth-measurement placeholder codes with their '
            'specified LOINC codes, setting measurement_source_value, '
            'measurement_source_concept_id and measurement_concept_id.')

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[MEASUREMENT],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self, *args, **keyword_args):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        sandbox_query = {
            cdr_consts.QUERY:
                SANDBOX_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(MEASUREMENT),
                    replacements=PLACEHOLDER_REPLACEMENTS)
        }

        update_query = {
            cdr_consts.QUERY:
                UPDATE_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(MEASUREMENT))
        }

        return [sandbox_query, update_query]

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client, *args, **keyword_args):
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client, *args, **keyword_args):
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self._affected_tables
        ]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(ReplacePediatricMeasurementCodes,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(ReplacePediatricMeasurementCodes,)])
