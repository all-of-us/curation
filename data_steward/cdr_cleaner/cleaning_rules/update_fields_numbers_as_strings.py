"""
Original Issues: DC-1052

Background
It has been discovered that the field type for some PPI survey answers is incorrect: there are several instances of
numeric answers being saved as ‘string’ field types. The expected long-term fix is for PTSC to correct the field type
on their end; however, there is no anticipated timeline for the completion of this work. As a result, the Curation team
will need to create a cleaning rule to correct these errors.

This cleaning rule should only apply to specific numeric values collected in COPE surveys, a list of specific
observation_source_values that are affected in code.

Cleaning rule to fill null values in value_as_number with values in value_as_string,
EXCEPT when it’s a ‘PMI Skip’ for each of the observation_source_value

Rule should be applied to the RDR export
"""
# python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

# observation_source_values to replace
OBSERVATION_SOURCE_VALUES = "('basics_xx', 'basics_xx20', 'ipaq_1_cope_a_24', 'ipaq_2_cope_a_160', " \
                            "'ipaq_2_cope_a_85', 'ipaq_3_cope_a_24', 'ipaq_4_cope_a_160', 'ipaq_4_cope_a_85', " \
                            "'ipaq_5_cope_a_24', 'ipaq_6_cope_a_160', 'ipaq_6_cope_a_85', 'cope_a_160', 'cope_a_85', " \
                            "'copect_50_xx19_cope_a_57', 'copect_50_xx19_cope_a_198', 'copect_50_xx19_cope_a_152', " \
                            "'lifestyle_2_xx12_cope_a_57', 'lifestyle_2_xx12_cope_a_198', " \
                            "'lifestyle_2_xx12_cope_a_152', 'ipaq_1_cope_a_85', 'ipaq_3_cope_a_85', " \
                            "'ipaq_5_cope_a_85', 'ipaq_7_cope_a_160', 'ipaq_7_cope_a_85', 'cdc_covid_19_n_a2')"

# Query to sandbox original observation table before CR
SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` as(
    SELECT
        *
    FROM
        `{{project}}.{{dataset}}.observation`
    WHERE
        observation_source_value IN {{observation_source_values}}
        AND value_as_number IS NULL
        AND value_as_string != 'PMI Skip'
        AND REGEXP_CONTAINS(value_as_string, r'^\d+$')
)
""")

# Query to update value_as_number field
NUMBERS_AS_STRINGS_QUERY = JINJA_ENV.from_string("""
SELECT
  observation_id,
  person_id,
  observation_concept_id,
  observation_date,
  observation_datetime,
  observation_type_concept_id,
  CASE WHEN observation_source_value IN {{observation_source_values}}
  AND value_as_number IS NULL AND value_as_string != 'PMI Skip' AND REGEXP_CONTAINS(value_as_string, r'^\d+$')
  THEN CAST(value_as_string AS INT64) ELSE value_as_number END AS value_as_number,
  CASE WHEN observation_source_value IN {{observation_source_values}}
  AND value_as_number IS NULL AND value_as_string != 'PMI Skip' AND REGEXP_CONTAINS(value_as_string, r'^\d+$')
  THEN NULL ELSE value_as_string END AS value_as_string,
  value_as_concept_id,
  qualifier_concept_id,
  unit_concept_id,
  provider_id,
  visit_occurrence_id,
  visit_detail_id,
  observation_source_value,
  observation_source_concept_id,
  unit_source_value,
  qualifier_source_value,
  value_source_concept_id,
  value_source_value,
  questionnaire_response_id
FROM `{{project}}.{{dataset}}.observation`
""")

tables = ['observation']


class UpdateFieldsNumbersAsStrings(BaseCleaningRule):
    """
    Cleaning rule will fill null values in value_as_number with values in value_as_string, EXCEPT when it’s a ‘PMI Skip’
    for each of the observation_source_value
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Fixing null values in value_as_number with the values in value_as_string'
        )
        super().__init__(issue_numbers=['DC1052'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=tables,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         run_for_synthetic=True)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        query_list = []
        for i, table in enumerate(tables):
            query_list.append({
                cdr_consts.QUERY:
                    SANDBOX_QUERY.render(
                        project=self.project_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.get_sandbox_tablenames()[i],
                        dataset=self.dataset_id,
                        observation_source_values=OBSERVATION_SOURCE_VALUES),
            })
            query_list.append({
                cdr_consts.QUERY:
                    NUMBERS_AS_STRINGS_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        observation_source_values=OBSERVATION_SOURCE_VALUES),
                cdr_consts.DESTINATION_TABLE:
                    table,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE
            })

        return query_list

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        sandbox_table_names = list()
        for i in range(0, len(self._affected_tables)):
            sandbox_table_names.append(self._issue_numbers[0].lower() + '_' +
                                       self._affected_tables[i])
        return sandbox_table_names


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(UpdateFieldsNumbersAsStrings,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(UpdateFieldsNumbersAsStrings,)])
