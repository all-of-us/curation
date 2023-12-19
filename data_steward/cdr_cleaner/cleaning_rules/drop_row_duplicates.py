"""
Removes duplicate responses where row fields differ only on observation_id.

Original Issues: DC-3630

This CR sandboxes and removes duplicate rows where all fields except for observation_id are the same.


"""

# Python imports
import logging

# Project imports
from common import JINJA_ENV, OBSERVATION
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

SANDBOX_TEMPLATE = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset}}.{{sandbox_table}}` AS
WITH id_duplicates AS (
SELECT 
observation_id,
person_id,
observation_concept_id,
observation_date,
observation_datetime,
observation_type_concept_id,
value_as_number,
COALESCE(value_as_string,'vas_null') value_as_string,
COALESCE(value_as_concept_id,1111) value_as_concept_id,
qualifier_concept_id,
unit_concept_id,
provider_id,
COALESCE(visit_occurrence_id,3333) visit_occurrence_id,
visit_detail_id,
COALESCE(observation_source_value,'osv_null') observation_source_value,
observation_source_concept_id,
unit_source_value,
qualifier_source_value,
COALESCE(value_source_concept_id,2222) value_source_concept_id,
COALESCE(value_source_value,'vsv_null') value_source_value,
COALESCE(questionnaire_response_id,000) questionnaire_response_id
FROM `{{project_id}}.{{dataset_id}}.observation`
),

ranked_rows as
(SELECT *, ROW_NUMBER() OVER (PARTITION BY 
person_id, observation_concept_id, value_source_value, observation_source_value, value_as_string, value_as_concept_id,
visit_occurrence_id, value_source_concept_id, questionnaire_response_id ORDER BY observation_id) row_num
FROM id_duplicates)

SELECT *
FROM ranked_rows
WHERE row_num != 1
""")

DELETION_TEMPLATE = JINJA_ENV.from_string("""
DELETE
FROM
  `{{project_id}}.{{dataset_id}}.observation`
WHERE
  observation_id IN (SELECT observation_id FROM `{{project_id}}.{{sandbox_dataset}}.{{sandbox_table}}`)
""")


class DropRowDuplicates(BaseCleaningRule):
    """
    Removes duplicate responses where row fields differ only on observation_id.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'Removes duplicate responses where row fields differ only on observation_id.'
        super().__init__(issue_numbers=['DC3630'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=['observation'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        sandbox_duplicates_dict = {
            cdr_consts.QUERY:
                SANDBOX_TEMPLATE.render(
                    project_id=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(OBSERVATION),
                    dataset_id=self.dataset_id)
        }

        delete_observations_dict = {
            cdr_consts.QUERY:
                DELETION_TEMPLATE.render(
                    project_id=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(OBSERVATION),
                    dataset_id=self.dataset_id)
        }

        return [sandbox_duplicates_dict, delete_observations_dict]

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
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(DropRowDuplicates,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropRowDuplicates,)])
