"""
Background

Some values for procedure_source_concept_id in the procedure_occurrence table have been identified as CPT modifiers,
which are supposed to be in the modifier_concept_id field.

While the CPT modifiers could be moved to the correct field, this would leave these rows with
procedure_source_concept_id=0, which has little utility. Therefore, these rows should be dropped.

Cleaning rule to remove records where:
-procedure_concept_id is not a standard concept in the procedure domain
AND
-procedure_source_concept_id is not in the procedure domain (they ARE allowed to be non-standard).

Original Issues: DC-583, DC-845
"""
#Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import JINJA_ENV, PROCEDURE_OCCURRENCE

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC583', 'DC845']

SANDBOX_INVALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
`{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS
SELECT *
FROM
  `{{project}}.{{dataset}}.{{table}}` p
WHERE p.procedure_concept_id NOT IN (
  SELECT
    concept_id
  FROM
    `{{project}}.{{dataset}}.concept`
  WHERE
    domain_id = 'Procedure'
    AND TRIM(concept_class_id) IN ('Procedure', 'CPT4')
    AND standard_concept = 'S'
)
AND
p.procedure_source_concept_id IN (
 SELECT
    concept_id
  FROM
    `{{project}}.{{dataset}}.concept`
  WHERE
    TRIM(concept_class_id) = 'CPT4 Modifier'
)
""")

DELETE_INVALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY = JINJA_ENV.from_string("""
DELETE 
FROM 
  `{{project}}.{{dataset}}.{{table}}`
WHERE procedure_occurrence_id IN (
  SELECT DISTINCT procedure_occurrence_id
  FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`
)
""")


class RemoveInvalidProcedureSourceRecords(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Rule to address CPT modifiers in the procedure_source_concept_id field'
        )
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.EHR],
                         affected_tables=[PROCEDURE_OCCURRENCE],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self, *args, **keyword_args):
        """
        runs the query which removes records that contain incorrect values in the procedure_source_concept_id field
        invalid procedure_source_concept_ids are where it is not in the procedure domain and
        procedure_concept_id is not standard in the procedure domain

        :return:  A list of dictionaries. Each dictionary contains a single query
             and a specification for how to execute that query. The specifications
             are optional but the query is required.
        """

        # query to sandbox invalid procedure source records
        sandbox_invalid_records = {
            cdr_consts.QUERY:
                SANDBOX_INVALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    table=PROCEDURE_OCCURRENCE,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.get_sandbox_tablenames()[0])
        }

        # query to delete invalid procedure source records
        delete_invalid_records = {
            cdr_consts.QUERY:
                DELETE_INVALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    table=PROCEDURE_OCCURRENCE,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.get_sandbox_tablenames()[0])
        }

        return [sandbox_invalid_records, delete_invalid_records]

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
        """
        generates sandbox table names
        """
        sandbox_table = self.sandbox_table_for(PROCEDURE_OCCURRENCE)
        return [sandbox_table]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(RemoveInvalidProcedureSourceRecords,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RemoveInvalidProcedureSourceRecords,)])
