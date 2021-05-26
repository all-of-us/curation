"""
Original Issues: DC-975

As part of the effort to continue to improve data quality, rows with 0 source and standard concept_ids need to be
removed from the clean dataset

Affected tables and columns (both columns need to be zero or NULL):
condition_occurrence
    - condition_source_concept_id
    - condition_concept_id
procedure_occurrence
    - procedure_source_concept_id
    - procedure_concept_id
visit_occurrence
    - visit_source_concept_id
    - visit_concept_id
drug_exposure
    - drug_source_concept_id
    - drug_concept_id
device_exposure
    - device_source_concept_id
    - device_concept_id
observation
    - observation_source_concept_id
    - observation_concept_id
measurement
    - measurement_source_concept_id
    - measurement_concept_id

Remove those rows from the clean dataset
Archive/sandbox those rows

As of DC-1661, the death table has been removed.
This allows the death table with suppressed cause_concept_id and
cause_source_concept_id to persist without being deleted.
"""

# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

tables = [
    'condition_occurrence', 'procedure_occurrence', 'visit_occurrence',
    'drug_exposure', 'device_exposure', 'observation', 'measurement'
]
unique_identifier = {
    'condition_occurrence': 'condition_occurrence_id',
    'procedure_occurrence': 'procedure_occurrence_id',
    'visit_occurrence': 'visit_occurrence_id',
    'drug_exposure': 'drug_exposure_id',
    'device_exposure': 'device_exposure_id',
    'observation': 'observation_id',
    'measurement': 'measurement_id',
}

source_concept_id_columns = {
    'condition_occurrence': 'condition_source_concept_id',
    'procedure_occurrence': 'procedure_source_concept_id',
    'visit_occurrence': 'visit_source_concept_id',
    'drug_exposure': 'drug_source_concept_id',
    'device_exposure': 'device_source_concept_id',
    'observation': 'observation_source_concept_id',
    'measurement': 'measurement_source_concept_id'
}
concept_id_columns = {
    'condition_occurrence': 'condition_concept_id',
    'procedure_occurrence': 'procedure_concept_id',
    'visit_occurrence': 'visit_concept_id',
    'drug_exposure': 'drug_concept_id',
    'device_exposure': 'device_concept_id',
    'observation': 'observation_concept_id',
    'measurement': 'measurement_concept_id'
}

# Query to create tables in sandbox with the rows that will be removed per cleaning rule
SANDBOX_ZERO_CONCEPT_IDS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` as(
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE
({{source_concept_id}} is NULL or {{source_concept_id}} = 0)
AND ({{concept_id}} is NULL or {{concept_id}} = 0)
)
""")

DROP_ZERO_CONCEPT_IDS_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project}}.{{dataset}}.{{table}}`
WHERE
{{unique_identifier}} IN (
SELECT {{unique_identifier}}
FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`
)
""")


class DropZeroConceptIDs(BaseCleaningRule):
    """
    Remove rows with a concept_id and source concept_id's containing zero or NULL.
    Both columns need to be one or the other to drop.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Drops rows with concept_id and source_concept_ids containing zero or null'
        )
        super().__init__(issue_numbers=['DC975'],
                         description=desc,
                         affected_datasets=[
                             cdr_consts.DEID_CLEAN,
                             cdr_consts.CONTROLLED_TIER_DEID_CLEAN
                         ],
                         affected_tables=tables,
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
        sandbox_queries_list = []
        drop_queries_list = []
        for i, table in enumerate(tables):
            sandbox_queries_list.append({
                cdr_consts.QUERY:
                    SANDBOX_ZERO_CONCEPT_IDS_QUERY.render(
                        project=self.project_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.get_sandbox_tablenames()[i],
                        dataset=self.dataset_id,
                        table=table,
                        source_concept_id=source_concept_id_columns[table],
                        concept_id=concept_id_columns[table])
            })

            drop_queries_list.append({
                cdr_consts.QUERY:
                    DROP_ZERO_CONCEPT_IDS_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table=table,
                        unique_identifier=unique_identifier[table],
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.get_sandbox_tablenames()[i])
            })

        return sandbox_queries_list + drop_queries_list

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
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(DropZeroConceptIDs,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropZeroConceptIDs,)])
