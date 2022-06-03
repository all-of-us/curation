"""
Participant ID validation must be performed on site submission data for it to be included in the CDR. 
Ideally, sites conduct participant matching and only submit data for matched participants,
along with a table detailing their matching process.
The DRC also performs a matching process on data received to validate matching done at the site level.

Some sites may not provide participant matching tables for the launch dataset or may not run any 
matching processes for some of the data.
In these cases, the DRC matching algorithm should run and any non-matching PIDs identified 
by the algorithm should be dropped from the launch dataset.
This will ensure at least one level of identity validation is occurring.
"""

# Python imports
import logging

# Project imports
from gcloud.bq import BigQueryClient
import resources
from common import JINJA_ENV, IDENTITY_MATCH, PARTICIPANT_MATCH
from validation.participants import readers
from cdr_cleaner.cleaning_rules import sandbox_and_remove_pids as remove_pids
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from constants.validation.participants.identity_match import (PERSON_ID_FIELD,
                                                              FIRST_NAME_FIELD,
                                                              LAST_NAME_FIELD,
                                                              BIRTH_DATE_FIELD)
from constants.validation.participants.writers import ALGORITHM_FIELD

LOGGER = logging.getLogger(__name__)

NUM_OF_MISSING_KEY_FIELDS = 2
NUM_OF_MISSING_ALL_FIELDS = 4

NOT_MATCH_TABLE = '_not_match_person_id'

KEY_FIELDS = [FIRST_NAME_FIELD, LAST_NAME_FIELD, BIRTH_DATE_FIELD]
IDENTITY_MATCH_EXCLUDED_FIELD = [PERSON_ID_FIELD, ALGORITHM_FIELD]

CREATE_NOT_MATCH_TABLE = JINJA_ENV.from_string("""
CREATE TABLE IF NOT EXISTS `{{project_id}}.{{sandbox_dataset_id}}.{{not_match_table}}`
(source_table STRING, person_id INT64)
""")

INSERT_NOT_MATCH_PERSON_ID = JINJA_ENV.from_string("""
INSERT INTO  `{{project_id}}.{{sandbox_dataset_id}}.{{not_match_table}}`
(source_table, person_id)
WITH non_match_participants AS
(
  SELECT 
    person_id,
    ({{key_fields_criteria}}) AS key_fields_criteria,
    ({{all_fields_criteria}}) AS all_fields_criteria
  FROM `{{project_id}}.{{validation_dataset_id}}.{{identity_match_table}}`
)
SELECT '{{identity_match_table}}', person_id
FROM non_match_participants
WHERE (key_fields_criteria IS TRUE OR all_fields_criteria IS TRUE)
{{only_not_validated_id_condition}}
""")

ONLY_NOT_VALIDATED_ID_CONDITION = JINJA_ENV.from_string("""
AND person_id IN (
    SELECT person_id FROM `{{project_id}}.{{ehr_dataset_id}}.{{participant_match_table}}`
    WHERE LOWER(algorithm_validation) != 'yes'
    AND LOWER(manual_validation) != 'yes'
)
""")

CAST_MISSING_COLUMN = JINJA_ENV.from_string(
    """CAST({{column}} <> 'match' AS int64)""")

CRITERIA_COLUMN_TEMPLATE = JINJA_ENV.from_string(
    """({{column_expr}}) >= {{num_of_missing}}""")


class RemoveNonMatchingParticipant(BaseCleaningRule):
    """
    Removes records with person_ids that are not validated by sites and non-matching.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None,
                 ehr_dataset_id=None,
                 validation_dataset_id=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """

        self.ehr_dataset_id = ehr_dataset_id
        self.validation_dataset_id = validation_dataset_id

        if ehr_dataset_id is None:
            raise RuntimeError('Required parameter ehr_dataset_id not set.')

        if validation_dataset_id is None:
            raise RuntimeError(
                'Required parameter validation_dataset_id not set')

        desc = 'Removes non-matching and not validated participant records from the combined dataset.'
        super().__init__(issue_numbers=['DC468', 'DC823'],
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def setup_rule(self, client: BigQueryClient):
        """
        Define affected_tables and create the lookup table NOT_MATCH_TABLE.
        This table has person_ids that need to be deleted from the CDM tables. 
        It also has the source table info for debug purpose.

        :param client: A BigQueryClient
        """
        person_table_query = remove_pids.PERSON_TABLE_QUERY.format(
            project=self.project_id, dataset=self.dataset_id)
        person_tables = client.query(person_table_query).to_dataframe()[
            remove_pids.TABLE_NAME_COLUMN].values.tolist()
        self.affected_tables = [
            table_name for table_name in person_tables
            if '_mapping' not in table_name
        ]

        create_not_match_table = CREATE_NOT_MATCH_TABLE.render(
            project_id=self.project_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            not_match_table=NOT_MATCH_TABLE)
        job = client.query(create_not_match_table)
        job.result()

        for hpo_id in readers.get_hpo_site_names():

            identity_match = resources.get_table_id(IDENTITY_MATCH,
                                                    hpo_id=hpo_id)
            participant_match = resources.get_table_id(PARTICIPANT_MATCH,
                                                       hpo_id=hpo_id)

            if not client.table_exists(identity_match,
                                       self.validation_dataset_id):
                LOGGER.info(
                    f'{hpo_id} is missing the identity_match table. Skipping {hpo_id}...'
                )
                continue

            if client.table_exists(participant_match, self.ehr_dataset_id):
                only_not_validated_id_condition = ONLY_NOT_VALIDATED_ID_CONDITION.render(
                    project_id=self.project_id,
                    ehr_dataset_id=self.ehr_dataset_id,
                    participant_match_table=participant_match)
            else:
                only_not_validated_id_condition = ''
                LOGGER.info(
                    f'{hpo_id} is missing the participant_match table. '
                    f'All data from {hpo_id} is treated as not validated.')

            key_fields_criteria, all_fields_criteria = self.get_fields_criteria(
            )

            not_validated_participants_query = INSERT_NOT_MATCH_PERSON_ID.render(
                project_id=self.project_id,
                sandbox_dataset_id=self.sandbox_dataset_id,
                not_match_table=NOT_MATCH_TABLE,
                participant_match_table=participant_match,
                key_fields_criteria=key_fields_criteria,
                all_fields_criteria=all_fields_criteria,
                validation_dataset_id=self.validation_dataset_id,
                identity_match_table=identity_match,
                only_not_validated_id_condition=only_not_validated_id_condition)

            job = client.query(not_validated_participants_query)
            job.result()

    def setup_validation(self, client: BigQueryClient) -> None:
        pass

    def validate_rule(self, client: BigQueryClient) -> None:
        pass

    def get_sandbox_tablenames(self) -> list:
        """
        Return a list table names created to backup deleted data.
        """
        return [self.sandbox_table_for(table) for table in self.affected_tables]

    def get_fields_criteria(self) -> str:
        """
        Generates the criteria that decides if the person_id is not match or not.
        For key_fields, if any of the two of first_name, last_name and birthday are missing, it is a not match.
        For all_fields, if the total number of missings is equal to or bigger than 4, it is a not match.

        Returns: the criteria for key fields and for all fields, each in string.
        """

        key_fields_criteria = CRITERIA_COLUMN_TEMPLATE.render(
            column_expr=self.get_missing_criteria(KEY_FIELDS),
            num_of_missing=NUM_OF_MISSING_KEY_FIELDS)

        identity_match_fields = [
            field['name']
            for field in resources.fields_for(IDENTITY_MATCH)
            if field['name'] not in IDENTITY_MATCH_EXCLUDED_FIELD
        ]
        all_fields_criteria = CRITERIA_COLUMN_TEMPLATE.render(
            column_expr=self.get_missing_criteria(identity_match_fields),
            num_of_missing=NUM_OF_MISSING_ALL_FIELDS)

        return key_fields_criteria, all_fields_criteria

    def get_missing_criteria(self, field_names) -> str:
        """
        Generates a bigquery column expression for missing criteria.

        :param field_names: a list of field names for counting `missing`s
        :return: Multiple CAST statements joined by ' + '.
        """
        joined_column_expr = ' + '.join([
            CAST_MISSING_COLUMN.render(column=field_name)
            for field_name in field_names
        ])
        return joined_column_expr

    def get_query_specs(self) -> list:
        """
        Return a list of dictionary query specifications.
        The list contains the queries that delete participants and their corresponding data points,
        for which the participant_match data is missing and DRC matching algorithm flags it as a no match

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        sandbox_queries = remove_pids.get_sandbox_queries(
            self.project_id,
            self.dataset_id,
            '_'.join(self.issue_numbers),
            sandbox_dataset_id=self.sandbox_dataset_id,
            lookup_table=NOT_MATCH_TABLE)

        remove_pids_queries = remove_pids.get_remove_pids_queries(
            self.project_id,
            self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            lookup_table=NOT_MATCH_TABLE)

        return sandbox_queries + remove_pids_queries


def parse_args():
    """
    This function expands the default argument list defined in cdr_cleaner.args_parser
    :return: an expanded argument list object
    """

    import cdr_cleaner.args_parser as parser

    additional_arguments = [{
        parser.SHORT_ARGUMENT: '-e',
        parser.LONG_ARGUMENT: '--ehr_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'ehr_dataset_id',
        parser.HELP: 'ehr_dataset_id',
        parser.REQUIRED: True
    }, {
        parser.SHORT_ARGUMENT: '-v',
        parser.LONG_ARGUMENT: '--validation_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'validation_dataset_id',
        parser.HELP: 'validation_dataset_id',
        parser.REQUIRED: True
    }]
    args = parser.default_parse_args(additional_arguments)
    return args


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(RemoveNonMatchingParticipant,)],
            ehr_dataset_id=ARGS.ehr_dataset_id,
            validation_dataset_id=ARGS.validation_dataset_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(RemoveNonMatchingParticipant,)],
            ehr_dataset_id=ARGS.ehr_dataset_id,
            validation_dataset_id=ARGS.validation_dataset_id)
