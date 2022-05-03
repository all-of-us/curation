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

# Third party imports
import oauth2client
import googleapiclient
from google.cloud.exceptions import NotFound

# Project imports
import bq_utils
from constants.cdr_cleaner import clean_cdr as cdr_consts
import resources
from common import JINJA_ENV, IDENTITY_MATCH, PARTICIPANT_MATCH
from gcloud.bq import BigQueryClient
from validation.participants import readers
from cdr_cleaner.cleaning_rules import sandbox_and_remove_pids as remove_pids
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.validation.participants.identity_match import (PERSON_ID_FIELD,
                                                              FIRST_NAME_FIELD,
                                                              LAST_NAME_FIELD,
                                                              BIRTH_DATE_FIELD)
from constants.validation.participants.writers import ALGORITHM_FIELD

LOGGER = logging.getLogger(__name__)

TICKET_NUMBER = 'DC-468'

NUM_OF_MISSING_KEY_FIELDS = 2
NUM_OF_MISSING_ALL_FIELDS = 4

KEY_FIELDS = [FIRST_NAME_FIELD, LAST_NAME_FIELD, BIRTH_DATE_FIELD]

IDENTITY_MATCH_EXCLUDED_FIELD = [PERSON_ID_FIELD, ALGORITHM_FIELD]

CAST_MISSING_COLUMN = JINJA_ENV.from_string(
    """CAST({{column}} <> 'match' AS int64)""")

CRITERION_COLUMN_TEMPLATE = JINJA_ENV.from_string(
    """({{column_expr}}) >= {{num_of_missing}}""")

SELECT_NON_MATCH_PARTICIPANTS_QUERY = JINJA_ENV.from_string("""
WITH non_match_participants AS
(
  SELECT 
    *,
    ({{key_fields_criteria}}) AS key_fields_criteria,
    ({{all_fields_criteria}}) AS all_fields_criteria
  FROM `{{project_id}}.{{validation_dataset_id}}.{{identity_match_table}}`
)
SELECT person_id
FROM non_match_participants
WHERE (key_fields_criteria IS TRUE OR all_fields_criteria IS TRUE)
{{not_validated_participants_expr}}
""")

NOT_VALIDATED_PARTICIPANTS_TEMPLATE = JINJA_ENV.from_string("""
AND person_id IN ({{pids}})                                                            
""")

SELECT_NOT_VALIDATED_PARTICIPANTS_QUERY = JINJA_ENV.from_string("""
SELECT person_id
FROM `{{project_id}}.{{ehr_dataset_id}}.{{participant_match_table}}`
WHERE LOWER(algorithm_validation) != 'yes'
AND LOWER(manual_validation) != 'yes'
""")


class RemoveNonMatchingParticipant(BaseCleaningRule):
    """
    Removes records with person_ids that are not validated by sites and non-matching.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 ehr_dataset_id, validation_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """

        self.ehr_dataset_id = ehr_dataset_id
        self.validation_dataset_id = validation_dataset_id

        desc = 'Removes non-matching and not validated participant records from the combined dataset.'
        super().__init__(issue_numbers=['DC468', 'DC823'],
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

        self.client = BigQueryClient(self.project_id)

    def setup_rule(self, client) -> None:
        pass

    def setup_validation(self, client) -> None:
        pass

    def validate_rule(self, client) -> None:
        pass

    def get_sandbox_tablenames(self) -> list:
        """
        Return a list table names created to backup deleted data.
        """
        return [
            f'{table}_{TICKET_NUMBER}'
            for table in remove_pids.get_tables_with_person_id(
                self.project_id, self.dataset_id)
        ]

    def exist_participant_match(self, ehr_dataset_id, hpo_id) -> bool:
        """
        Checks if the hpo has submitted the participant_match table or not.

        :param ehr_dataset_id: Unioned EHR dataset ID.
        :param hpo_id: HPO site ID.
        :return: True if participant match table exists. Otherwise, False.
        """
        return bq_utils.table_exists(
            bq_utils.get_table_id(hpo_id, PARTICIPANT_MATCH), ehr_dataset_id)

    def exist_identity_match(self, table_id) -> bool:
        """
        Checks if the hpo has valid the identity_match table or not.

        :param table_id: ID of the identity_match table.
        :return: True if identity match table exists. Otherwise, False.
        """
        try:
            self.client.get_table(table_id)
            return True
        except NotFound:
            return False

    def get_missing_criterion(self, field_names) -> str:
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

    def get_non_match_participants(self,
                                   validation_dataset_id,
                                   hpo_id,
                                   pids=None) -> list:
        """
        Retrieves a list of non-match participants.
        If identity match table does not exist, returns an empty list.

        :param validation_dataset_id: Validation dataset ID.
        :param hpo_id: HPO site ID.
        :return: list of non-match participants.
        """
        identity_match_table = bq_utils.get_table_id(hpo_id, IDENTITY_MATCH)

        fq_identity_match_table = f'{self.project_id}.{validation_dataset_id}.{identity_match_table}'
        if not self.exist_identity_match(fq_identity_match_table):
            LOGGER.info(f'Identify match table does not exist for {hpo_id}.')
            return []

        non_match_participants_query = self.get_non_match_participant_query(
            validation_dataset_id, identity_match_table, pids=pids)

        try:
            results = bq_utils.query(q=non_match_participants_query)
        except (oauth2client.client.HttpAccessTokenRefreshError,
                googleapiclient.errors.HttpError) as exp:
            LOGGER.exception(
                f'Could not execute the query \n{non_match_participants_query}')
            raise exp

        query_job_id = results['jobReference']['jobId']
        incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
        if incomplete_jobs:
            raise bq_utils.BigQueryJobWaitError(incomplete_jobs)

        # return the person_ids only
        return [row[PERSON_ID_FIELD] for row in bq_utils.response2rows(results)]

    def get_not_validated_participants(self, ehr_dataset_id, hpo_id) -> list:
        """
        Retrieves a list of participants that are not validated by sites

        :param ehr_dataset_id: Unioned EHR dataset ID.
        :param hpo_id: HPO site ID
        :return: list of not validated participants.
        """
        not_validated_participants_query = SELECT_NOT_VALIDATED_PARTICIPANTS_QUERY.render(
            project_id=self.project_id,
            ehr_dataset_id=ehr_dataset_id,
            participant_match_table=f'{hpo_id}_{PARTICIPANT_MATCH}')
        results = bq_utils.query(q=not_validated_participants_query)
        # return the person_ids only
        result = [
            row[PERSON_ID_FIELD] for row in bq_utils.response2rows(results)
        ]
        return result

    def get_non_match_participant_query(self,
                                        validation_dataset_id,
                                        identity_match_table,
                                        pids=None) -> str:
        """
        Generates the query for identifying non_match participants query flagged by the DRC match algorithm.

        :param validation_dataset_id: Validation dataset ID.
        :param identity_match_table: ID of the identity match table.
        :return: Query to identify person_ids that need to be removed from the dataset.
        """

        # if any of the two of first_name, last_name and birthday are missing, this is a non-match
        num_of_missing_key_fields = CRITERION_COLUMN_TEMPLATE.render(
            column_expr=self.get_missing_criterion(KEY_FIELDS),
            num_of_missing=NUM_OF_MISSING_KEY_FIELDS)

        identity_match_fields = [
            field['name']
            for field in resources.fields_for(IDENTITY_MATCH)
            if field['name'] not in IDENTITY_MATCH_EXCLUDED_FIELD
        ]
        # if the total number of missings is equal to and bigger than 4, this is a non-match
        num_of_missing_all_fields = CRITERION_COLUMN_TEMPLATE.render(
            column_expr=self.get_missing_criterion(identity_match_fields),
            num_of_missing=NUM_OF_MISSING_ALL_FIELDS)

        # instantiate the query for identifying the non-match participants in the validation_dataset
        select_non_match_participants_query = SELECT_NON_MATCH_PARTICIPANTS_QUERY.render(
            project_id=self.project_id,
            validation_dataset_id=validation_dataset_id,
            identity_match_table=identity_match_table,
            key_fields_criteria=num_of_missing_key_fields,
            all_fields_criteria=num_of_missing_all_fields,
            not_validated_participants_expr=NOT_VALIDATED_PARTICIPANTS_TEMPLATE.
            render(pids=", ".join(str(pid) for pid in pids)) if pids else '')

        return select_non_match_participants_query

    def get_query_specs(self) -> list:
        """
        Return a list of dictionary query specifications.
        The list contains the queries that delete participants and their corresponding data points, 
        for which the participant_match data is missing and DRC matching algorithm flags it as a no match

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        if self.ehr_dataset_id is None:
            raise RuntimeError('Required parameter ehr_dataset_id not set.')

        if self.validation_dataset_id is None:
            raise RuntimeError(
                'Required parameter validation_dataset_id not set')

        non_matching_person_ids = []

        for hpo_id in readers.get_hpo_site_names():
            if self.exist_participant_match(self.ehr_dataset_id, hpo_id):
                not_validated_participants = self.get_not_validated_participants(
                    self.ehr_dataset_id, hpo_id)

                if not_validated_participants:
                    LOGGER.info(
                        f'{hpo_id} submitted the participant_match table, but some of its data is not validated.'
                    )
                    non_matching_person_ids.extend(
                        self.get_non_match_participants(
                            self.validation_dataset_id,
                            hpo_id,
                            pids=not_validated_participants))

                else:
                    LOGGER.info(
                        f'{hpo_id} submitted the participant_match table, and all data is validated.'
                    )

            else:
                LOGGER.info(f'{hpo_id} is missing the participant_match table.')
                non_matching_person_ids.extend(
                    self.get_non_match_participants(self.validation_dataset_id,
                                                    hpo_id))

        if non_matching_person_ids:

            sandbox_queries = remove_pids.get_sandbox_queries(
                self.project_id, self.dataset_id, non_matching_person_ids,
                TICKET_NUMBER)

            remove_pids_queries = remove_pids.get_remove_pids_queries(
                self.project_id, self.dataset_id, non_matching_person_ids)

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
