"""
Sandbox and remove all data for participants flagged as under 18 at consent.

DL-2416 downgraded `RemoveParticipantsUnder18Years` to a lookup-only flag, so
under-18 participants now flow through the RDR, combined and tier cleaning
stages. This rule performs the removal that used to happen at the RDR stage,
but at the tier stages instead, reading the participants to remove from the
`_under18_participants` lookup that the flag rule writes to the RDR sandbox.

An optional `age_band_to_retain` keeps one band in place. The CT+ pediatric run
retains ages 0 to 6, which is what `RemoveFlaggedUnder18ParticipantsCtPlus`
pins.

This rule matches on `person_id` and the lookup is keyed by the original
participant ID, so it must run before person IDs are re-keyed to research IDs.

Original Issues: DL2418
"""

# Python imports
import logging

# Third party imports
from google.cloud.exceptions import GoogleCloudError

# Project imports
import common
import constants.cdr_cleaner.clean_cdr as cdr_consts
from resources import get_person_id_tables
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.clean_cdr_utils import get_tables_in_dataset

LOGGER = logging.getLogger(__name__)

# The two band values written by FlagParticipantsUnder18Years. Keep in step with
# the IF(MIN(age_at_consent) <= 6, '0-6', '7-17') expression in that rule.
AGE_BANDS = ['0-6', '7-17']

CT_PLUS_AGE_BAND_TO_RETAIN = '0-6'

AFFECTED_TABLES = [table for table in get_person_id_tables(common.CATI_TABLES)]

SANDBOX_ROWS = common.JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
  `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
  SELECT
    *
  FROM
    `{{project}}.{{dataset}}.{{domain_table}}`
  WHERE
    person_id IN (
    SELECT
      person_id
    FROM
      `{{project}}.{{lookup_dataset}}.{{under18_participant_lookup_table}}`
    {% if age_band_to_retain %}
    WHERE age_band IS DISTINCT FROM '{{age_band_to_retain}}'
    {% endif %}
    )
  )
""")

DROP_ROWS = common.JINJA_ENV.from_string("""
DELETE
FROM
  `{{project}}.{{dataset}}.{{domain_table}}`
WHERE
  person_id IN (
  SELECT
    person_id
  FROM
    `{{project}}.{{lookup_dataset}}.{{under18_participant_lookup_table}}`
  {% if age_band_to_retain %}
  WHERE age_band IS DISTINCT FROM '{{age_band_to_retain}}'
  {% endif %}
  )
""")


class RemoveFlaggedUnder18Participants(BaseCleaningRule):
    """
    Sandbox and remove all data for participants flagged in the
    _under18_participants lookup, optionally retaining one age band.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 under18_lookup_dataset_id,
                 age_band_to_retain=None,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!

        :param under18_lookup_dataset_id: dataset holding the
            _under18_participants lookup, which FlagParticipantsUnder18Years
            writes to the RDR stage sandbox dataset
        :param age_band_to_retain: one of AGE_BANDS to keep in place, or None to
            remove every flagged participant
        """
        if age_band_to_retain is not None and age_band_to_retain not in AGE_BANDS:
            raise ValueError(
                f'age_band_to_retain must be one of {AGE_BANDS} or None, '
                f'got {age_band_to_retain}')

        desc = (
            'All data associated with a participant flagged as younger than 18 years old '
            'at consent is sandboxed and dropped, except for the retained age band, if any.'
        )

        super().__init__(
            issue_numbers=['DL2418'],
            description=desc,
            affected_datasets=[
                cdr_consts.REGISTERED_TIER_PRE_DEID,
                cdr_consts.CONTROLLED_TIER_DEID,
                cdr_consts.CONTROLLED_TIER_PLUS_DEID
            ],
            affected_tables=AFFECTED_TABLES,
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            table_namer=table_namer,
        )

        self.under18_lookup_dataset_id = under18_lookup_dataset_id
        self.age_band_to_retain = age_band_to_retain

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        sandbox_queries = []
        drop_queries = []

        for table in self.affected_tables:

            sandbox_query = {
                cdr_consts.QUERY:
                    SANDBOX_ROWS.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(table),
                        domain_table=table,
                        lookup_dataset=self.under18_lookup_dataset_id,
                        under18_participant_lookup_table=common.
                        UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                        age_band_to_retain=self.age_band_to_retain)
            }
            sandbox_queries.append(sandbox_query)

            drop_query = {
                cdr_consts.QUERY:
                    DROP_ROWS.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        domain_table=table,
                        lookup_dataset=self.under18_lookup_dataset_id,
                        under18_participant_lookup_table=common.
                        UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                        age_band_to_retain=self.age_band_to_retain)
            }
            drop_queries.append(drop_query)

        return sandbox_queries + drop_queries

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.

        Narrows the affected tables to those that exist in the dataset and fails
        early if the lookup table is not where under18_lookup_dataset_id says it
        is, so a mis-pointed dataset cannot turn the removal into a silent no-op.
        """
        try:
            self.affected_tables = get_tables_in_dataset(
                client, self.project_id, self.dataset_id, self.affected_tables)
        except GoogleCloudError as error:
            LOGGER.error(error)
            raise

        lookup_tables = get_tables_in_dataset(
            client, self.project_id, self.under18_lookup_dataset_id,
            [common.UNDER18_PARTICIPANTS_LOOKUP_TABLE])
        if common.UNDER18_PARTICIPANTS_LOOKUP_TABLE not in lookup_tables:
            raise RuntimeError(
                f'{common.UNDER18_PARTICIPANTS_LOOKUP_TABLE} not found in '
                f'{self.project_id}.{self.under18_lookup_dataset_id}. Check the '
                f'under18_lookup_dataset_id parameter points at the RDR stage '
                f'sandbox dataset written by FlagParticipantsUnder18Years.')

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
        Returns an iterable of sandbox table names
        """
        return [self.sandbox_table_for(table) for table in self.affected_tables]


class RemoveFlaggedUnder18ParticipantsCtPlus(RemoveFlaggedUnder18Participants):
    """
    CT+ variant: retains participants aged 0 to 6, removes 7 to 17.

    The retained band is pinned in code rather than passed at run time. The
    engine only forwards CLI kwargs a constructor declares, so this subclass not
    declaring age_band_to_retain is what makes the pinned value unoverridable.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 under18_lookup_dataset_id,
                 table_namer=None):
        super().__init__(project_id,
                         dataset_id,
                         sandbox_dataset_id,
                         under18_lookup_dataset_id=under18_lookup_dataset_id,
                         age_band_to_retain=CT_PLUS_AGE_BAND_TO_RETAIN,
                         table_namer=table_namer)


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument(
        '--under18_lookup_dataset_id',
        dest='under18_lookup_dataset_id',
        action='store',
        help=('Dataset holding the _under18_participants lookup, which is the '
              'RDR stage sandbox dataset.'),
        required=True)
    ext_parser.add_argument(
        '--age_band_to_retain',
        dest='age_band_to_retain',
        action='store',
        choices=AGE_BANDS,
        default=None,
        help=('Age band to keep in place. Omit to remove every flagged '
              'participant.'),
        required=False)
    ARGS = ext_parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(RemoveFlaggedUnder18Participants,)],
            under18_lookup_dataset_id=ARGS.under18_lookup_dataset_id,
            age_band_to_retain=ARGS.age_band_to_retain)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(RemoveFlaggedUnder18Participants,)],
            under18_lookup_dataset_id=ARGS.under18_lookup_dataset_id,
            age_band_to_retain=ARGS.age_band_to_retain)
