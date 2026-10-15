import logging

from common import (AIAN_LIST, AOU_DEATH, CDM_TABLES, DEID_MAP, JINJA_ENV,
                    OBSERVATION, PERSON, UNDER18_PARTICIPANTS_LOOKUP_TABLE)
from constants import bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.concept_suppression import (
    AbstractConceptSuppression, AbstractInMemoryLookupTableConceptSuppression)
from cdr_cleaner.cleaning_rules.deid.ct_plus_dob_indicators import \
    get_dob_indicator_concept_ids
from resources import get_concept_id_fields, has_person_id

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC1358']
CT_PLUS_ISSUE_NUMBERS = ISSUE_NUMBERS + ['DL2495']

# Rows carrying an indicators-of-birth concept in any concept column, restricted to
# AIAN and pediatric participants. Both lookups are keyed by participant id, so they
# go through _deid_map, the map CtPlusPIDtoRID re-keyed person_id with.
EXCLUDED_PARTICIPANT_ROWS = JINJA_ENV.from_string("""
SELECT
  d.*
FROM `{{project}}.{{dataset}}.{{domain_table}}` AS d
WHERE EXISTS (
    SELECT 1
    FROM UNNEST([{{concept_ids | join(', ')}}]) AS c
    WHERE c IN ({{concept_fields | join(', ')}})
)
AND (
{% for excluded_table in excluded_tables %}
    {% if not loop.first %}OR {% endif %}EXISTS (
        SELECT 1
        FROM `{{project}}.{{excluded_table}}` x
        JOIN `{{project}}.{{sandbox_dataset}}.{{deid_map}}` m
          ON m.person_id = x.person_id
        WHERE m.research_id = d.person_id
    )
{% endfor %}
)
""")


class BirthInformationSuppression(AbstractInMemoryLookupTableConceptSuppression
                                 ):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Sandbox and record suppress all records with a concept_id or concept_code '
            'relating to birth information. ')
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.CONTROLLED_TIER_DEID,
                             cdr_consts.REGISTERED_TIER_DEID
                         ],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[OBSERVATION],
                         table_namer=table_namer)

    def get_suppressed_concept_ids(self):
        # https://athena.ohdsi.org/search-terms/terms/1585259
        # https://athena.ohdsi.org/search-terms/terms/4083587
        return [1585259, 4083587, 3022007]

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass


class BirthInformationSuppressionCtPlus(AbstractConceptSuppression):
    """
    CT+ variant: suppress the whole indicators-of-birth set, but only for AIAN and
    pediatric participants.

    The CT+ date-of-birth add-on delivers these rows to everyone else, so the CT+
    rules after this one leave them inline and split_ct_plus_dob_indicators.py
    moves them out at the end of the run. AIAN participants cannot receive them,
    and pediatric participants cannot either, since date of birth is provisioned
    separately from the pediatrics add-on. Removing their rows here, once, is what
    lets every later rule release the set without knowing who is eligible. Each
    row removed here would have been removed by CT anyway.

    Must run after CtPlusPIDtoRID, which writes _deid_map, and before
    YearOfBirthRecordsSuppressionCtPlus.

    ct_plus_dob_indicators.py describes the whole sequence.

    Original Issues: DC-1358, DL-2495
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 rdr_sandbox_id,
                 under18_lookup_dataset_id,
                 table_namer=None):
        """
        :param rdr_sandbox_id: dataset holding aian_list, the RDR stage sandbox
        :param under18_lookup_dataset_id: dataset holding the
            _under18_participants lookup, the RDR stage sandbox
        """
        desc = (
            'Sandbox and suppress the indicators-of-birth rows of AIAN and '
            'pediatric participants, who cannot receive the CT+ date-of-birth '
            'add-on.')
        # person is left out: a birth concept never sits there, and the delete
        # would remove the participant's person row
        affected_tables = [
            table for table in sorted(set(CDM_TABLES + [AOU_DEATH]))
            if table != PERSON and has_person_id(table)
        ]
        super().__init__(
            issue_numbers=CT_PLUS_ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_PLUS_DEID],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            affected_tables=affected_tables,
            table_namer=table_namer)

        self.rdr_sandbox_id = rdr_sandbox_id
        self.under18_lookup_dataset_id = under18_lookup_dataset_id

    def get_sandbox_query(self, table_name):
        """
        Sandbox the table's indicators-of-birth rows that belong to AIAN or
        pediatric participants.

        :param table_name: domain table to sandbox from
        :return: query dictionary
        """
        concept_fields = [
            f'd.{field}' for field in get_concept_id_fields(table_name)
        ]
        query = EXCLUDED_PARTICIPANT_ROWS.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            domain_table=table_name,
            concept_ids=get_dob_indicator_concept_ids(),
            concept_fields=concept_fields,
            deid_map=DEID_MAP,
            excluded_tables=[
                f'{self.rdr_sandbox_id}.{AIAN_LIST}',
                f'{self.under18_lookup_dataset_id}.'
                f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}'
            ])

        return {
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: self.sandbox_table_for(table_name),
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id
        }

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(BirthInformationSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(BirthInformationSuppression,)])
