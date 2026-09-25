"""
Capture dates of birth for the CT+ date-of-birth linked dataset.

NullPersonBirthdate nulls birth_datetime, month_of_birth and day_of_birth in
place, so they are gone once it runs. CaptureCtPlusBirthdate copies them into a
side table before that, then PruneCtPlusBirthdate and ConvertCtPlusBirthdateIds
reconcile it with person and re-key it to CT+ ids. ct_plus_side_tables.py
describes the three steps and their ordering constraint.

year_of_birth is not captured: the CT+ mainline already publishes it. CT+
applies no date shift, so the values are taken as they stand.

Original Issues: DL2436
"""

# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import CT_PLUS_BIRTHDATE, JINJA_ENV, PERSON
from cdr_cleaner.cleaning_rules.ct_plus_side_tables import (
    ConvertCtPlusSideTableIds, CtPlusSideTableRule, PruneCtPlusSideTable,
    excluded_participants)

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DL2436']

CAPTURE_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{storage_table}}` AS (
SELECT
    p.person_id,
    p.birth_datetime,
    p.month_of_birth,
    p.day_of_birth
FROM `{{project_id}}.{{dataset_id}}.{{person}}` p
WHERE p.birth_datetime IS NOT NULL
{{excluded_participants}}
)""")


class CaptureCtPlusBirthdate(CtPlusSideTableRule):
    """
    Copy each eligible participant's date of birth into the side table, before
    NullPersonBirthdate nulls it.
    """
    storage_table = CT_PLUS_BIRTHDATE

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
        desc = ('Capture dates of birth, excluding AIAN and pediatric '
                'participants, before they are nulled.')
        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_PLUS_DEID],
            affected_tables=[PERSON],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            table_namer=table_namer)

        self.rdr_sandbox_id = rdr_sandbox_id
        self.under18_lookup_dataset_id = under18_lookup_dataset_id

    def get_query_specs(self):
        """
        :return: a list of query dictionaries
        """
        query = CAPTURE_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            storage_table=self.storage_table,
            person=PERSON,
            excluded_participants=excluded_participants(
                self.project_id, self.sandbox_dataset_id, self.rdr_sandbox_id,
                self.under18_lookup_dataset_id, 'p'))
        return [{cdr_consts.QUERY: query}]


class PruneCtPlusBirthdate(PruneCtPlusSideTable):
    """
    Drop captured dates of birth for participants no longer in person.
    """
    storage_table = CT_PLUS_BIRTHDATE

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = ('Drop captured dates of birth for participants no longer in '
                'the person table.')
        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_PLUS_DEID],
            affected_tables=[PERSON],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[CaptureCtPlusBirthdate],
            table_namer=table_namer)


class ConvertCtPlusBirthdateIds(ConvertCtPlusSideTableIds):
    """
    Re-key the captured dates of birth from CT research ids to CT+ research ids.
    """
    storage_table = CT_PLUS_BIRTHDATE

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 ct_plus_ids_view,
                 table_namer=None):
        """
        :param ct_plus_ids_view: view in pipeline_tables mapping
            controlled_tier_id to controlled_tier_plus_id, the one
            regenerate_ct_plus_ids.py reads
        """
        desc = ('Convert the captured dates of birth from CT research ids to '
                'CT+ research ids.')
        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_PLUS_DEID],
            affected_tables=[],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[PruneCtPlusBirthdate],
            table_namer=table_namer)

        self.ct_plus_ids_view = ct_plus_ids_view


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument('--rdr_sandbox_id',
                            dest='rdr_sandbox_id',
                            action='store',
                            help='RDR sandbox dataset id containing aian_list',
                            required=True)
    ext_parser.add_argument(
        '--under18_lookup_dataset_id',
        dest='under18_lookup_dataset_id',
        action='store',
        help=('Dataset holding the _under18_participants lookup, which is the '
              'RDR stage sandbox dataset.'),
        required=True)
    ext_parser.add_argument(
        '--ct_plus_ids_view',
        dest='ct_plus_ids_view',
        action='store',
        help='View in pipeline_tables mapping CT ids to CT+ ids',
        required=True)
    ARGS = ext_parser.parse_args()

    RULES = [(CaptureCtPlusBirthdate,), (PruneCtPlusBirthdate,),
             (ConvertCtPlusBirthdateIds,)]
    KWARGS = {
        'rdr_sandbox_id': ARGS.rdr_sandbox_id,
        'under18_lookup_dataset_id': ARGS.under18_lookup_dataset_id,
        'ct_plus_ids_view': ARGS.ct_plus_ids_view
    }

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id, RULES,
                                                 **KWARGS)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, RULES, **KWARGS)
