"""
Capture five-digit zip codes for the CT+ Zip5 linked dataset.

GeneralizeZipCodes cuts zip codes to three digits in place, so the full value
is gone once it runs. CaptureCtPlusZip5 copies the latest zip per participant
into a side table before that, then PruneCtPlusZip5 and ConvertCtPlusZip5Ids
reconcile it with person and re-key it to CT+ ids. ct_plus_side_tables.py
describes the three steps and their ordering constraint.

Original Issues: DL2437
"""

# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import CT_PLUS_ZIP5, JINJA_ENV, OBSERVATION, PERSON
from cdr_cleaner.cleaning_rules.ct_plus_side_tables import (
    ConvertCtPlusSideTableIds, CtPlusSideTableRule, PruneCtPlusSideTable,
    excluded_participants)

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DL2437']

ZIP_CODE_CONCEPT_ID = 1585250

# The eligibility regex is the one GeneralizeZipCodes applies.
CAPTURE_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{storage_table}}` AS (
SELECT
    o.person_id,
    o.observation_source_concept_id,
    o.observation_datetime,
    SUBSTR(o.value_as_string, 1, 5) AS value_as_string
FROM `{{project_id}}.{{dataset_id}}.{{observation}}` o
WHERE o.observation_source_concept_id = {{zip_code_concept_id}}
AND REGEXP_CONTAINS(o.value_as_string, r'^[0-9]{5}')
{{excluded_participants}}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY o.person_id
    ORDER BY o.observation_datetime DESC, o.observation_id DESC
) = 1
)""")


class CaptureCtPlusZip5(CtPlusSideTableRule):
    """
    Copy each eligible participant's latest five-digit zip into the side table,
    before GeneralizeZipCodes cuts it to three digits.
    """
    storage_table = CT_PLUS_ZIP5

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
        desc = ('Capture five-digit zip codes, excluding AIAN and pediatric '
                'participants, before they are generalized.')
        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_PLUS_DEID],
            affected_tables=[OBSERVATION],
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
            observation=OBSERVATION,
            zip_code_concept_id=ZIP_CODE_CONCEPT_ID,
            excluded_participants=excluded_participants(
                self.project_id, self.sandbox_dataset_id, self.rdr_sandbox_id,
                self.under18_lookup_dataset_id, 'o'))
        return [{cdr_consts.QUERY: query}]


class PruneCtPlusZip5(PruneCtPlusSideTable):
    """
    Drop captured zip codes for participants no longer in person.
    """
    storage_table = CT_PLUS_ZIP5

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = ('Drop captured zip codes for participants no longer in the '
                'person table.')
        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_PLUS_DEID],
            affected_tables=[PERSON],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[CaptureCtPlusZip5],
            table_namer=table_namer)


class ConvertCtPlusZip5Ids(ConvertCtPlusSideTableIds):
    """
    Re-key the captured zip codes from CT research ids to CT+ research ids.
    """
    storage_table = CT_PLUS_ZIP5

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
        desc = ('Convert the captured zip codes from CT research ids to CT+ '
                'research ids.')
        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_PLUS_DEID],
            affected_tables=[],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[PruneCtPlusZip5],
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

    RULES = [(CaptureCtPlusZip5,), (PruneCtPlusZip5,), (ConvertCtPlusZip5Ids,)]
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
