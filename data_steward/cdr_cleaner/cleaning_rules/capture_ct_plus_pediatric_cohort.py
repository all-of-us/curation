"""
Capture the CT+ pediatric cohort, so the pediatrics add-on dataset can be split out of
the re-keyed CT+ dataset.

The tier under-18 removal retains the '0-6' band in the CT+ run, so the cohort's
records flow through the run and land inside its single output dataset. By the time the
split runs, that dataset carries CT+ research IDs and no column marks who is pediatric,
so the cohort is recorded here, inside the pipeline, where each ID space is still valid.
Deriving it later from year_of_birth would key a release-scoped separation on
current-date arithmetic rather than on the age-at-consent band the run applied.

Two rules, because the cohort has to cross two ID spaces and the map for the first is
not built until after the capture runs:

CaptureCtPlusPediatricCohort runs immediately after RemoveFlaggedUnder18ParticipantsCtPlus,
while person_id is still the participant ID, and records the retained participants.

ConvertCtPlusPediatricCohortIds runs last and resolves each captured participant to its
controlled_tier_plus_id: through the stage's _deid_map to the controlled tier research
ID, then through the research IDs view. The _deid_map is built by the PID to RID rule's
setup, which runs after the capture, so the capture cannot perform that hop itself.

Original Issues: DL2537
"""

# Python imports
import logging

# Project imports
import common
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.deid.remove_flagged_under18_participants import \
    CT_PLUS_AGE_BAND_TO_RETAIN
from cdr_cleaner.clean_cdr_utils import get_tables_in_dataset

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DL2537']

# Joined to person so the cohort is who the run retained, not everyone the lookup
# flagged: a flagged participant with no person row has no records to split.
CAPTURE_COHORT = common.JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{cohort_table}}` AS (
  SELECT DISTINCT
    u.person_id AS participant_id,
    u.age_band
  FROM `{{project}}.{{lookup_dataset}}.{{lookup_table}}` u
  JOIN `{{project}}.{{dataset}}.person` p
    USING (person_id)
  WHERE u.age_band = '{{age_band}}'
)
""")

# The two hops, shared by the resolution check and the conversion so the check is
# about exactly the rows the conversion writes. DISTINCT on the view side because it
# has been observed carrying byte identical duplicate rows.
RESOLVED_COHORT = """
  SELECT
    c.participant_id,
    c.age_band,
    d.research_id AS controlled_tier_id,
    v.controlled_tier_plus_id
  FROM `{{project}}.{{sandbox_dataset}}.{{cohort_table}}` c
  LEFT JOIN `{{project}}.{{sandbox_dataset}}.{{deid_map}}` d
    ON c.participant_id = d.person_id
  LEFT JOIN (
    SELECT DISTINCT controlled_tier_id, controlled_tier_plus_id
    FROM `{{project}}.{{ids_dataset}}.{{ids_view}}`
    WHERE controlled_tier_id IS NOT NULL
      AND controlled_tier_plus_id IS NOT NULL
  ) v
    ON d.research_id = v.controlled_tier_id
"""

CHECK_RESOLUTION = common.JINJA_ENV.from_string("""
WITH resolved AS (""" + RESOLVED_COHORT + """)
SELECT
  COUNT(DISTINCT participant_id) AS cohort_count,
  COUNT(DISTINCT IF(controlled_tier_plus_id IS NULL, participant_id, NULL))
    AS unresolved_count,
  COUNT(*) - COUNT(DISTINCT participant_id) AS ambiguous_count,
  ARRAY_AGG(IF(controlled_tier_plus_id IS NULL, participant_id, NULL)
            IGNORE NULLS ORDER BY participant_id LIMIT 5) AS unresolved_examples
FROM resolved
""")

# Rebuilt from participant_id rather than updated in place, so a rerun converts from
# the captured value again instead of trying to convert an ID it already converted.
CONVERT_COHORT = common.JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{cohort_table}}` AS (
  SELECT
    participant_id,
    age_band,
    controlled_tier_id,
    controlled_tier_plus_id AS person_id
  FROM (""" + RESOLVED_COHORT + """)
)
""")


class CaptureCtPlusPediatricCohort(BaseCleaningRule):
    """
    Record the participants the CT+ run retains in the '0-6' band.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 under18_lookup_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!

        :param under18_lookup_dataset_id: dataset holding the _under18_participants
            lookup, the RDR stage sandbox
        """
        desc = (
            'Record the participants the CT+ run retains in the pediatric age '
            'band, so the pediatrics add-on dataset can be split out after the '
            're-key.')

        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_PLUS_DEID],
            affected_tables=[],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            table_namer=table_namer)

        self.under18_lookup_dataset_id = under18_lookup_dataset_id

    def get_query_specs(self):
        """
        :return: a list of query dicts
        """
        # The band comes from the removal rule's own constant, so the retained band and
        # the captured band cannot differ.
        return [{
            cdr_consts.QUERY:
                CAPTURE_COHORT.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    cohort_table=common.CT_PLUS_PEDIATRIC_COHORT,
                    lookup_dataset=self.under18_lookup_dataset_id,
                    lookup_table=common.UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                    age_band=CT_PLUS_AGE_BAND_TO_RETAIN)
        }]

    def setup_rule(self, client):
        """
        Fail if the lookup is missing, so a mis-pointed dataset cannot turn the capture
        into an empty cohort and the split into a silent no-op.
        """
        lookup_tables = get_tables_in_dataset(
            client, self.project_id, self.under18_lookup_dataset_id,
            [common.UNDER18_PARTICIPANTS_LOOKUP_TABLE])
        if common.UNDER18_PARTICIPANTS_LOOKUP_TABLE not in lookup_tables:
            raise RuntimeError(
                f'{common.UNDER18_PARTICIPANTS_LOOKUP_TABLE} not found in '
                f'{self.project_id}.{self.under18_lookup_dataset_id}. '
                f'under18_lookup_dataset_id must name the RDR stage sandbox.')

    def setup_validation(self, client):
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [common.CT_PLUS_PEDIATRIC_COHORT]


class ConvertCtPlusPediatricCohortIds(BaseCleaningRule):
    """
    Resolve the captured cohort to controlled_tier_plus_id, failing the run on any
    participant that does not resolve.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None,
                 ids_dataset_id=common.PIPELINE_TABLES,
                 ids_view_id=common.RDR_PARTICIPANT_RESEARCH_IDS_VIEW):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!

        :param ids_dataset_id: dataset holding the research IDs view
        :param ids_view_id: view mapping controlled_tier_id to controlled_tier_plus_id
        """
        desc = (
            'Convert the captured CT+ pediatric cohort to CT+ research IDs, the '
            'ID space the re-keyed dataset the split reads is in.')

        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_PLUS_DEID],
            affected_tables=[],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[CaptureCtPlusPediatricCohort],
            table_namer=table_namer)

        self.ids_dataset_id = ids_dataset_id
        self.ids_view_id = ids_view_id

    def _render_params(self):
        return dict(project=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    cohort_table=common.CT_PLUS_PEDIATRIC_COHORT,
                    deid_map=common.DEID_MAP,
                    ids_dataset=self.ids_dataset_id,
                    ids_view=self.ids_view_id)

    def get_query_specs(self):
        """
        :return: a list of query dicts
        """
        return [{
            cdr_consts.QUERY: CONVERT_COHORT.render(**self._render_params())
        }]

    def setup_rule(self, client):
        """
        Stop the run unless every captured participant resolves to exactly one CT+ ID.

        A participant that misses keeps no CT+ ID, so the split would leave their
        records in the mainline. One that resolves twice would be split under two
        IDs. Either way the release is wrong with nothing erroring, so both fail here,
        before the table is rewritten.

        :raises RuntimeError: if any captured participant is unresolved or ambiguous
        """
        query = CHECK_RESOLUTION.render(**self._render_params())
        row = list(client.query(query).result())[0]
        cohort_count = row['cohort_count']
        unresolved_count = row['unresolved_count']
        ambiguous_count = row['ambiguous_count']

        if unresolved_count or ambiguous_count:
            raise RuntimeError(
                f'{common.CT_PLUS_PEDIATRIC_COHORT} cannot be converted: of '
                f'{cohort_count} captured participants, {unresolved_count} resolve to '
                f'no CT+ research ID, for example {row["unresolved_examples"]}, and '
                f'{ambiguous_count} extra rows come from participants resolving to '
                f'more than one. A missing participant is absent from '
                f'{common.DEID_MAP} or from {self.ids_dataset_id}.{self.ids_view_id}.'
            )

        LOGGER.info(
            f'{cohort_count} captured pediatric participants all resolve '
            f'to a CT+ research ID.')

    def setup_validation(self, client):
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [common.CT_PLUS_PEDIATRIC_COHORT]


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
    ARGS = ext_parser.parse_args()

    rules = [(CaptureCtPlusPediatricCohort,),
             (ConvertCtPlusPediatricCohortIds,)]

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            rules,
            under18_lookup_dataset_id=ARGS.under18_lookup_dataset_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            rules,
            under18_lookup_dataset_id=ARGS.under18_lookup_dataset_id)
