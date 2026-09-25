"""
Capture five-digit zip codes for the CT+ Zip5 linked dataset.

GeneralizeZipCodes cuts zip codes to three digits in place, so the full value
is gone once it runs. These three rules keep it in a person-level side table in
the stage sandbox, one row per participant:

1. CaptureCtPlusZip5, between RtCtPIDtoRID and GeneralizeZipCodes, copies the
   latest zip per participant. AIAN and pediatric participants get no row:
   Zip5 and pediatric access are granted separately.
2. PruneCtPlusZip5, after DropOrphanedPIDS, drops participants the rules in
   between removed from person.
3. ConvertCtPlusZip5Ids, right after the prune, re-keys person_id from the CT
   research id to the CT+ research id. regenerate_ct_plus_ids.py re-keys the
   CDM tables after the pipeline but never reaches this table.

The prune compares against a CT-keyed person table, so it must run before the
conversion. Run after it, it would match nobody and empty the table.

Original Issues: DL2437
"""

# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import (AIAN_LIST, CT_PLUS_ZIP5, DEID_MAP, JINJA_ENV, OBSERVATION,
                    PERSON, PIPELINE_TABLES, UNDER18_PARTICIPANTS_LOOKUP_TABLE)
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DL2437']

ZIP_CODE_CONCEPT_ID = 1585250

CT_PERSON_ID_COLUMN = 'controlled_tier_id'
CT_PLUS_PERSON_ID_COLUMN = 'controlled_tier_plus_id'

# aian_list and _under18_participants are keyed by participant id, so both go
# through _deid_map, the copy of primary_pid_rid_mapping RtCtPIDtoRID re-keyed
# with. The eligibility regex is the one GeneralizeZipCodes applies.
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
{% for excluded_table in excluded_tables %}
AND NOT EXISTS (
    SELECT 1
    FROM `{{project_id}}.{{excluded_table}}` x
    JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{deid_map}}` m
      ON m.person_id = x.person_id
    WHERE m.research_id = o.person_id
)
{% endfor %}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY o.person_id
    ORDER BY o.observation_datetime DESC, o.observation_id DESC
) = 1
)""")

PRUNE_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{sandbox_dataset_id}}.{{storage_table}}` z
WHERE NOT EXISTS (
    SELECT 1
    FROM `{{project_id}}.{{dataset_id}}.{{person}}` p
    WHERE p.person_id = z.person_id
)""")

# The ids view repeats some participants identically across every column, and
# UPDATE ... FROM fails on a target row that matches two source rows. DISTINCT
# collapses the repeats; a CT id with two different CT+ ids still fails.
CT_PLUS_IDS = JINJA_ENV.from_string("""
SELECT DISTINCT {{ct_column}}, {{ct_plus_column}}
FROM `{{project_id}}.{{pipeline_dataset_id}}.{{ct_plus_ids_view}}`
WHERE {{ct_plus_column}} IS NOT NULL""")

COUNT_ROWS_QUERY = JINJA_ENV.from_string("""
SELECT
    COUNT(*) AS total_rows,
    COUNTIF(v.{{ct_column}} IS NULL) AS unresolved_rows
FROM `{{project_id}}.{{sandbox_dataset_id}}.{{storage_table}}` z
LEFT JOIN ({{ct_plus_ids}}) v
  ON v.{{ct_column}} = z.person_id""")

CONVERT_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{sandbox_dataset_id}}.{{storage_table}}` z
SET z.person_id = v.{{ct_plus_column}}
FROM ({{ct_plus_ids}}) v
WHERE z.person_id = v.{{ct_column}}""")


class _CtPlusZip5Rule(BaseCleaningRule):
    """
    Shared shape of the three rules: they write only the side table, which
    lives in the sandbox, so none sandboxes rows or has anything to validate.
    """

    def get_sandbox_tablenames(self):
        return [CT_PLUS_ZIP5]

    def setup_rule(self, client):
        pass

    def setup_validation(self, client):
        pass

    def validate_rule(self, client):
        pass


class CaptureCtPlusZip5(_CtPlusZip5Rule):
    """
    Copy each eligible participant's latest five-digit zip into the side table,
    before GeneralizeZipCodes cuts it to three digits.
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
        query = CAPTURE_QUERY.render(project_id=self.project_id,
                                     dataset_id=self.dataset_id,
                                     sandbox_dataset_id=self.sandbox_dataset_id,
                                     storage_table=CT_PLUS_ZIP5,
                                     observation=OBSERVATION,
                                     zip_code_concept_id=ZIP_CODE_CONCEPT_ID,
                                     deid_map=DEID_MAP,
                                     excluded_tables=[
                                         f'{self.rdr_sandbox_id}.{AIAN_LIST}',
                                         f'{self.under18_lookup_dataset_id}.'
                                         f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}'
                                     ])
        return [{cdr_consts.QUERY: query}]


class PruneCtPlusZip5(_CtPlusZip5Rule):
    """
    Drop captured participants that the rules after the capture removed from
    person. Both sides hold CT research ids, so this must run before the
    conversion.
    """

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

    def get_query_specs(self):
        """
        :return: a list of query dictionaries
        """
        query = PRUNE_QUERY.render(project_id=self.project_id,
                                   dataset_id=self.dataset_id,
                                   sandbox_dataset_id=self.sandbox_dataset_id,
                                   storage_table=CT_PLUS_ZIP5,
                                   person=PERSON)
        return [{cdr_consts.QUERY: query}]


class ConvertCtPlusZip5Ids(_CtPlusZip5Rule):
    """
    Re-key the side table's person_id from the CT research id to the CT+
    research id.

    UPDATE ... FROM leaves an unmatched row on its old value, which here would
    ship a CT research id. setup_rule therefore stops the run if any row does
    not resolve.
    """

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

    def _ct_plus_ids(self):
        return CT_PLUS_IDS.render(project_id=self.project_id,
                                  pipeline_dataset_id=PIPELINE_TABLES,
                                  ct_plus_ids_view=self.ct_plus_ids_view,
                                  ct_column=CT_PERSON_ID_COLUMN,
                                  ct_plus_column=CT_PLUS_PERSON_ID_COLUMN)

    def setup_rule(self, client):
        """
        Stop the run if any captured row has no CT+ id, and log the row count
        so an empty table is visible.
        """
        query = COUNT_ROWS_QUERY.render(
            project_id=self.project_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            storage_table=CT_PLUS_ZIP5,
            ct_plus_ids=self._ct_plus_ids(),
            ct_column=CT_PERSON_ID_COLUMN)
        row = list(client.query(query).result())[0]

        if row.unresolved_rows:
            raise RuntimeError(
                f'{row.unresolved_rows} of {row.total_rows} rows in '
                f'{self.sandbox_dataset_id}.{CT_PLUS_ZIP5} have no '
                f'{CT_PLUS_PERSON_ID_COLUMN} in '
                f'{PIPELINE_TABLES}.{self.ct_plus_ids_view}.')

        LOGGER.info(f'Converting {row.total_rows} rows in '
                    f'{self.sandbox_dataset_id}.{CT_PLUS_ZIP5} to CT+ ids.')

    def get_query_specs(self):
        """
        :return: a list of query dictionaries
        """
        query = CONVERT_QUERY.render(project_id=self.project_id,
                                     sandbox_dataset_id=self.sandbox_dataset_id,
                                     storage_table=CT_PLUS_ZIP5,
                                     ct_plus_ids=self._ct_plus_ids(),
                                     ct_column=CT_PERSON_ID_COLUMN,
                                     ct_plus_column=CT_PLUS_PERSON_ID_COLUMN)
        return [{cdr_consts.QUERY: query}]


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
