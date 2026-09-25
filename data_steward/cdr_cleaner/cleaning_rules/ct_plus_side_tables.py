"""
Shared pieces of the CT+ linked-dataset side tables (Zip5, date of birth).

Each side table is built by three rules in the CT+ deid stage:

1. A capture, right after RtCtPIDtoRID, copies the values before the CT rule
   that destroys them. AIAN and pediatric participants get no row: pediatric
   access is granted separately from each linked dataset.
2. A prune, after DropOrphanedPIDS, drops participants the rules in between
   removed from person.
3. A conversion, right after the prune, re-keys person_id from the CT research
   id to the CT+ research id. regenerate_ct_plus_ids.py re-keys the CDM tables
   after the pipeline but never reaches the sandbox.

The prune compares against a CT-keyed person table, so it must run before the
conversion. Run after it, it would match nobody and empty the table.
"""

# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import (AIAN_LIST, DEID_MAP, JINJA_ENV, PERSON, PIPELINE_TABLES,
                    UNDER18_PARTICIPANTS_LOOKUP_TABLE)
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

CT_PERSON_ID_COLUMN = 'controlled_tier_id'
CT_PLUS_PERSON_ID_COLUMN = 'controlled_tier_plus_id'

# aian_list and _under18_participants are keyed by participant id, so both go
# through _deid_map, the copy of primary_pid_rid_mapping RtCtPIDtoRID re-keyed
# with.
EXCLUDED_PARTICIPANTS = JINJA_ENV.from_string("""
{% for excluded_table in excluded_tables %}
AND NOT EXISTS (
    SELECT 1
    FROM `{{project_id}}.{{excluded_table}}` x
    JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{deid_map}}` m
      ON m.person_id = x.person_id
    WHERE m.research_id = {{alias}}.person_id
)
{% endfor %}""")

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


def excluded_participants(project_id, sandbox_dataset_id, rdr_sandbox_id,
                          under18_lookup_dataset_id, alias):
    """
    The AND NOT EXISTS clauses that drop AIAN and pediatric participants.

    :param alias: alias of the captured table, whose person_id holds CT
        research ids
    :return: SQL to append to the capture's WHERE clause
    """
    return EXCLUDED_PARTICIPANTS.render(
        project_id=project_id,
        sandbox_dataset_id=sandbox_dataset_id,
        deid_map=DEID_MAP,
        alias=alias,
        excluded_tables=[
            f'{rdr_sandbox_id}.{AIAN_LIST}',
            f'{under18_lookup_dataset_id}.{UNDER18_PARTICIPANTS_LOOKUP_TABLE}'
        ])


class CtPlusSideTableRule(BaseCleaningRule):
    """
    A rule that writes only a side table in the sandbox, so it sandboxes no
    rows and has nothing to validate. Subclasses set storage_table.
    """
    storage_table = None

    def get_sandbox_tablenames(self):
        return [self.storage_table]

    def setup_rule(self, client):
        pass

    def setup_validation(self, client):
        pass

    def validate_rule(self, client):
        pass


class PruneCtPlusSideTable(CtPlusSideTableRule):
    """
    Drop captured participants no longer in person. Both sides hold CT
    research ids, so this must run before the conversion.
    """

    def get_query_specs(self):
        """
        :return: a list of query dictionaries
        """
        query = PRUNE_QUERY.render(project_id=self.project_id,
                                   dataset_id=self.dataset_id,
                                   sandbox_dataset_id=self.sandbox_dataset_id,
                                   storage_table=self.storage_table,
                                   person=PERSON)
        return [{cdr_consts.QUERY: query}]


class ConvertCtPlusSideTableIds(CtPlusSideTableRule):
    """
    Re-key person_id from the CT research id to the CT+ research id, reading
    the view named by ct_plus_ids_view in pipeline_tables. Subclasses set
    ct_plus_ids_view.

    UPDATE ... FROM leaves an unmatched row on its old value, which here would
    ship a CT research id. setup_rule therefore stops the run if any row does
    not resolve. It runs immediately before this rule's queries.
    """
    ct_plus_ids_view = None

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
            storage_table=self.storage_table,
            ct_plus_ids=self._ct_plus_ids(),
            ct_column=CT_PERSON_ID_COLUMN)
        row = list(client.query(query).result())[0]

        if row.unresolved_rows:
            raise RuntimeError(
                f'{row.unresolved_rows} of {row.total_rows} rows in '
                f'{self.sandbox_dataset_id}.{self.storage_table} have no '
                f'{CT_PLUS_PERSON_ID_COLUMN} in '
                f'{PIPELINE_TABLES}.{self.ct_plus_ids_view}.')

        LOGGER.info(
            f'Converting {row.total_rows} rows in '
            f'{self.sandbox_dataset_id}.{self.storage_table} to CT+ ids.')

    def get_query_specs(self):
        """
        :return: a list of query dictionaries
        """
        query = CONVERT_QUERY.render(project_id=self.project_id,
                                     sandbox_dataset_id=self.sandbox_dataset_id,
                                     storage_table=self.storage_table,
                                     ct_plus_ids=self._ct_plus_ids(),
                                     ct_column=CT_PERSON_ID_COLUMN,
                                     ct_plus_column=CT_PLUS_PERSON_ID_COLUMN)
        return [{cdr_consts.QUERY: query}]
