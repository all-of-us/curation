"""
Regenerate Primary Key IDs for a Controlled Tier Plus (CT+) Dataset.

CT+ is built from an existing Controlled Tier (CT) dataset. Every identifier a CT+
researcher sees has to be regenerated so CT+ rows cannot be aligned back to CT rows.
This script is the CT+ counterpart of regenerate_rt_ids.py and differs from it in
four ways:

1) The person mapping reads controlled_tier_id -> controlled_tier_plus_id from
   pipeline_tables.rdr_participant_research_ids_view. The input dataset is CT, so its
   person_id is already controlled_tier_id; joining on research_id would miss every
   row and, because all person joins are LEFT JOINs, would silently produce a full
   dataset with person_id NULL throughout.

2) Row IDs are drawn as a random permutation instead of ROW_NUMBER() ordered by the
   source ID. An order preserving map would let a holder of both tiers align rows by
   sorting. The random value is materialized in an inner subquery so no
   nondeterministic function is evaluated inside a window ORDER BY. The mapping table
   is persisted, so the draw does not need to be reproducible and no salt or long
   lived secret is introduced. Mappings are appended to across releases rather than
   re-cut, so a row published once keeps its CT+ id for good. New ids sit above the
   running maximum, which orders releases relative to each other, but the releases
   ship as separate datasets so that boundary is already public.

3) survey_conduct gets its provider_id, visit_occurrence_id and
   response_visit_occurrence_id foreign keys remapped. regenerate_rt_ids.py returns
   from its survey_conduct branch before reaching the generic foreign key loop and
   passes those three columns through unchanged.

4) fact_relationship is remapped rather than copied. It has no primary key and no
   person_id, so regenerate_rt_ids.py lands it in the copy as is path and leaves
   fact_id_1 and fact_id_2 pointing at CT values that dangle against every re-keyed
   CT+ row. See DOMAIN_CONCEPT_ID_TO_TABLE below.

The script otherwise behaves like regenerate_rt_ids.py:

a) Creates empty output tables with proper schema, clustering, etc.

b) For all tables (INCLUDING person and survey_conduct, EXCEPT death) that have
   numeric primary key columns, creates mapping tables used to assign new IDs in
   output.

   Special handling for aou_death:
   - aou_death_id (GUID) is preserved (not regenerated), matching CT and RT
   - person_id foreign key is updated with new person_id from mapping

   The structure of a mapping table follows this convention:

   _mapping_<cdm_table> (
     src_table_id:       STRING, -- identifies input table whose ids are to be mapped
     src_<cdm_table>_id: INTEGER, -- original value of unique identifier in source table
     <cdm_table>_id:     INTEGER  -- new unique identifier which will be used in output
   )

c) For all tables, composes a query to fetch records from the input dataset and save
   the results in output with new IDs where applicable.

## Notes
Required Environment variables:
 * GOOGLE_APPLICATION_CREDENTIALS: path to service account key json file
 * APPLICATION_ID: GCP project ID
 * BIGQUERY_DATASET_ID: Input/output dataset_id

The mapping dataset must be empty or produced by a previous CT+ run. Pointing this
script at a mapping dataset an RT run produced would otherwise reuse that run's
_mapping_person and hand CT+ the RT person_ids; the script fails fast instead. Point
every release at the same mapping dataset, or ids will not persist between them.

When to run: after the deid_clean stage, as the last step before the dataset is
published, never between deid and deid_base. Two reasons. CreateDerivedTables runs in
both CONTROLLED_TIER_DEID_BASE_CLEANING_CLASSES and
CONTROLLED_TIER_DEID_CLEAN_CLEANING_CLASSES and mints observation_period_id,
drug_era_id and condition_era_id from scratch, and MoveNLPtoDomains mints
condition_occurrence_id off a running maximum at every stage, so ids assigned earlier
are overwritten or joined by un-re-keyed ones. The first CT+ release also re-keys a
copy of CT deid_clean, which fixes the mapping key space at that stage; a later
release re-keying at any other stage would not match it.

Usage:
  python regenerate_ct_plus_ids.py \
    --project_id <project> \
    --input_dataset_id <controlled_tier_dataset> \
    --output_dataset_id <controlled_tier_plus_dataset> \
    --pipeline_dataset_id <pipeline_tables_dataset> \
    --ct_plus_ids_view <rdr_participant_research_ids_view> \
    --mapping_dataset_id <mapping_tables_dataset> \
    --mapping_namespace <optional, defaults to DEFAULT_MAPPING_NAMESPACE>
"""
import argparse
import logging
from google.cloud.bigquery import Table
import bq_utils

from common import (AOU_DEATH, DEATH, MAPPING_PREFIX, PERSON, SURVEY_CONDUCT,
                    VISIT_DETAIL, VISIT_OCCURRENCE, CARE_SITE, LOCATION, NOTE,
                    NOTE_NLP, FITBIT_TABLES, VOCABULARY_TABLES, ACHILLES_TABLES,
                    ACHILLES_HEEL_TABLES, CONDITION_OCCURRENCE, DRUG_EXPOSURE,
                    FACT_RELATIONSHIP, MEASUREMENT, OBSERVATION,
                    PROCEDURE_OCCURRENCE, PROVIDER,
                    MEASUREMENT_DOMAIN_CONCEPT_ID,
                    OBSERVATION_DOMAIN_CONCEPT_ID)

from gcloud.bq import BigQueryClient
from resources import fields_for, has_primary_key, CDM_TABLES
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

# Tables that should not have IDs regenerated
SKIP_ID_REGEN_TABLES = [DEATH]

# Column in the RDR research ids view that the CT input's person_id already carries
CT_PERSON_ID_COLUMN = 'controlled_tier_id'

# Column in the RDR research ids view holding the new CT+ person_id
CT_PLUS_PERSON_ID_COLUMN = 'controlled_tier_plus_id'

# Marks a _mapping_person built by this script, so a mapping dataset produced by an
# RT run is not silently reused. See assert_person_mapping_is_ct_plus().
CT_PLUS_PERSON_MAPPING_SUFFIX = 'ct_plus'

# Namespace stamped into src_table_id on every mapping row. It has to be a constant
# rather than the input dataset name: both the append filter in mapping_query() and
# the read side join in table_query() match on this literal, so a value derived from
# the input dataset would stop matching the moment a later release runs against a
# differently named dataset. Every source id would then be assigned a second, higher
# id and the load would pick that one, silently breaking ID persistence across
# releases with no error. Do not override this without changing both call sites.
DEFAULT_MAPPING_NAMESPACE = 'ct_plus'

# fact_relationship stores a domain concept id beside each fact id rather than a
# foreign key column, so the remap has to switch on it. The domains below are the
# ones observed in the CT input; anything else is left for the unmapped domain
# handling in fact_relationship_query(). Concept ids resolve as:
#   10 Procedure, 13 Drug, 19 Condition, 21 Measurement, 27 Observation, 57 Care site
DOMAIN_CONCEPT_ID_TO_TABLE = {
    10: PROCEDURE_OCCURRENCE,
    13: DRUG_EXPOSURE,
    19: CONDITION_OCCURRENCE,
    MEASUREMENT_DOMAIN_CONCEPT_ID: MEASUREMENT,
    OBSERVATION_DOMAIN_CONCEPT_ID: OBSERVATION,
    57: CARE_SITE,
}

# Extension tables that need ID updates
EXT_TABLES = [
    'person_ext', 'visit_occurrence_ext', 'condition_occurrence_ext',
    'device_exposure_ext', 'drug_exposure_ext', 'measurement_ext', 'note_ext',
    'note_nlp_ext', 'observation_ext', 'observation_period_ext',
    'procedure_occurrence_ext', 'survey_conduct_ext', 'condition_era_ext',
    'drug_era_ext'
]


def output_table_for(table_id):
    """
    Get the name of the output table. For CT+ regeneration, it's the same as input.

    :param table_id: name of a CDM table
    :return: name of the table in output dataset
    """
    return table_id


def mapping_table_for(domain_table):
    """
    Get the name of the mapping table for a given domain table.

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :return: mapping table name
    """
    return f'{MAPPING_PREFIX}{domain_table}'


def person_mapping_src_table_id(pipeline_dataset_id, ct_plus_ids_view):
    """
    Get the src_table_id value stamped on rows of the CT+ person mapping.

    The suffix distinguishes a CT+ person mapping from the RT one, which is written to
    the same table name from the same view. Without it, a mapping dataset shared with
    an RT run would silently hand CT+ the RT person_ids.

    :param pipeline_dataset_id: identifies the pipeline_tables dataset
    :param ct_plus_ids_view: view containing person_id to CT+ id mapping
    :return: the src_table_id literal
    """
    return f'{pipeline_dataset_id}.{ct_plus_ids_view}.{CT_PLUS_PERSON_MAPPING_SUFFIX}'


def mapping_query(table_name,
                  input_dataset_id,
                  project_id,
                  pipeline_dataset_id,
                  ct_plus_ids_view,
                  mapping_namespace,
                  mapping_dataset_id,
                  append=False):
    """
    Get the query used to generate new IDs for a CDM table.

    Row IDs are a random permutation of the row set rather than a rank preserving
    sequence, so CT+ IDs cannot be aligned to CT IDs by sorting. RAND() is
    materialized in the inner subquery because BigQuery does not accept a
    nondeterministic expression directly in a window ORDER BY.

    When appending, ids already mapped by an earlier release keep their value and only
    unmapped source ids are drawn, so a row published in one release carries the same
    CT+ id in every later one. New ids sit above the running maximum, which does order
    releases relative to each other, but the releases ship as separate datasets so that
    boundary is already public.

    :param table_name: name of CDM table
    :param input_dataset_id: identifies the BQ dataset containing the input table
    :param project_id: identifies the GCP project containing the dataset
    :param pipeline_dataset_id: identifies the pipeline_tables dataset (for person mapping)
    :param ct_plus_ids_view: view containing person_id to CT+ id mapping
    :param mapping_namespace: constant stamped into src_table_id, see DEFAULT_MAPPING_NAMESPACE
    :param mapping_dataset_id: identifies the dataset where mapping tables are stored
    :param append: if True, preserve existing mappings and only draw ids for new rows
    :return: the query
    """
    # Special handling for person table - use existing mapping from pipeline_tables.
    # The input dataset is CT, so its person_id is already controlled_tier_id.
    if table_name == PERSON and pipeline_dataset_id:
        src_table_id = person_mapping_src_table_id(pipeline_dataset_id,
                                                   ct_plus_ids_view)
        # DISTINCT because the ids view has been observed carrying byte identical
        # duplicate participant rows. Every person join here is a LEFT JOIN, so a
        # second row for one participant does not fail, it doubles that participant's
        # rows in every loaded table. DISTINCT only collapses duplicates that agree on
        # both ids; a participant carrying two different CT+ ids still survives it, and
        # that case is what assert_person_mapping_is_one_to_one() catches.
        return f'''
        SELECT DISTINCT
            '{src_table_id}' AS src_table_id,
            {CT_PERSON_ID_COLUMN} AS src_person_id,
            {CT_PLUS_PERSON_ID_COLUMN} AS person_id
        FROM `{project_id}.{pipeline_dataset_id}.{ct_plus_ids_view}`
        WHERE {CT_PERSON_ID_COLUMN} IS NOT NULL
          AND {CT_PLUS_PERSON_ID_COLUMN} IS NOT NULL
        '''

    mapping_table = mapping_table_for(table_name)
    id_field = f'{table_name}_id'

    # When appending, start above every id already handed out and skip source ids that
    # already have one, so previously published rows keep their CT+ id.
    if append:
        offset_expr = (
            f'(SELECT COALESCE(MAX({id_field}), 0) '
            f'FROM `{project_id}.{mapping_dataset_id}.{mapping_table}`)')
        where_clause = f"""
    WHERE NOT EXISTS (
        SELECT 1
        FROM `{project_id}.{mapping_dataset_id}.{mapping_table}` mt
        WHERE mt.src_{id_field} = t.src_{id_field} AND
              mt.src_table_id = '{mapping_namespace}.{table_name}'
    )"""
    else:
        offset_expr = '0'
        where_clause = ''

    # For all other tables, draw a random permutation.
    return f'''
    SELECT
        '{mapping_namespace}.{table_name}' AS src_table_id,
        src_{id_field},
        {offset_expr} + ROW_NUMBER() OVER (ORDER BY shuffle_key) AS {id_field}
    FROM (
        -- RAND() is drawn outside the DISTINCT: pulling it into the same SELECT
        -- would make every duplicate of {id_field} a distinct row and survive it
        SELECT src_{id_field}, RAND() AS shuffle_key
        FROM (
            SELECT DISTINCT {id_field} AS src_{id_field}
            FROM `{project_id}.{input_dataset_id}.{table_name}`
        )
    ) t
    {where_clause}
    '''


def assert_person_mapping_is_ct_plus(client, project_id, mapping_dataset_id,
                                     pipeline_dataset_id, ct_plus_ids_view):
    """
    Stop the run if an existing _mapping_person was not produced by a CT+ run.

    regenerate_rt_ids.py writes _mapping_person under the same name into whatever
    mapping dataset it is given. Reusing one of those would give CT+ the RT person_ids
    with no error, so the src_table_id stamp is checked before anything is written.

    :param client: BigQueryClient
    :param project_id: identifies the GCP project
    :param mapping_dataset_id: identifies the dataset where mapping tables are stored
    :param pipeline_dataset_id: identifies the pipeline_tables dataset
    :param ct_plus_ids_view: view containing person_id to CT+ id mapping
    :raises RuntimeError: if the existing mapping carries a different src_table_id
    """
    mapping_person = mapping_table_for(PERSON)
    if not client.table_exists(mapping_person, mapping_dataset_id):
        return

    expected = person_mapping_src_table_id(pipeline_dataset_id,
                                           ct_plus_ids_view)
    q = f'''
    SELECT DISTINCT src_table_id
    FROM `{project_id}.{mapping_dataset_id}.{mapping_person}`
    '''
    found = {row['src_table_id'] for row in client.query(q).result()}
    unexpected = found - {expected}
    if unexpected:
        raise RuntimeError(
            f'{mapping_dataset_id}.{mapping_person} carries src_table_id '
            f'{sorted(unexpected)}, which was not produced by a CT+ run. '
            f'Expected {expected}. Point --mapping_dataset_id at an empty dataset '
            f'or one from a previous CT+ run.')


def assert_person_mapping_is_one_to_one(client, project_id, mapping_dataset_id):
    """
    Stop the run if _mapping_person is not one row per participant, both ways.

    This is checked after the mapping is built and before any table is loaded, because
    neither direction fails on its own. Every person join is a LEFT JOIN keyed on
    src_person_id, so one participant holding two CT+ ids silently doubles that
    participant's rows in every loaded table. One CT+ id held by two participants is
    worse and just as quiet: it merges two people into one in the released data.

    The DISTINCT in mapping_query() already absorbs whole row duplicates from the ids
    view, which have been observed there and are harmless. What is left for this check
    is the disagreeing case, which is a genuine upstream defect and must not be
    absorbed.

    :param client: BigQueryClient
    :param project_id: identifies the GCP project
    :param mapping_dataset_id: identifies the dataset where mapping tables are stored
    :raises RuntimeError: if any participant or any CT+ id appears more than once
    """
    mapping_person = mapping_table_for(PERSON)
    if not client.table_exists(mapping_person, mapping_dataset_id):
        return

    q = f'''
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT src_person_id) AS src_count,
        COUNT(DISTINCT person_id) AS ct_plus_count
    FROM `{project_id}.{mapping_dataset_id}.{mapping_person}`
    '''
    row = list(client.query(q).result())[0]
    row_count, src_count, ct_plus_count = (row['row_count'], row['src_count'],
                                           row['ct_plus_count'])
    if row_count == src_count == ct_plus_count:
        return

    raise RuntimeError(
        f'{mapping_dataset_id}.{mapping_person} is not one to one: {row_count} rows '
        f'over {src_count} participants and {ct_plus_count} CT+ ids. '
        f'{"Some participant carries more than one CT+ id. " if src_count < row_count else ""}'
        f'{"Some CT+ id is shared by more than one participant. " if ct_plus_count < row_count else ""}'
        f'Loading against this mapping would duplicate or merge participants across '
        f'every table. Fix the ids view before rerunning.')


def assert_output_dataset_is_safe(client, output_dataset_id, allow_replace):
    """
    Stop the run if the output dataset already holds tables.

    main() deletes and recreates every CDM table in the output dataset before loading
    anything, so a mistyped or stale --output_dataset_id destroys whatever is there
    with no prompt and no way back: the delete is followed immediately by a create
    under the same name, which can put the prior version out of reach of time travel.
    Nothing else in this script is destructive, so this one check covers the whole
    blast radius.

    An empty or absent dataset passes. A non-empty one requires --allow_replace, which
    exists so re-running a release is possible but has to be typed deliberately.

    :param client: BigQueryClient
    :param output_dataset_id: identifies the dataset the run would write to
    :param allow_replace: True if the caller has accepted that existing tables go away
    :raises RuntimeError: if the dataset holds tables and allow_replace is False
    """
    if allow_replace:
        LOGGER.info(
            f'--allow_replace given, existing tables in {output_dataset_id} '
            f'will be replaced.')
        return

    try:
        existing = [
            table.table_id for table in client.list_tables(output_dataset_id)
        ]
    except Exception:
        # No dataset yet. main() creates it further down.
        return

    if existing:
        raise RuntimeError(
            f'{output_dataset_id} already holds {len(existing)} tables, for example '
            f'{sorted(existing)[:5]}. This run would delete every CDM table in it '
            f'before loading. Point --output_dataset_id at a new or empty dataset, '
            f'or pass --allow_replace if replacing them is intended.')


def mapping(domain_table, input_dataset_id, output_dataset_id, project_id,
            pipeline_dataset_id, ct_plus_ids_view, mapping_namespace,
            mapping_dataset_id):
    """
    Create or extend the table that assigns new ids to records.

    Mappings persist across releases. An existing mapping table is appended to rather
    than truncated, so a row published in one release keeps its CT+ id in every later
    one. Only source ids with no mapping yet are drawn.

    person is the exception and is always rebuilt from the ids view, which is the source
    of truth for controlled_tier_plus_id and is stable there. Skipping the rebuild would
    leave participants who first appear in a later release with no mapping at all, and
    because every person join is a LEFT JOIN their rows would load with person_id NULL
    instead of failing.

    :param domain_table: name of the CDM table
    :param input_dataset_id: identifies dataset with source data
    :param output_dataset_id: NOT USED. Kept for legacy compatibility.
    :param project_id: identifies GCP project that contains the datasets
    :param pipeline_dataset_id: identifies pipeline_tables dataset (for person mapping)
    :param ct_plus_ids_view: view containing person_id to CT+ id mapping
    :param mapping_namespace: constant stamped into src_table_id, see DEFAULT_MAPPING_NAMESPACE
    :param mapping_dataset_id: identifies the dataset where mapping tables are stored
    :return:
    """
    mapping_namespace = mapping_namespace if mapping_namespace else DEFAULT_MAPPING_NAMESPACE
    mapping_table = mapping_table_for(domain_table)
    client = BigQueryClient(project_id)

    append = (domain_table != PERSON and
              client.table_exists(mapping_table, mapping_dataset_id))
    if append:
        LOGGER.info(f"Appending new '{domain_table}' ids to {mapping_table}. "
                    f"Existing mappings are preserved.")
        write_disposition = 'WRITE_APPEND'
    else:
        LOGGER.info(f"Creating mapping table {mapping_table}...")
        write_disposition = 'WRITE_TRUNCATE'

    q = mapping_query(domain_table, input_dataset_id, project_id,
                      pipeline_dataset_id, ct_plus_ids_view, mapping_namespace,
                      mapping_dataset_id, append)

    LOGGER.info(f'Query for {mapping_table} is {q}')
    bq_utils.query(q,
                   destination_dataset_id=mapping_dataset_id,
                   destination_table_id=mapping_table,
                   write_disposition=write_disposition)


def fact_relationship_query(input_dataset_id, project_id, mapping_dataset_id,
                            mapping_namespace):
    """
    Returns a query that resolves fact_relationship fact ids to their new CT+ values.

    fact_relationship has no primary key and no person_id, so it would otherwise be
    copied unchanged and its fact ids would dangle against the re-keyed CT+ rows. The
    table names no foreign key column: each fact id is qualified by the domain concept
    id beside it, so each side needs one join per domain in DOMAIN_CONCEPT_ID_TO_TABLE.

    Rows whose domain concept id is not in that map, or whose fact id has no mapping
    row, resolve to NULL and are dropped. This matches how the combined build loads the
    table (see constants/tools/create_combined_backup_dataset.py FACT_RELATIONSHIP_QUERY,
    which applies the same filter after remapping measurement and observation).

    :param input_dataset_id: identifies dataset containing source data
    :param project_id: identifies the GCP project
    :param mapping_dataset_id: identifies the dataset where mapping tables are stored
    :param mapping_namespace: original dataset used to create the mappings
    :return: the query
    """
    case_exprs = {}
    join_exprs = []

    for side in ('1', '2'):
        whens = []
        for index, (domain_concept_id, domain_table) in enumerate(
                DOMAIN_CONCEPT_ID_TO_TABLE.items()):
            alias = f'm{index}_{side}'
            mapping_tbl = mapping_table_for(domain_table)
            src_field = f'src_{domain_table}_id'
            id_field = f'{domain_table}_id'
            whens.append(
                f'''WHEN fr.domain_concept_id_{side} = {domain_concept_id}
              THEN {alias}.{id_field}''')
            join_exprs.append(f'''
    LEFT JOIN `{project_id}.{mapping_dataset_id}.{mapping_tbl}` AS {alias}
      ON fr.fact_id_{side} = {alias}.{src_field}
     AND fr.domain_concept_id_{side} = {domain_concept_id}
     AND {alias}.src_table_id = '{mapping_namespace}.{domain_table}' ''')
        case_exprs[side] = '\n            '.join(whens)

    joins = ''.join(join_exprs)

    # The NULL filter sits outside so it can reference the resolved fact ids
    return f'''
    SELECT *
    FROM (
        SELECT
            fr.domain_concept_id_1,
            CASE
            {case_exprs['1']}
            END AS fact_id_1,
            fr.domain_concept_id_2,
            CASE
            {case_exprs['2']}
            END AS fact_id_2,
            fr.relationship_concept_id
        FROM `{project_id}.{input_dataset_id}.{FACT_RELATIONSHIP}` AS fr
        {joins}
    )
    WHERE fact_id_1 IS NOT NULL
      AND fact_id_2 IS NOT NULL
    '''


def table_query(table_name, input_dataset_id, output_dataset_id, project_id,
                pipeline_dataset_id, ct_plus_ids_view, mapping_dataset_id,
                mapping_namespace):
    """
    Returns a query to retrieve all records from an input table with new IDs.

    This function constructs a query to select data from the source table and join it
    with the necessary mapping tables to replace primary and foreign keys with newly
    generated IDs. It handles special cases for tables like aou_death, survey_conduct,
    and tables without primary keys.

    :param table_name: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :param input_dataset_id: identifies dataset containing source data
    :param output_dataset_id: identifies dataset where final results are stored
    :param project_id: identifies the GCP project
    :param pipeline_dataset_id: identifies the pipeline_tables dataset (for person mapping)
    :param ct_plus_ids_view: view containing person_id to CT+ id mapping
    :param mapping_dataset_id: identifies the dataset where mapping tables are stored
    :param mapping_namespace: original dataset used to create the mappings

    :return: the query
    """
    mapping_namespace = mapping_namespace if mapping_namespace else DEFAULT_MAPPING_NAMESPACE
    person_src_table_id = person_mapping_src_table_id(pipeline_dataset_id,
                                                      ct_plus_ids_view)

    def _mapping_join(join_table, join_alias, on_field, src_table_alias='t'):
        """Helper to create a LEFT JOIN expression for a mapping table."""
        mapping_tbl = mapping_table_for(join_table)
        src_field = f'src_{join_table}_id'
        return f'''
        LEFT JOIN `{project_id}.{mapping_dataset_id}.{mapping_tbl}` {join_alias}
          ON {src_table_alias}.{on_field} = {join_alias}.{src_field}
         AND {join_alias}.src_table_id = '{mapping_namespace}.{join_table}'
        '''

    if table_name == FACT_RELATIONSHIP:
        return fact_relationship_query(input_dataset_id, project_id,
                                       mapping_dataset_id, mapping_namespace)

    # Fitbit tables do not have a domain-specific primary key. They only need person_id updated.
    # This logic is similar to tables without a primary key.
    if table_name in FITBIT_TABLES:
        fields = fields_for(table_name)
        col_exprs = [
            'mp.person_id'
            if field['name'] == 'person_id' else f"t.{field['name']}"
            for field in fields
        ]
        cols = ',\n        '.join(col_exprs)
        mapping_person = mapping_table_for(PERSON)
        return f'''
    SELECT {cols}
    FROM `{project_id}.{input_dataset_id}.{table_name}` t
    LEFT JOIN `{project_id}.{mapping_dataset_id}.{mapping_person}` mp ON t.person_id = mp.src_person_id
    '''

    # Special handling for aou_death - keep aou_death_id but update person_id
    if table_name == AOU_DEATH:
        fields = fields_for(table_name)
        col_exprs = []
        for field in fields:
            field_name = field['name']
            if field_name == 'person_id':
                col_exprs.append('mp.person_id')
            else:
                col_exprs.append(f't.{field_name}')
        cols = ',\n        '.join(col_exprs)
        mapping_person = mapping_table_for(PERSON)
        return f'''
    SELECT {cols}
    FROM `{project_id}.{input_dataset_id}.{table_name}` t
    LEFT JOIN `{project_id}.{mapping_dataset_id}.{mapping_person}` mp
        ON t.person_id = mp.src_person_id AND
           mp.src_table_id = '{person_src_table_id}'
    '''

    # Special handling for survey_conduct - update both survey_conduct_id and
    # survey_source_identifier. This branch returns before the generic foreign key
    # loop below, so the foreign keys survey_conduct carries are remapped here
    # explicitly. regenerate_rt_ids.py leaves them at their source values.
    if table_name == SURVEY_CONDUCT:
        fields = fields_for(table_name)
        col_exprs = []
        for field in fields:
            field_name = field['name']
            if field_name == 'survey_conduct_id':
                col_exprs.append('m.survey_conduct_id')
            elif field_name == 'survey_source_identifier':
                # survey_source_identifier should match survey_conduct_id
                col_exprs.append(
                    'CAST(m.survey_conduct_id AS STRING) AS survey_source_identifier'
                )
            elif field_name == 'person_id':
                col_exprs.append('mp.person_id')
            elif field_name == 'provider_id':
                col_exprs.append('mpr.provider_id')
            elif field_name == 'visit_occurrence_id':
                col_exprs.append('mvo.visit_occurrence_id')
            elif field_name == 'response_visit_occurrence_id':
                col_exprs.append(
                    'rvo.visit_occurrence_id AS response_visit_occurrence_id')
            else:
                col_exprs.append(f't.{field_name}')
        cols = ',\n        '.join(col_exprs)
        mapping_table = mapping_table_for(table_name)
        mapping_person = mapping_table_for(PERSON)
        provider_join_expr = _mapping_join(PROVIDER, 'mpr', 'provider_id')
        visit_occurrence_join_expr = _mapping_join(VISIT_OCCURRENCE, 'mvo',
                                                   'visit_occurrence_id')
        response_visit_join_expr = _mapping_join(
            VISIT_OCCURRENCE, 'rvo', 'response_visit_occurrence_id')
        return f'''
    SELECT
        {cols}
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY nm.survey_conduct_id) AS row_num
        FROM
            `{project_id}.{input_dataset_id}.{table_name}` AS nm) AS t
    JOIN
        `{project_id}.{mapping_dataset_id}.{mapping_table}` AS m
    ON
        t.survey_conduct_id = m.src_survey_conduct_id AND
        m.src_table_id = '{mapping_namespace}.{table_name}'
    LEFT JOIN
        `{project_id}.{mapping_dataset_id}.{mapping_person}` AS mp
    ON
        t.person_id = mp.src_person_id AND
        mp.src_table_id = '{person_src_table_id}'
    {provider_join_expr}
    {visit_occurrence_join_expr}
    {response_visit_join_expr}
    WHERE
        row_num = 1
    '''

    # Tables that don't need ID remapping (death only)
    if table_name in SKIP_ID_REGEN_TABLES:
        fields = fields_for(table_name)
        col_exprs = [field['name'] for field in fields]
        cols = ',\n        '.join(col_exprs)
        return f'''
    SELECT {cols} 
    FROM `{project_id}.{input_dataset_id}.{table_name}`'''

    # Tables without primary keys - copy as-is but update person_id if present
    if not has_primary_key(table_name):
        fields = fields_for(table_name)
        col_exprs = []
        has_person_id = False

        for field in fields:
            field_name = field['name']
            if field_name == 'person_id':
                col_exprs.append('mp.person_id')
                has_person_id = True
            else:
                col_exprs.append(f't.{field_name}')

        cols = ',\n        '.join(col_exprs)

        if has_person_id:
            # Join with person mapping if table has person_id
            mapping_person = mapping_table_for(PERSON)
            return f'''
            SELECT {cols}
            FROM `{project_id}.{input_dataset_id}.{table_name}` t
            LEFT JOIN `{project_id}.{mapping_dataset_id}.{mapping_person}` mp
                ON t.person_id = mp.src_person_id AND
                   mp.src_table_id = '{person_src_table_id}'
            '''
        else:
            # No person_id, just copy as-is. The column expressions are still
            # qualified with t, so the source has to carry that alias.
            return f'''
    SELECT {cols}
    FROM `{project_id}.{input_dataset_id}.{table_name}` t'''

    # Tables that need ID remapping
    mapping_table = mapping_table_for(table_name)
    fields = fields_for(table_name)
    id_col = f'{table_name}_id'
    col_exprs = []

    # Track which foreign keys are present
    has_visit_occurrence_id = False
    has_preceding_visit_occurrence_id = False
    has_visit_detail_id = False
    has_preceding_visit_detail_id = False
    has_visit_detail_parent_id = False
    has_care_site_id = False
    has_location_id = False
    has_note_id = False
    has_questionnaire_response_id = False
    has_provider_id = False

    # Track if table has person_id
    has_person_id = False

    for field in fields:
        field_name = field['name']

        if field_name == id_col:
            # Use mapping for record ID column
            col_expr = f'm.{field_name}'
        elif field_name == 'person_id' and table_name != PERSON:
            # Update person_id foreign key with new person_id
            col_expr = 'mp.person_id'
            has_person_id = True
        elif field_name == 'visit_occurrence_id' and table_name != VISIT_OCCURRENCE:
            col_expr = 'mvo.visit_occurrence_id'
            has_visit_occurrence_id = True
        elif field_name == 'preceding_visit_occurrence_id':
            col_expr = 'pvo.visit_occurrence_id AS preceding_visit_occurrence_id'
            has_preceding_visit_occurrence_id = True
        elif field_name == 'visit_detail_id' and table_name != VISIT_DETAIL:
            col_expr = 'mvd.visit_detail_id'
            has_visit_detail_id = True
        elif field_name == 'preceding_visit_detail_id':
            col_expr = 'pvd.visit_detail_id AS preceding_visit_detail_id'
            has_preceding_visit_detail_id = True
        elif field_name == 'visit_detail_parent_id':
            col_expr = 'ppvd.visit_detail_id AS visit_detail_parent_id'
            has_visit_detail_parent_id = True
        elif field_name == 'care_site_id' and table_name != CARE_SITE:
            col_expr = 'mcs.care_site_id'
            has_care_site_id = True
        elif field_name == 'provider_id' and table_name != PROVIDER:
            # provider is re-keyed in its own table, so every reference to it has to
            # follow. regenerate_rt_ids.py leaves these at their source values, which
            # would point CT+ rows at the wrong provider.
            col_expr = 'mpr.provider_id'
            has_provider_id = True
        elif field_name == 'location_id' and table_name != LOCATION:
            col_expr = 'loc.location_id'
            has_location_id = True
        elif field_name == 'note_id' and table_name != NOTE:
            col_expr = 'ni.note_id'
            has_note_id = True
        elif field_name == 'questionnaire_response_id':
            # questionnaire_response_id maps to survey_conduct_id
            col_expr = 'mqr.survey_conduct_id AS questionnaire_response_id'
            has_questionnaire_response_id = True
        elif field_name in ('snippet', 'offset') and table_name == NOTE_NLP:
            col_expr = f'CAST({field_name} AS STRING) AS {field_name}'
        else:
            col_expr = field_name

        col_exprs.append(col_expr)

    cols = ',\n        '.join(col_exprs)

    # Build JOIN expressions for foreign keys
    person_join_expr = ''
    visit_occurrence_join_expr = ''
    preceding_visit_occurrence_join_expr = ''
    visit_detail_join_expr = ''
    preceding_visit_detail_join_expr = ''
    visit_detail_parent_join_expr = ''
    location_join_expr = ''
    care_site_join_expr = ''
    note_join_expr = ''
    questionnaire_response_join_expr = ''
    provider_join_expr = ''
    visit_detail_filter_expr = ''

    if has_person_id:
        mapping_person = mapping_table_for(PERSON)
        person_join_expr = f'''
        LEFT JOIN `{project_id}.{mapping_dataset_id}.{mapping_person}` mp
            ON t.person_id = mp.src_person_id
            AND mp.src_table_id = '{person_src_table_id}'
        '''

    if has_visit_occurrence_id:
        visit_occurrence_join_expr = _mapping_join(VISIT_OCCURRENCE, 'mvo',
                                                   'visit_occurrence_id')

    if has_preceding_visit_occurrence_id:
        preceding_visit_occurrence_join_expr = _mapping_join(
            VISIT_OCCURRENCE, 'pvo', 'preceding_visit_occurrence_id')

    if has_visit_detail_id:
        visit_detail_join_expr = _mapping_join(VISIT_DETAIL, 'mvd',
                                               'visit_detail_id')

    if has_preceding_visit_detail_id:
        preceding_visit_detail_join_expr = _mapping_join(
            VISIT_DETAIL, 'pvd', 'preceding_visit_detail_id')

    if has_visit_detail_parent_id:
        visit_detail_parent_join_expr = _mapping_join(VISIT_DETAIL, 'ppvd',
                                                      'visit_detail_parent_id')

    if has_care_site_id:
        care_site_join_expr = _mapping_join(CARE_SITE, 'mcs', 'care_site_id')

    if has_location_id:
        location_join_expr = _mapping_join(LOCATION, 'loc', 'location_id')

    if has_note_id:
        note_join_expr = _mapping_join(NOTE, 'ni', 'note_id')

    if has_provider_id:
        provider_join_expr = _mapping_join(PROVIDER, 'mpr', 'provider_id')

    if has_questionnaire_response_id:
        # questionnaire_response_id maps to survey_conduct_id
        mapping_survey_conduct = mapping_table_for(SURVEY_CONDUCT)
        questionnaire_response_join_expr = f'''
        LEFT JOIN `{project_id}.{mapping_dataset_id}.{mapping_survey_conduct}` mqr
            ON t.questionnaire_response_id = mqr.src_survey_conduct_id AND mqr.src_table_id = '{mapping_namespace}.{SURVEY_CONDUCT}'
        '''

    if table_name == PERSON:
        return f'''
        SELECT
            {cols}
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY nm.person_id) AS row_num
            FROM
                `{project_id}.{input_dataset_id}.{table_name}` AS nm) AS t
        JOIN
            `{project_id}.{mapping_dataset_id}.{mapping_table}` AS m
        ON
            t.person_id = m.src_person_id
        AND m.src_table_id = '{person_src_table_id}'
        {location_join_expr}
        {care_site_join_expr}
        {provider_join_expr}
        WHERE
            row_num = 1
        '''

    if table_name == VISIT_DETAIL:
        visit_detail_filter_expr = '''
        AND mvo.visit_occurrence_id IS NOT NULL
        '''

    # For tables with primary keys that need remapping
    return f'''
    SELECT
        {cols}
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY nm.{table_name}_id) AS row_num
        FROM
            `{project_id}.{input_dataset_id}.{table_name}` AS nm) AS t
    JOIN (
        SELECT * EXCEPT(rn) FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY src_{table_name}_id ORDER BY {table_name}_id) as rn
            FROM `{project_id}.{mapping_dataset_id}.{mapping_table}`
            WHERE src_table_id = '{mapping_namespace}.{table_name}'
        ) WHERE rn = 1
    ) AS m
    ON
        t.{table_name}_id = m.src_{table_name}_id
    {person_join_expr}
    {visit_occurrence_join_expr}
    {visit_detail_join_expr}
    {care_site_join_expr}
    {preceding_visit_occurrence_join_expr}
    {preceding_visit_detail_join_expr}
    {visit_detail_parent_join_expr}
    {location_join_expr}
    {note_join_expr}
    {questionnaire_response_join_expr}
    {provider_join_expr}
    WHERE
        row_num = 1
    {visit_detail_filter_expr}
    '''


def load(client, cdm_table, input_dataset_id, output_dataset_id, project_id,
         pipeline_dataset_id, ct_plus_ids_view, mapping_dataset_id,
         mapping_namespace):
    """
    Loads a single domain table into the output dataset with new IDs.

    :param client: BigQueryClient
    :param cdm_table: name of the CDM table (e.g. 'person', 'visit_occurrence')
    :param input_dataset_id: identifies dataset containing input data
    :param output_dataset_id: identifies dataset where result should be output
    :param project_id: identifies the GCP project
    :param pipeline_dataset_id: identifies the pipeline_tables dataset (for person mapping)
    :param mapping_dataset_id: identifies the dataset where mapping tables are stored
    :param mapping_namespace: original dataset used to create the mappings
    :param ct_plus_ids_view: view containing person_id to CT+ id mapping
    :return:
    """
    mapping_namespace = mapping_namespace if mapping_namespace else DEFAULT_MAPPING_NAMESPACE
    output_table = output_table_for(cdm_table)
    LOGGER.info(
        f'Loading {cdm_table} from {input_dataset_id} into {output_table}')

    # Check if the source table exists before proceeding
    try:
        client.get_table(f'{project_id}.{input_dataset_id}.{cdm_table}')
    except Exception:
        LOGGER.info(
            f'Table {cdm_table} not found in source dataset {input_dataset_id}. Skipping.'
        )
        return None

    q = table_query(cdm_table, input_dataset_id, output_dataset_id, project_id,
                    pipeline_dataset_id, ct_plus_ids_view, mapping_dataset_id,
                    mapping_namespace)
    if cdm_table == 'visit_detail' or cdm_table == 'visit_occurrence':
        LOGGER.info(q)
    query_result = bq_utils.query(q,
                                  destination_table_id=output_table,
                                  destination_dataset_id=output_dataset_id)
    query_job_id = query_result['jobReference']['jobId']
    bq_utils.wait_on_jobs([query_job_id])
    LOGGER.info(f'Job {query_job_id} completed for {cdm_table}')
    return query_result


def update_ext_table(ext_table_name, input_dataset_id, output_dataset_id,
                     project_id, mapping_dataset_id, pipeline_dataset_id,
                     ct_plus_ids_view, mapping_namespace):
    """
    Update an extension table with new IDs from its corresponding mapping table.

    :param ext_table_name: name of the extension table (e.g. 'person_ext')
    :param input_dataset_id: identifies dataset containing source data
    :param output_dataset_id: identifies dataset where result should be output
    :param mapping_dataset_id: identifies the dataset where mapping tables are stored
    :param project_id: identifies the GCP project
    :param pipeline_dataset_id: identifies the pipeline_tables dataset (for person mapping)
    :param ct_plus_ids_view: view containing person_id to CT+ id mapping
    :param mapping_namespace: original dataset used to create the mappings

    :return: query result
    """
    # Extract base table name (e.g. 'person' from 'person_ext')
    base_table = ext_table_name.replace('_ext', '')
    id_field = f'{base_table}_id'
    mapping_table = mapping_table_for(base_table)

    # Check if ext table exists
    try:
        client = BigQueryClient(project_id)  # Re-instantiate to be safe
        client.get_table(f'{input_dataset_id}.{ext_table_name}')
    except Exception:
        LOGGER.info(
            f'Extension table {ext_table_name} does not exist, skipping')
        return None

    LOGGER.info(f'Updating extension table {ext_table_name}')

    # Use the original source dataset for the join condition if provided, else use current input
    source_dataset = mapping_namespace if mapping_namespace else DEFAULT_MAPPING_NAMESPACE
    person_src_table_id = person_mapping_src_table_id(pipeline_dataset_id,
                                                      ct_plus_ids_view)

    q = f'''
    SELECT
        m.{id_field},
        e.* EXCEPT({id_field})
    FROM `{project_id}.{input_dataset_id}.{ext_table_name}` e
    JOIN `{project_id}.{mapping_dataset_id}.{mapping_table}` m
        ON e.{id_field} = m.src_{id_field} AND m.src_table_id = '{source_dataset}.{base_table}'
        '''

    if base_table == PERSON:
        q = f'''
    SELECT
        m.{id_field},
        e.* EXCEPT({id_field})
    FROM `{project_id}.{input_dataset_id}.{ext_table_name}` e
    JOIN `{project_id}.{mapping_dataset_id}.{mapping_table}` m
        ON e.{id_field} = m.src_{id_field}
        AND m.src_table_id = '{person_src_table_id}'
    '''

    return bq_utils.query(q,
                          destination_table_id=ext_table_name,
                          destination_dataset_id=output_dataset_id,
                          write_disposition='WRITE_TRUNCATE')


def main(input_dataset_id,
         output_dataset_id,
         project_id,
         pipeline_dataset_id,
         ct_plus_ids_view,
         mapping_dataset_id,
         mapping_namespace,
         allow_replace=False):
    """
    Create a new CDM dataset with regenerated IDs

    :param input_dataset_id: identifies the source dataset
    :param output_dataset_id: identifies the dataset to store the new CDM in
    :param project_id: project containing the datasets
    :param mapping_dataset_id: dataset to store/lookup mapping tables
    :param pipeline_dataset_id: dataset containing rdr_participant_research_ids_view for person mapping
    :param ct_plus_ids_view: view containing person_id to CT+ id mapping
    :param mapping_namespace: original dataset used to create the mappings
    :param allow_replace: True to permit deleting tables already in the output dataset
    :returns: list of tables generated successfully
    """
    bq_client = BigQueryClient(project_id)
    person_src_table_id = person_mapping_src_table_id(pipeline_dataset_id,
                                                      ct_plus_ids_view)

    LOGGER.info('CT+ ID regeneration started')

    # Create mapping dataset if it doesn't exist
    try:
        bq_client.get_dataset(mapping_dataset_id)
        LOGGER.info(f'Dataset {mapping_dataset_id} already exists')
    except Exception:
        LOGGER.info(f'Creating dataset {mapping_dataset_id}')
        bq_client.create_dataset(mapping_dataset_id, exists_ok=True)

    # Refuse to run against a mapping dataset another tier produced. Doing so would
    # reuse that tier's _mapping_person and give CT+ its person_ids.
    assert_person_mapping_is_ct_plus(bq_client, project_id, mapping_dataset_id,
                                     pipeline_dataset_id, ct_plus_ids_view)

    # Refuse to destroy an output dataset that already holds tables. This must come
    # before the delete loop below, which drops every CDM table in it.
    assert_output_dataset_is_safe(bq_client, output_dataset_id, allow_replace)

    # Create output dataset if it doesn't exist
    try:
        bq_client.get_dataset(output_dataset_id)
        LOGGER.info(f'Dataset {output_dataset_id} already exists')
    except Exception:
        LOGGER.info(f'Creating dataset {output_dataset_id}')
        bq_client.create_dataset(output_dataset_id, exists_ok=True)

    # Create empty output tables to ensure proper schema, clustering, etc.
    for table in CDM_TABLES:
        result_table = output_table_for(table)
        LOGGER.info(f'Creating {output_dataset_id}.{result_table}...')

        # Get schema for the table
        schema_list = bq_client.get_table_schema(table)

        # Create table with proper schema and clustering
        fq_table_name = f'{project_id}.{output_dataset_id}.{result_table}'

        clustering_fields = None
        # Add clustering for tables with person_id
        if any(field.name == 'person_id' for field in schema_list):
            clustering_fields = ['person_id']

        table_to_create = Table(fq_table_name, schema=schema_list)
        if clustering_fields:
            table_to_create.clustering_fields = clustering_fields

        bq_client.delete_table(fq_table_name, not_found_ok=True)
        bq_client.create_table(table_to_create)

    # Create mapping tables if they don't already exist
    LOGGER.info(f'Generating mapping tables in {mapping_dataset_id}...')
    for domain_table in CDM_TABLES:
        if domain_table in SKIP_ID_REGEN_TABLES or not has_primary_key(
                domain_table):
            continue

        mapping(domain_table, input_dataset_id, output_dataset_id, project_id,
                pipeline_dataset_id, ct_plus_ids_view, mapping_namespace,
                mapping_dataset_id)

    # Refuse to load against a person mapping that would duplicate or merge
    # participants. This has to come after the mapping loop that builds it.
    assert_person_mapping_is_one_to_one(bq_client, project_id,
                                        mapping_dataset_id)

    # Load all tables with new IDs
    for table_name in CDM_TABLES:
        if table_name == DEATH:
            LOGGER.info(f'Skipping {table_name} load.')
            continue
        LOGGER.info(f'Loading table {table_name}...')
        load(bq_client, table_name, input_dataset_id, output_dataset_id,
             project_id, pipeline_dataset_id, ct_plus_ids_view,
             mapping_dataset_id, mapping_namespace)

    # Load Fitbit tables if they exist
    LOGGER.info('Processing Fitbit tables (if they exist)...')
    for fitbit_table in FITBIT_TABLES:
        if bq_client.table_exists(fitbit_table, input_dataset_id):
            LOGGER.info(f'Loading Fitbit table {fitbit_table}...')
            load(bq_client, fitbit_table, input_dataset_id, output_dataset_id,
                 project_id, pipeline_dataset_id, ct_plus_ids_view,
                 mapping_dataset_id, mapping_namespace)
        else:
            LOGGER.info(f'Fitbit table {fitbit_table} does not exist, skipping')

    # Update extension tables with new IDs
    LOGGER.info('Updating extension tables...')
    for ext_table in EXT_TABLES:
        update_ext_table(ext_table, input_dataset_id, output_dataset_id,
                         project_id, mapping_dataset_id, pipeline_dataset_id,
                         ct_plus_ids_view, mapping_namespace)

    # Discover and process any remaining tables
    tables_to_skip = set(CDM_TABLES) | set(FITBIT_TABLES) | set(EXT_TABLES)
    copy_only_tables = set(VOCABULARY_TABLES) | set(ACHILLES_TABLES) | set(
        ACHILLES_HEEL_TABLES)

    all_input_tables = [
        table.table_id for table in bq_client.list_tables(input_dataset_id)
    ]

    unprocessed_tables = [
        t for t in all_input_tables
        if t not in tables_to_skip and t not in copy_only_tables
    ]

    LOGGER.info(
        f"Processing other tables: {', '.join(unprocessed_tables) if unprocessed_tables else 'None'}"
    )

    for table_name in unprocessed_tables:
        table = bq_client.get_table(
            f'{project_id}.{input_dataset_id}.{table_name}')
        schema = table.schema
        id_field = f'{table_name}_id'
        has_person_id = any(field.name == 'person_id' for field in schema)
        has_pk = any(field.name == id_field for field in schema)

        if has_person_id and has_pk:
            LOGGER.info(
                f"Table '{table_name}' has a primary key and person_id. Full remapping will be applied."
            )
            # 1. Create mapping for the primary key
            LOGGER.info(f"Creating mapping for {table_name}...")
            mapping(table_name, input_dataset_id, output_dataset_id, project_id,
                    pipeline_dataset_id, ct_plus_ids_view, mapping_namespace,
                    mapping_dataset_id)
            # 2. Load table with remapped PK and FKs
            LOGGER.info(f"Loading table {table_name}...")
            load(bq_client, table_name, input_dataset_id, output_dataset_id,
                 project_id, pipeline_dataset_id, ct_plus_ids_view,
                 mapping_dataset_id, mapping_namespace)
        elif has_person_id:
            LOGGER.info(
                f"Table '{table_name}' has person_id. Remapping person_id only."
            )
            col_exprs = [
                'mp.person_id'
                if field.name == 'person_id' else f't.{field.name}'
                for field in schema
            ]
            cols = ', '.join(col_exprs)
            mapping_person = mapping_table_for(PERSON)
            q = f"""SELECT {cols}
                    FROM `{project_id}.{input_dataset_id}.{table_name}` t
                    JOIN `{project_id}.{mapping_dataset_id}.{mapping_person}` mp
                        ON t.person_id = mp.src_person_id
                        AND mp.src_table_id = '{person_src_table_id}'"""
            bq_utils.query(q,
                           destination_table_id=table_name,
                           destination_dataset_id=output_dataset_id,
                           write_disposition='WRITE_TRUNCATE')
        else:
            LOGGER.info(
                f"Table '{table_name}' has no person_id. Copying as-is.")
            bq_client.copy_table(
                f'{project_id}.{input_dataset_id}.{table_name}',
                f'{project_id}.{output_dataset_id}.{table_name}')

    # Copy vocabulary and Achilles tables directly
    LOGGER.info(
        f"Copying vocabulary and Achilles tables: {', '.join(copy_only_tables)}"
    )
    for table_name in copy_only_tables:
        if bq_client.table_exists(table_name, input_dataset_id):
            bq_client.copy_table(
                f'{project_id}.{input_dataset_id}.{table_name}',
                f'{project_id}.{output_dataset_id}.{table_name}')

    LOGGER.info('CT+ ID regeneration complete')


if __name__ == '__main__':
    pipeline_logging.configure(logging.INFO, add_console_handler=True)
    parser = argparse.ArgumentParser(
        description='Regenerate IDs in a Controlled Tier Plus dataset',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--project_id',
                        dest='project_id',
                        required=True,
                        help='Project associated with the datasets')
    parser.add_argument('--input_dataset_id',
                        dest='input_dataset_id',
                        required=True,
                        help='Dataset containing source data')
    parser.add_argument('--output_dataset_id',
                        dest='output_dataset_id',
                        required=True,
                        help='Dataset where results should be stored')
    parser.add_argument(
        '--pipeline_dataset_id',
        dest='pipeline_dataset_id',
        required=True,
        help=
        'Dataset containing rdr_participant_research_ids_view for person mapping'
    )
    parser.add_argument(
        '--ct_plus_ids_view',
        dest='ct_plus_ids_view',
        required=True,
        help=
        'View containing controlled_tier_id to controlled_tier_plus_id mapping')
    parser.add_argument(
        '--mapping_dataset_id',
        dest='mapping_dataset_id',
        required=True,
        help='Dataset to store/lookup mapping tables. Can be a sandbox dataset. '
        'Use the same one for every release so ids persist between them.')
    parser.add_argument(
        '--mapping_namespace',
        dest='mapping_namespace',
        required=False,
        default=DEFAULT_MAPPING_NAMESPACE,
        help='Constant stamped into src_table_id on every mapping row. Leave at '
        f"the default ('{DEFAULT_MAPPING_NAMESPACE}'); a per release value breaks "
        'id persistence silently.')
    parser.add_argument(
        '--allow_replace',
        dest='allow_replace',
        action='store_true',
        help='Permit deleting the tables already in --output_dataset_id. Without '
        'this the run stops rather than destroying them. Only needed when '
        'rebuilding an output dataset on purpose.')

    args = parser.parse_args()
    main(args.input_dataset_id, args.output_dataset_id, args.project_id,
         args.pipeline_dataset_id, args.ct_plus_ids_view,
         args.mapping_dataset_id, args.mapping_namespace, args.allow_replace)
