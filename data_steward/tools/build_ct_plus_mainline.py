"""
Write the publishable CT+ mainline by subtracting the add-on datasets from the re-key
output.

regenerate_ct_plus_ids.py writes CP{release_tag}_deid_clean_pre_split, which still holds
every add-on component inline: the pediatric cohort, the expanded element rows and the
indicators-of-birth rows. Each add-on producer copies its rows out of that dataset and
removes nothing. This tool writes CP{release_tag}_deid_clean, the dataset every CT+
researcher receives, as the input minus every row the named add-on datasets hold.

Rows are matched on what the producers copied them by, so no predicate is restated here
and the mainline cannot disagree with an add-on about a row:

- a table's own row ID, <table>_id, where it has one;
- an _ext table's parent row ID;
- fact_relationship's five columns, since it has no row ID;
- person_id for any other person-keyed table, such as death or the Fitbit tables. Only
  the pediatrics split claims these, and it claims a participant's rows whole.

Not subtracted:

- pediatric_relationship_ext is dropped outright. It has no row ID and is pediatric
  content in full, so the pediatrics dataset must hold it.
- _cdr_metadata and zip3_ses_map are dropped outright. The input's _cdr_metadata
  describes the release it was copied from, and zip3_ses_map is a pipeline lookup; the
  release runbook adds both to the mainline as its last step.
- Views are recreated over the mainline rather than copied. A view's SQL names the
  dataset it reads, so a copy would still read the input, add-on rows included.
- The Zip5 and date-of-birth person-level tables, zip5 and dob. The pipeline removed
  those values before the input existed, so there is nothing to subtract, and neither
  table has a row ID an input row could match.

Every refusal runs before anything is written:

- an input that is not this release's pre-split dataset, or an add-on dataset that is
  not this release's;
- a named add-on dataset that does not exist;
- an input table outside CT_PLUS_PUBLISHABLE_TABLES;
- a view reading any dataset but the input;
- pediatric_relationship_ext in the input and not in the pediatrics dataset;
- an add-on table the input does not hold;
- a table where the rows the key removes differ from the rows the add-ons hold: an
  add-on row matching no input row, or a key removing rows no add-on claims;
- an output dataset that already holds tables.

The add-on datasets are an explicit argument rather than discovered. Nothing here can
tell a producer that never ran from one that claimed nothing, so the named set is what
catches a missing one.

Usage:
  python build_ct_plus_mainline.py \
    --project_id <project> \
    --release_tag <e.g. 2025q4r7> \
    --input_dataset_id <CP{release_tag}_deid_clean_pre_split> \
    --addon_dataset_ids CP{release_tag}_pediatrics CP{release_tag}_dob ...
"""
# Python imports
import argparse
import logging
import re

# Third party imports
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import CopyJobConfig, Table

# Project imports
from common import (CONTROLLED_PLUS, CT_PLUS_PEDIATRICS,
                    CT_PLUS_PRE_SPLIT_SUFFIX, CT_PLUS_PUBLISHABLE_TABLES,
                    EXT_SUFFIX, FACT_RELATIONSHIP, JINJA_ENV,
                    PEDIATRIC_RELATIONSHIP_EXT, TIER_DATASET_PREFIX,
                    ZIP3_SES_MAP, get_ct_plus_addon_dataset_name)
from gcloud.bq import BigQueryClient
from tools.split_ct_plus_pediatric_dataset import (
    assert_output_dataset_is_empty, write_table)
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

# Label on the mainline, beside the ct_plus_component label each add-on carries.
MAINLINE_COMPONENT = 'mainline'

# add_cdr_metadata.METADATA_TABLE, not imported since that module pulls in every
# cleaning rule.
CDR_METADATA = '_cdr_metadata'

# In the input, and never in the mainline. Only pediatric_relationship_ext has to be
# held by an add-on; the runbook adds the other two back after the assembly.
DROPPED_TABLES = frozenset(
    {PEDIATRIC_RELATIONSHIP_EXT, CDR_METADATA, ZIP3_SES_MAP})

# Written by the Zip5 and date-of-birth promotions. Their values left the pipeline before
# the input existed, so there is nothing in the input to subtract for them.
PERSON_LEVEL_TABLES = frozenset({'zip5', 'dob'})

FACT_RELATIONSHIP_KEY = [
    'domain_concept_id_1', 'fact_id_1', 'domain_concept_id_2', 'fact_id_2',
    'relationship_concept_id'
]

TABLE_COLUMNS = JINJA_ENV.from_string("""
SELECT table_name, column_name
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
""")

VIEWS = JINJA_ENV.from_string("""
SELECT table_name
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.TABLES`
WHERE table_type = 'VIEW'
""")

# Backticked references of the form project.dataset.table, or dataset.table
TABLE_REFERENCE = re.compile(r'`(?:[\w-]+\.)?([\w-]+)\.[\w$-]+`')

# A multi-column key is compared as one JSON string, which also matches NULL to NULL.
KEY = (
    '{% macro key(alias) %}'
    '{% if key_columns | length == 1 %}{{alias}}.{{key_columns[0]}}'
    '{% else %}TO_JSON_STRING(STRUCT('
    '{% for c in key_columns %}{{alias}}.{{c}}{% if not loop.last %}, {% endif %}'
    '{% endfor %})){% endif %}'
    '{% endmacro %}')

CLAIMED = """
claimed AS (
{% for dataset in addon_datasets %}
  SELECT {{ key('a') }} AS k
  FROM `{{project}}.{{dataset}}.{{table}}` a
{% if not loop.last %}  UNION ALL
{% endif %}
{% endfor %}
)"""

# removed_rows reads the input, claimed_rows the add-ons, so any disagreement between
# the key and what a producer copied shows up as a difference between the two.
CLAIM_CHECK = JINJA_ENV.from_string(KEY + """
WITH""" + CLAIMED + """
SELECT
  (SELECT COUNT(*) FROM `{{project}}.{{input_dataset}}.{{table}}`) AS input_rows,
  (SELECT COUNT(*)
   FROM `{{project}}.{{input_dataset}}.{{table}}` t
   WHERE EXISTS (SELECT 1 FROM claimed c WHERE c.k = {{ key('t') }})
  ) AS removed_rows,
  (SELECT {% if whole_participant %}COUNT(*){% else %}COUNT(DISTINCT k){% endif %}
   FROM claimed) AS claimed_rows
""")

MAINLINE_ROWS = JINJA_ENV.from_string(KEY + """
WITH""" + CLAIMED + """
SELECT t.*
FROM `{{project}}.{{input_dataset}}.{{table}}` t
WHERE NOT EXISTS (SELECT 1 FROM claimed c WHERE c.k = {{ key('t') }})
""")


def validate_dataset_names(release_tag, input_dataset_id, addon_dataset_ids):
    """
    Stop the run unless every dataset belongs to this release, and name the output.

    :param release_tag: release tag, e.g. '2025q4r7'
    :param input_dataset_id: the re-key output
    :param addon_dataset_ids: the add-on datasets to subtract
    :return: the mainline's name, the input's without CT_PLUS_PRE_SPLIT_SUFFIX
    :raises ValueError: on an input that is not CP{release_tag}_..._pre_split, an
        add-on dataset outside the release, or no add-on dataset at all
    """
    prefix = f'{TIER_DATASET_PREFIX[CONTROLLED_PLUS]}{release_tag}_'
    if not (input_dataset_id.startswith(prefix) and
            input_dataset_id.endswith(CT_PLUS_PRE_SPLIT_SUFFIX)):
        raise ValueError(
            f'--input_dataset_id {input_dataset_id} must start with {prefix} and end '
            f'with {CT_PLUS_PRE_SPLIT_SUFFIX}: it is the re-key output the add-ons '
            f'were split from.')

    if not addon_dataset_ids:
        raise ValueError(
            '--addon_dataset_ids is empty. Name every add-on dataset produced for '
            'this release; a producer left off is published inside the mainline.'
        )

    foreign = sorted(d for d in addon_dataset_ids if not d.startswith(prefix))
    if foreign:
        raise ValueError(
            f'{foreign} are not add-on datasets of release {release_tag}: each must '
            f'start with {prefix}.')

    return input_dataset_id[:-len(CT_PLUS_PRE_SPLIT_SUFFIX)]


def get_table_columns(client, project_id, dataset_id):
    """
    :return: {table name: [column names]} for every table in the dataset
    :raises RuntimeError: if the dataset does not exist
    """
    try:
        client.get_dataset(f'{project_id}.{dataset_id}')
    except NotFound:
        raise RuntimeError(
            f'{dataset_id} does not exist. Every named add-on dataset must have been '
            f'produced before the mainline is assembled.')

    columns = {}
    query = TABLE_COLUMNS.render(project=project_id, dataset=dataset_id)
    for row in client.query(query).result():
        columns.setdefault(row['table_name'], []).append(row['column_name'])
    return columns


def get_subtraction_key(table, columns):
    """
    Choose how an input table's rows are matched against the add-ons.

    :param table: input table name
    :param columns: the table's column names
    :return: (key columns, True if the key selects a participant's rows whole)
    :raises RuntimeError: if the table has nothing to match on
    """
    if table == FACT_RELATIONSHIP:
        return FACT_RELATIONSHIP_KEY, False

    row_id = f'{table}_id'
    if row_id in columns:
        return [row_id], False

    if table.endswith(EXT_SUFFIX):
        parent_id = f'{table[:-len(EXT_SUFFIX)]}_id'
        if parent_id in columns:
            return [parent_id], False

    if 'person_id' in columns:
        return ['person_id'], True

    raise RuntimeError(
        f'{table} has no row ID, parent row ID or person_id, so its rows cannot be '
        f'subtracted. An add-on dataset should not carry it.')


def get_views(client, project_id, dataset_id):
    """
    :return: names of the views in the dataset
    """
    query = VIEWS.render(project=project_id, dataset=dataset_id)
    return {row['table_name'] for row in client.query(query).result()}


def plan(client, project_id, input_dataset_id, addon_dataset_ids,
         pediatrics_dataset_id):
    """
    Refuse on anything the input or the add-ons declare wrongly, and say which add-ons
    claim rows from each input table.

    A view is never claimed, even when an add-on holds a table of its name: the
    mainline's copy reads the mainline's tables, which have already lost those rows.

    :return: ({input table: [columns]}, {input table: [add-on datasets holding it]},
        {input views})
    :raises RuntimeError: listing every failure found
    """
    input_columns = get_table_columns(client, project_id, input_dataset_id)
    views = get_views(client, project_id, input_dataset_id)
    addon_tables = {
        dataset: set(get_table_columns(client, project_id, dataset))
        for dataset in addon_dataset_ids
    }

    failures = []

    undeclared = sorted(
        set(input_columns) - CT_PLUS_PUBLISHABLE_TABLES - DROPPED_TABLES)
    if undeclared:
        failures.append(
            f'{input_dataset_id} holds tables outside the publishable table set: '
            f'{undeclared}')

    if (PEDIATRIC_RELATIONSHIP_EXT in input_columns and
            PEDIATRIC_RELATIONSHIP_EXT not in addon_tables.get(
                pediatrics_dataset_id, set())):
        failures.append(
            f'{PEDIATRIC_RELATIONSHIP_EXT} is in {input_dataset_id} but not in '
            f'{pediatrics_dataset_id}, so dropping it would lose it')

    claims = {}
    for dataset, tables in sorted(addon_tables.items()):
        for table in sorted(tables - PERSON_LEVEL_TABLES - DROPPED_TABLES):
            if table not in input_columns:
                failures.append(
                    f'{dataset}.{table} matches no table in {input_dataset_id}')
                continue
            if table in views:
                LOGGER.info(
                    f'{dataset}.{table} is a view in {input_dataset_id}; '
                    f'the mainline recreates it rather than subtracting')
                continue
            claims.setdefault(table, []).append(dataset)

    for table in sorted(claims):
        try:
            get_subtraction_key(table, input_columns[table])
        except RuntimeError as e:
            failures.append(str(e))

    if failures:
        raise RuntimeError('The mainline cannot be assembled: ' +
                           '; '.join(failures) + '.')

    return input_columns, claims, views


def check_claims(client, project_id, input_dataset_id, input_columns, claims):
    """
    Stop the run unless, for every claimed table, the rows the key removes from the
    input are exactly the rows the add-ons hold.

    :return: {table: (input_rows, removed_rows)}
    :raises RuntimeError: naming every table that disagrees
    """
    counts = {}
    failures = []
    for table, datasets in sorted(claims.items()):
        key_columns, whole_participant = get_subtraction_key(
            table, input_columns[table])
        row = list(
            client.query(
                CLAIM_CHECK.render(
                    project=project_id,
                    input_dataset=input_dataset_id,
                    table=table,
                    addon_datasets=datasets,
                    key_columns=key_columns,
                    whole_participant=whole_participant)).result())[0]
        counts[table] = (row['input_rows'], row['removed_rows'])
        if row['removed_rows'] != row['claimed_rows']:
            failures.append(
                f'{table}: {datasets} hold {row["claimed_rows"]} rows and matching on '
                f'{key_columns} removes {row["removed_rows"]}')

    if failures:
        raise RuntimeError(
            'The add-ons and the input disagree. A shortfall means an add-on row '
            'matches nothing in the input; an excess means the key removes rows no '
            'add-on holds: ' + '; '.join(failures) + '.')

    return counts


def get_view_queries(client, project_id, input_dataset_id, output_dataset_id,
                     views):
    """
    Rewrite each input view's SQL to read the mainline instead of the input.

    :return: {view: rewritten SQL}
    :raises RuntimeError: if a view reads any dataset other than the input
    """
    queries = {}
    failures = []
    for view in sorted(views):
        query = client.get_table(
            f'{project_id}.{input_dataset_id}.{view}').view_query
        foreign = sorted({
            dataset for dataset in TABLE_REFERENCE.findall(query)
            if dataset != input_dataset_id
        })
        if foreign:
            failures.append(f'{view} reads {foreign}')
            continue
        queries[view] = query.replace(f'{input_dataset_id}.',
                                      f'{output_dataset_id}.')

    if failures:
        raise RuntimeError(
            f'Views in {input_dataset_id} read datasets other than it, so they cannot '
            f'be pointed at the mainline: ' + '; '.join(failures) + '.')

    return queries


def recreate_views(client, project_id, output_dataset_id, view_queries):
    """
    Create each view in the mainline, after any view it reads.

    :raises RuntimeError: if the views read each other in a cycle
    """
    remaining = dict(view_queries)
    while remaining:
        ready = [
            view for view, query in sorted(remaining.items()) if not any(
                re.search(rf'\b{other}\b', query)
                for other in remaining if other != view)
        ]
        if not ready:
            raise RuntimeError(
                f'Views {sorted(remaining)} read each other in a cycle.')
        for view in ready:
            table = Table(f'{project_id}.{output_dataset_id}.{view}')
            table.view_query = remaining.pop(view)
            client.create_table(table)
            LOGGER.info(f'{view}: recreated over {output_dataset_id}')


def write_mainline(client,
                   project_id,
                   input_dataset_id,
                   output_dataset_id,
                   input_columns,
                   claims,
                   counts,
                   views=frozenset()):
    """
    Write every publishable input table into the mainline, minus what the add-ons
    claim, and confirm each claimed table lost exactly the rows it should. Views are
    left to recreate_views.

    :return: {table: rows written}
    :raises RuntimeError: if a written table's row count is not input minus removed
    """
    written = {}
    for table in sorted(set(input_columns) - DROPPED_TABLES - set(views)):
        if table not in claims:
            job = client.copy_table(
                f'{project_id}.{input_dataset_id}.{table}',
                f'{project_id}.{output_dataset_id}.{table}',
                job_config=CopyJobConfig(write_disposition='WRITE_EMPTY'))
            job.result()
            written[table] = client.get_table(
                f'{project_id}.{output_dataset_id}.{table}').num_rows
            continue

        key_columns, _ = get_subtraction_key(table, input_columns[table])
        query = MAINLINE_ROWS.render(project=project_id,
                                     input_dataset=input_dataset_id,
                                     table=table,
                                     addon_datasets=claims[table],
                                     key_columns=key_columns)
        written[table] = write_table(client, project_id, input_dataset_id,
                                     output_dataset_id, table, query)

        input_rows, removed_rows = counts[table]
        if written[table] != input_rows - removed_rows:
            raise RuntimeError(
                f'{output_dataset_id}.{table} holds {written[table]} rows, not the '
                f'{input_rows} - {removed_rows} expected. Remove {output_dataset_id} '
                f'before rerunning.')

    return written


def log_counts(counts, written, claims):
    for table, rows in sorted(written.items()):
        if table in claims:
            input_rows, removed_rows = counts[table]
            LOGGER.info(f'{table}: {input_rows} in, {removed_rows} claimed by '
                        f'{claims[table]}, {rows} in the mainline')
        else:
            LOGGER.info(f'{table}: {rows} copied whole')


def main(project_id, release_tag, input_dataset_id, addon_dataset_ids):
    """
    Validate the input and the add-ons, then write CP{release_tag}_deid_clean.

    :return: {table: rows written}
    """
    output_dataset_id = validate_dataset_names(release_tag, input_dataset_id,
                                               addon_dataset_ids)
    pediatrics_dataset_id = get_ct_plus_addon_dataset_name(
        release_tag, CT_PLUS_PEDIATRICS)

    client = BigQueryClient(project_id)
    input_columns, claims, views = plan(client, project_id, input_dataset_id,
                                        addon_dataset_ids,
                                        pediatrics_dataset_id)
    counts = check_claims(client, project_id, input_dataset_id, input_columns,
                          claims)
    view_queries = get_view_queries(client, project_id, input_dataset_id,
                                    output_dataset_id, views)
    assert_output_dataset_is_empty(client, project_id, output_dataset_id)

    dataset = client.define_dataset(
        output_dataset_id,
        f'CT+ mainline for release {release_tag}: {input_dataset_id} minus '
        f'{sorted(addon_dataset_ids)}',
        {'ct_plus_component': MAINLINE_COMPONENT})
    client.create_dataset(dataset, exists_ok=True)

    written = write_mainline(client, project_id, input_dataset_id,
                             output_dataset_id, input_columns, claims, counts,
                             views)
    recreate_views(client, project_id, output_dataset_id, view_queries)
    log_counts(counts, written, claims)
    return written


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description='Write the CT+ mainline by subtracting the add-on datasets.'
    )
    parser.add_argument('--project_id',
                        required=True,
                        help='Project holding the input and output datasets')
    parser.add_argument('--release_tag',
                        required=True,
                        help='Release tag, e.g. 2025q4r7')
    parser.add_argument(
        '--input_dataset_id',
        required=True,
        help=f'The re-key output, CP<release_tag>_..._{CT_PLUS_PRE_SPLIT_SUFFIX}'
    )
    parser.add_argument(
        '--addon_dataset_ids',
        required=True,
        nargs='+',
        help=('Every add-on dataset produced for this release. One left off is '
              'published inside the mainline.'))
    return parser


if __name__ == '__main__':
    pipeline_logging.configure(level=logging.INFO, add_console_handler=True)
    ARGS = get_arg_parser().parse_args()
    main(ARGS.project_id, ARGS.release_tag, ARGS.input_dataset_id,
         ARGS.addon_dataset_ids)
