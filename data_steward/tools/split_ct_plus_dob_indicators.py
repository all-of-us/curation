"""
Write the indicators-of-birth rows of the CT+ date-of-birth add-on out of the re-keyed
CT+ dataset.

The CT+ rules that remove these rows in CT leave them inline for every participant who
can receive the add-on (ct_plus_dob_indicators.py describes how), so they reach the
re-key output with every value-level transform applied and with CT+ row and person
IDs. This tool copies the rows carrying an indicators-of-birth concept into
CP{release_tag}_dob, with their _ext rows, row IDs and person IDs unchanged. It removes
nothing and writes no mainline: the mainline assembly subtracts this dataset from the
same input by row ID.

What is claimed: the rows of every table with person_id and its own row ID that carry
a concept from the set in any concept column. A table the add-on cannot carry (person,
death, visit_occurrence, visit_detail, or one without its own row ID) must hold no such
row, since the assembly could not subtract it; the run stops if one does.

The person-level date of birth is added to the same dataset by the date-of-birth
dataset promotion, which runs after this tool.

Usage:
  python split_ct_plus_dob_indicators.py \
    --project_id <project> \
    --release_tag <e.g. 2025q4r7> \
    --input_dataset_id <CP{release_tag}_deid_clean_pre_split> \
    --rdr_sandbox_id <RDR stage sandbox holding aian_list> \
    --under18_lookup_dataset_id <dataset holding _under18_participants> \
    --ct_plus_ids_view <view in pipeline_tables mapping participant IDs to CT+ IDs>
"""
# Python imports
import argparse
import logging

# Project imports
from cdr_cleaner.cleaning_rules.deid.ct_plus_dob_indicators import \
    get_dob_indicator_concept_ids
from common import (AIAN_LIST, CT_PLUS_DOB_INDICATORS, CT_PLUS_PRE_SPLIT_SUFFIX,
                    DEATH, EXT_SUFFIX, JINJA_ENV, PERSON, PIPELINE_TABLES,
                    UNDER18_PARTICIPANTS_LOOKUP_TABLE, VISIT_DETAIL,
                    VISIT_OCCURRENCE, get_ct_plus_addon_dataset_name)
from gcloud.bq import BigQueryClient
from tools.split_ct_plus_pediatric_dataset import (
    CHILD_ROWS, assert_output_dataset_is_empty, validate_dataset_names,
    write_table)
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

# Parent tables: other rows point at them, so subtracting one from the mainline would
# leave its children dangling. death has no row ID to subtract by.
UNCARRIABLE_TABLES = frozenset([PERSON, DEATH, VISIT_OCCURRENCE, VISIT_DETAIL])

TABLE_COLUMNS = JINJA_ENV.from_string("""
SELECT table_name, column_name, data_type
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
WHERE NOT STARTS_WITH(table_name, '_')
ORDER BY table_name, ordinal_position
""")

LOOKUP_COUNT = JINJA_ENV.from_string("""
SELECT COUNT(*) AS lookup_count
FROM `{{project}}.{{lookup_dataset}}.{{lookup_table}}`
""")

DOB_ROW = JINJA_ENV.from_string("""
EXISTS (
    SELECT 1
    FROM UNNEST([{{concept_ids | join(', ')}}]) AS c
    WHERE c IN ({% for column in concept_columns %}{% if not loop.first %}, {% endif %}t.{{column}}{% endfor %})
)""")

# Each row is mapped back to its participant through the ids view, since both lookups
# are keyed by participant ID and the input by CT+ research ID.
CHECKS = JINJA_ENV.from_string("""
SELECT
  COUNT(*) AS dob_rows,
  COUNTIF(v.controlled_tier_plus_id IS NULL) AS non_ct_plus_rows,
  COUNTIF(a.person_id IS NOT NULL) AS aian_rows,
  COUNTIF(u.person_id IS NOT NULL) AS pediatric_rows
FROM `{{project}}.{{input_dataset}}.{{table}}` t
LEFT JOIN (
  SELECT DISTINCT participant_id, controlled_tier_plus_id
  FROM `{{project}}.{{ids_dataset}}.{{ids_view}}`
  WHERE controlled_tier_plus_id IS NOT NULL
) v
  ON v.controlled_tier_plus_id = t.person_id
LEFT JOIN (
  SELECT DISTINCT person_id FROM `{{project}}.{{rdr_sandbox}}.{{aian_list}}`
) a
  ON a.person_id = v.participant_id
LEFT JOIN (
  SELECT DISTINCT person_id FROM `{{project}}.{{under18_dataset}}.{{under18_table}}`
) u
  ON u.person_id = v.participant_id
WHERE {{dob_row}}
""")

DOB_ROWS = JINJA_ENV.from_string("""
SELECT t.*
FROM `{{project}}.{{input_dataset}}.{{table}}` t
WHERE {{dob_row}}
""")

FAILURES = {
    'non_ct_plus_rows': 'have a person_id that is not a CT+ research ID',
    'aian_rows': 'belong to AIAN participants',
    'pediatric_rows': 'belong to pediatric participants',
}


def classify_tables(client, project_id, input_dataset_id):
    """
    Sort the input's tables by whether the add-on can carry their birth rows.

    :return: (carried, uncarriable), each a dict of table name to its integer
        concept_id columns. carried tables have person_id and their own row ID;
        uncarriable ones have person_id and concept columns but cannot be
        subtracted from the mainline by row ID.
    """
    columns = {}
    concept_columns = {}
    for row in client.query(
            TABLE_COLUMNS.render(project=project_id,
                                 dataset=input_dataset_id)).result():
        table, column = row['table_name'], row['column_name']
        columns.setdefault(table, set()).add(column)
        if (row['data_type'].upper() == 'INT64' and
                column.lower().endswith('concept_id')):
            concept_columns.setdefault(table, []).append(column)

    carried, uncarriable = {}, {}
    for table, table_concept_columns in concept_columns.items():
        # _ext rows follow the rows they describe rather than being claimed alone
        if 'person_id' not in columns[table] or table.endswith(EXT_SUFFIX):
            continue
        if (table in UNCARRIABLE_TABLES or f'{table}_id' not in columns[table]):
            uncarriable[table] = table_concept_columns
        else:
            carried[table] = table_concept_columns

    return carried, uncarriable


def dob_row(concept_ids, concept_columns):
    """
    :return: the predicate matching a row of alias t that carries a concept from
        concept_ids in any of concept_columns
    """
    return DOB_ROW.render(concept_ids=concept_ids,
                          concept_columns=concept_columns)


def assert_split_is_publishable(client, project_id, input_dataset_id,
                                rdr_sandbox_id, under18_lookup_dataset_id,
                                ids_dataset_id, ids_view_id, carried,
                                uncarriable, concept_ids):
    """
    Stop the run unless every birth row can be published and subtracted.

    The exclusion is enforced upstream by BirthInformationSuppressionCtPlus, and fails
    open there: an empty lookup excludes nobody, and a birth row created after it ran,
    for example by MoveNLPtoDomains in a later stage, is never seen by it. So it is
    checked again here, where the rows become a deliverable. Every failure is reported
    together.

    :raises RuntimeError: on an empty lookup, or any birth row that belongs to an
        AIAN or pediatric participant, is not keyed to a CT+ research ID, or sits in
        a table the add-on cannot carry
    """
    for lookup_dataset, lookup_table, flag in [
        (rdr_sandbox_id, AIAN_LIST, '--rdr_sandbox_id'),
        (under18_lookup_dataset_id, UNDER18_PARTICIPANTS_LOOKUP_TABLE,
         '--under18_lookup_dataset_id'),
    ]:
        row = list(
            client.query(
                LOOKUP_COUNT.render(project=project_id,
                                    lookup_dataset=lookup_dataset,
                                    lookup_table=lookup_table)).result())[0]
        if not row['lookup_count']:
            raise RuntimeError(
                f'{lookup_dataset}.{lookup_table} is empty, so the exclusion check '
                f'would pass vacuously. Check {flag}.')

    failures = []
    for table, concept_columns in sorted({**carried, **uncarriable}.items()):
        row = list(
            client.query(
                CHECKS.render(project=project_id,
                              input_dataset=input_dataset_id,
                              table=table,
                              ids_dataset=ids_dataset_id,
                              ids_view=ids_view_id,
                              rdr_sandbox=rdr_sandbox_id,
                              aian_list=AIAN_LIST,
                              under18_dataset=under18_lookup_dataset_id,
                              under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                              dob_row=dob_row(concept_ids,
                                              concept_columns))).result())[0]

        if table in uncarriable and row['dob_rows']:
            failures.append(
                f'{row["dob_rows"]} birth rows sit in {table}, which the add-on '
                f'cannot carry, so the mainline assembly could not subtract them'
            )
        for column, reason in FAILURES.items():
            if row[column]:
                failures.append(f'{row[column]} birth rows in {table} {reason}')

    if failures:
        raise RuntimeError(f'{input_dataset_id} cannot be split: ' +
                           '; '.join(failures) + '.')


def split(client, project_id, input_dataset_id, output_dataset_id, carried,
          concept_ids):
    """
    Write every carried table's birth rows, then their _ext rows.

    Every carried table is written, empty or not, so the dataset has the same
    table shape whichever domains hold birth rows in a given release.

    :return: rows written per table, the set the mainline assembly subtracts
    """
    params = dict(project=project_id,
                  input_dataset=input_dataset_id,
                  output_dataset=output_dataset_id)

    counts = {}
    for table, concept_columns in sorted(carried.items()):
        query = DOB_ROWS.render(table=table,
                                dob_row=dob_row(concept_ids, concept_columns),
                                **params)
        counts[table] = write_table(client, project_id, input_dataset_id,
                                    output_dataset_id, table, query)

    input_tables = {
        table.table_id
        for table in client.list_tables(f'{project_id}.{input_dataset_id}')
    }
    for table in sorted(carried):
        ext_table = f'{table}{EXT_SUFFIX}'
        if ext_table not in input_tables:
            continue
        query = CHILD_ROWS.render(table=ext_table,
                                  key=f'{table}_id',
                                  parent=table,
                                  **params)
        counts[ext_table] = write_table(client, project_id, input_dataset_id,
                                        output_dataset_id, ext_table, query)

    for table, count in sorted(counts.items()):
        LOGGER.info(f'Claimed {count} rows of {table}')

    return counts


def main(project_id,
         release_tag,
         input_dataset_id,
         rdr_sandbox_id,
         under18_lookup_dataset_id,
         ct_plus_ids_view,
         ids_dataset_id=PIPELINE_TABLES):
    """
    Validate the input and write the birth rows into CP{release_tag}_dob.

    Both refusals run before the dataset is created, so a refused run leaves nothing
    behind.

    :return: rows written per table
    """
    validate_dataset_names(release_tag, input_dataset_id)
    output_dataset_id = get_ct_plus_addon_dataset_name(release_tag,
                                                       CT_PLUS_DOB_INDICATORS)

    client = BigQueryClient(project_id)
    concept_ids = get_dob_indicator_concept_ids()
    carried, uncarriable = classify_tables(client, project_id, input_dataset_id)

    assert_split_is_publishable(client, project_id, input_dataset_id,
                                rdr_sandbox_id, under18_lookup_dataset_id,
                                ids_dataset_id, ct_plus_ids_view, carried,
                                uncarriable, concept_ids)
    assert_output_dataset_is_empty(client, project_id, output_dataset_id)

    dataset = client.define_dataset(
        output_dataset_id,
        f'CT+ date-of-birth add-on dataset for release {release_tag}, with '
        f'indicators-of-birth rows split from {input_dataset_id}',
        {'ct_plus_component': CT_PLUS_DOB_INDICATORS})
    client.create_dataset(dataset, exists_ok=True)

    LOGGER.info(
        f'Splitting the indicators-of-birth rows from {input_dataset_id} '
        f'into {output_dataset_id}')
    return split(client, project_id, input_dataset_id, output_dataset_id,
                 carried, concept_ids)


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description=('Write the indicators-of-birth rows of the CT+ '
                     'date-of-birth add-on dataset.'))
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
    parser.add_argument('--rdr_sandbox_id',
                        required=True,
                        help=f'RDR stage sandbox holding {AIAN_LIST}')
    parser.add_argument(
        '--under18_lookup_dataset_id',
        required=True,
        help=f'Dataset holding {UNDER18_PARTICIPANTS_LOOKUP_TABLE}')
    parser.add_argument(
        '--ct_plus_ids_view',
        required=True,
        help=(f'View in {PIPELINE_TABLES} mapping participant_id to '
              f'controlled_tier_plus_id'))
    return parser


if __name__ == '__main__':
    pipeline_logging.configure(level=logging.INFO, add_console_handler=True)
    ARGS = get_arg_parser().parse_args()
    main(ARGS.project_id, ARGS.release_tag, ARGS.input_dataset_id,
         ARGS.rdr_sandbox_id, ARGS.under18_lookup_dataset_id,
         ARGS.ct_plus_ids_view)
