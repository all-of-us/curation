"""
Write the CT+ per-element add-on datasets out of the re-keyed CT+ dataset.

The CT+ rules leave expanded concept rows inline (ct_plus_expanded_concepts.py describes
how), so they reach the re-key output with every value-level transform applied and with
CT+ row and person IDs. This tool copies them into one dataset per data element,
CP{release_tag}_{data_element}, with their _ext rows, row IDs and person IDs unchanged.
It removes nothing and writes no mainline: the mainline assembly subtracts these
datasets from the same input by row ID.

What is claimed: the rows of each table a CT concept rule reaches that carry a concept
of the element, on the columns that rule reads. A concept in several elements is written
to each, so the datasets overlap and their union is what the mainline loses.

What is not:

- Parent rows. person, death, visit_occurrence, visit_detail, provider and care_site
  stay empty, since a concept add-on's rows resolve into the mainline. A table the
  add-on cannot carry must hold no expanded row outside the pediatric cohort, since the
  assembly could not subtract it; the run stops if one does.
- The pediatric cohort's rows. Their parents are not in the mainline, and pediatric
  values reach a study only through the pediatrics add-on, which already holds them
  whole.
- AIAN participants' rows. AianExpandedConceptSuppressionCtPlus removed them, and the
  run stops if any remain, since they would be published in the mainline.

Every dataset has the same table shape, whichever domains hold rows in a given release:
each OMOP table of the input that the mainline assembly can match, empty if need be.

The lookup is rebuilt in the sandbox from the input's vocabulary before anything else.
The check that the datasets account for every expanded row reads what was written, so
it runs last; a failure there names the datasets to remove before rerunning.

Usage:
  python split_ct_plus_expanded_concepts.py \
    --project_id <project> \
    --release_tag <e.g. 2025q4r7> \
    --input_dataset_id <CP{release_tag}_deid_clean_pre_split> \
    --cohort_dataset_id <controlled_plus deid stage sandbox> \
    --rdr_sandbox_id <RDR stage sandbox holding aian_list> \
    --ct_plus_ids_view <view in pipeline_tables mapping participant IDs to CT+ IDs>
"""
# Python imports
import argparse
import logging

# Project imports
from cdr_cleaner.cleaning_rules.deid.ct_plus_expanded_concepts import (
    build_expanded_lookup, expanded_row, get_expanded_tables, get_row_owner)
from common import (AIAN_LIST, AOU_DEATH, CARE_SITE, CDM_TABLES,
                    CT_PLUS_ELEMENT_DELTA, CT_PLUS_PEDIATRIC_COHORT,
                    CT_PLUS_PRE_SPLIT_SUFFIX, DEATH, EXT_SUFFIX, JINJA_ENV,
                    NOTE_NLP, PERSON, PIPELINE_TABLES, PROVIDER, SURVEY_CONDUCT,
                    VISIT_DETAIL, VISIT_OCCURRENCE,
                    get_ct_plus_addon_dataset_name)
from gcloud.bq import BigQueryClient
from resources import has_person_id
from tools.build_ct_plus_mainline import (get_subtraction_key,
                                          get_table_columns, get_views)
from tools.split_ct_plus_pediatric_dataset import (
    CHILD_ROWS, assert_cohort_is_usable, assert_output_dataset_is_empty,
    validate_dataset_names, write_table)
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

# Parent and reference tables. Other rows point at them, and a concept add-on never
# carries them.
UNCARRIABLE_TABLES = frozenset(
    [PERSON, DEATH, VISIT_OCCURRENCE, VISIT_DETAIL, PROVIDER, CARE_SITE])

# The OMOP tables every element dataset has, with their _ext tables
SHAPE_TABLES = frozenset(CDM_TABLES + [AOU_DEATH, SURVEY_CONDUCT])

LOOKUP_COUNT = JINJA_ENV.from_string("""
SELECT COUNT(*) AS lookup_count
FROM `{{project}}.{{rdr_sandbox}}.{{aian_list}}`
""")

UNCARRIABLE_ROWS = JINJA_ENV.from_string("""
SELECT COUNT(*) AS expanded_rows
FROM `{{project}}.{{input_dataset}}.{{table}}` t
WHERE {{expanded_row}}
{% if not_in_cohort %}  AND {{not_in_cohort}}{% endif %}
""")

# Each row is mapped back to its participant through the ids view, since aian_list is
# keyed by participant ID and the input by CT+ research ID.
CHECKS = JINJA_ENV.from_string("""
SELECT
  COUNTIF(v.controlled_tier_plus_id IS NULL) AS non_ct_plus_rows,
  COUNTIF(a.person_id IS NOT NULL) AS aian_rows
FROM `{{project}}.{{input_dataset}}.{{table}}` t
{{owner_join}}
LEFT JOIN (
  SELECT DISTINCT participant_id, controlled_tier_plus_id
  FROM `{{project}}.{{ids_dataset}}.{{ids_view}}`
  WHERE controlled_tier_plus_id IS NOT NULL
) v
  ON v.controlled_tier_plus_id = {{person_id}}
LEFT JOIN (
  SELECT DISTINCT person_id FROM `{{project}}.{{rdr_sandbox}}.{{aian_list}}`
) a
  ON a.person_id = v.participant_id
WHERE {{expanded_row}}
""")

NOT_IN_COHORT = JINJA_ENV.from_string("""
NOT EXISTS (
    SELECT 1
    FROM `{{project}}.{{cohort_dataset}}.{{cohort_table}}` c
    WHERE c.person_id = {{person_id}}
)""")

ELEMENT_ROWS = JINJA_ENV.from_string("""
SELECT t.*
FROM `{{project}}.{{input_dataset}}.{{table}}` t
{{owner_join}}
WHERE {{expanded_row}}
  AND {{not_in_cohort}}
""")

NO_ROWS = JINJA_ENV.from_string("""
SELECT t.*
FROM `{{project}}.{{input_dataset}}.{{table}}` t
WHERE FALSE
""")

# input_rows reads the input and claimed_rows the element datasets, so a row left behind
# shows up as a difference between the two. The datasets overlap by design, so
# claimed_rows counts a row once however many hold it, and repeated_rows is what catches
# a row written twice to one dataset.
COMPLETENESS = JINJA_ENV.from_string("""
SELECT
  (SELECT COUNT(*)
   FROM `{{project}}.{{input_dataset}}.{{table}}` t
   {{owner_join}}
   WHERE {{expanded_row}}
     AND {{not_in_cohort}}
  ) AS input_rows,
  (SELECT COUNT(DISTINCT {{key}})
   FROM (
{% for dataset in output_datasets %}
     SELECT {{key}} FROM `{{project}}.{{dataset}}.{{table}}`
{% if not loop.last %}     UNION ALL
{% endif %}
{% endfor %}
   )
  ) AS claimed_rows,
  (0
{% for dataset in output_datasets %}
   + (SELECT COUNT(*) - COUNT(DISTINCT {{key}})
      FROM `{{project}}.{{dataset}}.{{table}}`)
{% endfor %}
  ) AS repeated_rows
""")

FAILURES = {
    'non_ct_plus_rows': 'have a person_id that is not a CT+ research ID',
    'aian_rows': 'belong to AIAN participants',
}


def classify_tables(input_columns, views):
    """
    Sort the expanded tables the input holds by whether an add-on can carry their rows.

    :param input_columns: {input table: [columns]}
    :param views: names of the input's views
    :return: (carried, uncarriable), each a dict of table name to the concept_id columns
        the input holds. carried tables have their own row ID and a way to reach the
        participant; uncarriable ones cannot be claimed.
    """
    carried, uncarriable = {}, {}
    for table, concept_fields in get_expanded_tables().items():
        if table not in input_columns or table in views:
            continue

        columns = input_columns[table]
        present = [field for field in concept_fields if field in columns]
        if not present:
            continue

        has_owner = 'person_id' in columns or table == NOTE_NLP
        has_row_id = f'{table}_id' in columns
        if table in UNCARRIABLE_TABLES or not has_owner or not has_row_id:
            uncarriable[table] = present
        else:
            carried[table] = present

    return carried, uncarriable


def get_shape_tables(input_columns, views):
    """
    :param input_columns: {input table: [columns]}
    :param views: names of the input's views
    :return: sorted tables every element dataset has. A table the mainline assembly has
        no key for is left out, since it refuses any add-on holding one.
    """
    tables = []
    for table, columns in input_columns.items():
        base = (table[:-len(EXT_SUFFIX)]
                if table.endswith(EXT_SUFFIX) else table)
        if table in views or base not in SHAPE_TABLES:
            continue
        try:
            get_subtraction_key(table, columns)
        except RuntimeError:
            continue
        tables.append(table)
    return sorted(tables)


def assert_split_is_publishable(client, project_id, input_dataset_id,
                                sandbox_dataset_id, rdr_sandbox_id,
                                ids_dataset_id, ids_view_id, carried,
                                uncarriable):
    """
    Stop the run unless every expanded row can be published and subtracted.

    AianExpandedConceptSuppressionCtPlus fails open: an empty aian_list removes nobody.
    So AIAN is checked again here, where the rows become a deliverable. Every failure is
    reported together.

    A pediatric row in a table the add-on cannot carry is not a failure: the pediatrics
    add-on claims every table with a person_id, so the assembly subtracts it through
    that dataset.

    :raises RuntimeError: on an empty aian_list, or any expanded row that belongs to an
        AIAN participant, is not keyed to a CT+ research ID, or sits outside the
        pediatric cohort in a table the add-on cannot carry
    """
    row = list(
        client.query(
            LOOKUP_COUNT.render(project=project_id,
                                rdr_sandbox=rdr_sandbox_id,
                                aian_list=AIAN_LIST)).result())[0]
    if not row['lookup_count']:
        raise RuntimeError(
            f'{rdr_sandbox_id}.{AIAN_LIST} is empty, so the exclusion check would pass '
            f'vacuously. Check --rdr_sandbox_id.')

    failures = []
    for table, concept_fields in sorted(uncarriable.items()):
        row = list(
            client.query(
                UNCARRIABLE_ROWS.render(
                    project=project_id,
                    input_dataset=input_dataset_id,
                    table=table,
                    expanded_row=expanded_row(project_id, sandbox_dataset_id,
                                              table, concept_fields, 't'),
                    not_in_cohort=not_in_cohort(project_id, sandbox_dataset_id,
                                                't.person_id')
                    if has_person_id(table) else None)).result())[0]
        if row['expanded_rows']:
            failures.append(
                f'{row["expanded_rows"]} expanded rows sit in {table}, which an '
                f'add-on cannot carry, so the mainline assembly could not subtract '
                f'them')

    for table, concept_fields in sorted(carried.items()):
        owner_join, person_id = get_row_owner(project_id, input_dataset_id,
                                              table, 't')
        row = list(
            client.query(
                CHECKS.render(project=project_id,
                              input_dataset=input_dataset_id,
                              table=table,
                              owner_join=owner_join,
                              person_id=person_id,
                              ids_dataset=ids_dataset_id,
                              ids_view=ids_view_id,
                              rdr_sandbox=rdr_sandbox_id,
                              aian_list=AIAN_LIST,
                              expanded_row=expanded_row(project_id,
                                                        sandbox_dataset_id,
                                                        table, concept_fields,
                                                        't'))).result())[0]
        for column, reason in FAILURES.items():
            if row[column]:
                failures.append(
                    f'{row[column]} expanded rows in {table} {reason}')

    if failures:
        raise RuntimeError(f'{input_dataset_id} cannot be split: ' +
                           '; '.join(failures) + '.')


def not_in_cohort(project_id, cohort_dataset_id, person_id):
    """
    :return: the predicate leaving out the pediatric cohort's rows
    """
    return NOT_IN_COHORT.render(project=project_id,
                                cohort_dataset=cohort_dataset_id,
                                cohort_table=CT_PLUS_PEDIATRIC_COHORT,
                                person_id=person_id)


def write_element(client, project_id, input_dataset_id, sandbox_dataset_id,
                  output_dataset_id, data_element, carried, shape_tables):
    """
    Write one element dataset: its rows, then their _ext rows, then the empty tables.

    Order matters: the _ext selections read the claimed rows back out of the output.

    :return: rows written per table
    """
    params = dict(project=project_id,
                  input_dataset=input_dataset_id,
                  output_dataset=output_dataset_id)

    counts = {}
    for table, concept_fields in sorted(carried.items()):
        owner_join, person_id = get_row_owner(project_id, input_dataset_id,
                                              table, 't')
        query = ELEMENT_ROWS.render(
            table=table,
            owner_join=owner_join,
            expanded_row=expanded_row(project_id, sandbox_dataset_id, table,
                                      concept_fields, 't', data_element),
            not_in_cohort=not_in_cohort(project_id, sandbox_dataset_id,
                                        person_id),
            **params)
        counts[table] = write_table(client, project_id, input_dataset_id,
                                    output_dataset_id, table, query)

    for table in sorted(carried):
        ext_table = f'{table}{EXT_SUFFIX}'
        if ext_table not in shape_tables:
            continue
        query = CHILD_ROWS.render(table=ext_table,
                                  key=f'{table}_id',
                                  parent=table,
                                  **params)
        counts[ext_table] = write_table(client, project_id, input_dataset_id,
                                        output_dataset_id, ext_table, query)

    for table in shape_tables:
        if table not in counts:
            counts[table] = write_table(client, project_id, input_dataset_id,
                                        output_dataset_id, table,
                                        NO_ROWS.render(table=table, **params))

    return counts


def assert_nothing_left_behind(client, project_id, input_dataset_id,
                               sandbox_dataset_id, output_dataset_ids, carried):
    """
    Stop the run unless the element datasets hold every expanded row of the input.

    The mainline assembly checks that what the add-ons hold is what it removes. This is
    the other direction: a row nothing claimed passes that check and is published.

    :raises RuntimeError: naming every table that disagrees and the datasets to remove
    """
    failures = []
    for table, concept_fields in sorted(carried.items()):
        owner_join, person_id = get_row_owner(project_id, input_dataset_id,
                                              table, 't')
        row = list(
            client.query(
                COMPLETENESS.render(
                    project=project_id,
                    input_dataset=input_dataset_id,
                    table=table,
                    owner_join=owner_join,
                    expanded_row=expanded_row(project_id, sandbox_dataset_id,
                                              table, concept_fields, 't'),
                    not_in_cohort=not_in_cohort(project_id, sandbox_dataset_id,
                                                person_id),
                    key=f'{table}_id',
                    output_datasets=output_dataset_ids)).result())[0]
        if row['input_rows'] != row['claimed_rows']:
            failures.append(
                f'{table}: {row["input_rows"]} expanded rows in the '
                f'input, {row["claimed_rows"]} in the add-ons')
        if row['repeated_rows']:
            failures.append(
                f'{table}: {row["repeated_rows"]} rows written more than once to '
                f'one add-on')

    if failures:
        raise RuntimeError(
            'The element datasets do not account for every expanded row: ' +
            '; '.join(failures) +
            f'. Remove {sorted(output_dataset_ids)} before rerunning.')


def main(project_id,
         release_tag,
         input_dataset_id,
         cohort_dataset_id,
         rdr_sandbox_id,
         ct_plus_ids_view,
         ids_dataset_id=PIPELINE_TABLES):
    """
    Validate the input and write one dataset per data element.

    Every refusal but the last runs before a dataset is created, so a refused run leaves
    nothing behind but the sandbox lookup.

    :return: {data element: {table: rows written}}
    """
    validate_dataset_names(release_tag, input_dataset_id)

    client = BigQueryClient(project_id)
    assert_cohort_is_usable(client, project_id, cohort_dataset_id,
                            input_dataset_id)

    data_elements = build_expanded_lookup(client, project_id, input_dataset_id,
                                          cohort_dataset_id)
    output_dataset_ids = {
        data_element:
            get_ct_plus_addon_dataset_name(release_tag, CT_PLUS_ELEMENT_DELTA,
                                           data_element)
        for data_element in data_elements
    }

    input_columns = get_table_columns(client, project_id, input_dataset_id)
    views = get_views(client, project_id, input_dataset_id)
    carried, uncarriable = classify_tables(input_columns, views)
    shape_tables = get_shape_tables(input_columns, views)

    assert_split_is_publishable(client, project_id, input_dataset_id,
                                cohort_dataset_id, rdr_sandbox_id,
                                ids_dataset_id, ct_plus_ids_view, carried,
                                uncarriable)
    for output_dataset_id in output_dataset_ids.values():
        assert_output_dataset_is_empty(client, project_id, output_dataset_id)

    counts = {}
    for data_element, output_dataset_id in output_dataset_ids.items():
        dataset = client.define_dataset(
            output_dataset_id,
            f'CT+ {data_element} add-on dataset for release {release_tag}, split '
            f'from {input_dataset_id}', {
                'ct_plus_component': CT_PLUS_ELEMENT_DELTA,
                'data_element': data_element
            })
        client.create_dataset(dataset, exists_ok=True)

        LOGGER.info(f'Splitting {data_element} from {input_dataset_id} into '
                    f'{output_dataset_id}')
        counts[data_element] = write_element(client, project_id,
                                             input_dataset_id,
                                             cohort_dataset_id,
                                             output_dataset_id, data_element,
                                             carried, shape_tables)
        for table, count in sorted(counts[data_element].items()):
            if count:
                LOGGER.info(f'{data_element}: claimed {count} rows of {table}')

    assert_nothing_left_behind(client, project_id, input_dataset_id,
                               cohort_dataset_id,
                               list(output_dataset_ids.values()), carried)

    LOGGER.info('Pass these to the mainline assembly: '
                f'{" ".join(sorted(output_dataset_ids.values()))}')
    return counts


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description='Write the CT+ per-element add-on datasets.')
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
        '--cohort_dataset_id',
        required=True,
        help=(f'The controlled_plus deid stage sandbox, which holds '
              f'{CT_PLUS_PEDIATRIC_COHORT} and receives the concept lookup'))
    parser.add_argument('--rdr_sandbox_id',
                        required=True,
                        help=f'RDR stage sandbox holding {AIAN_LIST}')
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
         ARGS.cohort_dataset_id, ARGS.rdr_sandbox_id, ARGS.ct_plus_ids_view)
