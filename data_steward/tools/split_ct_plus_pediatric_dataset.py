"""
Write the CT+ pediatrics add-on dataset out of the re-keyed CT+ dataset.

The CT+ run retains the '0-6' band, so the pediatric cohort's records sit inline in
the re-key output alongside everyone else's. This tool copies what the cohort claims
into CP{release_tag}_pediatrics with row IDs and person IDs unchanged. It removes
nothing and writes no mainline: the mainline assembly subtracts this dataset from the
same input by row ID, so what is claimed here is exactly what it removes.

What is claimed:

- Every table with a person_id column, restricted to the cohort. That includes person
  and visit_occurrence: participants aged 0 to 6 are absent from the base, so the
  dataset carries their own rows rather than resolving into it.
- The guardian-about observation records. They are keyed to the linked adult's
  person_id, so the cohort predicate misses them; they are selected through the
  pediatric survey_conduct row their questionnaire_response_id points at.
- Rows that identify a claimed row without carrying person_id: the _ext rows of claimed
  rows, note_nlp rows of claimed notes, and fact_relationship rows with either side
  naming a claimed row, which includes the person domain adult-child linkage.
- pediatric_relationship_ext, whole, since every row in it is pediatric.

What is not: provider, care_site and location. They are shared reference data that
adult records point at too, so they stay in the base and a pediatric foreign key
resolves there. No adult person row is written either, since the base supplies it.

The cohort comes from ct_plus_pediatric_cohort in the controlled_plus deid stage
sandbox, written by CaptureCtPlusPediatricCohort and converted to CT+ research IDs by
ConvertCtPlusPediatricCohortIds.

Usage:
  python split_ct_plus_pediatric_dataset.py \
    --project_id <project> \
    --release_tag <e.g. 2025q4r7> \
    --input_dataset_id <CP{release_tag}_deid_clean_pre_split> \
    --cohort_dataset_id <controlled_plus deid stage sandbox>
"""
# Python imports
import argparse
import logging

# Third party imports
from google.cloud.bigquery import QueryJobConfig, Table

# Project imports
from common import (CONDITION_OCCURRENCE, CT_PLUS_PEDIATRIC_COHORT,
                    CT_PLUS_PEDIATRICS, CT_PLUS_PRE_SPLIT_SUFFIX, DRUG_EXPOSURE,
                    EXT_SUFFIX, FACT_RELATIONSHIP, JINJA_ENV, MEASUREMENT, NOTE,
                    NOTE_NLP, OBSERVATION, PEDIATRIC_RELATIONSHIP_EXT, PERSON,
                    PROCEDURE_OCCURRENCE, SURVEY_CONDUCT, CONTROLLED_PLUS,
                    TIER_DATASET_PREFIX, get_ct_plus_addon_dataset_name)
from gcloud.bq import BigQueryClient
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

# fact_relationship qualifies each fact id with a domain concept id. These are the
# domains whose rows this tool can claim; a side naming any other domain, such as
# care site 57, never makes a row pediatric.
FACT_DOMAIN_TO_TABLE = {
    10: PROCEDURE_OCCURRENCE,
    13: DRUG_EXPOSURE,
    19: CONDITION_OCCURRENCE,
    21: MEASUREMENT,
    27: OBSERVATION,
    56: PERSON,
}

PERSON_KEYED_TABLES = JINJA_ENV.from_string("""
SELECT table_name
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
WHERE column_name = 'person_id'
  AND NOT STARTS_WITH(table_name, '_')
ORDER BY table_name
""")

COHORT_OVERLAP = JINJA_ENV.from_string("""
SELECT
  COUNT(DISTINCT c.person_id) AS cohort_count,
  COUNT(DISTINCT p.person_id) AS matched_count
FROM `{{project}}.{{cohort_dataset}}.{{cohort_table}}` c
LEFT JOIN `{{project}}.{{input_dataset}}.person` p
  USING (person_id)
""")

COHORT = ('SELECT person_id FROM '
          '`{{project}}.{{cohort_dataset}}.{{cohort_table}}`')

PERSON_KEYED_ROWS = JINJA_ENV.from_string("""
SELECT t.*
FROM `{{project}}.{{input_dataset}}.{{table}}` t
WHERE t.person_id IN (""" + COHORT + """)
{% if guardian_about %}
  -- Guardian-about records are keyed to the adult, so they are found through the --
  -- pediatric survey_conduct row they belong to. --
  OR t.questionnaire_response_id IN (
    SELECT survey_conduct_id
    FROM `{{project}}.{{input_dataset}}.{{survey_conduct}}`
    WHERE person_id IN (""" + COHORT + """)
  )
{% endif %}
""")

# Selected against what was already written to the output, so an _ext or note_nlp row
# follows exactly the rows claimed, guardian-about records included.
CHILD_ROWS = JINJA_ENV.from_string("""
SELECT c.*
FROM `{{project}}.{{input_dataset}}.{{table}}` c
WHERE c.{{key}} IN (
  SELECT {{key}} FROM `{{project}}.{{output_dataset}}.{{parent}}`
)
""")

FACT_RELATIONSHIP_ROWS = JINJA_ENV.from_string("""
SELECT fr.*
FROM `{{project}}.{{input_dataset}}.{{fact_relationship}}` fr
WHERE FALSE
{% for side in ['1', '2'] %}{% for domain_concept_id, table in claimed_domains %}
  OR (fr.domain_concept_id_{{side}} = {{domain_concept_id}}
      AND fr.fact_id_{{side}} IN (
        SELECT {{table}}_id FROM `{{project}}.{{output_dataset}}.{{table}}`))
{% endfor %}{% endfor %}
""")

WHOLE_TABLE = JINJA_ENV.from_string("""
SELECT * FROM `{{project}}.{{input_dataset}}.{{table}}`
""")


def validate_dataset_names(release_tag, input_dataset_id):
    """
    Stop the run unless the input is this release's pre-split re-key output.

    The pre-split dataset still holds every add-on component inline, and it is the one
    input every producer and the mainline assembly share. Reading a published dataset
    or another release's input would split the wrong rows with nothing erroring.

    :param release_tag: release tag, e.g. '2025q4r7'
    :param input_dataset_id: dataset the split reads
    :raises ValueError: if the input is not CP{release_tag}_..._pre_split
    """
    prefix = f'{TIER_DATASET_PREFIX[CONTROLLED_PLUS]}{release_tag}_'
    if not (input_dataset_id.startswith(prefix) and
            input_dataset_id.endswith(CT_PLUS_PRE_SPLIT_SUFFIX)):
        raise ValueError(
            f'--input_dataset_id {input_dataset_id} must start with {prefix} and end '
            f'with {CT_PLUS_PRE_SPLIT_SUFFIX}: the split reads this release\'s re-key '
            f'output, which still holds every add-on component inline.')


def assert_cohort_is_usable(client, project_id, cohort_dataset_id,
                            input_dataset_id):
    """
    Stop the run unless the cohort is converted, non-empty, and in the input's ID space.

    Each failure would otherwise produce a pediatrics dataset that is empty or wrong
    while the pediatric records ship inside the mainline:

    - No person_id column: ConvertCtPlusPediatricCohortIds did not run, so the table
      still holds pre-deid participant IDs.
    - No rows: the capture found nobody, which is what an age-at-consent derivation
      that misses the pediatric cohort looks like.
    - No cohort member in the input's person table: the cohort is in a different ID
      space from the input, most likely a different release's.

    A partial overlap is logged rather than refused, since the pipeline can drop a
    participant after the capture ran, for example one left with no records.

    :raises RuntimeError: on any of the three failures above
    """
    cohort = client.get_table(
        f'{project_id}.{cohort_dataset_id}.{CT_PLUS_PEDIATRIC_COHORT}')
    if 'person_id' not in [field.name for field in cohort.schema]:
        raise RuntimeError(
            f'{cohort_dataset_id}.{CT_PLUS_PEDIATRIC_COHORT} has no person_id column, '
            f'so ConvertCtPlusPediatricCohortIds has not run and it still holds '
            f'pre-deid participant IDs.')

    row = list(
        client.query(
            COHORT_OVERLAP.render(project=project_id,
                                  cohort_dataset=cohort_dataset_id,
                                  cohort_table=CT_PLUS_PEDIATRIC_COHORT,
                                  input_dataset=input_dataset_id)).result())[0]
    cohort_count, matched_count = row['cohort_count'], row['matched_count']

    if not cohort_count:
        raise RuntimeError(
            f'{cohort_dataset_id}.{CT_PLUS_PEDIATRIC_COHORT} is empty. The CT+ run '
            f'retained no pediatric participant, so this split would write an empty '
            f'dataset while any pediatric records present ship in the mainline.'
        )
    if not matched_count:
        raise RuntimeError(
            f'None of the {cohort_count} cohort members is in '
            f'{input_dataset_id}.person, so the cohort and the input are in different '
            f'ID spaces. Check that both come from the same release.')
    if matched_count < cohort_count:
        LOGGER.warning(
            f'{cohort_count - matched_count} of {cohort_count} cohort members have no '
            f'person row in {input_dataset_id} and contribute nothing.')
    else:
        LOGGER.info(
            f'All {cohort_count} cohort members are in {input_dataset_id}.')


def assert_output_dataset_is_empty(client, project_id, output_dataset_id):
    """
    Stop the run if the output dataset already holds tables.

    Every table is written with WRITE_EMPTY semantics, so a rerun into a populated
    dataset would fail part way and leave it half written. Nothing here deletes a
    table; clearing a previous attempt is the operator's decision.

    :raises RuntimeError: if the dataset exists and holds tables
    """
    try:
        existing = [
            table.table_id
            for table in client.list_tables(f'{project_id}.{output_dataset_id}')
        ]
    except Exception:
        # No dataset yet. It is created before anything is written.
        return

    if existing:
        raise RuntimeError(
            f'{output_dataset_id} already holds {len(existing)} tables, for example '
            f'{sorted(existing)[:5]}. Remove them before rerunning the split.')


def get_person_keyed_tables(client, project_id, input_dataset_id):
    """
    :return: names of the input tables carrying a person_id column
    """
    query = PERSON_KEYED_TABLES.render(project=project_id,
                                       dataset=input_dataset_id)
    return [row['table_name'] for row in client.query(query).result()]


def write_table(client, project_id, input_dataset_id, output_dataset_id, table,
                query):
    """
    Create the output table with the input table's schema and fill it from query.

    The table is created first rather than by the query so its schema, clustering and
    partitioning match the input's exactly.

    :return: number of rows written
    """
    source = client.get_table(f'{project_id}.{input_dataset_id}.{table}')
    destination = Table(f'{project_id}.{output_dataset_id}.{table}',
                        schema=source.schema)
    destination.clustering_fields = source.clustering_fields
    destination.time_partitioning = source.time_partitioning
    client.create_table(destination)

    job = client.query(query,
                       job_config=QueryJobConfig(
                           destination=destination,
                           write_disposition='WRITE_APPEND'))
    job.result()
    return client.get_table(destination).num_rows


def split(client, project_id, input_dataset_id, cohort_dataset_id,
          output_dataset_id):
    """
    Write every row the pediatric cohort claims into the output dataset.

    Order matters: the person-keyed tables are written first, because the _ext,
    note_nlp and fact_relationship selections read the claimed rows back out of the
    output dataset.

    :return: rows written per table, the set the mainline assembly subtracts
    """
    params = dict(project=project_id,
                  input_dataset=input_dataset_id,
                  output_dataset=output_dataset_id,
                  cohort_dataset=cohort_dataset_id,
                  cohort_table=CT_PLUS_PEDIATRIC_COHORT)

    input_tables = {
        table.table_id
        for table in client.list_tables(f'{project_id}.{input_dataset_id}')
    }
    person_keyed = get_person_keyed_tables(client, project_id, input_dataset_id)
    guardian_about = SURVEY_CONDUCT in input_tables

    counts = {}
    for table in person_keyed:
        query = PERSON_KEYED_ROWS.render(
            table=table,
            guardian_about=(table == OBSERVATION and guardian_about),
            survey_conduct=SURVEY_CONDUCT,
            **params)
        counts[table] = write_table(client, project_id, input_dataset_id,
                                    output_dataset_id, table, query)

    # Before the _ext loop, so note_nlp_ext can follow the claimed note_nlp rows
    if NOTE_NLP in input_tables and NOTE in counts:
        query = CHILD_ROWS.render(table=NOTE_NLP,
                                  key='note_id',
                                  parent=NOTE,
                                  **params)
        counts[NOTE_NLP] = write_table(client, project_id, input_dataset_id,
                                       output_dataset_id, NOTE_NLP, query)

    # _ext tables without person_id follow their base table's claimed rows
    for table in sorted(input_tables):
        base = table[:-len(EXT_SUFFIX)]
        if (not table.endswith(EXT_SUFFIX) or table in person_keyed or
                base not in counts):
            continue
        query = CHILD_ROWS.render(table=table,
                                  key=f'{base}_id',
                                  parent=base,
                                  **params)
        counts[table] = write_table(client, project_id, input_dataset_id,
                                    output_dataset_id, table, query)

    if FACT_RELATIONSHIP in input_tables:
        claimed_domains = [
            (domain_concept_id, table)
            for domain_concept_id, table in FACT_DOMAIN_TO_TABLE.items()
            if table in counts
        ]
        query = FACT_RELATIONSHIP_ROWS.render(
            fact_relationship=FACT_RELATIONSHIP,
            claimed_domains=claimed_domains,
            **params)
        counts[FACT_RELATIONSHIP] = write_table(client, project_id,
                                                input_dataset_id,
                                                output_dataset_id,
                                                FACT_RELATIONSHIP, query)

    if PEDIATRIC_RELATIONSHIP_EXT in input_tables:
        query = WHOLE_TABLE.render(table=PEDIATRIC_RELATIONSHIP_EXT, **params)
        counts[PEDIATRIC_RELATIONSHIP_EXT] = write_table(
            client, project_id, input_dataset_id, output_dataset_id,
            PEDIATRIC_RELATIONSHIP_EXT, query)
    else:
        LOGGER.warning(
            f'{PEDIATRIC_RELATIONSHIP_EXT} is not in {input_dataset_id}, so the '
            f'pediatrics dataset carries no researcher-facing relationship table.'
        )

    for table, count in sorted(counts.items()):
        LOGGER.info(f'Claimed {count} rows of {table}')

    return counts


def main(project_id, release_tag, input_dataset_id, cohort_dataset_id):
    """
    Validate the inputs and write CP{release_tag}_pediatrics.

    :return: rows written per table
    """
    validate_dataset_names(release_tag, input_dataset_id)
    output_dataset_id = get_ct_plus_addon_dataset_name(release_tag,
                                                       CT_PLUS_PEDIATRICS)

    client = BigQueryClient(project_id)
    assert_cohort_is_usable(client, project_id, cohort_dataset_id,
                            input_dataset_id)
    assert_output_dataset_is_empty(client, project_id, output_dataset_id)

    dataset = client.define_dataset(
        output_dataset_id,
        f'CT+ pediatrics add-on dataset for release {release_tag}, split from '
        f'{input_dataset_id}', {'ct_plus_component': CT_PLUS_PEDIATRICS})
    client.create_dataset(dataset, exists_ok=True)

    LOGGER.info(f'Splitting the pediatric cohort from {input_dataset_id} into '
                f'{output_dataset_id}')
    return split(client, project_id, input_dataset_id, cohort_dataset_id,
                 output_dataset_id)


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description='Write the CT+ pediatrics add-on dataset.')
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
              f'{CT_PLUS_PEDIATRIC_COHORT}'))
    return parser


if __name__ == '__main__':
    pipeline_logging.configure(level=logging.INFO, add_console_handler=True)
    ARGS = get_arg_parser().parse_args()
    main(ARGS.project_id, ARGS.release_tag, ARGS.input_dataset_id,
         ARGS.cohort_dataset_id)
