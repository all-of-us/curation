"""
Add the captured dates of birth to the CT+ date-of-birth add-on dataset.

CaptureCtPlusBirthdate copies each eligible participant's birth_datetime,
month_of_birth and day_of_birth into the controlled_plus deid stage sandbox before
NullPersonBirthdate nulls them; PruneCtPlusBirthdate and ConvertCtPlusBirthdateIds
then reconcile the table with person and re-key it to CT+ research IDs. That sandbox
is not a deliverable, so this tool promotes the table into CP{release_tag}_dob as dob.

split_ct_plus_dob_indicators.py creates CP{release_tag}_dob and writes its
indicators-of-birth event tables, so this tool runs after it: it refuses if the
dataset is missing or holds no tables, and adds its one table without touching the
others. The split in turn refuses a dataset that already holds tables, so the order
is enforced from both sides.

This is a promotion, not a split: nothing is subtracted from the mainline, because
NullPersonBirthdate already removed these values from it. person, visit_occurrence,
provider and care_site are not written; every person_id resolves in the base.

Each property checked before writing is already guaranteed by a capture rule, and each
fails open there if it regresses: an empty AIAN or under-18 lookup satisfies its
exclusion, and an UPDATE that matches nothing leaves a CT research ID in place. So the
run stops here, where the rows become a deliverable, rather than warning.

Usage:
  python build_ct_plus_dob_dataset.py \
    --project_id <project> \
    --release_tag <e.g. 2025q4r7> \
    --input_dataset_id <CP{release_tag}_deid_clean_pre_split> \
    --capture_dataset_id <controlled_plus deid stage sandbox> \
    --rdr_sandbox_id <RDR stage sandbox, holding aian_list> \
    --under18_lookup_dataset_id <RDR stage sandbox, holding _under18_participants> \
    --ct_plus_ids_view rdr_participant_research_ids_view
"""
# Python imports
import argparse
import logging

# Third party imports
from google.api_core.exceptions import NotFound

# Project imports
from common import (AIAN_LIST, CT_PLUS_BIRTHDATE_TABLE, CT_PLUS_DOB_INDICATORS,
                    CT_PLUS_PRE_SPLIT_SUFFIX, JINJA_ENV, PERSON,
                    PIPELINE_TABLES, UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                    get_ct_plus_addon_dataset_name)
from gcloud.bq import BigQueryClient
from tools.build_ct_plus_zip5_dataset import (assert_lookups_are_populated,
                                              validate_dataset_names)
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

# The researcher-facing table name inside CP{release_tag}_dob. It sits beside the
# split's tables, which are named after CDM tables, so it cannot collide with them.
DELIVERED_TABLE = 'dob'

# Listed rather than SELECT *, so a column added to the capture is not published
# without a decision. year_of_birth is absent: the mainline already publishes it.
DELIVERED_COLUMNS = [
    'person_id', 'birth_datetime', 'month_of_birth', 'day_of_birth'
]

# Each joined set is made distinct first, so no join can multiply capture rows and
# the duplicate count reads the capture alone.
CAPTURE_CHECKS = JINJA_ENV.from_string("""
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) - COUNT(DISTINCT b.person_id) AS duplicate_rows,
  COUNTIF(p.person_id IS NULL) AS unresolved_rows,
  COUNTIF(c.person_id IS NULL) AS non_ct_plus_rows,
  COUNTIF(a.person_id IS NOT NULL) AS aian_rows,
  COUNTIF(u.person_id IS NOT NULL) AS pediatric_rows,
  COUNTIF(b.birth_datetime IS NULL) AS missing_birth_datetime_rows
FROM `{{project}}.{{capture_dataset}}.{{capture_table}}` b
LEFT JOIN (
  SELECT DISTINCT person_id
  FROM `{{project}}.{{input_dataset}}.{{person}}`
) p
  ON p.person_id = b.person_id
LEFT JOIN (
  SELECT DISTINCT controlled_tier_plus_id AS person_id
  FROM `{{project}}.{{ids_dataset}}.{{ids_view}}`
  WHERE controlled_tier_plus_id IS NOT NULL
) c
  ON c.person_id = b.person_id
LEFT JOIN (
  -- aian_list is keyed by participant ID, so it is mapped to CT+ IDs --
  SELECT DISTINCT v.controlled_tier_plus_id AS person_id
  FROM `{{project}}.{{rdr_sandbox}}.{{aian_list}}` al
  JOIN `{{project}}.{{ids_dataset}}.{{ids_view}}` v
    ON v.participant_id = al.person_id
  WHERE v.controlled_tier_plus_id IS NOT NULL
) a
  ON a.person_id = b.person_id
LEFT JOIN (
  -- _under18_participants is keyed by participant ID too --
  SELECT DISTINCT v.controlled_tier_plus_id AS person_id
  FROM `{{project}}.{{under18_dataset}}.{{under18_table}}` un
  JOIN `{{project}}.{{ids_dataset}}.{{ids_view}}` v
    ON v.participant_id = un.person_id
  WHERE v.controlled_tier_plus_id IS NOT NULL
) u
  ON u.person_id = b.person_id
""")

COVERAGE = JINJA_ENV.from_string("""
SELECT
  COUNT(*) AS input_participants,
  COUNTIF(b.person_id IS NULL) AS without_dob
FROM `{{project}}.{{input_dataset}}.{{person}}` p
LEFT JOIN (
  SELECT DISTINCT person_id
  FROM `{{project}}.{{capture_dataset}}.{{capture_table}}`
) b
  ON b.person_id = p.person_id
""")

WRITE_DELIVERED_TABLE = JINJA_ENV.from_string("""
CREATE TABLE `{{project}}.{{output_dataset}}.{{delivered_table}}` AS
SELECT {{columns | join(', ')}}
FROM `{{project}}.{{capture_dataset}}.{{capture_table}}`
""")


def assert_capture_is_publishable(client, project_id, input_dataset_id,
                                  capture_dataset_id, rdr_sandbox_id,
                                  under18_lookup_dataset_id, ids_dataset_id,
                                  ids_view_id):
    """
    Stop the run unless every captured row is safe to publish.

    - aian_list and _under18_participants are non-empty, since an empty lookup
      satisfies its exclusion vacuously.
    - The capture is non-empty.
    - At most one row per participant.
    - Every person_id resolves in the input's person table.
    - Every person_id is a CT+ research ID, so a row ConvertCtPlusBirthdateIds left
      on its CT research ID cannot ship. As in the Zip5 tool, this assumes no value
      is both a CT and a CT+ research ID in the ids view.
    - No AIAN participant.
    - No pediatric participant, since pediatric dates of birth reach a study only
      through the pediatrics add-on.
    - Every row carries a birth_datetime, so a capture that ran after
      NullPersonBirthdate cannot publish nulls as dates of birth.

    :return: rows in the capture table
    :raises RuntimeError: listing every failed property
    """
    assert_lookups_are_populated(client, project_id, rdr_sandbox_id,
                                 under18_lookup_dataset_id)

    row = list(
        client.query(
            CAPTURE_CHECKS.render(
                project=project_id,
                input_dataset=input_dataset_id,
                capture_dataset=capture_dataset_id,
                capture_table=CT_PLUS_BIRTHDATE_TABLE,
                rdr_sandbox=rdr_sandbox_id,
                aian_list=AIAN_LIST,
                under18_dataset=under18_lookup_dataset_id,
                under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                ids_dataset=ids_dataset_id,
                ids_view=ids_view_id,
                person=PERSON)).result())[0]
    source = f'{capture_dataset_id}.{CT_PLUS_BIRTHDATE_TABLE}'

    if not row['total_rows']:
        raise RuntimeError(
            f'{source} is empty, so the capture found nobody or the prune removed '
            f'everyone.')

    failures = []
    if row['duplicate_rows']:
        failures.append(f'{row["duplicate_rows"]} rows repeat a participant')
    if row['unresolved_rows']:
        failures.append(f'{row["unresolved_rows"]} rows have no person row in '
                        f'{input_dataset_id}')
    if row['non_ct_plus_rows']:
        failures.append(
            f'{row["non_ct_plus_rows"]} rows carry a person_id that is not a CT+ '
            f'research ID in {ids_dataset_id}.{ids_view_id}, most likely a CT '
            f'research ID ConvertCtPlusBirthdateIds did not convert')
    if row['aian_rows']:
        failures.append(f'{row["aian_rows"]} rows belong to AIAN participants')
    if row['pediatric_rows']:
        failures.append(
            f'{row["pediatric_rows"]} rows belong to pediatric participants')
    if row['missing_birth_datetime_rows']:
        failures.append(f'{row["missing_birth_datetime_rows"]} rows carry no '
                        f'birth_datetime')

    if failures:
        raise RuntimeError(f'{source} is not publishable: ' +
                           '; '.join(failures) + '.')

    LOGGER.info(f'All {row["total_rows"]} rows in {source} passed the checks.')
    return row['total_rows']


def log_coverage(client, project_id, input_dataset_id, capture_dataset_id):
    """
    Log how many input participants carry no captured date of birth.

    It separates a partial capture from a complete one, but it is a reading, not a
    threshold: participants with no birth_datetime, and the AIAN and pediatric
    participants the capture excludes, legitimately have none.
    """
    row = list(
        client.query(
            COVERAGE.render(project=project_id,
                            input_dataset=input_dataset_id,
                            capture_dataset=capture_dataset_id,
                            capture_table=CT_PLUS_BIRTHDATE_TABLE,
                            person=PERSON)).result())[0]
    LOGGER.info(
        f'{row["without_dob"]} of {row["input_participants"]} participants in '
        f'{input_dataset_id} have no captured date of birth.')


def assert_output_dataset_is_ready(client, project_id, output_dataset_id):
    """
    Stop the run unless the indicators-of-birth split has created and filled the
    output dataset, and this tool has not written to it yet.

    Publishing the dates of birth without the birth events delivers half of the
    add-on, so a missing or empty dataset stops the run rather than being created
    here. Nothing here deletes or overwrites; clearing a previous attempt is the
    operator's decision.

    :raises RuntimeError: if the dataset is missing, is not the date-of-birth
        add-on, holds no tables, or already holds the delivered table
    """
    try:
        dataset = client.get_dataset(f'{project_id}.{output_dataset_id}')
    except NotFound:
        raise RuntimeError(
            f'{output_dataset_id} does not exist. Run '
            f'split_ct_plus_dob_indicators.py first; it creates the dataset and '
            f'writes the indicators-of-birth tables.')

    component = (dataset.labels or {}).get('ct_plus_component')
    if component != CT_PLUS_DOB_INDICATORS:
        raise RuntimeError(
            f'{output_dataset_id} is labelled ct_plus_component={component}, not '
            f'{CT_PLUS_DOB_INDICATORS}, so it was not created by the '
            f'indicators-of-birth split.')

    existing = {
        table.table_id
        for table in client.list_tables(f'{project_id}.{output_dataset_id}')
    }
    if DELIVERED_TABLE in existing:
        raise RuntimeError(
            f'{output_dataset_id}.{DELIVERED_TABLE} already exists. Remove it '
            f'before rerunning.')
    if not existing:
        raise RuntimeError(
            f'{output_dataset_id} holds no tables, so the indicators-of-birth '
            f'split did not finish. Rerun split_ct_plus_dob_indicators.py first.'
        )


def write_delivered_table(client, project_id, capture_dataset_id,
                          output_dataset_id):
    """
    Copy the capture into the output dataset under its delivered name.

    CREATE TABLE fails rather than replacing an existing table, and no other table
    in the dataset is touched.

    :return: rows written
    """
    query = WRITE_DELIVERED_TABLE.render(project=project_id,
                                         output_dataset=output_dataset_id,
                                         delivered_table=DELIVERED_TABLE,
                                         columns=DELIVERED_COLUMNS,
                                         capture_dataset=capture_dataset_id,
                                         capture_table=CT_PLUS_BIRTHDATE_TABLE)
    client.query(query).result()
    return client.get_table(
        f'{project_id}.{output_dataset_id}.{DELIVERED_TABLE}').num_rows


def main(project_id,
         release_tag,
         input_dataset_id,
         capture_dataset_id,
         rdr_sandbox_id,
         under18_lookup_dataset_id,
         ct_plus_ids_view,
         ids_dataset_id=PIPELINE_TABLES):
    """
    Validate the capture and add it to CP{release_tag}_dob.

    :return: rows written
    """
    validate_dataset_names(release_tag, input_dataset_id)
    output_dataset_id = get_ct_plus_addon_dataset_name(release_tag,
                                                       CT_PLUS_DOB_INDICATORS)

    client = BigQueryClient(project_id)
    assert_output_dataset_is_ready(client, project_id, output_dataset_id)
    assert_capture_is_publishable(client, project_id, input_dataset_id,
                                  capture_dataset_id, rdr_sandbox_id,
                                  under18_lookup_dataset_id, ids_dataset_id,
                                  ct_plus_ids_view)
    log_coverage(client, project_id, input_dataset_id, capture_dataset_id)

    count = write_delivered_table(client, project_id, capture_dataset_id,
                                  output_dataset_id)
    LOGGER.info(f'Wrote {count} rows to {output_dataset_id}.{DELIVERED_TABLE}')
    return count


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description=('Add the captured dates of birth to the CT+ date-of-birth '
                     'add-on dataset.'))
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
        '--capture_dataset_id',
        required=True,
        help=(f'The controlled_plus deid stage sandbox, which holds '
              f'{CT_PLUS_BIRTHDATE_TABLE}'))
    parser.add_argument('--rdr_sandbox_id',
                        required=True,
                        help=f'The RDR stage sandbox, which holds {AIAN_LIST}')
    parser.add_argument(
        '--under18_lookup_dataset_id',
        required=True,
        help=(
            f'Dataset holding {UNDER18_PARTICIPANTS_LOOKUP_TABLE}, which is the '
            f'RDR stage sandbox'))
    parser.add_argument(
        '--ct_plus_ids_view',
        required=True,
        help=f'View in {PIPELINE_TABLES} mapping participant IDs to CT+ IDs')
    return parser


if __name__ == '__main__':
    pipeline_logging.configure(level=logging.INFO, add_console_handler=True)
    ARGS = get_arg_parser().parse_args()
    main(ARGS.project_id, ARGS.release_tag, ARGS.input_dataset_id,
         ARGS.capture_dataset_id, ARGS.rdr_sandbox_id,
         ARGS.under18_lookup_dataset_id, ARGS.ct_plus_ids_view)
