"""
Write the CT+ Zip5 add-on dataset from the captured five-digit zip codes.

CaptureCtPlusZip5 copies each eligible participant's latest five-digit zip into the
controlled_plus deid stage sandbox before GeneralizeZipCodes cuts it to three digits;
PruneCtPlusZip5 and ConvertCtPlusZip5Ids then reconcile it with person and re-key it to
CT+ research IDs. That sandbox is not a deliverable, so this tool promotes the table
into CP{release_tag}_zip5 as its one table, zip5.

This is a promotion, not a split: nothing is subtracted from the mainline, because the
generalization already left it with three-digit values. person, visit_occurrence,
provider and care_site are not written; every person_id resolves in the base.

Each property checked before writing is already guaranteed by a capture rule, and each
fails open there if it regresses: an empty AIAN or under-18 lookup satisfies its
exclusion, and an UPDATE that matches nothing leaves a CT research ID in place. So the
run stops here, where the rows become a deliverable, rather than warning.

Usage:
  python build_ct_plus_zip5_dataset.py \
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
from common import (AIAN_LIST, CONTROLLED_PLUS, CT_PLUS_PRE_SPLIT_SUFFIX,
                    CT_PLUS_ZIP5, CT_PLUS_ZIP5_TABLE, JINJA_ENV, PERSON,
                    PIPELINE_TABLES, TIER_DATASET_PREFIX,
                    UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                    get_ct_plus_addon_dataset_name)
from gcloud.bq import BigQueryClient
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

# The researcher-facing table name inside CP{release_tag}_zip5.
DELIVERED_TABLE = 'zip5'

# Listed rather than SELECT *, so a column added to the capture is not published
# without a decision.
DELIVERED_COLUMNS = [
    'person_id', 'observation_source_concept_id', 'observation_datetime',
    'value_as_string'
]

LOOKUP_COUNT = JINJA_ENV.from_string("""
SELECT COUNT(*) AS lookup_count
FROM `{{project}}.{{lookup_dataset}}.{{lookup_table}}`
""")

# Each joined set is made distinct first, so no join can multiply capture rows and
# the duplicate count reads the capture alone.
CAPTURE_CHECKS = JINJA_ENV.from_string("""
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) - COUNT(DISTINCT z.person_id) AS duplicate_rows,
  COUNTIF(p.person_id IS NULL) AS unresolved_rows,
  COUNTIF(c.person_id IS NULL) AS non_ct_plus_rows,
  COUNTIF(a.person_id IS NOT NULL) AS aian_rows,
  COUNTIF(u.person_id IS NOT NULL) AS pediatric_rows,
  COUNTIF(NOT REGEXP_CONTAINS(IFNULL(z.value_as_string, ''), r'^[0-9]{5}$'))
    AS not_five_digit_rows
FROM `{{project}}.{{capture_dataset}}.{{capture_table}}` z
LEFT JOIN (
  SELECT DISTINCT person_id
  FROM `{{project}}.{{input_dataset}}.{{person}}`
) p
  ON p.person_id = z.person_id
LEFT JOIN (
  SELECT DISTINCT controlled_tier_plus_id AS person_id
  FROM `{{project}}.{{ids_dataset}}.{{ids_view}}`
  WHERE controlled_tier_plus_id IS NOT NULL
) c
  ON c.person_id = z.person_id
LEFT JOIN (
  -- aian_list is keyed by participant ID, so it is mapped to CT+ IDs --
  SELECT DISTINCT v.controlled_tier_plus_id AS person_id
  FROM `{{project}}.{{rdr_sandbox}}.{{aian_list}}` al
  JOIN `{{project}}.{{ids_dataset}}.{{ids_view}}` v
    ON v.participant_id = al.person_id
  WHERE v.controlled_tier_plus_id IS NOT NULL
) a
  ON a.person_id = z.person_id
LEFT JOIN (
  -- _under18_participants is keyed by participant ID too --
  SELECT DISTINCT v.controlled_tier_plus_id AS person_id
  FROM `{{project}}.{{under18_dataset}}.{{under18_table}}` un
  JOIN `{{project}}.{{ids_dataset}}.{{ids_view}}` v
    ON v.participant_id = un.person_id
  WHERE v.controlled_tier_plus_id IS NOT NULL
) u
  ON u.person_id = z.person_id
""")

COVERAGE = JINJA_ENV.from_string("""
SELECT
  COUNT(*) AS input_participants,
  COUNTIF(z.person_id IS NULL) AS without_zip5
FROM `{{project}}.{{input_dataset}}.{{person}}` p
LEFT JOIN (
  SELECT DISTINCT person_id
  FROM `{{project}}.{{capture_dataset}}.{{capture_table}}`
) z
  ON z.person_id = p.person_id
""")

WRITE_DELIVERED_TABLE = JINJA_ENV.from_string("""
CREATE TABLE `{{project}}.{{output_dataset}}.{{delivered_table}}` AS
SELECT {{columns | join(', ')}}
FROM `{{project}}.{{capture_dataset}}.{{capture_table}}`
""")


def validate_dataset_names(release_tag, input_dataset_id):
    """
    Stop the run unless the input is this release's pre-split re-key output.

    The resolve check reads that dataset's person table, since the mainline is
    assembled from it after every producer has run. Another release's input would
    resolve nothing, and a published dataset is not what the mainline is built from.

    :param release_tag: release tag, e.g. '2025q4r7'
    :param input_dataset_id: dataset the resolve check reads
    :raises ValueError: if the input is not CP{release_tag}_..._pre_split
    """
    prefix = f'{TIER_DATASET_PREFIX[CONTROLLED_PLUS]}{release_tag}_'
    if not (input_dataset_id.startswith(prefix) and
            input_dataset_id.endswith(CT_PLUS_PRE_SPLIT_SUFFIX)):
        raise ValueError(
            f'--input_dataset_id {input_dataset_id} must start with {prefix} and end '
            f'with {CT_PLUS_PRE_SPLIT_SUFFIX}: it is the re-key output the mainline '
            f'is assembled from.')


def assert_lookups_are_populated(client, project_id, rdr_sandbox_id,
                                 under18_lookup_dataset_id):
    """
    Stop the run if aian_list or _under18_participants is empty, since an empty
    lookup satisfies its exclusion vacuously.

    :raises RuntimeError: naming the flag that points at the empty lookup
    """
    for lookup_dataset, lookup_table, flag in [
        (rdr_sandbox_id, AIAN_LIST, '--rdr_sandbox_id'),
        (under18_lookup_dataset_id, UNDER18_PARTICIPANTS_LOOKUP_TABLE,
         '--under18_lookup_dataset_id'),
    ]:
        lookup = list(
            client.query(
                LOOKUP_COUNT.render(project=project_id,
                                    lookup_dataset=lookup_dataset,
                                    lookup_table=lookup_table)).result())[0]
        if not lookup['lookup_count']:
            raise RuntimeError(
                f'{lookup_dataset}.{lookup_table} is empty, so its exclusion cannot '
                f'be checked. Point {flag} at the RDR stage sandbox the CT+ run '
                f'read.')


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
    - Every person_id is a CT+ research ID, so a row ConvertCtPlusZip5Ids left on
      its CT research ID cannot ship. This assumes no value is both a CT and a CT+
      research ID in the ids view: a CT research ID equal to some participant's CT+
      research ID would pass. Nothing here checks that the two ID spaces are
      disjoint.
    - No AIAN participant.
    - No pediatric participant, since pediatric values reach a study only through
      the pediatrics add-on.
    - Every value is exactly five digits, so a generalized value such as '354**'
      cannot stand in for the one this dataset preserves.

    :return: rows in the capture table
    :raises RuntimeError: listing every failed property
    """
    params = dict(project=project_id,
                  input_dataset=input_dataset_id,
                  capture_dataset=capture_dataset_id,
                  capture_table=CT_PLUS_ZIP5_TABLE,
                  rdr_sandbox=rdr_sandbox_id,
                  aian_list=AIAN_LIST,
                  under18_dataset=under18_lookup_dataset_id,
                  under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                  ids_dataset=ids_dataset_id,
                  ids_view=ids_view_id,
                  person=PERSON)

    assert_lookups_are_populated(client, project_id, rdr_sandbox_id,
                                 under18_lookup_dataset_id)

    row = list(client.query(CAPTURE_CHECKS.render(**params)).result())[0]
    source = f'{capture_dataset_id}.{CT_PLUS_ZIP5_TABLE}'

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
            f'research ID ConvertCtPlusZip5Ids did not convert')
    if row['aian_rows']:
        failures.append(f'{row["aian_rows"]} rows belong to AIAN participants')
    if row['pediatric_rows']:
        failures.append(
            f'{row["pediatric_rows"]} rows belong to pediatric participants')
    if row['not_five_digit_rows']:
        failures.append(
            f'{row["not_five_digit_rows"]} values are not exactly five digits')

    if failures:
        raise RuntimeError(f'{source} is not publishable: ' +
                           '; '.join(failures) + '.')

    LOGGER.info(f'All {row["total_rows"]} rows in {source} passed the checks.')
    return row['total_rows']


def log_coverage(client, project_id, input_dataset_id, capture_dataset_id):
    """
    Log how many input participants carry no captured zip.

    It separates a partial capture from a complete one, but it is a reading, not a
    threshold: participants whose zip was never five digits, and the AIAN and
    pediatric participants the capture excludes, legitimately have none.
    """
    row = list(
        client.query(
            COVERAGE.render(project=project_id,
                            input_dataset=input_dataset_id,
                            capture_dataset=capture_dataset_id,
                            capture_table=CT_PLUS_ZIP5_TABLE,
                            person=PERSON)).result())[0]
    LOGGER.info(
        f'{row["without_zip5"]} of {row["input_participants"]} participants in '
        f'{input_dataset_id} have no captured zip5.')


def assert_output_dataset_is_absent(client, project_id, output_dataset_id):
    """
    Stop the run if the output dataset exists.

    Nothing here deletes or overwrites; clearing a previous attempt is the
    operator's decision.

    :raises RuntimeError: if the dataset exists
    """
    try:
        client.get_dataset(f'{project_id}.{output_dataset_id}')
    except NotFound:
        return

    raise RuntimeError(
        f'{output_dataset_id} already exists. Remove it before rerunning.')


def write_delivered_table(client, project_id, capture_dataset_id,
                          output_dataset_id):
    """
    Copy the capture into the output dataset under its delivered name.

    CREATE TABLE fails rather than replacing an existing table.

    :return: rows written
    """
    query = WRITE_DELIVERED_TABLE.render(project=project_id,
                                         output_dataset=output_dataset_id,
                                         delivered_table=DELIVERED_TABLE,
                                         columns=DELIVERED_COLUMNS,
                                         capture_dataset=capture_dataset_id,
                                         capture_table=CT_PLUS_ZIP5_TABLE)
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
    Validate the capture and write CP{release_tag}_zip5.

    If the write fails after the dataset is created, the empty dataset stays behind
    and the next run refuses on it. Remove it before rerunning.

    :return: rows written
    """
    validate_dataset_names(release_tag, input_dataset_id)
    output_dataset_id = get_ct_plus_addon_dataset_name(release_tag,
                                                       CT_PLUS_ZIP5)

    client = BigQueryClient(project_id)
    assert_output_dataset_is_absent(client, project_id, output_dataset_id)
    assert_capture_is_publishable(client, project_id, input_dataset_id,
                                  capture_dataset_id, rdr_sandbox_id,
                                  under18_lookup_dataset_id, ids_dataset_id,
                                  ct_plus_ids_view)
    log_coverage(client, project_id, input_dataset_id, capture_dataset_id)

    dataset = client.define_dataset(
        output_dataset_id,
        f'CT+ Zip5 add-on dataset for release {release_tag}, promoted from '
        f'{capture_dataset_id}.{CT_PLUS_ZIP5_TABLE}',
        {'ct_plus_component': CT_PLUS_ZIP5})
    client.create_dataset(dataset, exists_ok=False)

    count = write_delivered_table(client, project_id, capture_dataset_id,
                                  output_dataset_id)
    LOGGER.info(f'Wrote {count} rows to {output_dataset_id}.{DELIVERED_TABLE}')
    return count


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description='Write the CT+ Zip5 add-on dataset.')
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
              f'{CT_PLUS_ZIP5_TABLE}'))
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
