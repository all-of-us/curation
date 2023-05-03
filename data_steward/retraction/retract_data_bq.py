"""
This script retracts rows for specified pids from datasets.
The pid table must have person_id and research_id for retraction.
The pid table must be located in the sandbox_dataset.
Retraction is performed for each dataset based on its data stage.
"""
# Python imports
import argparse
import logging
from itertools import product

# Third party imports

# Project imports
from utils import pipeline_logging
from gcloud.bq import BigQueryClient
from common import (AOU_REQUIRED, CARE_SITE, CATI_TABLES, CONDITION_ERA, DEATH,
                    DOSE_ERA, DRUG_ERA, FACT_RELATIONSHIP, ID_CONSTANT_FACTOR,
                    JINJA_ENV, LOCATION, MAPPING_PREFIX, MEASUREMENT, NOTE,
                    NOTE_NLP, OBSERVATION, OBSERVATION_PERIOD,
                    PAYER_PLAN_PERIOD, PERSON, PII_TABLES, PROVIDER,
                    UNIONED_EHR)
from resources import mapping_table_for
from retraction.retract_utils import (get_datasets_list, get_dataset_type,
                                      is_combined_dataset, is_deid_dataset,
                                      is_ehr_dataset, is_fitbit_dataset,
                                      is_rdr_dataset, is_sandbox_dataset,
                                      is_unioned_dataset)
from constants.retraction.retract_utils import (NONE, PERSON_ID, RESEARCH_ID)

LOGGER = logging.getLogger(__name__)

RETRACTION_RDR_EHR = 'rdr_and_ehr'
RETRACTION_ONLY_EHR = 'only_ehr'

NON_PID_TABLES = [CARE_SITE, LOCATION, FACT_RELATIONSHIP, PROVIDER]
OTHER_PID_TABLES = [
    CONDITION_ERA, DOSE_ERA, DRUG_ERA, NOTE_NLP, OBSERVATION_PERIOD,
    PAYER_PLAN_PERIOD
]

# person from RDR data should not be removed, while person EHR data may be
# removed.  If retracting the entire record, then the data will be dropped
# from the person table.  But if only retracting EHR data, the data will
# not be dropped from the person table.
NON_EHR_TABLES = [PERSON]

TABLES_FOR_RETRACTION = set(PII_TABLES + CATI_TABLES +
                            OTHER_PID_TABLES) - set(NON_PID_TABLES +
                                                    NON_EHR_TABLES)

RETRACT_QUERY = """
{% if sandbox %}
CREATE TABLE `{{project}}.{{sb_dataset}}.{{sb_table}}` AS SELECT *
{% else %}
DELETE
{% endif %}
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id IN (
    SELECT {{person_id}} FROM `{{project}}.{{sb_dataset}}.{{lookup_table_id}}`
)
{% if is_sandbox and retraction_type == 'only_ehr' %}
-- NOTE Though 'only_ehr', this condition also drops RDR data that are affected by --
-- the CR `domain_alignment`. We cannot avoid it under our current pipeline design. --
AND {{domain_id}} > {{id_const}}
{% elif table != 'death' and retraction_type == 'only_ehr' %}
AND {{table}}_id IN (
    {% if is_deid %}
    SELECT {{table}}_id FROM `{{project}}.{{dataset}}.{{table}}_ext` WHERE src_id LIKE 'EHR%'
    {% else %}
    SELECT {{table}}_id FROM `{{project}}.{{dataset}}._mapping_{{table}}` WHERE src_hpo_id != 'rdr'
    {% endif %}
)
{% endif %}
"""

RETRACT_QUERY_FACT_RELATIONSHIP = """
{% if sandbox %}
CREATE TABLE `{{project}}.{{sb_dataset}}.{{sb_table}}` AS SELECT *
{% else %}
DELETE
{% endif %}
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE
  (
    domain_concept_id_1 = 56
    AND fact_id_1 IN (
      SELECT {{person_id}} FROM `{{project}}.{{sb_dataset}}.{{lookup_table_id}}`
    )
  )
OR
  (
    domain_concept_id_2 = 56
    AND fact_id_2 IN (
      SELECT {{person_id}} FROM `{{project}}.{{sb_dataset}}.{{lookup_table_id}}`
    )
  )
"""

RETRACT_QUERY_FACT_RELATIONSHIP_ONLY_EHR = """
{% if sandbox %}
CREATE TABLE `{{project}}.{{sb_dataset}}.{{sb_table}}` AS SELECT f.*
FROM `{{project}}.{{dataset}}.{{table}}` f
INNER JOIN (
{% else %}
MERGE `{{project}}.{{dataset}}.{{table}}` f
USING(
{% endif %}
    SELECT fr.*
    FROM `{{project}}.{{dataset}}.{{table}}` fr
    LEFT JOIN (
    SELECT mp.*
    FROM `{{project}}.{{dataset}}._mapping_measurement` mp
    LEFT JOIN `{{project}}.{{dataset}}.measurement` m
    USING (measurement_id)
    WHERE m.person_id IN (
        SELECT {{person_id}} FROM `{{project}}.{{sb_dataset}}.{{lookup_table_id}}`
    )
    AND mp.src_hpo_id != 'rdr'
    ) m1
    ON m1.measurement_id = fr.fact_id_1 AND domain_concept_id_1 = 21
    LEFT JOIN (
    SELECT mp.*
    FROM `{{project}}.{{dataset}}._mapping_measurement` mp
    LEFT JOIN `{{project}}.{{dataset}}.measurement` m
    USING (measurement_id)
    WHERE m.person_id IN (
        SELECT {{person_id}} FROM `{{project}}.{{sb_dataset}}.{{lookup_table_id}}`
    )
    AND mp.src_hpo_id != 'rdr'
    ) m2
    ON m2.measurement_id = fr.fact_id_2 AND domain_concept_id_2 = 21
    LEFT JOIN (
    SELECT mp.*
    FROM `{{project}}.{{dataset}}._mapping_observation` mp
    LEFT JOIN `{{project}}.{{dataset}}.observation` m
    USING (observation_id)
    WHERE m.person_id IN (
        SELECT {{person_id}} FROM `{{project}}.{{sb_dataset}}.{{lookup_table_id}}`
    )
    AND mp.src_hpo_id != 'rdr'
    ) o1
    ON o1.observation_id = fr.fact_id_1 AND domain_concept_id_1 = 27
    LEFT JOIN (
    SELECT mp.*
    FROM `{{project}}.{{dataset}}._mapping_observation` mp
    LEFT JOIN `{{project}}.{{dataset}}.observation` m
    USING (observation_id)
    WHERE m.person_id IN (
        SELECT {{person_id}} FROM `{{project}}.{{sb_dataset}}.{{lookup_table_id}}`
    )
    AND mp.src_hpo_id != 'rdr'
    ) o2
    ON o2.observation_id = fr.fact_id_2 AND domain_concept_id_2 = 27
    WHERE m1.measurement_id IS NOT NULL
    OR m2.measurement_id IS NOT NULL
    OR o1.observation_id IS NOT NULL
    OR o2.observation_id IS NOT NULL
) f_from_ehr
ON f.domain_concept_id_1 = f_from_ehr.domain_concept_id_1
AND f.fact_id_1 = f_from_ehr.fact_id_1
AND f.domain_concept_id_2 = f_from_ehr.domain_concept_id_2
AND f.fact_id_2 = f_from_ehr.fact_id_2
AND f.relationship_concept_id = f_from_ehr.relationship_concept_id
{% if not sandbox %}
WHEN MATCHED THEN delete
{% endif %}
"""


def get_retraction_queries(client: BigQueryClient,
                           dataset_id,
                           sb_dataset_id,
                           lookup_table_id,
                           skip_sandboxing,
                           retraction_type=None,
                           hpo_id=None) -> list:
    """
    Gets list of queries for retraction.
    :param client: BigQuery client
    :param dataset_id: dataset to run retraction for
    :param sb_dataset_id: sandbox dataset. lookup table must be in it.
    :param lookup_table_id: table containing the person_ids and research_ids
    :param skip_sandboxing: True if you wish not to sandbox the retracted data.
    :param retraction_type: string indicating whether all data needs to be removed including RDR,
        or if RDR data needs to be kept intact. Can take the values 'rdr_and_ehr', 'only_ehr', or None.
        'rdr_and_ehr' and None work the same way (= not reference mapping tables).
    :return: list of queries
    """
    tables_to_retract = get_tables_to_retract(client,
                                              dataset_id,
                                              retraction_type=retraction_type,
                                              hpo_id=hpo_id)

    LOGGER.info(
        f"Tables to retract in {dataset_id}:\n"
        f"Tables with person_id column... {', '.join(tables_to_retract)}\n"
        f"If it's a non-deid dataset, {FACT_RELATIONSHIP} will be retracted too."
    )

    person_id = RESEARCH_ID if is_deid_dataset(dataset_id) else PERSON_ID
    action_list = ['delete'] if skip_sandboxing else ['sandbox', 'delete']

    queries = []

    for table in tables_to_retract:
        for action in action_list:
            q = JINJA_ENV.from_string(RETRACT_QUERY).render(
                sandbox=action == 'sandbox',
                project=client.project,
                sb_dataset=sb_dataset_id,
                sb_table=f'retract_{dataset_id}_{table}',
                dataset=dataset_id,
                table=table,
                person_id=person_id,
                lookup_table_id=lookup_table_id,
                is_sandbox=is_sandbox_dataset(dataset_id),
                domain_id=get_primary_key_for_sandbox_table(
                    client, dataset_id, table),
                id_const=2 * ID_CONSTANT_FACTOR,
                retraction_type=retraction_type,
                is_deid=is_deid_dataset(dataset_id))
            queries.append(q)

    if not is_deid_dataset(dataset_id) and not is_fitbit_dataset(
            dataset_id) and not is_sandbox_dataset(dataset_id):
        queries.extend(
            get_retraction_queries_fact_relationship(client, dataset_id,
                                                     sb_dataset_id,
                                                     lookup_table_id,
                                                     skip_sandboxing,
                                                     retraction_type))

    return queries


def get_retraction_queries_fact_relationship(client: BigQueryClient,
                                             dataset_id,
                                             sb_dataset_id,
                                             lookup_table_id,
                                             skip_sandboxing,
                                             retraction_type=None,
                                             hpo_id=None) -> list:
    """
    Get list of queries for retracting fact_relationship table.

    :param client: BigQuery client
    :param dataset_id: dataset to run retraction for
    :param sb_dataset_id: sandbox dataset. lookup table must be in it.
    :param lookup_table_id: table containing the person_ids and research_ids
    :param skip_sandboxing: True if you wish not to sandbox the retracted data.
    :param retraction_type: string indicating whether all data needs to be removed including RDR,
        or if RDR data needs to be kept intact. Can take the values 'rdr_and_ehr' or 'only_ehr'
    :return: list of queries
    """
    if not client.table_exists(FACT_RELATIONSHIP, dataset_id):
        LOGGER.info(f"Skipping {FACT_RELATIONSHIP}. It does not exist.")
        return []

    action_list = ['delete'] if skip_sandboxing else ['sandbox', 'delete']

    if is_combined_dataset(
            dataset_id) and retraction_type == RETRACTION_ONLY_EHR and (
                not client.table_exists(mapping_table_for(MEASUREMENT)) or
                not client.table_exists(mapping_table_for(OBSERVATION))):
        LOGGER.info(
            f"Skipping {FACT_RELATIONSHIP}. Mapping tables missing for its ONLY_EHR retraction."
        )
        return []

    queries = []
    sb_table = f'retract_{dataset_id}_{FACT_RELATIONSHIP}'

    tables = [
        f'{hpo_id}_{FACT_RELATIONSHIP}', f"{UNIONED_EHR}_{FACT_RELATIONSHIP}"
    ] if is_ehr_dataset(dataset_id) else [FACT_RELATIONSHIP]

    person_id = RESEARCH_ID if is_deid_dataset(dataset_id) else PERSON_ID

    for table in tables:
        for action in action_list:
            if retraction_type == RETRACTION_ONLY_EHR:
                q = JINJA_ENV.from_string(
                    RETRACT_QUERY_FACT_RELATIONSHIP_ONLY_EHR).render(
                        sandbox=action == 'sandbox',
                        sb_dataset=sb_dataset_id,
                        sb_table=sb_table,
                        project=client.project,
                        dataset=dataset_id,
                        lookup_table_id=lookup_table_id,
                        person_id=person_id,
                        table=table)
            else:
                q = JINJA_ENV.from_string(
                    RETRACT_QUERY_FACT_RELATIONSHIP).render(
                        sandbox=action == 'sandbox',
                        sb_dataset=sb_dataset_id,
                        sb_table=sb_table,
                        dataset=dataset_id,
                        person_id=person_id,
                        lookup_table_id=lookup_table_id,
                        table=table)
            queries.append(q)

    return queries


def get_tables_to_retract(client: BigQueryClient,
                          dataset,
                          hpo_id='',
                          retraction_type=None) -> list:
    """
    Creates a list of tables that need retraction in the dataset.
    :param client: BigQuery client
    :param dataset: Dataset to run retraction on
    :param hpo_id: HPO ID that needs retraction. Mandatory only for EHR dataset.
    :param retraction_type: only_ehr or rdr_and_ehr
    :return: list of table names for retraction
    """

    LOGGER.info(f'Checking tables to retract in {client.project}.{dataset}...')
    if is_ehr_dataset(dataset) and (not hpo_id or hpo_id == NONE):
        LOGGER.warning(
            f'{dataset} is an EHR dataset but hpo_id is not specified or set to {NONE}. '
            f'Retraction will run against all the tables with person_id in {dataset}. '
            'It will take a while since there are more tables in EHR datasets than other datasets.'
        )

    if is_ehr_dataset(dataset) and (hpo_id and hpo_id != NONE):
        tables_to_retract = [
            f'{prefix}_{table}' for prefix, table in
            product([hpo_id, UNIONED_EHR], TABLES_FOR_RETRACTION |
                    set(NON_EHR_TABLES))
            if client.table_exists(f'{prefix}_{table}', dataset)
        ]
    else:
        existing_tables = [
            table.table_id
            for table in client.list_tables(f'{client.project}.{dataset}')
        ]
        tables_to_retract = [
            table for table in existing_tables
            if any(col.name == 'person_id' for col in client.get_table(
                f'{client.project}.{dataset}.{table}').schema) and
            not skip_table_retraction(client, dataset, table, retraction_type)
        ]

    return tables_to_retract


def skip_table_retraction(client: BigQueryClient, dataset_id, table_id,
                          retraction_type) -> bool:
    """
    Some tables have person_id but do not need retraction depending on how we
    want to retract. This function returns True if the table does not need retraction.

    :param client: BigQuery client
    :param dataset_id: dataset to run retraction for
    :param table_id: table to run retraction for
    :param retraction_type: string indicating whether all data needs to be removed including RDR,
        or if RDR data needs to be kept intact. Can take the values 'rdr_and_ehr' or 'only_ehr'
    :return: True if the table should be skipped. False if we need to retract the table.
    """
    msg_only_rdr = f"Skipping {PERSON} table because it has only RDR data."
    msg_no_mapping_table = (
        f"Skipping {table_id} table because it does not have a extension table or a mapping table."
    )
    msg_view = f"Skipping {table_id} since it's a VIEW."
    msg_unknown_domain_sb_table = (
        f"Skipping {table_id}. This sandbox table either has an irregular naming, "
        "does not have `domain`_id column, or only contains non-EHR data.")

    if client.get_table(
            f'{client.project}.{dataset_id}.{table_id}').table_type == 'VIEW':
        LOGGER.info(msg_view)
        return True

    if retraction_type == RETRACTION_ONLY_EHR:

        if is_sandbox_dataset(dataset_id):
            if get_primary_key_for_sandbox_table(client, dataset_id, table_id):
                return False
            else:
                LOGGER.info(msg_unknown_domain_sb_table)
                return True

        if table_id == DEATH:
            return False

        elif table_id == PERSON:
            LOGGER.info(msg_only_rdr)
            return True
        elif is_deid_dataset(dataset_id) and not client.table_exists(
                f'{table_id}_ext', dataset_id):
            LOGGER.info(msg_no_mapping_table)
            return True
        elif is_combined_dataset(dataset_id) and not client.table_exists(
                mapping_table_for(table_id), dataset_id):
            LOGGER.info(msg_no_mapping_table)
            return True

    return False


def skip_dataset_retraction(dataset_id, retraction_type) -> bool:
    """
    Some datasets do not need retraction depending on how we want to retract.
    This function returns True if the dataset does not need retraction.
    :param dataset_id: dataset to run retraction for
    :param retraction_type: string indicating whether all data needs to be removed including RDR,
        or if RDR data needs to be kept intact. Can take the values 'rdr_and_ehr' or 'only_ehr'
    :return: True if the dataset should be skipped. False if we need to retract the dataset.
    """
    msg_skip = f'Skipping retraction for the dataset {dataset_id}, '
    msg_no_ehr = (
        f'because {RETRACTION_ONLY_EHR} is specified. This is a {get_dataset_type(dataset_id)}'
        ' dataset and it does not contain data from EHR.')

    if retraction_type == RETRACTION_ONLY_EHR:
        if is_rdr_dataset(dataset_id) or is_fitbit_dataset(dataset_id):
            LOGGER.warning(f'{msg_skip}{msg_no_ehr}')
            return True

    return False


def get_primary_key_for_sandbox_table(client: BigQueryClient, dataset,
                                      table: str) -> str:
    """
    NOTE This is intended for `only_ehr` retraction for sandbox datasets.

    When retracting sandbox tables for `only_ehr`, map/ext tables do not
    exist for them. We must look at its primary key instead, in addition to person_id.

    This function looks at the sandbox table name, gets its domain, checks if 
    the domain_id and person_id columns exists in the table, and returns the domain_id
    we can use for retraction. Sandbox tables for mapping/ext tables are skipped.

    :param client: BigQuery client
    :param dataset: Dataset to run retraction on
    :param table: name of the sandbox table that needs retraction
    :return: column name that the retraction must run on. 
        Returns '' if the domain table cannot be identified, the domain table
        does not contain person_id and/or domain_id, the domain table does 
        not contain EHR data, or the table is a sandbox table for a mapping/ext table.
    """
    domain_tables = set(AOU_REQUIRED + OTHER_PID_TABLES) - set(NON_PID_TABLES)

    if MAPPING_PREFIX in table:
        return ''

    col_names = [
        col.name for col in client.get_table(
            f'{client.project}.{dataset}.{table}').schema
    ]

    if all(col_name != f"{PERSON}_id" for col_name in col_names):
        return ''

    for domain in domain_tables:
        if domain in table:

            if domain == DEATH:
                domain_id = f"{PERSON}_id"

            elif domain == OBSERVATION and OBSERVATION_PERIOD in table:
                domain_id = f"{OBSERVATION_PERIOD}_id"

            elif domain == NOTE and NOTE_NLP in table:
                domain_id = f"{NOTE_NLP}_id"

            else:
                domain_id = f"{domain}_id"

            if any(col_name == domain_id for col_name in col_names):
                return domain_id

    return ''


def retraction_query_runner(client: BigQueryClient, queries):
    """
    Runs the retraction queries one by one.
    :param client: BigQuery client
    :param queries: List of queries to run
    """
    for query in queries:
        job = client.query(query)
        LOGGER.info(f'Running query for job_id {job.job_id}. Query:\n{query}')
        result = job.result()
        LOGGER.info(
            f'Removed {job.num_dml_affected_rows} rows for job_id {job.job_id}')


def run_bq_retraction(project_id,
                      sandbox_dataset_id,
                      lookup_table_id,
                      hpo_id,
                      dataset_list,
                      retraction_type,
                      skip_sandboxing=False,
                      bq_client=None):
    """
    Main function to perform retraction.
    Lookup table must have person_id and research_id, and it must reside in sandbox_dataset_id.
    This function removes rows from all tables containing person_ids if they exist in the lookup table.
    If only_ehr is specified, it removes only the records that originate from EHR.
    For non-deid datasets, fact_relationship gets retracted here though it does not have person_id column.
    For deid datasets, fact_relationship is empty by default so it does not get retracted.

    :param project_id: project id.
    :param sandbox_dataset_id: sandbox dataset ID. Lookup table must be in this dataset.
    :param lookup_table_id: table containing person_ids and research_ids for retraction
    :param hpo_id: hpo_id of the site to retract from
    :param dataset_list: list of datasets to retract from. If containing only 'all_datasets',
        retracts from all datasets. If containing only 'none', skips retraction from BigQuery datasets
    :param retraction_type: string indicating whether all data needs to be removed including RDR,
        or if RDR data needs to be kept intact. Can take the values 'rdr_and_ehr' or 'only_ehr'
    :param skip_sandboxing: True if you wish not to sandbox the retracted data.
    :param bq_client: BigQuery client. Reuse the client if one already exists. If not, a new one will be created.
    :return:
    """

    # Skip retraction if type is incorrect
    if retraction_type not in [RETRACTION_ONLY_EHR, RETRACTION_RDR_EHR]:
        LOGGER.info(
            f"{retraction_type} is not {RETRACTION_ONLY_EHR} or {RETRACTION_RDR_EHR}. Skipping BQ retraction."
        )
        return

    client = bq_client if bq_client else BigQueryClient(project_id)

    # NOTE get_datasets_list() excludes datasets that are type=OTHER.
    dataset_ids = get_datasets_list(client, dataset_list)

    for dataset in dataset_ids:

        if skip_dataset_retraction(dataset, retraction_type):
            continue

        # Argument hpo_id is effective for only EHR dataset.
        hpo_id = hpo_id if is_ehr_dataset(dataset) else ''

        queries = get_retraction_queries(
            client,
            dataset,
            sandbox_dataset_id,
            lookup_table_id,
            skip_sandboxing,
            retraction_type=retraction_type
            if not (is_ehr_dataset(dataset) and is_unioned_dataset(dataset))
            else '',
            hpo_id=hpo_id)

        LOGGER.info(f"Started retracting from dataset {dataset}")
        retraction_query_runner(client, queries)
        LOGGER.info(f"Completed retracting from dataset {dataset}")

    LOGGER.info('Retraction completed.')


if __name__ == '__main__':
    pipeline_logging.configure(logging.DEBUG, add_console_handler=True)

    parser = argparse.ArgumentParser(
        description='Runs retraction based on the specified conditions.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Identifies the project',
                        required=True)
    parser.add_argument('-s',
                        '--sandbox_dataset_id',
                        action='store',
                        dest='sandbox_dataset_id',
                        help='Identifies the dataset containing the pid table',
                        required=True)
    parser.add_argument(
        '-t',
        '--pid_table_id',
        action='store',
        dest='pid_table_id',
        help=
        'Identifies the table containing the person_ids and research_ids for retraction',
        required=True)
    parser.add_argument('-i',
                        '--hpo_id',
                        action='store',
                        dest='hpo_id',
                        help='Identifies the site to retract data from',
                        required=False)
    parser.add_argument(
        '-d',
        '--dataset_ids',
        action='store',
        dest='dataset_ids',
        nargs='+',
        help=(
            'Identifies the datasets to retract from, separated by spaces '
            'specified as -d dataset_id_1 dataset_id_2 dataset_id_3 and so on. '
            f'If set as -d {NONE}, skips retraction from BigQuery datasets. '
            'If set as -d all_datasets, retracts from all datasets in project.'
        ),
        required=True)
    parser.add_argument(
        '-r',
        '--retraction_type',
        action='store',
        dest='retraction_type',
        help=(
            f'Identifies whether all data needs to be removed, including RDR, '
            f'or if RDR data needs to be kept intact. Can take the values '
            f'"{RETRACTION_RDR_EHR}" or "{RETRACTION_ONLY_EHR}"'),
        required=True)
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    parser.add_argument(
        '--skip_sandboxing',
        dest='skip_sandboxing',
        action='store_true',
        required=False,
        help=
        'Specify this option if you do not want this script to sanbox the retracted records.'
    )
    args = parser.parse_args()

    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    run_bq_retraction(args.project_id, args.sandbox_dataset_id,
                      args.pid_table_id, args.hpo_id, args.dataset_ids,
                      args.retraction_type, args.skip_sandboxing)
