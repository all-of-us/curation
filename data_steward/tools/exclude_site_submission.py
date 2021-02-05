"""
Empty all CDM tables associated with one or more HPO sites from an EHR dataset. 
Tables are NOT dropped- their rows are deleted and the empty tables are preserved. 

This can be used to exclude sites' data from the EHR dataset before 
generating the unioned dataset which will be used for the CDR.

NOTE: This does not affect non-clinical tables (e.g. achilles_*)
"""
import logging
from typing import List

from google.cloud import bigquery

from utils import bq
import bq_utils
import resources

LOGGER = logging.getLogger(__name__)

DELETE_QUERY_TPL = bq.JINJA_ENV.from_string("""
{%- for table in tables_to_empty -%}
DELETE FROM `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}` WHERE 1=1;
{%- endfor -%}
""")


def _filter_hpo_tables(tables: List[bigquery.table.TableListItem],
                       hpo_id: str) -> List[bigquery.table.TableListItem]:
    """
    Given a list of tables get those associated with an HPO submission

    :param tables: list of tables to filter
    :param hpo_id: identifies the HPO
    :return: list of tables associated with the HPO
    """
    expected_tables = [
        bq_utils.get_table_id(hpo_id, table) for table in resources.CDM_TABLES
    ]
    return [table for table in tables if table.table_id in expected_tables]


def exclude_site_submission(client: bigquery.Client,
                            dataset: bigquery.DatasetReference,
                            hpo_ids: List[str]) -> bigquery.QueryJob:
    """
    Empty all CDM tables associated with one or more HPO sites
    
    :param client: Active bigquery client object 
    :param dataset: the dataset to exclude site data from
    :param hpo_ids: Identifies the HPO sites whose data should be excluded
    :return: Query job associated with removing all the records
    :raises RuntimeError if CDM tables associated with a site are not found in the dataset
    """
    LOGGER.debug(
        f'exclude_site_submission called with dataset={dataset.dataset_id} and hpo_ids={hpo_ids}'
    )
    all_tables = list(bq.list_tables(client, dataset))
    tables_to_empty = []
    for hpo_id in hpo_ids:
        hpo_tables = _filter_hpo_tables(all_tables, hpo_id)
        if not hpo_tables:
            raise RuntimeError(
                f'No tables found for {hpo_id} in dataset {dataset.dataset_id}. '
                f'Ensure the specified arguments are correct.')
        tables_to_empty.extend(hpo_tables)
    script = DELETE_QUERY_TPL.render(tables_to_empty=tables_to_empty)
    LOGGER.debug(f'exclude_site_submission about to start script:\n {script}')
    return client.query(script)


def main(credentials, project_id, dataset_id, hpo_ids: List[str]):
    client = bigquery.Client.from_service_account_json(credentials)
    dataset = bigquery.DatasetReference(project_id, dataset_id)
    query_job = exclude_site_submission(client, dataset, hpo_ids)
    LOGGER.info(f'Query job {query_job.job_id} started.')


if __name__ == "__main__":
    import argparse
    from utils import pipeline_logging

    pipeline_logging.configure()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-c',
                        '--credentials',
                        required=True,
                        help='Path to GCP credentials file')
    parser.add_argument('-p',
                        '--project_id',
                        required=True,
                        help='Identifies the project')
    parser.add_argument('-d',
                        '--dataset_id',
                        required=True,
                        help='Identifies the dataset to exclude site data from')
    parser.add_argument(
        '--hpo_ids',
        required=True,
        nargs='+',
        help='Identifies the HPO sites whose submissions should be excluded')
    ARGS = parser.parse_args()
    main(ARGS.credentials, ARGS.project_id, ARGS.dataset_id, ARGS.hpo_ids)
