from typing import List
import logging

from google.cloud.bigquery import Client
from google.cloud.exceptions import GoogleCloudError

from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)
TABLE_ID = 'table_id'

GET_ALL_TABLES_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT
  table_id
FROM `{{project}}.{{dataset}}.__TABLES__`
WHERE table_id IN (
{% for table_name in table_names %}
    {% if loop.previtem is defined %}, {% else %}  {% endif %} '{{table_name}}'
{% endfor %}
)
""")


def get_tables_in_dataset(client: Client, project_id, dataset_id,
                          table_names) -> List[str]:
    """
    This function retrieves tables that exist in dataset for an inital list table_names . This 
    function raises GoogleCloudError if the query throws an error
    
    :param client: 
    :param project_id: 
    :param dataset_id: 
    :param table_names: 
    :return: a list of tables that exist in the given dataset
    """
    # The following makes sure the tables exist in the dataset
    query_job = client.query(
        GET_ALL_TABLES_QUERY_TEMPLATE.render(project=project_id,
                                             dataset=dataset_id,
                                             table_names=table_names))

    try:
        result = query_job.result()
        # Raise the Runtime Error if the errors are neither GoogleCloudError nor TimeoutError
        if query_job.errors:
            raise RuntimeError(result.errors)

        return [dict(row.items())[TABLE_ID] for row in result]

    except (GoogleCloudError, TimeoutError, RuntimeError) as e:
        # Catch GoogleCloudError and TimeoutError that could be raised by query_job.result()
        # Also catch the RuntimeError raised from the try block
        # Log the error and raise it again
        LOGGER.error(f"Error running job {result.job_id}: {e}")
        raise
