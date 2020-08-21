import os
import tempfile

from utils.bq import JINJA_ENV

FILENAME = os.path.join(tempfile.gettempdir(), 'cleaner.log')

QUERY_RUN_MESSAGE = '''
Clean rule {{module_name}}.{{function_name}} query {{query_no+1}}/{{query_count}}"
'''

QUERY_RUN_MESSAGE_TEMPLATE = JINJA_ENV.from_string(QUERY_RUN_MESSAGE)

SUCCESS_MESSAGE = '''
Successfully executed query {{query_no+1}}/{{query_count}} for {{module_name}} with job_id {{query_job.job_id}}
'''

SUCCESS_MESSAGE_TEMPLATE = JINJA_ENV.from_string(SUCCESS_MESSAGE)

FAILURE_MESSAGE = """
The failed query was generated from the below module:
    module_name={{module_name}}
    function_name={{function_name}}
    line_no={{line_no}}

The failed query ran with the following configuration:
    project_id={{project_id}}
    {% if query_job %}
    job_id={{query_job.job_id}}
    {% if query_job.errors %}
    job_errors={{query_job.errors}}
    {% endif %}
    {% endif %}
    {% if destination_dataset_id %}
    destination_dataset_id={{destination_dataset_id}}
    {% endif %}
    {% if destination_table %}
    destination_table_id={{destination_table_id}}
    {% endif %}
    {% if disposition %}
    disposition={{disposition}}
    {% endif %}
    query={{query}}
    {% if exception %}
    exception={{exception}}
    {% endif %}
"""

FAILURE_MESSAGE_TEMPLATE = JINJA_ENV.from_string(FAILURE_MESSAGE)
