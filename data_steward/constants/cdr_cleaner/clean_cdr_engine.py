import os
import tempfile

from utils.bq import JINJA_ENV

FILENAME = os.path.join(tempfile.gettempdir(), 'cleaner.log')

QUERY_RUN_MESSAGE = '''
Cleaning rule {{module_name}}.{{function_name}}
Rule {{rule_no}} out of {{rule_count}} 
Query {{query_no}} out of {{query_count}}
    query={{query}}"
'''

QUERY_RUN_MESSAGE_TEMPLATE = JINJA_ENV.from_string(QUERY_RUN_MESSAGE)

FAILURE_MESSAGE = '''
The failed query was generated from the below module:
    module_name={{module_name}}
    function_name={{function_name}}
    line_no={{line_no}}

The failed query ran with the following configuration:
    project_id={{project_id}}
    destination_dataset_id={{destination_dataset_id}}
    destination_table={{destination_table}}
    disposition={{disposition}}
    query={{query}}
    exception={{exception}}
'''

FAILURE_MESSAGE_TEMPLATE = JINJA_ENV.from_string(FAILURE_MESSAGE)
