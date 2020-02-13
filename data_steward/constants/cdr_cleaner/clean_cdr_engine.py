import os
import tempfile

FILENAME = os.path.join(tempfile.gettempdir(), 'cleaner.log')

FAILURE_MESSAGE_TEMPLATE = '''
The failed query was generated from the below module:
    module_name={module_name}
    function_name={function_name}
    line_no={line_no}

The failed query ran with the following configuration:
    project_id={project_id}
    destination_dataset_id={destination_dataset_id}
    destination_table={destination_table}
    disposition={disposition}
    query={query}
    exception={exception}
'''
