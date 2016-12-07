import inspect
import os

base_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
resource_path = os.path.join(base_path, 'resources')
example_path = os.path.join(base_path, 'examples')
cdm_metadata_path = os.path.join(resource_path, 'cdm.csv')
hpo_csv_path = os.path.join(resource_path, 'hpo.csv')

# Configuration

# location of files to validate, evaluate
csv_dir = 'path/to/csv_files'

# sprint number being validated against
sprint_num = 0

# Submissions and logs stored here
# For more examples and requirements see http://docs.sqlalchemy.org/en/latest/core/engines.html
db_url = 'mssql+pymssql://localhost/pmi_sprint_1'