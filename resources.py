import inspect
import os

base_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
resource_path = os.path.join(base_path, 'resources')
example_path = os.path.join(base_path, 'examples')
cdm_csv_path = os.path.join(resource_path, 'cdm.csv')
hpo_csv_path = os.path.join(resource_path, 'hpo.csv')
pmi_tables_csv_path = os.path.join(resource_path, 'pmi_tables.csv')
