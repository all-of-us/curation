"""
Module contains runtime configuration variables
"""

import pandas

import resources
import settings

all_hpos = pandas.read_csv(resources.hpo_csv_path)
all_hpo_ids = all_hpos.hpo_id.unique()
multi_schema_supported = False

if settings.hpo_id == 'all':
    hpo_ids = all_hpo_ids
else:
    if settings.hpo_id.lower() not in all_hpo_ids:
        raise RuntimeError('%s not a valid hpo_id' % settings.hpo_id)
    hpo_ids = [settings.hpo_id]

if len(hpo_ids) > 1 and not multi_schema_supported:
    raise Exception('Cannot process. Multiple schemas not supported by configured engine.')

use_multi_schemas = multi_schema_supported and (len(hpo_ids) > 1 or settings.force_multi_schema)
cdm_dialect = None


def permitted_file_names():
    cdm_df = pandas.read_csv(resources.cdm_csv_path)
    included_tables = pandas.read_csv(resources.pmi_tables_csv_path).table_name.unique()
    tables = cdm_df[cdm_df['table_name'].isin(included_tables)].groupby(['table_name'])
    sprint_num = settings.sprint_num
    for hpo_id in all_hpo_ids:
        for table_name, _ in tables:
            yield '%(hpo_id)s_%(table_name)s_datasprint_%(sprint_num)s.csv' % locals()
