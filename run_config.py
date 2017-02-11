"""
Module contains runtime configuration variables
"""

import pandas
from sqlalchemy import DateTime
from sqlalchemy import create_engine

import resources
import settings
from sqlalchemy.dialects.mssql import DATETIME2

engine = create_engine(settings.conn_str)

all_hpos = pandas.read_csv(resources.hpo_csv_path)
all_hpo_ids = all_hpos.hpo_id.unique()
multi_schema_supported = engine.dialect.name in ['mssql', 'postgresql', 'oracle']

if settings.hpo_id == 'all':
    hpo_ids = all_hpo_ids
else:
    if settings.hpo_id.lower() not in all_hpo_ids:
        raise RuntimeError('%s not a valid hpo_id' % settings.hpo_id)
    hpo_ids = [settings.hpo_id]

if len(hpo_ids) > 1 and not multi_schema_supported:
    raise Exception('Cannot process. Multiple schemas not supported by configured engine.')

use_multi_schemas = multi_schema_supported and (len(hpo_ids) > 1 or settings.force_multi_schema)
datetime_tpe = DATETIME2 if 'mssql' in settings.conn_str else DateTime(True)
cdm_dialect = None
if 'mssql' in settings.conn_str:
    cdm_dialect = 'sql server'
elif 'oracle' in settings.conn_str:
    cdm_dialect = 'oracle'
elif 'postgres' in settings.conn_str:
    cdm_dialect = 'postgres'
