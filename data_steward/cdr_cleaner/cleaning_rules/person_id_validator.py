"""
Run the person_id validation clean rule.

1.  The person_id in each of the defined tables exists in the person table.
    If not valid, remove the record.
2.  The person_id is consenting.  If not consenting, remove EHR records.
    Keep PPI records.
"""
import logging

import common
from cdr_cleaner.cleaning_rules import drop_rows_for_missing_persons
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as clean_consts
import resources

LOGGER = logging.getLogger(__name__)

# The below query translates to:
# Find all consented participants.
# Find all non-ehr consented participants.
# Find all mappings not like ehr (i.e. ppi mappings).
# Join the table with consented participants then union that
# with the table joined with non-ehr consented participants and joined with
# non-ehr mappings mappings
EXISTING_AND_VALID_CONSENTING_RECORDS = (
    'WITH consented AS ( '
    'SELECT person_id '
    'FROM ( '
    'SELECT person_id, value_source_concept_id, observation_datetime, '
    'ROW_NUMBER() OVER( '
    'PARTITION BY person_id ORDER BY observation_datetime DESC, '
    'value_source_concept_id ASC) AS rn '
    'FROM `{project}.{dataset}.observation` '
    'WHERE observation_source_value = \'EHRConsentPII_ConsentPermission\')'
    'WHERE rn=1 AND value_source_concept_id = 1586100),'
    'unconsented AS ( '
    'SELECT person_id '
    'FROM ( '
    'SELECT person_id, value_source_concept_id, observation_datetime, '
    'ROW_NUMBER() OVER( '
    'PARTITION BY person_id ORDER BY observation_datetime DESC, '
    'value_source_concept_id ASC) AS rn '
    'FROM `{project}.{dataset}.observation` '
    'WHERE observation_source_value = \'EHRConsentPII_ConsentPermission\')'
    'WHERE rn=1 AND value_source_concept_id != 1586100),'
    'ppi_mappings AS ( '
    'SELECT {table}_id '
    'FROM `{project}.{dataset}._mapping_{table}` '
    'WHERE src_dataset_id not like \'%ehr%\') '
    # get all consented rows
    'SELECT {fields} FROM `{project}.{dataset}.{table}` AS entry '
    'JOIN consented AS cons '
    'ON entry.person_id = cons.person_id '
    'UNION ALL '
    # get all unconsented non-ehr rows
    'SELECT {fields} FROM `{project}.{dataset}.{table}` AS entry '
    'JOIN unconsented as cons on entry.person_id = cons.person_id '
    'JOIN ppi_mappings AS maps ON maps.{table}_id = entry.{table}_id '
)

def get_person_id_validation_queries(project=None, dataset=None):
    """
    Return query list of queries to ensure valid people are in the tables.

    The non-consenting queries rely on the mapping tables.  When using the
    combined and unidentified dataset, the last portion of the dataset name is
    removed to access these tables.  Any other dataset is expected to have
    these tables and uses the mapping tables from within the same dataset.

    :return:  A list of string queries that can be executed to delete invalid
        records for invalid persons
    """
    query_list = []

    # TODO:  pull into a curation utils module somewhere
    if dataset.endswith('_deid'):
        mapping_ds = dataset[0:-5]
    else:
        mapping_ds = dataset

    # generate queries to remove EHR records of non-ehr consented persons
    for table in common.MAPPED_CLINICAL_DATA_TABLES:
        field_names = ['entry.' + field['name'] for field in resources.fields_for(table)]
        fields = ', '.join(field_names)
        consent_query = EXISTING_AND_VALID_CONSENTING_RECORDS.format(
            project=project,
            dataset=dataset,
            table=table,
            mapping_dataset=mapping_ds,
            fields=fields,
        )

        query_dict = {
            clean_consts.QUERY: consent_query,
            clean_consts.DESTINATION_TABLE: table,
            clean_consts.DESTINATION_DATASET: dataset,
            clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
        }
        query_list.append(query_dict)

    # generate queries to remove person_ids of people not in the person table
    query_list.extend(drop_rows_for_missing_persons.get_queries(project, dataset))

    return query_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    Q_LIST = get_person_id_validation_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, Q_LIST)
