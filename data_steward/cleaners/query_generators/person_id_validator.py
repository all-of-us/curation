"""
Run the person_id validation clean rule.

1.  The person_id in each of the defined tables exists in the person table.
    If not valid, remove the record.
2.  The person_id is consenting.  If not consenting, remove EHR records.
    Keep PPI records.
"""

import constants.bq_utils as bq_consts
import constants.cleaners.clean_cdr as clean_consts
import resources

MAPPED_VALIDATION_TABLES = [
    'visit_occurrence',
    'condition_occurrence',
    'drug_exposure',
    'measurement',
    'procedure_occurrence',
    'observation',
    'device_exposure',
    'specimen',
]

UNMAPPED_VALIDATION_TABLES = [
    # mapping tables do not exist for the following tables
    'death',
]

# The below query translates to:
# Find all consented participants.
# Find all mappings like ehr
# Join the table with consented participants and join again with mappings
NOT_CONSENTING_PERSON_IDS = (
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
    'RIGHT JOIN consented AS cons '
    'ON entry.person_id = cons.person_id '
    'UNION ALL '
    # get all unconsented non-ehr rows
    'SELECT {fields} FROM `{project}.{dataset}.{table}` AS entry '
    'RIGHT JOIN unconsented as cons on entry.person_id = cons.person_id '
    'JOIN ppi_mappings AS maps ON maps.{table}_id = entry.{table}_id '
)

# drop rows of person_ids not in the person table
DELETE_ORPHANED_PERSON_IDS = (
    'SELECT {fields} FROM `{project}.{dataset}.{table}` AS entry '
    'RIGHT JOIN `{project}.{dataset}.person` AS person '
    'ON entry.person_id = person.person_id'
)

def get_person_id_validation_queries(project=None, dataset=None):
    """
    Return query list of queries to ensure valid people are in the tables.

    The non-consenting queries rely on the mapping tables.  When using the
    combined and unidentified dataset, the last portion of the dataset name is
    removed to access these tables.  Any other dataset is expectedt to have
    these tables and uses the mapping tables from within the same dataset.

    :return:  A list of string queries that can be exexcuted to delete invalid
        records for invalid persons
    """
    query_list = []

    if dataset.endswith('_deid'):
        mapping_ds = dataset[0:-5]
    else:
        mapping_ds = dataset

    # generate queries to remove EHR records of non-ehr consented persons
    for table in MAPPED_VALIDATION_TABLES:
        field_names = ['entry.' + field['name'] for field in resources.fields_for(table)]
        fields = ', '.join(field_names)
        consent_query = NOT_CONSENTING_PERSON_IDS.format(
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

    all_tables = MAPPED_VALIDATION_TABLES
    all_tables.extend(UNMAPPED_VALIDATION_TABLES)

    # generate queries to remove person_ids of people not in the person table
    for table in all_tables:
        field_names = [field['name'] for field in resources.fields_for(table)]
        fields = ', '.join(field_names)

        delete_query = DELETE_ORPHANED_PERSON_IDS.format(
            project=project, dataset=dataset, table=table, fields=fields
        )

        query_dict = {
            clean_consts.QUERY: delete_query,
            clean_consts.DESTINATION_TABLE: table,
            clean_consts.DESTINATION_DATASET: dataset,
            clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
        }
        query_list.append(query_dict)

    return query_list

if __name__ == '__main__':
    import argparse

    import cleaners.clean_cdr_engine

    PARSER = argparse.ArgumentParser(
        description='Parse project_id and dataset_id',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    PARSER.add_argument('project_id',
                        help='Project associated with the input and output datasets')
    PARSER.add_argument('dataset_id',
                        help='Dataset where cleaning rules are to be applied')
    ARGS = PARSER.parse_args()

    if ARGS.dataset_id:
        Q_LIST = get_person_id_validation_queries(ARGS.project_id, ARGS.dataset_id)
        clean_cdr_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, Q_LIST)
