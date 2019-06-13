"""
Run the person_id validation clean rule.

1.  The person_id in each of the defined tables exists in the person table.
    If not valid, remove the record.
2.  The person_id is consenting.  If not consenting, remove EHR records.
    Keep PPI records.
"""

MAPPED_VALIDATION_TABLES = [
    'visit_occurrence',
    'condition_occurrence',
    'drug_exposure',
    'measurement',
    'procedure_occurrence',
    'observation',
    'device_exposure',
    'specimen'
]

UNMAPPED_VALIDATION_TABLES = [
    # mapping tables do not exist for the following tables
    'death',
]

# The below query translates to:
#DELETE `{project}.{dataset}.{table}` entry
#WHERE entry.person_id IN (non-ehr-consenting person_ids)
#AND entry.{table}_id IN (ehr mapping table results)
NOT_CONSENTING_PERSON_IDS = """
DELETE `{project}.{dataset}.{table}` entry
WHERE entry.person_id IN (
        SELECT person_id
        FROM (
            SELECT person_id, value_source_concept_id, observation_datetime,
            ROW_NUMBER() OVER(
                PARTITION BY person_id ORDER BY observation_datetime DESC,
                value_source_concept_id ASC) AS rn
            FROM `{project}.{dataset}.observation`
            WHERE observation_source_value = 'EHRConsentPII_ConsentPermission')
        WHERE rn=1 AND value_source_concept_id != 1586100)
AND entry.{table}_id IN (
    SELECT {table}_id
    FROM `{project}.{mapping_dataset}._mapping_{table}`
    WHERE src_dataset_id like '%ehr%')
"""

DELETE_ORPHANED_PERSON_IDS = """
DELETE `{project}.{dataset}.{table}` entry
WHERE entry.person_id NOT IN (SELECT person_id FROM `{project}.{dataset}.person`)
"""

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
        consent_query = NOT_CONSENTING_PERSON_IDS.format(
            project=project, dataset=dataset, table=table, mapping_dataset=mapping_ds
        )
        query_list.append(consent_query)

    all_tables = MAPPED_VALIDATION_TABLES
    all_tables.extend(UNMAPPED_VALIDATION_TABLES)

    # generate queries to remove person_ids of people not in the person table
    for table in all_tables:
        delete_query = DELETE_ORPHANED_PERSON_IDS.format(
            project=project, dataset=dataset, table=table
        )
        query_list.append(delete_query)

    return query_list

if __name__ == '__main__':
    import argparse

    import data_steward.cleaners.clean_cdr_engine

    parser = argparse.ArgumentParser(
        description='Parse project_id and dataset_id',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id',
                        help='Project associated with the input and output datasets')
    parser.add_argument('dataset_id',
                        help='Dataset where cleaning rules are to be applied')
    args = parser.parse_args()

    if args.dataset_id:
        q_list = get_person_id_validation_queries(args.project_id, args.dataset_id)
        #clean_cdr_engine.clean_dataset(args.project_id, args.dataset_id, query_list)
        for query in q_list:
            print query
            print '\n'
