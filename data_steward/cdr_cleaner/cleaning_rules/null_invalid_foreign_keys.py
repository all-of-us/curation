"""
 Cleaning Rule 1 : Foreign key references (i.e. visit_occurrence_id in the condition table) should be valid.

(Existing Achilles rule - validating for foreign keys include provider_id,
care_site_id, location_id, person_id, visit_occurrence_id)

Valid means an existing foreign key exists in the table it references.
"""
import logging

# Project Imports
import bq_utils
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
import resources

LOGGER = logging.getLogger(__name__)

FOREIGN_KEYS_FIELDS = [
    'person_id', 'visit_occurrence_id', 'location_id', 'care_site_id',
    'provider_id'
]

INVALID_FOREIGN_KEY_QUERY = ('SELECT {cols} '
                             'FROM `{project}.{dataset_id}.{table_name}` t '
                             '{join_expr}')

LEFT_JOIN = ('LEFT JOIN `{dataset_id}.{table}` {prefix} '
             'ON t.{field} = {prefix}.{field} ')


def _mapping_table_for(domain_table):
    """
    Get name of mapping table generated for a domain table

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence',
        'condition_occurrence')
    :return: mapping table name
    """
    return '_mapping_' + domain_table


def null_invalid_foreign_keys(project_id, dataset_id):
    """
    This method gets the queries required to make invalid foreign keys null

    :param project_id: Project associated with the input and output datasets
    :param dataset_id: Dataset where cleaning rules are to be applied
    :return: a list of queries
    """
    queries_list = []
    for table in resources.CDM_TABLES:
        field_names = [field['name'] for field in resources.fields_for(table)]
        foreign_keys_flags = []
        fields_to_join = []

        for field_name in field_names:
            if field_name in FOREIGN_KEYS_FIELDS and field_name != table + '_id':
                fields_to_join.append(field_name)
                foreign_keys_flags.append(field_name)

        if fields_to_join:
            col_exprs = []
            for field in field_names:
                if field in fields_to_join:
                    if field in foreign_keys_flags:
                        col_expr = '{x}.'.format(x=field[:3]) + field
                else:
                    col_expr = field
                col_exprs.append(col_expr)
            cols = ', '.join(col_exprs)

            join_expression = []
            for key in FOREIGN_KEYS_FIELDS:
                if key in foreign_keys_flags:
                    if key == 'person_id':
                        table_alias = cdr_consts.PERSON_TABLE_NAME
                    else:
                        table_alias = _mapping_table_for(
                            '{x}'.format(x=key)[:-3])
                    join_expression.append(
                        LEFT_JOIN.format(dataset_id=dataset_id,
                                         prefix=key[:3],
                                         field=key,
                                         table=table_alias))

            full_join_expression = " ".join(join_expression)
            query = dict()
            query[cdr_consts.QUERY] = INVALID_FOREIGN_KEY_QUERY.format(
                cols=cols,
                table_name=table,
                dataset_id=dataset_id,
                project=project_id,
                join_expr=full_join_expression)
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query[cdr_consts.DESTINATION_DATASET] = dataset_id
            queries_list.append(query)
    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 [(null_invalid_foreign_keys,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(null_invalid_foreign_keys,)])
