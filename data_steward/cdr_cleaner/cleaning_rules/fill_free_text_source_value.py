"""
DC - 399

De-id for registered tier removes all free text fields. We are re-populating those fields with the concept_code
value for the concept_id where possible to improve the clarity/readability of the resource.

list of free text source_value fields which will be re-populated with concept_code.

visit_occurrence - [visit_source_value,
                    admitting_source_value,
                    discharge_to_source_value]
device_exposure - [device_source_value]
measurement - [measurement_source_value,
               unit_source_value,
               value_source_value]
death - [cause_source_value]
procedure_occurrence - [procedure_source_value,
                        qualifier_source_value]
provider - [specialty_source_value,
            gender_source_value]
specimen - [specimen_source_value,
            unit_source_value,
            anatomic_site_source_value,
            disease_status_source_value]
condition_occurrence - [condition_source_value,
        `               condition_status_source_value]
care_site - [place_of_service_source_value]
procedure_cost - [revenue_code_source_value]
observation - [value_as_string,
               observation_source_value,
               unit_source_value,
               qualifier_source_value,
               value_source_value]
person - [gender_source_value,
          race_source_value,
          ethnicity_source_value]
drug_exposure - [drug_source_value,
                 route_source_value]

Following listed fields are the ones which are not being re-populated at this point of time because no corresponding
concept_id field is available

provider : provider_source_value
person : person_source_value
care_site : care_site_source_value
drug_exposure : dose_unit_source_value
note : note_source_value
"""

import resources
import constants.cdr_cleaner.clean_cdr as cdr_consts
import constants.bq_utils as bq_consts

LEFT_JOIN = (
    'LEFT JOIN `{project}.{dataset}.concept` as {prefix} '
    'on m.{concept_id_field} = {prefix}.concept_id '
)

FIELD_REPLACE_QUERY = (
    'select {columns} '
    '    from `{project}.{dataset}.{table_name}` as m '
    '    {join_expression}'
)


def get_fields_dict(table_name, fields):
    """

    This method goes into list of fields of particular table and find out the fields which match a specified conditions,
    if found they will be added to a dictionary along with the field which needs to be joined on.

    :param table_name: Name of a domain table
    :param fields: list of fields of a particular table
    :return: a dictionary
    """
    fields_to_replace = dict()
    prefix_counter = 0
    for field in fields:
        prefix_counter += 1
        # Check if the field is _source_value field and has corresponding _source_concept_id field
        # using string comprehension to remove value from source_value to source_concept_id
        if '_source_value' in field and field[:-5] + 'concept_id' in fields:
            fields_to_replace[field] = {'name': field, 'join_field': field[:-5] + 'concept_id',
                                        'prefix': field[:3] + '_{prefix}'.format(prefix=prefix_counter)}
        # Check if the field is _source_value field and has corresponding _concept_id field
        # if _source_concept_id is not available
        # using string comprehension to remove value from source_value to source_concept_id
        elif '_source_value' in field and field[:-12] + 'concept_id' in fields:
            fields_to_replace[field] = {'name': field, 'join_field': field[:-12] + 'concept_id',
                                        'prefix': field[:3] + '_{prefix}'.format(prefix=prefix_counter)}
        # Check if the field is _source_value field and has corresponding _concept_id field
        # if _source_concept_id is not available
        # using string comprehension to remove value from source_value to source_concept_id
        elif '_source_value' in field and field[:-12] + 'as_concept_id' in fields:
            fields_to_replace[field] = {'name': field, 'join_field': field[:-12] + 'as_concept_id',
                                        'prefix': field[:3] + '_{prefix}'.format(prefix=prefix_counter)}
        # Check if the field is value_as_string and has corresponding value_as_concept_id field
        # using string comprehension to remove string from value_as_string and replace with value_as_concept_id
        elif '_as_string' in field and field[:-6] + 'concept_id' in fields:
            fields_to_replace[field] = {'name': field, 'join_field': field[:-6] + 'concept_id',
                                        'prefix': field[:3] + '_{prefix}'.format(prefix=prefix_counter)}
        # if the table is procedure_occurrence check if the field is qualifier_Source_value if so.
        # it doesn't have qualifier_concept_id field or qualifier_source_concept_id field in this vocabulary version
        # it is fixed in the later versions. In the mean time we will be using modifier_concept_id
        # as the corresponding id_field
        elif table_name == cdr_consts.PROCEDURE_OCCURRENCE and field == cdr_consts.QUALIFIER_SOURCE_VALUE:
            fields_to_replace[field] = {'name': field, 'join_field': 'modifier_concept_id',
                                        'prefix': field[:3] + '_{prefix}'.format(prefix=prefix_counter)}

    return fields_to_replace


def get_modified_columns(fields, fields_to_replace):
    """

    This method updates the columns by adding prefix to each column if the column is being replaced and
    joins it with other columns.

    :param fields: list of fields of a particular table
    :param fields_to_replace: dictionary of fields of a table which needs to be updated
    :return: a string
    """
    col_exprs = []
    for field in fields:
        if field in fields_to_replace:
            col_expr = '{x}.concept_code as {y}'.format(x=fields_to_replace[field]['prefix'],
                                                        y=fields_to_replace[field]['name'])
        else:
            col_expr = field
        col_exprs.append(col_expr)
    cols = ', '.join(col_exprs)
    return cols


def get_full_join_expression(dataset_id, project_id, fields_to_replace):
    """

    This collects all the join expressions and joins them as a string and returns a string.

    :param dataset_id: Name of the dataset
    :param project_id: Name of the project
    :param fields_to_replace: dictionary of fields to be joined
    :return:
    """
    join_expr = []
    for field in fields_to_replace:
        left_join = LEFT_JOIN.format(project=project_id,
                                     dataset=dataset_id,
                                     concept_id_field=fields_to_replace[field]['join_field'],
                                     prefix='{}'.format(fields_to_replace[field]['prefix']))
        join_expr.append(left_join)
    return " ".join(join_expr)


def get_fill_freetext_source_value_fields_queries(project_id, dataset_id):
    """

    Generates queries to replace the source_value_fields with the concept_code.

    :param project_id: Name of the project where the dataset on which the rules are to be applied on
    :param dataset_id: Name of the dataset on which the rules are to be applied on
    :return: A list of queries to be run.
    """
    queries_list = []
    for table in resources.CDM_TABLES:
        fields = [field['name'] for field in resources.fields_for(table)]
        fields_to_replace = get_fields_dict(table, fields)

        if fields_to_replace:
            cols = get_modified_columns(fields, fields_to_replace)

            full_join_expression = get_full_join_expression(dataset_id, project_id, fields_to_replace)

            query = dict()
            query[cdr_consts.QUERY] = FIELD_REPLACE_QUERY.format(columns=cols,
                                                                 table_name=table,
                                                                 dataset=dataset_id,
                                                                 project=project_id,
                                                                 join_expression=full_join_expression
                                                                 )
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query[cdr_consts.DESTINATION_DATASET] = dataset_id
            queries_list.append(query)
    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_fill_freetext_source_value_fields_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
