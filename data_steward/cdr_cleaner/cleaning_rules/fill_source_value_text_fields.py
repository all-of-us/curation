"""
DC - 399

De-id for registered tier and controlled tier removes all free text fields with the cleaning rule StringFieldsSuppression. 
We are re-populating those fields with the concept_code value for the concept_id where possible to improve the clarity/readability of the resource.

list of free text source_value fields which will be re-populated with concept_code using concept_ids from the columns
mentioned below.

visit_occurrence - [visit_source_value : visit_source_concept_id,
                    admitting_source_value : admitting_source_concept_id,
                    discharge_to_source_value : discharge_to_concept_id]
device_exposure - [device_source_value : device_source_concept_id]
measurement - [measurement_source_value : measurement_source_concept_id,
               unit_source_value : unit_concept_id,
               value_source_value : value_as_concept_id]

death - [cause_source_value : cause_source_concept_id]
aou_death - [cause_source_value : cause_source_concept_id]

procedure_occurrence - [procedure_source_value : procedure_source_concept_id,
                        qualifier_source_value : modifier_concept_id]
provider - [specialty_source_value : specialty_source_concept_id,
            gender_source_value : gender_source_concept_id]
specimen - [specimen_source_value : specimen_concept_id,
            unit_source_value : unit_concept_id ,
            anatomic_site_source_value : anatomic_site_concept_id,
            disease_status_source_value : disease_status_concept_id]
condition_occurrence - [condition_source_value : condition_source_concept_id,
        `               condition_status_source_value : condition_status_concept_id]
care_site - [place_of_service_source_value : place_of_service_concept_id]
procedure_cost - [revenue_code_source_value : revenue_code_concept_id]
observation - [value_as_string : value_as_concept_id,
               observation_source_value : observation_source_concept_id,
               unit_source_value : unit_concept_id,
               qualifier_source_value : qualifier_concept_id,
               value_source_value : value_source_concept_id]
person - [gender_source_value : gender_source_concept_id,
          race_source_value : race_source_concept_id,
          ethnicity_source_value : ethnicity_source_concept_id]
drug_exposure - [drug_source_value : drug_source_concept_id,
                 route_source_value : route_concept_id]

Following listed fields are the ones which are not being re-populated at this point of time because no corresponding
concept_id field is available

provider : provider_source_value
person : person_source_value
care_site : care_site_source_value
drug_exposure : dose_unit_source_value
note : note_source_value
"""

import logging

import resources
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from cdr_cleaner.cleaning_rules.deid.string_fields_suppression import StringFieldsSuppression
from common import AOU_DEATH, JINJA_ENV, OBSERVATION
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)
JIRA_ISSUE_NUMBERS = ['DC399', 'DC812']

# TODO remove this and embed all the template logic in FIELD_REPLACE_QUERY
LEFT_JOIN = JINJA_ENV.from_string(
    """LEFT JOIN `{{project}}.{{dataset}}.concept` as {{prefix}}
             on m.{{concept_id_field}} = {{prefix}}.concept_id """)

FIELD_REPLACE_QUERY = JINJA_ENV.from_string("""select {{columns}}
                           from `{{project}}.{{dataset}}.{{table_name}}` as m
                           {{join_expression}}""")


def get_affected_tables():
    return resources.CDM_TABLES + [AOU_DEATH]


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
        if '_source_value' in field and field[:-5] + 'concept_id' in fields:
            fields_to_replace[field] = {
                'name':
                    field,
                'join_field':
                    field[:-5] + 'concept_id',
                'prefix':
                    field[:3] + '_{counter}'.format(counter=prefix_counter)
            }
        # Check if the field is _source_value field and has corresponding _concept_id field
        # if _source_concept_id is not available
        elif '_source_value' in field and field[:-12] + 'concept_id' in fields:
            fields_to_replace[field] = {
                'name':
                    field,
                'join_field':
                    field[:-12] + 'concept_id',
                'prefix':
                    field[:3] + '_{counter}'.format(counter=prefix_counter)
            }
        # if _concept_id is not available
        # Check if the field is _source_value field and has corresponding _as_concept_id field
        elif '_source_value' in field and field[:
                                                -12] + 'as_concept_id' in fields:
            fields_to_replace[field] = {
                'name':
                    field,
                'join_field':
                    field[:-12] + 'as_concept_id',
                'prefix':
                    field[:3] + '_{counter}'.format(counter=prefix_counter)
            }
        # Check if the field is value_as_string and has corresponding value_as_concept_id field
        elif '_as_string' in field and field[:-6] + 'concept_id' in fields:
            fields_to_replace[field] = {
                'name':
                    field,
                'join_field':
                    field[:-6] + 'concept_id',
                'prefix':
                    field[:3] + '_{counter}'.format(counter=prefix_counter)
            }

    return fields_to_replace


def get_modified_columns(fields, fields_to_replace, table=None):
    """

    This method updates the columns by adding prefix to each column if the column is being replaced and
    joins it with other columns.

    :param fields: list of fields of a particular table
    :param fields_to_replace: dictionary of fields of a table which needs to be updated
    :param table: table with fields to be updated
    :return: a string
    """
    col_exprs = []
    for field in fields:
        if field in fields_to_replace:
            if table == OBSERVATION and field == 'value_as_string':
                col_expr = """
                    IF(observation_source_concept_id = 1585250 AND REGEXP_CONTAINS(value_as_string, r'\*\*$'),
                        {name}, {prefix}.concept_code) as {name}
                """.format(prefix=fields_to_replace[field]['prefix'],
                           name=fields_to_replace[field]['name'])
            else:
                col_expr = '{prefix}.concept_code as {name}'.format(
                    prefix=fields_to_replace[field]['prefix'],
                    name=fields_to_replace[field]['name'])
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
        left_join = LEFT_JOIN.render(
            project=project_id,
            dataset=dataset_id,
            concept_id_field=fields_to_replace[field]['join_field'],
            prefix='{}'.format(fields_to_replace[field]['prefix']))
        join_expr.append(left_join)
    return " ".join(join_expr)


class FillSourceValueTextFields(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Populates each free text value field with the concept_code from the concept table that
        matches the concept_id field
        """
        desc = (
            'Populates each free text value field with the concept_code from the concept table that matches the '
            'concept_id field')

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.REGISTERED_TIER_DEID_BASE,
                             cdr_consts.CONTROLLED_TIER_DEID_BASE
                         ],
                         affected_tables=get_affected_tables(),
                         depends_on=[StringFieldsSuppression],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        queries_list = []

        for table in self.affected_tables:
            fields = [field['name'] for field in resources.fields_for(table)]
            fields_to_replace = get_fields_dict(table, fields)
            # Added this to stop validated_survey_source_value to be refilled as the data type is
            # integer in schema and if repopulated with concept code it will be changed to string
            # and fails to copy to final dataset.
            if table == 'survey_conduct':
                del fields_to_replace['validated_survey_source_value']

            if fields_to_replace:
                cols = get_modified_columns(fields,
                                            fields_to_replace,
                                            table=table)

                full_join_expression = get_full_join_expression(
                    self.dataset_id, self.project_id, fields_to_replace)

                query = dict()
                query[cdr_consts.QUERY] = FIELD_REPLACE_QUERY.render(
                    columns=cols,
                    table_name=table,
                    dataset=self.dataset_id,
                    project=self.project_id,
                    join_expression=full_join_expression)
                query[cdr_consts.DESTINATION_TABLE] = table
                query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
                query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
                queries_list.append(query)
        return queries_list

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(FillSourceValueTextFields,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(FillSourceValueTextFields,)])
