import logging
from typing import NamedTuple, Union, List

import resources
from common import CDM_TABLES, OBSERVATION, JINJA_ENV
from constants import bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC1369']

STRING_FIELD_SUPPRESSION_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT
{% for field in fields %}
{% if field['type'] == 'string' -%}
    {{'\t'}}{% if field['mode'] == 'required' %} '' {%- else -%}NULL {%- endif %} AS {{field['name']}}
{%- else -%}
    {{'\t'}}{{field['name']}}
{%- endif %} {%- if loop.nextitem is defined %}, {% endif %} --Add a comma at the end --  
{% endfor %}
FROM `{{project}}.{{dataset}}.{{domain_table}}` AS d
""")


class SuppressionException(NamedTuple):
    """
    A named tuple for storing exceptions that are excluded from the string field suppression
    """
    table_name: str
    field_name: str
    value: Union[str, int]


class StringFieldsSuppression(BaseCleaningRule):

    def get_rule_exceptions(self) -> List[SuppressionException]:
        pass

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'null all STRING type fields in all OMOP common data model tables'

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=CDM_TABLES)

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(OBSERVATION)]

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        queries = [{
            cdr_consts.QUERY:
                STRING_FIELD_SUPPRESSION_QUERY_TEMPLATE.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    domain_table=affected_table,
                    fields=resources.fields_for(affected_table)),
            cdr_consts.DESTINATION_TABLE:
                self.sandbox_table_for(affected_table),
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id
        } for affected_table in self.affected_tables]
        return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(StringFieldsSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(StringFieldsSuppression,)])
