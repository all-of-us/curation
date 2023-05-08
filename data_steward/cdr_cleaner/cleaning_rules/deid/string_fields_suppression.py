import logging
from typing import NamedTuple, Union, List
from google.cloud.bigquery.client import Table
from google.api_core.exceptions import GoogleAPIError
from google.cloud.exceptions import GoogleCloudError, NotFound

import resources
from common import AOU_DEATH, CDM_TABLES, OBSERVATION, JINJA_ENV
from constants import bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.cancer_concept_suppression import CancerConceptSuppression
from cdr_cleaner.cleaning_rules.section_participation_concept_suppression import SectionParticipationConceptSuppression
from cdr_cleaner.cleaning_rules.deid.registered_cope_survey_suppression import RegisteredCopeSurveyQuestionsSuppression
from cdr_cleaner.cleaning_rules.vehicular_accident_concept_suppression import VehicularAccidentConceptSuppression
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from cdr_cleaner.clean_cdr_utils import get_tables_in_dataset

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC1369']

OBSERVATION_SOURCE_CONCEPT_ID = 'observation_source_concept_id'
VALUE_AS_STRING = 'value_as_string'
PPI_ZIP_CODE_CONCEPT_ID = 1585250
APPROXIMATE_DATE_OF_SYMPTOMS = 715711

SUPPRESSION_EXCEPTION_SANDBOX_QUERY_TEMPLATE = JINJA_ENV.from_string("""
-- Only sandboxing records that are not in sandbox_table --
SELECT
    d.*
FROM `{{project}}.{{dataset}}.{{domain_table}}` AS d
LEFT JOIN `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS s
    ON d.{{domain_table}}_id = s.{{domain_table}}_id
WHERE d.{{field_name}} = {{field_value}} and s.{{domain_table}}_id IS NULL
""")

STRING_FIELD_SUPPRESSION_QUERY_TEMPLATE = JINJA_ENV.from_string("""
-- The query is written using REPLACE because it is easier for writing the integration test --
-- otherwise one would have to define all the fields in the test data  --
SELECT
    d.* 
    REPLACE(
    {% for field in string_fields %}
        CAST({% if field['mode'] == 'required' %} '' {%- else -%} NULL {%- endif %} AS STRING) AS {{field['name']}}
        {%- if loop.nextitem is defined %}, {% endif %} --Add a comma at the end --  
    {% endfor %}
    )
FROM `{{project}}.{{dataset}}.{{domain_table}}` AS d
""")

RECOVER_SUPPRESSION_EXCEPTION_QUERY_TEMPLATE = JINJA_ENV.from_string("""
UPDATE `{{project}}.{{dataset}}.{{domain_table}}` AS d
{% for restore_field in restore_fields -%}
    {{'\t'}}SET d.{{restore_field}} = s.{{restore_field}} 
    {%- if loop.nextitem is defined %}, {% endif %} --Add a comma at the end --  
{% endfor %}
FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS s
WHERE d.{{domain_table}}_id = s.{{domain_table}}_id 
    AND s.{{field_name}} = {{field_value}}
""")

VALIDATION_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT
  d.*
FROM `{{project}}.{{dataset}}.{{domain_table}}` AS d
WHERE
{% for field in string_fields -%}
    ({{field['name']}} IS NOT NULL OR {{field['name']}} <> '')
    {%- if loop.nextitem is defined %} OR {% endif -%}
{% endfor %}
""")


class SuppressionException(NamedTuple):
    """
    A named tuple for storing exceptions that are excluded from the string field suppression. For 
    example, observation records related to PPI zipcode are not subject to the string suppression 
    rule, therefore we sandbox those records and restore the value in value_as_string field only 
    
    domain_table: the table from which the records are identified to sandbox for later recovery.
    sandbox_table: the sandbox table for storing the records
    field_name: the field for identifying records 
    field_value: the value for identifying records
    restore_fields: a list of string fields to recover after suppression has been applied
    """
    domain_table: str
    sandbox_table: str
    field_name: str
    field_value: Union[str, int]
    restore_fields: List[str]


def get_string_fields(domain_table):
    """
    Get string fields associated for the table.
    For aou_death, excludes `aou_death_id` and `src_id` because we must not
    suppress them with this cleaning rule.
    :param domain_table: 
    :return: 
    """
    if domain_table == AOU_DEATH:
        fields = [
            field for field in resources.fields_for(domain_table)
            if field['type'] == 'string' and
            field['name'] not in ['aou_death_id', 'src_id']
        ]
    else:
        fields = [
            field for field in resources.fields_for(domain_table)
            if field['type'] == 'string'
        ]

    return fields


class StringFieldsSuppression(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'Null all STRING type fields in all OMOP common data model tables'

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.CONTROLLED_TIER_DEID,
                             cdr_consts.REGISTERED_TIER_DEID
                         ],
                         depends_on=[
                             CancerConceptSuppression,
                             SectionParticipationConceptSuppression,
                             VehicularAccidentConceptSuppression,
                             RegisteredCopeSurveyQuestionsSuppression,
                         ],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=CDM_TABLES + [AOU_DEATH],
                         table_namer=table_namer)

    def setup_rule(self, client, *args, **keyword_args):
        """
        Create the sandbox tablenames in advance because the list of exceptions returned by 
        get_rule_exceptions() might write to the same sandbox table.  
        
        :param client: 
        :param args: 
        :param keyword_args: 
        :return: 
        """

        # The following makes sure the tables exist in the dataset
        try:
            self.affected_tables = get_tables_in_dataset(
                client, self.project_id, self.dataset_id, self.affected_tables)
        except GoogleCloudError as error:
            LOGGER.error(error)
            raise

        # The following checks if all the tables defined in SuppressionException exist and also
        # creates sandbox tables
        for rule_exception in self.get_rule_exceptions():
            try:
                fq_table_name = f'{self.project_id}.{self.dataset_id}.{rule_exception.domain_table}'
                table = client.get_table(fq_table_name)
                destination = f'{self.project_id}.{self.sandbox_dataset_id}.' \
                              f'{rule_exception.sandbox_table}'
                client.create_table(Table(destination, schema=table.schema),
                                    exists_ok=True)
            except (GoogleAPIError, NotFound, OSError, AttributeError,
                    TypeError, ValueError) as e:
                LOGGER.exception(
                    f"Unable to create table {destination} due to the exception below\n {e}"
                )
                raise

    def get_sandbox_tablenames(self):
        """
        Get a list of sandbox_tablenames from get_rule_exceptions()
        :return: 
        """
        return [
            rule_exception.sandbox_table
            for rule_exception in self.get_rule_exceptions()
        ]

    def get_rule_exceptions(self) -> List[SuppressionException]:
        """
        This class method can be extended to accommodate more rule exceptions in the future
        :return: 
        """
        return [
            SuppressionException(
                domain_table=OBSERVATION,
                sandbox_table=self.sandbox_table_for(OBSERVATION),
                field_name=OBSERVATION_SOURCE_CONCEPT_ID,
                field_value=PPI_ZIP_CODE_CONCEPT_ID,
                restore_fields=[VALUE_AS_STRING]),
            SuppressionException(
                domain_table=OBSERVATION,
                sandbox_table=self.sandbox_table_for(OBSERVATION),
                field_name=OBSERVATION_SOURCE_CONCEPT_ID,
                field_value=APPROXIMATE_DATE_OF_SYMPTOMS,
                restore_fields=[VALUE_AS_STRING])
        ]

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        queries = []

        for rule_exception in self.get_rule_exceptions():
            rule_exception_query = SUPPRESSION_EXCEPTION_SANDBOX_QUERY_TEMPLATE.render(
                project=self.project_id,
                dataset=self.dataset_id,
                sandbox_dataset=self.sandbox_dataset_id,
                domain_table=rule_exception.domain_table,
                sandbox_table=rule_exception.sandbox_table,
                field_name=rule_exception.field_name,
                field_value=rule_exception.field_value)

            queries.append({
                cdr_consts.QUERY: rule_exception_query,
                cdr_consts.DESTINATION_TABLE: rule_exception.sandbox_table,
                cdr_consts.DISPOSITION: bq_consts.WRITE_APPEND,
                cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id
            })

        for affected_table in self.affected_tables:
            string_fields = get_string_fields(affected_table)
            if string_fields:
                string_suppression_query = STRING_FIELD_SUPPRESSION_QUERY_TEMPLATE.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    domain_table=affected_table,
                    string_fields=string_fields)
                queries.append({
                    cdr_consts.QUERY: string_suppression_query,
                    cdr_consts.DESTINATION_TABLE: affected_table,
                    cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
                    cdr_consts.DESTINATION_DATASET: self.dataset_id
                })

        # Post suppression, restore the records that are excluded from the suppression rule
        for rule_exception in self.get_rule_exceptions():
            recovery_query = RECOVER_SUPPRESSION_EXCEPTION_QUERY_TEMPLATE.render(
                project=self.project_id,
                dataset=self.dataset_id,
                sandbox_dataset=self.sandbox_dataset_id,
                domain_table=rule_exception.domain_table,
                sandbox_table=rule_exception.sandbox_table,
                field_name=rule_exception.field_name,
                field_value=rule_exception.field_value,
                restore_fields=rule_exception.restore_fields)

            queries.append({
                cdr_consts.QUERY: recovery_query,
                cdr_consts.DESTINATION_DATASET: self.dataset_id
            })

        return queries

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validate whether any string field is not null (nullable) or non-empty (required). 
        :param client: 
        :param args: 
        :param keyword_args: 
        :return: 
        """
        for table in self.affected_tables:
            string_fields = get_string_fields(table)
            if string_fields:
                validation_query = VALIDATION_QUERY_TEMPLATE.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    domain_table=table,
                    string_fields=string_fields)
                result = client.query(validation_query).result()
                if result.total_rows > 0:
                    raise RuntimeError(
                        f'{table} has {result.total_rows} records that have non-null string values'
                    )


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine
    from utils import pipeline_logging

    ARGS = parser.default_parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

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
