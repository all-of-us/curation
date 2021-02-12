"""
## Purpose
Given a CDM dataset, ensure the "primary" concept fields (e.g. condition_occurrence.condition_concept_id) contain
standard concept_ids (based on vocabulary in same dataset) in the tables in DOMAIN_TABLE_NAMES.

## Overview
For all primary concept fields in domain tables being processed we do the following:
If the concept id is 0, set it to the source concept id.
If the associated concept.standard_concept='S', keep it.
Otherwise, replace it with the concept having 'Maps to' relationship in concept_relationship.
If a standard concept mapping to the field cannot be found, find a standard concept associated with the source concept.
If a standard concept cannot be found to either, we keep the original concept

## Steps
As a cleaning rule, this module generates a list of queries to be run, but there are three main steps:
 1) Create an intermediate table _logging_standard_concept_id_replacement which describes, for each row in each domain
    table, what action is to be taken.
 2) Update the domain tables
 3) Update the mapping tables

## One to Many Standard Concepts
Some concepts map to multiple standard concepts. In these cases, we create multiple rows in the domain table.
For example, we may have condition_occurrence records whose condition_concept_id is

  19934 "Person boarding or alighting a pedal cycle injured in collision with fixed or stationary object"

which maps to the three standard concepts:

  4053428 "Accident while boarding or alighting from motor vehicle on road"
  438921 "Collision between pedal cycle and fixed object"
  433099 "Victim, cyclist in vehicular AND/OR traffic accident"

In this case we remove the original row and add three records having the three standard concept ids above. New ids
are generated for these records, and, during the last step, the mapping tables are updated.

TODO account for "non-primary" concept fields
TODO when the time comes, include care_site, death, note, provider, specimen
"""
import logging

from common import JINJA_ENV
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
import resources
from validation.ehr_union import mapping_table_for
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list, \
    get_delete_empty_sandbox_tables_queries

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC808', 'DC389', 'DC1245']

DOMAIN_TABLE_NAMES = [
    'condition_occurrence', 'procedure_occurrence', 'drug_exposure',
    'device_exposure', 'observation', 'measurement', 'visit_occurrence'
]

SRC_CONCEPT_ID_TABLE_NAME = '_logging_standard_concept_id_replacement'

SRC_CONCEPT_ID_MAPPING_QUERY = JINJA_ENV.from_string("""
{%- for table_name in table_names %}
{% set domain_concept_id = get_domain_concept_id(table_name) %}
{% set domain_source = get_domain_source_concept_id(table_name) %}
SELECT DISTINCT 
    '{{table_name}}' AS domain_table,
    domain.{{table_name}}_id AS src_id,
    domain.{{table_name}}_id AS dest_id,
    domain.{{domain_concept_id}} AS concept_id,
    domain.{{domain_source}} AS src_concept_id,
    coalesce(dcr.concept_id_2, scr.concept_id_2, domain.{{domain_concept_id}}) AS new_concept_id,
    CASE
        WHEN domain.{{domain_source}} = 0 THEN domain.{{domain_concept_id}}
        ELSE domain.{{domain_source}}
    END AS new_src_concept_id,
    dcr.concept_id_2 IS NOT NULL AS lookup_concept_id, 
    dcr.concept_id_2 IS NULL AND scr.concept_id_2 IS NOT NULL AS lookup_src_concept_id,
    domain.{{domain_source}} = 0 AND dcr.concept_id_2 IS NOT NULL AS is_src_concept_id_replaced,
    CASE
        WHEN dcr.concept_id_2 IS NOT NULL THEN 'replaced using concept_id'
        WHEN scr.concept_id_2 IS NOT NULL THEN 'replaced using source_concept_id'
        ELSE 'kept the original concept_id'
    END AS action
FROM `{{project}}.{{dataset}}.{{table_name}}` AS domain 
LEFT JOIN 
    `{{project}}.{{dataset}}.concept` AS dc 
ON 
    domain.{{domain_concept_id}} = dc.concept_id
LEFT JOIN 
    `{{project}}.{{dataset}}.concept_relationship` AS dcr 
ON 
    dcr.concept_id_1 = dc.concept_id AND dcr.relationship_id = 'Maps to' 
LEFT JOIN 
    `{{project}}.{{dataset}}.concept` AS sc 
ON 
    domain.{{domain_source}} = sc.concept_id 
LEFT JOIN 
    `{{project}}.{{dataset}}.concept_relationship` AS scr 
ON 
    scr.concept_id_1 = sc.concept_id AND scr.relationship_id = 'Maps to' 
WHERE 
    dc.standard_concept IS NULL or dc.standard_concept = 'C'
{% if loop.nextitem is defined %}
UNION ALL
{% endif -%}
{% endfor %}
""")

DUPLICATE_ID_UPDATE_QUERY = JINJA_ENV.from_string("""
UPDATE 
    `{{project}}.{{sandbox_dataset}}.{{logging_table}}` AS to_update 
SET 
    to_update.dest_id = v.dest_id
FROM 
(
    {%- for table_name in table_names %}
    SELECT
      a.src_id,
      a.domain_table,
      a.new_concept_id,
      ROW_NUMBER() OVER(ORDER BY a.src_id, a.new_concept_id) + src.max_id AS dest_id
    FROM
      `{{project}}.{{sandbox_dataset}}.{{logging_table}}` AS a
    JOIN (
        SELECT src_id
        FROM `{{project}}.{{sandbox_dataset}}.{{logging_table}}`
        WHERE domain_table = '{{table_name}}'
        GROUP BY src_id
        HAVING COUNT(*) > 1 ) b
    ON a.src_id = b.src_id AND a.domain_table = '{{table_name}}'
    CROSS JOIN (
      SELECT
        MAX({{table_name}}_id) AS max_id
      FROM `{{project}}.{{dataset}}.{{table_name}}`
    ) src
    {% if loop.nextitem is defined %}
    UNION ALL
    {% endif -%}
    {% endfor %}
) v
WHERE
    v.src_id = to_update.src_id
    AND v.domain_table = to_update.domain_table
    AND v.new_concept_id = to_update.new_concept_id
""")

SRC_CONCEPT_ID_UPDATE_QUERY = JINJA_ENV.from_string("""
SELECT
    {{cols}}
FROM `{{project}}.{{dataset}}.{{domain_table}}`
LEFT JOIN
    `{{project}}.{{sandbox_dataset}}.{{logging_table}}`
ON domain_table = '{{domain_table}}' AND src_id = {{domain_table}}_id	
""")

SANDBOX_SRC_CONCEPT_ID_UPDATE_QUERY = JINJA_ENV.from_string("""
SELECT DISTINCT
    d.*
FROM `{{project}}.{{dataset}}.{{domain_table}}` AS d
JOIN
    `{{project}}.{{sandbox_dataset}}.{{logging_table}}` AS l
ON l.domain_table = '{{domain_table}}' AND l.src_id = d.{{domain_table}}_id	
""")

UPDATE_MAPPING_TABLES_QUERY = JINJA_ENV.from_string("""
SELECT
    {{cols}}
FROM
    `{{project}}.{{dataset}}.{{mapping_table}}` as domain
LEFT JOIN
    `{{project}}.{{sandbox_dataset}}.{{logging_table}}` as log
ON src_id = {{domain_table}}_id AND domain_table = '{{domain_table}}'
""")


class ReplaceWithStandardConceptId(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Given a CDM dataset, ensure the "primary" concept fields '
            '(e.g. condition_occurrence.condition_concept_id) contain standard concept_ids '
            '(based on vocabulary in same dataset) in the tables in DOMAIN_TABLE_NAMES.'
        )

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         affected_tables=DOMAIN_TABLE_NAMES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self.affected_tables
        ]

    def parse_mapping_table_update_query(self, table_name, mapping_table_name):
        """

        Fill in mapping tables query so it either gets dest_id from the logging table or the domain table

        :param table_name: name of the domain table for which the query needs to be parsed
        :param mapping_table_name: name of the mapping_table for which the query needs to be parsed
        :return:
        """
        fields = [
            field['name'] for field in resources.fields_for(mapping_table_name)
        ]
        col_exprs = []
        for field_name in fields:
            if field_name == resources.get_domain_id_field(table_name):
                col_expr = 'coalesce(dest_id, {field}) AS {field}'.format(
                    field=field_name)
            else:
                col_expr = field_name
            col_exprs.append(col_expr)
        cols = ', '.join(col_exprs)
        return UPDATE_MAPPING_TABLES_QUERY.render(
            cols=cols,
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            mapping_table=mapping_table_name,
            logging_table=SRC_CONCEPT_ID_TABLE_NAME,
            domain_table=table_name)

    def get_mapping_table_update_queries(self):
        """
        Generates a list of query dicts for adding newly generated rows to corresponding 
        mapping_tables 
        :return: list of query dicts for updating mapping_tables 
        """
        queries = []
        for domain_table in self.affected_tables:
            mapping_table = mapping_table_for(domain_table)
            queries.append({
                cdr_consts.QUERY:
                    self.parse_mapping_table_update_query(
                        domain_table, mapping_table),
                cdr_consts.DESTINATION_TABLE:
                    mapping_table,
                cdr_consts.DISPOSITION:
                    bq_consts.WRITE_TRUNCATE,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id
            })

        return queries

    def parse_sandbox_src_concept_id_update_query(self, table_name):
        """
        Fill in template query used to sandbox the records that will be updated
        :param table_name: name of a domain table
        :return: parsed src_concept_id_update query
        """

        return SANDBOX_SRC_CONCEPT_ID_UPDATE_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            domain_table=table_name,
            logging_table=SRC_CONCEPT_ID_TABLE_NAME)

    def parse_src_concept_id_update_query(self, table_name):
        """
        Fill in template query used to generate updated domain table

        :param table_name: name of a domain table
        :return: parsed src_concept_id_update query
        """
        fields = [field['name'] for field in resources.fields_for(table_name)]
        col_exprs = []
        fields_to_replace = {
            resources.get_domain_id_field(table_name):
                'dest_id',
            resources.get_domain_concept_id(table_name):
                'new_concept_id',
            resources.get_domain_source_concept_id(table_name):
                'new_src_concept_id'
        }
        for field_name in fields:
            if field_name in fields_to_replace:
                col_expr = 'coalesce({replace_field}, {field}) AS {field}'.format(
                    replace_field=fields_to_replace[field_name],
                    field=field_name)
            else:
                col_expr = field_name
            col_exprs.append(col_expr)
        cols = ', '.join(col_exprs)

        return SRC_CONCEPT_ID_UPDATE_QUERY.render(
            cols=cols,
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            domain_table=table_name,
            logging_table=SRC_CONCEPT_ID_TABLE_NAME)

    def get_sandbox_src_concept_id_update_queries(self):
        """
        Generates a list of query dicts for sandboxing records that will be updated
        
        :return: a list of query dicts for updating the standard_concept_ids
        """
        queries = []
        for domain_table in self.affected_tables:
            sandbox_query = self.parse_sandbox_src_concept_id_update_query(
                domain_table)
            sandbox_table_name = self.sandbox_table_for(domain_table)
            queries.append({
                cdr_consts.QUERY: sandbox_query,
                cdr_consts.DESTINATION_TABLE: sandbox_table_name,
                cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
                cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id
            })
        return queries

    def get_src_concept_id_update_queries(self):
        """
        Generates a list of query dicts for replacing the standard concept ids in domain tables
        
        :return: a list of query dicts for updating the standard_concept_ids
        """

        queries = []
        for domain_table in self.affected_tables:
            queries.append({
                cdr_consts.QUERY:
                    self.parse_src_concept_id_update_query(domain_table),
                cdr_consts.DESTINATION_TABLE:
                    domain_table,
                cdr_consts.DISPOSITION:
                    bq_consts.WRITE_TRUNCATE,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id
            })
        return queries

    def parse_duplicate_id_update_query(self):
        """
        Generates a update query that combines all update sub queries generated per domain
        :return: 
        """
        update_logging_table_query = DUPLICATE_ID_UPDATE_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            logging_table=SRC_CONCEPT_ID_TABLE_NAME,
            table_names=self.affected_tables)
        return update_logging_table_query

    def parse_src_concept_id_logging_query(self):
        """
        Generates a query that combines all logging sub_queries generated per domain
        :return: 
        """
        return SRC_CONCEPT_ID_MAPPING_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            table_names=self.affected_tables,
            get_domain_concept_id=resources.get_domain_concept_id,
            get_domain_source_concept_id=resources.get_domain_source_concept_id)

    def get_src_concept_id_logging_queries(self):
        """
        Creates logging table and generates a list of query dicts for populating it
        :return: a list of query dicts to gather logging records
        """

        # Populate the logging table for keeping track of which records need to be updated
        queries = [{
            cdr_consts.QUERY: self.parse_src_concept_id_logging_query(),
            cdr_consts.DESTINATION_TABLE: SRC_CONCEPT_ID_TABLE_NAME,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id
        }]

        # For new rows added as a result of one-to-many standard concepts, we give newly generated
        # rows new ids. These queries need to be run after _logging_standard_concept_id_replacement
        # is populated.
        update_queries = [{
            cdr_consts.QUERY: self.parse_duplicate_id_update_query(),
            cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id
        }]
        return queries + update_queries

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        :return: a list of query dicts for replacing standard_concept_ids in domain_tables
        """
        queries_list = []
        queries_list.extend(self.get_src_concept_id_logging_queries())
        queries_list.extend(self.get_sandbox_src_concept_id_update_queries())
        queries_list.extend(self.get_src_concept_id_update_queries())
        queries_list.extend(self.get_mapping_table_update_queries())
        queries_list.extend(
            get_delete_empty_sandbox_tables_queries(
                self.project_id, self.sandbox_dataset_id,
                self.get_sandbox_tablenames()))
        return queries_list

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(ReplaceWithStandardConceptId,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(ReplaceWithStandardConceptId,)])
