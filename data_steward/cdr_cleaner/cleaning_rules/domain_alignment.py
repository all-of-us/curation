"""
COMBINED_SNAPSHOT should be set to create a new snapshot dataset while running this cleaning rule.
"""
# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.domain_mapping import DOMAIN_TABLE_NAMES, METADATA_DOMAIN
from common import COMBINED, JINJA_ENV, OBSERVATION
import constants.cdr_cleaner.clean_cdr as cdr_consts
from gcloud.bq import BigQueryClient
from resources import (FIELD_MAPPINGS_PATH, TABLE_MAPPINGS_PATH,
                       VALUE_MAPPINGS_PATH, csv_to_list, fields_for, get_domain,
                       get_domain_concept_id, mapping_table_for)

LOGGER = logging.getLogger(__name__)

LOOKUP_TABLE = 'lookup_domain_alignment'

CREATE_LOOKUP_ALIGNMENT_TMPL = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}`
(src_table STRING, dest_table STRING, src_id INT64, dest_id INT64, is_rerouted BOOL)
""")

INSERT_LOOKUP_BETWEEN_TABLES_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}`
(src_table, dest_table, src_id, dest_id, is_rerouted)
WITH max_id AS (
    SELECT MAX(dest_id) AS max_dest_id FROM (
        SELECT dest_id 
        FROM `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}`
        WHERE dest_table = '{{dest_table}}'
        UNION ALL
        SELECT {{dest_id}} AS dest_id
        FROM `{{project_id}}.{{dataset_id}}.{{dest_table}}`
    )
)
SELECT
    '{{src_table}}' AS src_table, 
    '{{dest_table}}' AS dest_table, 
    {{src_id}} AS src_id, 
    ROW_NUMBER() OVER(ORDER BY {{src_id}}) + max_id.max_dest_id AS dest_id, 
    True AS is_rerouted 
FROM `{{project_id}}.{{dataset_id}}.{{src_table}}` AS s 
JOIN `{{project_id}}.{{dataset_id}}.concept` AS c 
ON s.{{domain_concept_id}} = c.concept_id 
CROSS JOIN max_id
WHERE c.domain_id = '{{domain}}'
{% if criteria -%} AND {{criteria}}{% endif %}
""")

# TODO is this neccesary!?
INSERT_LOOKUP_WITHIN_A_TABLE_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}`
(src_table, dest_table, src_id, dest_id, is_rerouted)
SELECT 
    '{{table}}' AS src_table, 
    '{{table}}' AS dest_table, 
    {{domain_id}} AS src_id, 
    {{domain_id}} AS dest_id, 
    True AS is_rerouted 
FROM `{{project_id}}.{{dataset_id}}.{{table}}` AS s 
JOIN `{{project_id}}.{{dataset_id}}.concept` AS c 
ON s.{{domain_concept_id}} = c.concept_id 
WHERE c.domain_id in (
    {% for domain in domains %} 
    '{{domain}}'{% if not loop.last -%}, {% endif %}
    {% endfor %}
)
""")

# This function generates a query that generates id mappings in _logging_domain_alignment for the records
# that will get dropped during rerouting because those records either fail the rerouting criteria or rerouting
# is not possible between src_table and dest_table such as condition_occurrence -> measurement
INSERT_LOOKUP_TO_DROP_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}`
(src_table, dest_table, src_id, dest_id, is_rerouted)
SELECT
    '{{src_table}}' AS src_table, 
    NULL AS dest_table, 
    s.{{src_id}} AS src_id, 
    NULL AS dest_id, 
    False AS is_rerouted 
FROM `{{project_id}}.{{dataset_id}}.{{src_table}}` AS s 
LEFT JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}` AS m 
ON s.{{src_id}} = m.src_id AND m.src_table = '{{src_table}}' 
WHERE m.src_id IS NULL
""")

# !!! ADD cast if the datatypes are different between source column and destination column
INSERT_DOMAIN_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{dest_table}}`
({% for field in fields %}{{field['name']}}{% if not loop.last -%}, {% endif %}{% endfor %})
SELECT 
    {% for f in fields %}{% set dest_field = f['name'] %}
        {% if dest_field == dest_domain_id_field -%}
            m.dest_id AS {{dest_domain_id_field}}
        {% elif field_mappings[dest_field]['translation'] == '0' and dest_field in required_fields_dest -%}
        -- !!! Use coalsce --
            {% if f['type'] == 'timestamp' -%} timestamp('2015-07-15') AS {{dest_field}}
            {% elif f['type'] == 'date' -%} date('2015-07-15') AS {{dest_field}}
            {% elif f['type'] == 'string' -%} '' AS {{dest_field}}
            {% elif f['type'] == 'integer' -%} 0 AS {{dest_field}}
            {% elif f['type'] == 'float' -%} 0.0 AS {{dest_field}}
            {% endif %}
        {% elif field_mappings[dest_field]['translation'] == '0' -%}
            {{field_mappings[dest_field]['src_field']}} AS {{dest_field}}
        {% elif value_mappings and field_mappings[dest_field]['src_field'] == value_mappings[dest_field]['src_field'] %}
            CASE {{field_mappings[dest_field]['src_field']}}
            {% for src_field, f, src_value, dest_value in value_mappings %}
            WHEN {{src_value}} THEN {{dest_value}}
            {% endfor %}
            ELSE 0 END AS {{dest_field}}
        {% elif dest_field in required_fields_dest %}
            0 AS {{dest_field}}
        {% else %}
            NULL AS {{dest_field}}
        {% endif %}
        {%- if not loop.last -%}, {% endif %}
    {% endfor %}
FROM `{{project_id}}.{{dataset_id}}.{{src_table}}` AS s 
JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}` AS m 
ON s.{{src_domain_id_field}} = m.src_id 
AND m.src_table = '{{src_table}}' 
AND m.dest_table = '{{dest_table}}' 
AND m.is_rerouted = True 
""")

INSERT_MAPPING_RECORD_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_{{dest_table}}`
({% for field in fields %}{{field}}{% if not loop.last -%}, {% endif %}{% endfor %})
SELECT
    m.dest_id AS {{dest_table}}_id,
    src.src_dataset_id,
    src.src_{{src_table}}_id AS src_{{dest_table}}_id,
    src.src_hpo_id,
    src.src_table_id
FROM `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}` AS m
JOIN `{{project_id}}.{{dataset_id}}._mapping_{{src_table}}` AS src
    ON m.src_id = src.{{src_table}}_id 
        AND m.src_table = '{{src_table}}'
        AND m.dest_table = '{{dest_table}}'
WHERE m.is_rerouted = True
""")

SANDBOX_DOMAIN_RECORD_QUERY_TMPL = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}} AS (
SELECT d.*
FROM `{{project_id}}.{{dataset_id}}.{{domain_table}}` AS d
LEFT JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}` AS m
ON d.{{domain_table}}_id = m.dest_id 
    AND m.dest_table = '{{domain_table}}'
    AND m.is_rerouted = True 
WHERE m.dest_id IS NULL
-- exclude PPI records from sandboxing --
    AND d.{{domain_concept_id}} NOT IN (
        SELECT c.concept_id
        FROM `{{project_id}}.{{dataset_id}}.concept` c
        WHERE c.vocabulary_id = 'PPI'
    )
)
""")

CLEAN_DOMAIN_RECORD_QUERY_TMPL = JINJA_ENV.from_string("""
DELETE d
FROM `{{project_id}}.{{dataset_id}}.{% if is_mapping %}_mapping_{% endif %}{{domain_table}}` AS d
LEFT JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` AS s
  ON d.{{domain_table}}_id = s.{{domain_table}}_id
WHERE s.{{domain_table}}_id IS NOT NULL
""")


class DomainAlignment(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.
        """
        desc = ('.')
        super().__init__(
            issue_numbers=['DC402', 'DC814', 'DC1466'],
            description=desc,
            affected_datasets=[COMBINED],
            affected_tables=DOMAIN_TABLE_NAMES +
            [mapping_table_for(table) for table in DOMAIN_TABLE_NAMES],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            table_namer=table_namer)

        self.table_mappings = [
            row for row in csv_to_list(TABLE_MAPPINGS_PATH)
            if row['is_rerouted'] == '1'
        ]

    def setup_rule(self, client: BigQueryClient):
        """
        abc
        """

        # Lookup table for domain alignment
        setup_rule_queries = []
        setup_rule_queries.append(
            CREATE_LOOKUP_ALIGNMENT_TMPL.render(
                project_id=self.project_id,
                sandbox_dataset_id=self.sandbox_dataset_id,
                alignment_table=LOOKUP_TABLE))

        for row in self.table_mappings:
            setup_rule_queries.append(
                INSERT_LOOKUP_BETWEEN_TABLES_TMPL.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    alignment_table=LOOKUP_TABLE,
                    src_table=row['src_table'],
                    dest_table=row['dest_table'],
                    src_id=f"{row['src_table']}_id",
                    dest_id=f"{row['dest_table']}_id",
                    domain_concept_id=get_domain_concept_id(row['src_table']),
                    domain=get_domain(row['dest_table']),
                    criteria=row['rerouting_criteria']))

        # Create the query for creating field_mappings for the records moving between the same domain
        for table in DOMAIN_TABLE_NAMES:
            setup_rule_queries.append(
                INSERT_LOOKUP_WITHIN_A_TABLE_TMPL.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    alignment_table=LOOKUP_TABLE,
                    table=table,
                    domain_id=f'{table}_id',
                    domain_concept_id=get_domain_concept_id(table),
                    domains=[get_domain(table), METADATA_DOMAIN]))

        # Create the query for the records that are in the wrong domain but will not be moved
        # TODO maybe this part was not working at all before refactoring.
        for table in DOMAIN_TABLE_NAMES:
            setup_rule_queries.append(
                INSERT_LOOKUP_TO_DROP_TMPL.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    alignment_table=LOOKUP_TABLE,
                    src_table=table,
                    src_id=f'{table}_id'))

        for setup_rule_query in setup_rule_queries:
            job = client.query(setup_rule_query)
            job.result()

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self.affected_tables
        ]

    def get_query_specs(self):
        """

        This function returns a list of dictionaries containing query parameters required for applying domain alignment.

        :param project_id: the project_id in which the query is run
        :param dataset_id: the dataset_id in which the query is run
        :param sandbox_dataset_id: Identifies the sandbox dataset to store rows
        #TODO use sandbox_dataset_id for CR
        :return: a list of query dicts for rerouting the records to the corresponding destination table
        """
        queries_list = []

        # creates a new dataset called snapshot_dataset_id and copies all content from
        # dataset_id to it. It generates a list of query dicts for rerouting the records to the
        # corresponding destination table.
        for row in self.table_mappings:

            src_table, dest_table = row['src_table'], row['dest_table']

            field_mappings = {
                r['dest_field']: {
                    'src_field': r['src_field'],
                    'translation': r['translation']
                }
                for r in csv_to_list(FIELD_MAPPINGS_PATH)
                if r['src_table'] == src_table and r['dest_table'] == dest_table
            }

            # TODO This is causing the duplicates. Fix it.
            value_mappings = {
                r['dest_field']: {
                    'src_field': r['src_field'],
                    'src_value': r['src_value'],
                    'dest_value': r['dest_value']
                }
                for r in csv_to_list(VALUE_MAPPINGS_PATH)
                if r['src_table'] == src_table and r['dest_table'] == dest_table
            }

            required_fields_src = [
                field['name']
                for field in fields_for(dest_table)
                if field['mode'] == 'required'
            ]

            required_fields_dest = [
                field['name']
                for field in fields_for(dest_table)
                if field['mode'] == 'required'
            ]

            queries_list.append({
                cdr_consts.QUERY:
                    INSERT_DOMAIN_TMPL.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        alignment_table=LOOKUP_TABLE,
                        src_table=row['src_table'],
                        dest_table=row['dest_table'],
                        src_domain_id_field=f"{row['src_table']}_id",
                        dest_domain_id_field=f"{row['dest_table']}_id",
                        fields=fields_for(row['dest_table']),
                        required_fields_dest=required_fields_dest,
                        required_fields_src=required_fields_src,
                        field_mappings=field_mappings,
                        value_mappings=value_mappings)
            })

        for row in self.table_mappings:
            queries_list.append({
                cdr_consts.QUERY:
                    INSERT_MAPPING_RECORD_TMPL.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        alignment_table=LOOKUP_TABLE,
                        src_table=row['src_table'],
                        dest_table=row['dest_table'],
                        fields=[
                            field['name'] for field in fields_for(
                                mapping_table_for(row['dest_table']))
                        ])
            })

        queries = []
        sandbox_queries = []
        for domain_table in DOMAIN_TABLE_NAMES:
            # Use non-standard concept if table is observation
            if domain_table == OBSERVATION:
                domain_concept_id = 'observation_source_concept_id'
            else:
                domain_concept_id = get_domain_concept_id(domain_table)

            # TODO create sandbox table for mapping tables too
            sandbox_queries.append({
                cdr_consts.QUERY:
                    SANDBOX_DOMAIN_RECORD_QUERY_TMPL.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        domain_table=domain_table,
                        alignment_table=LOOKUP_TABLE,
                        sandbox_table=self.sandbox_table_for(domain_table),
                        domain_concept_id=domain_concept_id)
            })
            # add the clean-up query for the domain table
            queries.append({
                cdr_consts.QUERY:
                    CLEAN_DOMAIN_RECORD_QUERY_TMPL.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        domain_table=domain_table,
                        sandbox_table=self.sandbox_table_for(domain_table),
                        is_mapping=False)
            })
            # add the clean-up query for the corresponding mapping of the domain table
            queries.append({
                cdr_consts.QUERY:
                    CLEAN_DOMAIN_RECORD_QUERY_TMPL.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        domain_table=domain_table,
                        sandbox_table=self.sandbox_table_for(domain_table),
                        is_mapping=True)
            })

        queries_list.extend(sandbox_queries + queries)

        return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(DomainAlignment,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DomainAlignment,)])
