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

CREATE_LOOKUP_TMPL = JINJA_ENV.from_string("""
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
    {%- for domain in domains %} 
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

INSERT_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{dest_table}}`
({% for field in fields %}{{field['name']}}{% if not loop.last -%}, {% endif %}{% endfor %})
SELECT 
    {% for col in cols %}
    {{col}}
    {% if not loop.last %}, {% endif %}
    {% endfor %}
FROM `{{project_id}}.{{dataset_id}}.{{src_table}}` AS s 
JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}` AS m 
ON s.{{src_table}}_id = m.src_id 
AND m.src_table = '{{src_table}}' 
AND m.dest_table = '{{dest_table}}' 
AND m.is_rerouted = True 
""")

INSERT_MAPPING_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_{{dest_table}}`
({% for field in fields %}{{field['name']}}{% if not loop.last -%}, {% endif %}{% endfor %})
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

SANDBOX_TMPL = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` AS (
    SELECT d.*
    FROM `{{project_id}}.{{dataset_id}}.{{table}}` AS d
    LEFT JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}` AS m
    ON d.{{table}}_id = m.dest_id 
    AND m.dest_table = '{{table}}'
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

SANDBOX_MAPPING_TMPL = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` AS (
    SELECT * 
    FROM `{{project_id}}.{{dataset_id}}.{{mapping_table}}`
    WHERE {{table}}_id IN (
        SELECT {{table}}_id 
        FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_domain_table}}`
        WHERE {{table}}_id IS NOT NULL        
    )
)
""")

CLEAN_TMPL = JINJA_ENV.from_string("""
DELETE `{{project_id}}.{{dataset_id}}.{% if is_mapping %}_mapping_{% endif %}{{table}}`
WHERE {{table}}_id IN (
    SELECT {{table}}_id FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}`
    WHERE {{table}}_id IS NOT NULL    
)
""")

# These values are referenced when the source column is nullable but
# the destination column is mandatory.
VALUE_DICT = {
    'string': '',
    'integer': 0,
    'float': 0,
    'date': "DATE('1970-01-01')",
    'timestamp': "TIMESTAMP('1970-01-01')"
}


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
            CREATE_LOOKUP_TMPL.render(
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

        """
        queries = []

        # creates a new dataset called snapshot_dataset_id and copies all content from
        # dataset_id to it. It generates a list of query dicts for rerouting the records to the
        # corresponding destination table.
        for row in self.table_mappings:

            src_table, dest_table = row['src_table'], row['dest_table']
            select_list = self.get_select_list(src_table, dest_table)

            queries.append({
                cdr_consts.QUERY:
                    INSERT_TMPL.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        alignment_table=LOOKUP_TABLE,
                        src_table=src_table,
                        dest_table=dest_table,
                        fields=fields_for(dest_table),
                        cols=select_list)
            })

            queries.append({
                cdr_consts.QUERY:
                    INSERT_MAPPING_TMPL.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        alignment_table=LOOKUP_TABLE,
                        src_table=src_table,
                        dest_table=dest_table,
                        fields=fields_for(mapping_table_for(dest_table)))
            })

        for table in DOMAIN_TABLE_NAMES:
            # Use non-standard concept if table is observation
            if table == OBSERVATION:
                domain_concept_id = 'observation_source_concept_id'
            else:
                domain_concept_id = get_domain_concept_id(table)

            queries.append({
                cdr_consts.QUERY:
                    SANDBOX_TMPL.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        table=table,
                        alignment_table=LOOKUP_TABLE,
                        sandbox_table=self.sandbox_table_for(table),
                        domain_concept_id=domain_concept_id)
            })
            queries.append({
                cdr_consts.QUERY:
                    SANDBOX_MAPPING_TMPL.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        table=table,
                        mapping_table=mapping_table_for(table),
                        alignment_table=LOOKUP_TABLE,
                        sandbox_table=self.sandbox_table_for(
                            mapping_table_for(table)),
                        sandbox_domain_table=self.sandbox_table_for(table),
                        domain_concept_id=domain_concept_id)
            })
            # add the clean-up query for the domain table
            queries.append({
                cdr_consts.QUERY:
                    CLEAN_TMPL.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        table=table,
                        sandbox_table=self.sandbox_table_for(table),
                        is_mapping=False)
            })
            # add the clean-up query for the corresponding mapping of the domain table
            queries.append({
                cdr_consts.QUERY:
                    CLEAN_TMPL.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        table=table,
                        sandbox_table=self.sandbox_table_for(
                            mapping_table_for(table)),
                        is_mapping=True)
            })

        return queries

    def get_select_list(self, src_table, dest_table):
        """
        abc
        """
        cols = []

        for dest_field in fields_for(dest_table):

            dest: str = dest_field['name']
            dest_type: str = 'float64' if dest_field[
                'type'] == 'float' else dest_field['type']

            if dest == f"{dest_table}_id":
                cols.append(f"m.dest_id AS {dest_table}_id")
                continue

            field_mapping: dict = next(
                {
                    'src': r['src_field'],
                    'translation': r['translation']
                }
                for r in csv_to_list(FIELD_MAPPINGS_PATH)
                if (r['src_table'] == src_table and
                    r['dest_table'] == dest_table and r['dest_field'] == dest))

            src: str = field_mapping['src']
            src_type: str = next(
                ('float64' if f['type'] == 'float' else f['type']
                 for f in fields_for(src_table)
                 if f['name'] == src), None)

            is_required: bool = dest_field['mode'] == 'required'
            needs_cast: bool = src_type != dest_type
            needs_translation: bool = field_mapping['translation'] == '1'

            translation: dict = {
                r['src_value']: r['dest_value']
                for r in csv_to_list(VALUE_MAPPINGS_PATH)
                if (r['src_table'] == src_table and
                    r['dest_table'] == dest_table and r['dest_field'] == dest)
            }

            # Add some comments here
            if (not is_required and not needs_translation and
                    not needs_cast) or src == 'NULL':
                cols.append(f"{src} AS {dest}")

            elif not is_required and not needs_translation and needs_cast:
                cols.append(f"CAST({src} AS {dest_type}) AS {dest}")

            elif is_required and not needs_translation and not needs_cast:
                cols.append(
                    f"COALESCE({src}, {VALUE_DICT[dest_type]}) AS {dest}")

            elif is_required and not needs_translation and needs_cast:
                cols.append(
                    f"COALESCE(CAST({src} AS {dest_type}), {VALUE_DICT[dest_type]}) AS {dest}"
                )

            elif not is_required and needs_translation and not translation:
                cols.append(f"NULL AS {dest_table}_id")

            elif is_required and needs_translation and not translation:
                cols.append(f"{VALUE_DICT[dest_type]} AS {dest_table}_id")

            elif translation:
                cols.append(
                    f"CASE {src} "
                    f'{"".join(f"WHEN {src_val} THEN {translation[src_val]} " for src_val in translation)}'
                    f"ELSE 0 END AS {dest}")

            else:
                pass  # Throws error

        return cols


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
