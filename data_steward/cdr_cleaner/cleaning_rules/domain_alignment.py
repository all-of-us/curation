"""
Based on the current CONCEPT table, if the domain_id does not match the domain
in which the concept is found, then the row should be moved to the appropriate
domain.

For example, a condition row based on the condition_concept_id might need to
be moved to the observation table. In this case, the row would be removed from
the condition table and the values will be inserted into the observation table.

Original Issues: DC-402
"""
# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.domain_mapping import (
    DEST_FIELD, DEST_TABLE, DEST_VALUE, DOMAIN_TABLE_NAMES, IS_REROUTED,
    REROUTING_CRITERIA, SRC_FIELD, SRC_TABLE, SRC_VALUE, TRANSLATION)
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
(src_table STRING, dest_table STRING, src_id INT64, dest_id INT64)
""")

INSERT_LOOKUP_TO_MOVE_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}`
(src_table, dest_table, src_id, dest_id)
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
    ROW_NUMBER() OVER(ORDER BY {{src_id}}) + max_id.max_dest_id AS dest_id
FROM `{{project_id}}.{{dataset_id}}.{{src_table}}` AS s 
JOIN `{{project_id}}.{{dataset_id}}.concept` AS c 
ON s.{{domain_concept_id}} = c.concept_id 
CROSS JOIN max_id
WHERE c.domain_id = '{{domain}}'
{% if criteria -%} AND {{criteria}}{% endif %}
""")

INSERT_LOOKUP_TO_DROP_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}`
(src_table, dest_table, src_id, dest_id)
SELECT
    '{{src_table}}' AS src_table, 
    NULL AS dest_table, 
    {{src_id}} AS src_id, 
    NULL AS dest_id
FROM `{{project_id}}.{{dataset_id}}.{{src_table}}` AS s 
JOIN `{{project_id}}.{{dataset_id}}.concept` AS c 
ON s.{{domain_concept_id}} = c.concept_id 
WHERE c.domain_id = '{{domain}}'
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
""")

SANDBOX_TMPL = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` AS (
    SELECT d.*
    FROM `{{project_id}}.{{dataset_id}}.{{table}}` AS d
    WHERE d.{{table}}_id IN (
        SELECT DISTINCT src_id
        FROM `{{project_id}}.{{sandbox_dataset_id}}.{{alignment_table}}`
        WHERE src_table = '{{table}}'
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
    'STRING': '',
    'INT64': 0,
    'FLOAT64': 0,
    'DATE': "DATE('1970-01-01')",
    'TIMESTAMP': "TIMESTAMP('1970-01-01')"
}


class DomainAlignment(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        self.table_mappings_to_move:
            A list of dict that has source table -> destination table relatiohship info.
            The records are moved from the src table to the dest table.
        self.table_mappings_to_drop:
            A list of dict that has source table -> destination table relatiohship info.
            The records are dropped from the src table but not moved to the dest table.
            The records are dropped because rerouting is not possible between
            the src_table and the dest_table. (e.g. condition_occurrence -> measurement)
        """
        desc = (
            'Move records to the appropriate domain tables based on the CONCEPT table.'
        )

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

        self.table_mappings_to_move: list[dict[str, str]] = [
            row for row in csv_to_list(TABLE_MAPPINGS_PATH)
            if row[IS_REROUTED] == '1'
        ]

        self.table_mappings_to_drop: list[dict[str, str]] = [
            row for row in csv_to_list(TABLE_MAPPINGS_PATH)
            if row[IS_REROUTED] == '0'
        ]

    def setup_rule(self, client: BigQueryClient):
        """
        Create a lookup table that has source to destination relationship info between
        source tables and destination tables. This CR cleans the data based on this
        lookup table.
        """

        queries = []
        queries.append(
            CREATE_LOOKUP_TMPL.render(
                project_id=self.project_id,
                sandbox_dataset_id=self.sandbox_dataset_id,
                alignment_table=LOOKUP_TABLE))

        for row in self.table_mappings_to_move:
            queries.append(
                INSERT_LOOKUP_TO_MOVE_TMPL.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    alignment_table=LOOKUP_TABLE,
                    src_table=row[SRC_TABLE],
                    dest_table=row[DEST_TABLE],
                    src_id=f"{row[SRC_TABLE]}_id",
                    dest_id=f"{row[DEST_TABLE]}_id",
                    domain_concept_id=get_domain_concept_id(row[SRC_TABLE]),
                    domain=get_domain(row[DEST_TABLE]),
                    criteria=row[REROUTING_CRITERIA]))

        for row in self.table_mappings_to_drop:
            queries.append(
                INSERT_LOOKUP_TO_DROP_TMPL.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    alignment_table=LOOKUP_TABLE,
                    src_table=row[SRC_TABLE],
                    src_id=f'{row[SRC_TABLE]}_id',
                    domain_concept_id=get_domain_concept_id(row[SRC_TABLE]),
                    domain=get_domain(row[DEST_TABLE])))

        for q in queries:
            job = client.query(q)
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
        Return a list of dictionary query specifications.
        The list contains 6 types of queries.

        For each of the table mappings:
        1. Insert: Move records from src_table to dest_table.
        2. Insert: Same as 1, but for mapping table.

        For each of the domain tables:
        3. Create: Sandbox the records to be deleted.
        4. Create: Same as 3, but for mapping table.
        5. Delete: Delete the records from src_table.
        6. Delete: Same as 5, but for mapping table.
        """
        queries = []

        for row in self.table_mappings_to_move:

            src_table, dest_table = row[SRC_TABLE], row[DEST_TABLE]
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
            # Clean the domain table
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
            # Clean the mapping table
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

    def get_select_list(self, src_table, dest_table) -> list:
        """
        Get the mapping info for the src and dest tables from the
        [table|field|value]_mapping CSV files, and create statements
        that will be a part of the SELECT statement for inserting records
        from src table to dest table.

        :return: A list of strings that can be used in the SELECT statement
            for each of the columns in dest_table.
        """
        cols = []

        for dest_field in fields_for(dest_table):

            dest: str = dest_field['name']
            dest_type: str = self.get_bq_col_type(dest_field['type'])

            if dest == f"{dest_table}_id":
                cols.append(f"m.dest_id AS {dest_table}_id")
                continue

            field_mapping: dict = next(
                {
                    SRC_FIELD: r[SRC_FIELD],
                    TRANSLATION: r[TRANSLATION]
                }
                for r in csv_to_list(FIELD_MAPPINGS_PATH)
                if (r[SRC_TABLE] == src_table and
                    r[DEST_TABLE] == dest_table and r[DEST_FIELD] == dest))

            src: str = field_mapping[SRC_FIELD]
            src_type: str = next((self.get_bq_col_type(f['type'])
                                  for f in fields_for(src_table)
                                  if f['name'] == src), None)

            is_required: bool = dest_field['mode'] == 'required'
            needs_cast: bool = src_type != dest_type
            needs_translation: bool = field_mapping[TRANSLATION] == '1'

            translation: dict = {
                r[SRC_VALUE]: r[DEST_VALUE]
                for r in csv_to_list(VALUE_MAPPINGS_PATH)
                if (r[SRC_TABLE] == src_table and
                    r[DEST_TABLE] == dest_table and r[DEST_FIELD] == dest)
            }

            # Create the statement for each of the columns.
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
                raise RuntimeError(
                    f'Unable to create SELECT statement for the column {dest} in {dest_table}. '
                    'Make sure the column is properly listed in the mapping CSV files.'
                )

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
