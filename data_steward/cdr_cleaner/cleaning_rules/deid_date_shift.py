"""
The basic date shifting rule..

Original Issue:  DC-1005
"""
# Python Imports
import logging
from abc import abstractmethod

# Third party imports
from jinja2 import Environment

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

jinja_env = Environment(
    # help protect against cross-site scripting vulnerabilities
    autoescape=True,
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --')

SHIFT_EXP = jinja_env.from_string("""
  {{field_type}}_SUB( CAST({{field}} AS {{field_type}}), INTERVAL (
    SELECT
      shift
    FROM
    -- could be _deid_map or pid_rid_mapping --
      `{{project}}.{{map_dataset}}.{{map_table}}` AS map
    WHERE
      map.research_id = remodel.person_id) DAY) AS {{field}}
""")

SELECT_STATEMENT = jinja_env.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{dataset}}.{{table}}` AS (
SELECT 
{{fields}}
FROM `{{project}}.{{dataset}}.{{table}}` AS remodel) 
""")


class DateShiftFieldsRule(BaseCleaningRule):
    """
    Suppress rows by values in the observation_source_concept_id field.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 issue_numbers,
                 description,
                 affected_datasets,
                 affected_tables,
                 map_dataset,
                 map_table,
                 depends_on=[]):
        """
        Initialize the class.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (f'Date shift date and timestamp fields by the date shift '
                f'calculated in the static mapping table.')

        self._map_dataset_id = map_dataset
        self._map_tablename = map_table

        super().__init__(issue_numbers=issue_numbers,
                         description=description,
                         affected_datasets=affected_datasets,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=affected_tables,
                         depends_on=depends_on)

    @property
    def map_dataset_id(self):
        """
        Return dataset id where the mapping table is located.
        """
        return self._map_dataset_id

    @property
    def map_tablename(self):
        """
        Return table name of the mapping table.
        """
        return self._map_tablename

    @abstractmethod
    def get_tables_and_schemas(self):
        """
        Provide dictionary of table names and schemas.

        :returns: a dictionary whose key, value patterns are in the
            form of {"tablename": "json schema",}.
        """
        pass

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        date_shift_queries = []
        for table, schema in self.get_tables_and_schemas().items():
            LOGGER.info(f"Building Date Shifting query for {self.dataset_id}."
                        f"{table}")
            fields = []
            for field in schema:
                field_type = field.get('type').lower()
                field_name = field.get('name')
                if field_type in ['date', 'datetime', 'timestamp']:
                    shift_string = SHIFT_EXP.render(
                        project=self.project_id,
                        map_dataset=self.map_dataset_id,
                        map_table=self.map_tablename,
                        field_type=field_type.upper(),
                        field=field_name,
                        table=table)
                    fields.append(shift_string)
                else:
                    fields.append(field_name)

            fields_string = ',\n'.join(fields)

            query = SELECT_STATEMENT.render(project=self.project_id,
                                            dataset=self.dataset_id,
                                            table=table,
                                            fields=fields_string)

            date_shift_queries.append({'query': query})

        return date_shift_queries
