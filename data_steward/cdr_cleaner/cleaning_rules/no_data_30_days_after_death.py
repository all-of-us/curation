"""
If there is a death_date listed for a person_id, ensure that no temporal fields
(see the CDR cleaning spreadsheet tab labeled all temporal here) for that person_id exist more than
30 days after the death_date of the primary death record.
"""

# Python Imports
import logging
from collections import ChainMap

# Project Imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC816', 'DC404', 'DC2771', 'DC2788']

# add table names as keys and temporal representations as values into a dictionary
TEMPORAL_TABLES_WITH_START_DATE = {
    'visit_occurrence': 'visit_start_date',
    'visit_detail': 'visit_detail_start_date',
    'condition_occurrence': 'condition_start_date',
    'drug_exposure': 'drug_exposure_start_date',
    'drug_era': 'drug_era_start_date',
    'device_exposure': 'device_exposure_start_date'
}

TEMPORAL_TABLES_WITH_END_DATE = {
    'visit_occurrence': 'visit_end_date',
    'visit_detail': 'visit_detail_end_date',
    'condition_occurrence': 'condition_end_date',
    'drug_exposure': 'drug_exposure_end_date',
    'drug_era': 'drug_era_end_date',
    'device_exposure': 'device_exposure_end_date'
}

TEMPORAL_TABLES_WITH_DATE = {
    'person': 'birth_datetime',
    'measurement': 'measurement_date',
    'procedure_occurrence': 'procedure_date',
    'observation': 'observation_date',
    'specimen': 'specimen_date'
}

# Join AOU_DEATH to domain_table ON person_id
# check date field is not more than 30 days after the death date of the primary death record.
# select domain_table_id from the result
# use the above generated domain_table_ids as a list
# select rows in a domain_table where the domain_table_ids not in above generated list of ids

SANDBOX_DEATH_DATE_WITH_END_DATES_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
SELECT ma.*
FROM `{{project}}.{{dataset}}.{{table_name}}` AS ma
JOIN `{{project}}.{{dataset}}.aou_death` AS d
ON ma.person_id = d.person_id
WHERE date_diff(GREATEST(CAST(COALESCE(ma.{{start_date}}, ma.{{end_date}}) AS DATE), 
CAST(COALESCE(ma.{{end_date}}, ma.{{start_date}}) AS DATE)), d.death_date, DAY) > 30
AND d.primary_death_record = TRUE
)
""")

SANDBOX_DEATH_DATE_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
SELECT ma.*
FROM `{{project}}.{{dataset}}.{{table_name}}` AS ma
JOIN `{{project}}.{{dataset}}.aou_death` AS d
ON ma.person_id = d.person_id
WHERE date_diff(CAST({{date_column}} AS DATE), death_date, DAY) > 30
AND d.primary_death_record = TRUE
)
""")

REMOVE_DEATH_DATE_QUERY = JINJA_ENV.from_string("""
DELETE 
FROM `{{project}}.{{dataset}}.{{table_name}}`
WHERE {{table_name}}_id IN (
SELECT {{table_name}}_id 
FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table_name}}`
)
""")


class TableDateColumnException(Exception):

    def __init__(self, msg):
        super(TableDateColumnException, self).__init__(msg)


def get_affected_tables():
    """
    A helper function to retrieve the entire list of domain tables that gets affected
    :return: 
    """
    return list(
        ChainMap(TEMPORAL_TABLES_WITH_START_DATE, TEMPORAL_TABLES_WITH_END_DATE,
                 TEMPORAL_TABLES_WITH_DATE).keys())


def get_date(table):
    """
    A helper function to get the date column
    :param table: 
    :return: 
    """
    if table not in TEMPORAL_TABLES_WITH_DATE:
        raise TableDateColumnException(
            f"{table} does not have a date column defined in {TEMPORAL_TABLES_WITH_DATE}"
        )
    return TEMPORAL_TABLES_WITH_DATE[table]


def get_start_date(table):
    """
    A helper function to get the start date column
    :param table: 
    :return: 
    """
    if table not in TEMPORAL_TABLES_WITH_START_DATE:
        raise TableDateColumnException(
            f"{table} does not have a date column defined in {TEMPORAL_TABLES_WITH_START_DATE}"
        )
    return TEMPORAL_TABLES_WITH_START_DATE[table]


def get_end_date(table):
    """
    A helper function to get the end date column
    :param table: 
    :return: 
    """
    if table not in TEMPORAL_TABLES_WITH_END_DATE:
        raise TableDateColumnException(
            f"{table} does not have a date column defined in {TEMPORAL_TABLES_WITH_END_DATE}"
        )
    return TEMPORAL_TABLES_WITH_END_DATE[table]


class NoDataAfterDeath(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'If there is a death_date listed for a person_id, ensure that no temporal fields'
            '(see the CDR cleaning spreadsheet tab labeled all temporal here) for that '
            'person_id exist more than 30 days after the death_date of the primary death record.'
        )

        # get all affected tables by combining the two dicts

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         affected_tables=get_affected_tables(),
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=None)

    def get_sandbox_query_for(self, table):
        """
        Instantiate the sandbox query
        :param table: 
        :return: 
        """
        # Choose a template depending on whether the table has start/end date columns
        query_template = (SANDBOX_DEATH_DATE_QUERY
                          if table in TEMPORAL_TABLES_WITH_DATE else
                          SANDBOX_DEATH_DATE_WITH_END_DATES_QUERY)

        query_params = {
            'project': self.project_id,
            'dataset': self.dataset_id,
            'sandbox_dataset': self.sandbox_dataset_id,
            'sandbox_table': self.sandbox_table_for(table),
            'table_name': table
        }

        specific_params = {
            'date_column': get_date(table)
        } if table in TEMPORAL_TABLES_WITH_DATE else {
            'start_date': get_start_date(table),
            'end_date': get_end_date(table)
        }

        return query_template.render(**query_params, **specific_params)

    def get_query_for(self, table):
        """
        Instantiate the query
        :param table: 
        :return: 
        """
        return REMOVE_DEATH_DATE_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            table_name=table,
            sandbox_table_name=self.sandbox_table_for(table))

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        queries = []

        for table in get_affected_tables():
            queries.append(
                {cdr_consts.QUERY: self.get_sandbox_query_for(table)})

            queries.append({cdr_consts.QUERY: self.get_query_for(table)})
        return queries

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self.affected_tables
        ]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(NoDataAfterDeath,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(NoDataAfterDeath,)])
