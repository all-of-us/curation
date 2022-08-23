"""
Deletes records for participants who are missing from the person table.

Developed to assist drop_participants_without_any_basics.py in removing records
"""

# Python imports

# Third party imports

# Project imports
import common
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts

NON_PID_TABLES = [
    common.CARE_SITE, common.LOCATION, common.FACT_RELATIONSHIP, common.PROVIDER
]

TABLES_TO_DELETE_FROM = set(common.AOU_REQUIRED +
                            [common.OBSERVATION_PERIOD]) - set(NON_PID_TABLES +
                                                               [common.PERSON])

SELECT_QUERY = common.JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS
SELECT *
""")

# Delete rows in tables where the person_id is not in the person table
RECORDS_FOR_NON_EXISTING_PIDS = common.JINJA_ENV.from_string("""
{{query_type}}
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id NOT IN
(SELECT person_id
FROM `{{project}}.{{dataset}}.person`)
""")

ISSUE_NUMBERS = ["DC584", "DC706"]


class DropMissingParticipants(BaseCleaningRule):
    """
    Drops participant data for pids missing from the person table
    """

    def __init__(self,
                 issue_numbers,
                 description,
                 affected_datasets,
                 affected_tables,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer,
                 depends_on=None,
                 run_for_synthetic=False):
        desc = f'Sandbox and remove rows for PIDs missing from the person table.'

        super().__init__(
            issue_numbers=list(set(ISSUE_NUMBERS) | set(issue_numbers)),
            description=f'{description}\nAND\n{desc}',
            affected_datasets=affected_datasets,
            affected_tables=list(TABLES_TO_DELETE_FROM),
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=depends_on,
            table_namer=table_namer,
            run_for_synthetic=run_for_synthetic)

    def get_query_specs(self):
        """
        Return a list of queries to remove data for missing persons.

        Removes data from person_id linked tables for any persons which do not
        exist in the person table.
        :return:  A list of string queries that can be executed to delete data from
            other tables for non-person users.
        """
        query_list = []
        for table in self.affected_tables:
            create_sandbox_ddl = SELECT_QUERY.render(
                project=self.project_id,
                sandbox_dataset=self.sandbox_dataset_id,
                sandbox_table=self.sandbox_table_for(table))

            sandbox_query = RECORDS_FOR_NON_EXISTING_PIDS.render(
                query_type=create_sandbox_ddl,
                project=self.project_id,
                dataset=self.dataset_id,
                table=table)
            query_list.append({cdr_consts.QUERY: sandbox_query})

            delete_query = RECORDS_FOR_NON_EXISTING_PIDS.render(
                query_type="DELETE",
                project=self.project_id,
                dataset=self.dataset_id,
                table=table)
            query_list.append({cdr_consts.QUERY: delete_query})

        return query_list

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self.affected_tables
        ]

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass
