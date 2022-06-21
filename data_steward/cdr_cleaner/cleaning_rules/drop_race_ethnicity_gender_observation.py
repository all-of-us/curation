"""

"""
# Python imports
import logging

# Third party imports

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION, UNIONED_EHR
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

CREATE_SANDBOX_OBSERVATION = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
  SELECT *
  FROM `{{project}}.{{dataset}}.{{table}}`
  WHERE observation_concept_id in (4013886, 4135376, 4271761)
)
""")

DROP_RACE_ETHNICITY_GENDER_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project}}.{{dataset}}.{{table}}`
WHERE observation_id IN (
  SELECT observation_id
  FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`
)
""")


class DropRaceEthnicityGenderObservation(BaseCleaningRule):
    """
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = ('')
        super().__init__(issue_numbers=['DC2340'],
                         description=desc,
                         affected_datasets=[cdr_consts.EHR],
                         affected_tables=[f'{UNIONED_EHR}_{OBSERVATION}'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        Store the provided mappings and create required date shifts.

        :return: a list of SQL strings to run
        """
        sandbox_query = CREATE_SANDBOX_OBSERVATION.render(
            project=self.project_id,
            sandbox_dataset=self.sandbox_dataset_id,
            sandbox_table=self.sandbox_table_for(self.affected_tables[0]),
            dataset=self.dataset_id,
            table=self.affected_tables[0])

        drop_query = DROP_RACE_ETHNICITY_GENDER_QUERY.render(
            project=self.project_id,
            sandbox_dataset=self.sandbox_dataset_id,
            sandbox_table=self.sandbox_table_for(self.affected_tables[0]),
            dataset=self.dataset_id,
            table=self.affected_tables[0])

        sandbox_query_dict = {cdr_consts.QUERY: sandbox_query}
        insert_query_dict = {cdr_consts.QUERY: drop_query}

        return [sandbox_query_dict, insert_query_dict]

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]

    def setup_rule(self, client):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(DropRaceEthnicityGenderObservation,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropRaceEthnicityGenderObservation,)])
