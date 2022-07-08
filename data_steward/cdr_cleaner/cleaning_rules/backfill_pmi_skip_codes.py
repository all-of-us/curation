"""

"""
# Python imports
import logging

# Project imports
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import JINJA_ENV, OBSERVATION
from resources import fields_for

LOGGER = logging.getLogger(__name__)

SKIP_CODES = [
    1586135, 1586140, 1585838, 1585899, 1585940, 1585892, 1585889, 1585890,
    1585386, 1585389, 1585952, 1585375, 1585370, 1585879, 1585886, 1585857,
    1586166, 1586174, 1586182, 1586190, 1586198, 1585636, 1585766, 1585772,
    1585778, 1585711, 1585717, 1585723, 1585729, 1585735, 1585741, 1585747,
    1585748, 1585754, 1585760, 1585803, 1585815, 1585784
]

PMI_SKIP_FIX_QUERY = JINJA_ENV.from_string("""
  INSERT INTO `{{project}}.{{dataset}}.observation`
  ({{observation_fields}})
  SELECT 
    pb.observation_id,
    pb.person_id,
    pb.observation_concept_id,
    pb.default_observation_date AS observation_date,
    pb.default_observation_datetime AS observation_datetime,
    pb.observation_type_concept_id,
    NULL AS value_as_number,
    NULL AS value_as_string,
    903096 AS value_as_concept_id,
    0 AS qualifier_concept_id,
    0 AS unit_concept_id,
    NULL AS provider_id,
    NULL AS visit_occurrence_id,
    NULL AS visit_detail_id,
    pb.observation_source_value,
    pb.observation_source_concept_id,
    NULL AS unit_source_value,
    NULL AS qualifier_source_value,
    903096 AS value_source_concept_id,
    'PMI_Skip' AS value_source_value, 
    NULL AS questionnaire_response_id 
  FROM `{{project}}.{{dataset}}.observation` AS obs
  FULL OUTER JOIN (
    WITH potentially_skipped_person AS (
      SELECT DISTINCT 
        obs.person_id, 
        per.gender_concept_id
      FROM `{{project}}.{{dataset}}.observation` obs
      JOIN `{{project}}.{{dataset}}.person` AS per
      ON obs.person_id = per.person_id
      WHERE observation_source_concept_id IN ({{skip_codes}})
    ),
    backfill_observation AS (
      SELECT DISTINCT 
        observation_concept_id,
        observation_source_concept_id,
        observation_source_value,
        observation_type_concept_id
      FROM `{{project}}.{{dataset}}.observation`
      WHERE observation_source_concept_id IN ({{skip_codes}})
    ),
    default_date AS (
      SELECT
        person_id,
        MAX(observation_date) AS default_observation_date,
        MAX(observation_datetime) AS default_observation_datetime
      FROM `{{project}}.{{dataset}}.observation`
      WHERE observation_source_concept_id IN ({{skip_codes}})
      GROUP BY person_id
    )
    SELECT
      ROW_NUMBER() OVER() + 1000000000000 AS observation_id,
      potential_backfill.*,
      default_date.default_observation_date,
      default_date.default_observation_datetime
    FROM (
      SELECT
        potentially_skipped_person.person_id,
        observation_concept_id,
        observation_source_concept_id,
        observation_source_value,
        observation_type_concept_id
      FROM potentially_skipped_person, backfill_observation
      WHERE (observation_source_concept_id != 1585784)
      OR (
        potentially_skipped_person.gender_concept_id = 8532
        AND observation_source_concept_id = 1585784
      ) 
    ) potential_backfill
    JOIN default_date
    ON potential_backfill.person_id = default_date.person_id
    ORDER BY potential_backfill.person_id 
  ) AS pb
  ON obs.person_id = pb.person_id
  AND obs.observation_concept_id = pb.observation_concept_id
  WHERE obs.observation_id IS NULL
""")


class BackfillPmiSkipCodes(BaseCleaningRule):
    """
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        """
        desc = ()

        super().__init__(issue_numbers=['DC420', 'DC821'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=['xyz'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self):
        """
        runs the query which adds skipped rows in survey before 2019-04-10 as PMI_Skip
        no sandbox since this is only insert.
        """
        insert_query = PMI_SKIP_FIX_QUERY.render(
            dataset=self.dataset_id,
            project=self.project_id,
            observation_fields=', '.join(
                field['name'] for field in fields_for(OBSERVATION)),
            skip_codes=', '.join(map(str, SKIP_CODES)))

        insert_query_dict = {cdr_consts.QUERY: insert_query}

        return [insert_query_dict]

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return f'{self._issue_numbers[0].lower()}_{self._affected_tables[0]}'


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(BackfillPmiSkipCodes,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(BackfillPmiSkipCodes,)])
