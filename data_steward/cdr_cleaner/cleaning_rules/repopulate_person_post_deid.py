"""
Repopulate the person table from the de-identified observation table.

The de-id scripts removes all fields in the person table except for the person_id and the birthdate_time field.
Before CDR handoff to the Workbench team, we need to repopulate the following fields with demographic information
from the observation table.

These are the following fields in the person table will be repopulated:

gender_concept_id
year_of_birth
month_of_birth
day_of_birth
race_concept_id
ethnicity_concept_id
gender_source_value
gender_source_concept_id
race_source_value
race_source_concept_id
ethnicity_source_value
ethnicity_source_concept_id

As per ticket DC-1446, for participants who have not answered the question "What is you race/ethnicity" we need to set
    their race_concept_id to 2100000001.

Per ticket DC-1584, The sex_at_birth_concept_id, sex_at_ birth_source_concept_id, and sex_at_birth_source_value columns
were defined and set in multiple repopulate person scripts. This was redundant and caused unwanted schema changes for
the person table.  With the implementation of DC-1514 and DC-1570 these columns are to be removed from all
repopulate_person_* files.

Per ticket DC-2828, For both the Registered and Controlled tier person table repopulation scripts, set
race_source_concept_id = 2100000001 when race_concept_id is set to 2100000001
"""
import logging

import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from common import JINJA_ENV, PERSON

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC516', 'DC836', 'DC1446', 'DC1584', 'DC2828']

GENDER_CONCEPT_ID = 1585838
AOU_NONE_INDICATED_CONCEPT_ID = 2100000001

REPOPULATE_PERSON_QUERY = JINJA_ENV.from_string("""
WITH
  gender AS (
  SELECT
    p.person_id,
    COALESCE(o.value_as_concept_id,
      0) AS gender_concept_id,
    COALESCE(o.value_source_concept_id,
      0) AS gender_source_concept_id,
    COALESCE(c.concept_code,
      "No matching concept") AS gender_source_value
  FROM
    `{{project}}.{{dataset}}.person` p
  LEFT JOIN
    `{{project}}.{{dataset}}.observation` o
  ON
    p.person_id = o.person_id
    AND observation_source_concept_id = {{gender_concept_id}}
  LEFT JOIN
    `{{project}}.{{dataset}}.concept` c
  ON
    value_source_concept_id = concept_id ),
  repopulate_person_from_observation AS (
  SELECT
    DISTINCT *
  FROM (
    SELECT
      per.person_id,
      gender.gender_concept_id,
      EXTRACT(YEAR
      FROM
        birth_datetime) AS year_of_birth,
      EXTRACT(MONTH
      FROM
        birth_datetime) AS month_of_birth,
      EXTRACT(DAY
      FROM
        birth_datetime) AS day_of_birth,
      birth_datetime,
      /*Case statement to get proper race domain matches*/
      CASE race_ob.value_source_concept_id
        WHEN 1586142 THEN 8515 /*asian*/
        WHEN 1586143 THEN 8516 /*black/aa*/
        WHEN 1586146 THEN 8527 /*white*/
        WHEN 903079 THEN 1177221 /*PNA*/
        WHEN 0 THEN 2100000001 /*None Provided*/
      /*otherwise, just use the standard mapped answer (or 0)*/
      ELSE
      coalesce(race_ob.value_as_concept_id,
        0)
    END
      AS race_concept_id,
      /*Hardcode to the standard non-hispanic or hispanic code as applicable.*/
    IF
      (ethnicity_ob.value_as_concept_id IS NULL,
        /*Case this out based on the race_ob (race) values, ie if it's a skip/pna respect that.*/
        CASE
          WHEN race_ob.value_source_concept_id = 0 THEN 0 /*missing answer*/
          WHEN race_ob.value_source_concept_id IS NULL THEN 0 /*missing answer*/
          WHEN race_ob.value_source_concept_id = 903079 THEN 903079/*PNA*/
          WHEN race_ob.value_source_concept_id = 903096 THEN 903096 /*Skip*/
          WHEN race_ob.value_source_concept_id = 1586148 THEN 1586148 /*None of these*/
          WHEN race_ob.value_source_concept_id = 1586148 THEN 1586148 /*None of these*/
        /*otherwise, it's non-hispanic*/
        ELSE
        38003564
      END
        /*Assign HLS if it's present*/
        ,
        38003563) AS ethnicity_concept_id,
      location_id,
      per.provider_id,
      care_site_id,
      CAST(per.person_id AS STRING) AS person_source_value,
      gender.gender_source_value,
      gender.gender_source_concept_id,
      CASE WHEN race_ob.value_source_concept_id = 903079 THEN 'PMI_PreferNotToAnswer'/*PNA*/
      ELSE coalesce(race_ob.value_source_value,
           "No matching concept") END AS race_source_value,
      coalesce(race_ob.value_source_concept_id,
        0) AS race_source_concept_id
    FROM
      `{{project}}.{{dataset}}.person` AS per
    LEFT JOIN
      gender
    ON
      per.person_id = gender.person_id
    LEFT JOIN
      `{{project}}.{{dataset}}.observation` race_ob
    ON
      per.person_id = race_ob.person_id
      AND race_ob.observation_source_concept_id = 1586140
      AND race_ob.value_source_concept_id != 1586147
    LEFT JOIN
      `{{project}}.{{dataset}}.observation` ethnicity_ob
    ON
      per.person_id = ethnicity_ob.person_id
      AND ethnicity_ob.observation_source_concept_id=1586140
      AND ethnicity_ob.value_source_concept_id = 1586147) )


SELECT
  person_id,
  gender_concept_id,
  year_of_birth,
  month_of_birth,
  day_of_birth,
  birth_datetime,
  CASE
    WHEN race_concept_id = 0 THEN {{aou_custom_concept}}
  ELSE
  race_concept_id
END
  AS race_concept_id,
  ethnicity_concept_id,
  location_id,
  provider_id,
  care_site_id,
  person_source_value,
  gender_source_value,
  gender_source_concept_id,
  CASE
    WHEN race_concept_id = 0 THEN "None Indicated"
  ELSE
  race_source_value
END
  AS race_source_value,
    CASE
    WHEN race_concept_id = 2100000001 THEN 2100000001
  ELSE
  race_source_concept_id
END AS
  race_source_concept_id,
  ethnicity_concept.concept_code ethnicity_source_value,
  ethnicity_concept_id ethnicity_source_concept_id
FROM
  repopulate_person_from_observation r
  JOIN `{{project}}.{{dataset}}.concept` ethnicity_concept
    ON ethnicity_concept.concept_id = r.ethnicity_concept_id
""")


class RepopulatePersonPostDeid(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Returns a parsed query to repopulate the person table using observation.'
        )

        super().__init__(
            issue_numbers=JIRA_ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.REGISTERED_TIER_DEID_BASE],
            affected_tables=[PERSON],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            table_namer=table_namer)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        queries_list = []
        query = dict()
        query[cdr_consts.QUERY] = REPOPULATE_PERSON_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            gender_concept_id=GENDER_CONCEPT_ID,
            aou_custom_concept=AOU_NONE_INDICATED_CONCEPT_ID)
        query[cdr_consts.DESTINATION_TABLE] = PERSON
        query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        queries_list.append(query)

        return queries_list

    def setup_rule(self, client, *args, **keyword_args):
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
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(RepopulatePersonPostDeid,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RepopulatePersonPostDeid,)])
