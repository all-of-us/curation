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
sex_at_birth_concept_id (extension)
sex_at_birth_source_concept_id (extension)
sex_at_birth_source_value (extension)
"""
import logging

from jinja2 import Template

import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

PERSON_TABLE = 'person'
GENDER_CONCEPT_ID = 1585838
SEX_AT_BIRTH_CONCEPT_ID = 1585845
AOU_NONE_INDICATED_CONCEPT_ID = 2100000001

REPOPULATE_PERSON_QUERY = Template("""
WITH
  gender AS (
  SELECT
    p.person_id,
    COALESCE(o.value_as_concept_id, 0) AS gender_concept_id,
    COALESCE(o.value_source_concept_id, 0) AS gender_source_concept_id,
    COALESCE(c.concept_code, "No matching concept") AS gender_source_value
  FROM `{{project}}.{{dataset}}.person` p
  LEFT JOIN `{{project}}.{{dataset}}.observation` o
    ON p.person_id = o.person_id AND observation_source_concept_id = {{gender_concept_id}}
  LEFT JOIN `{{project}}.{{dataset}}.concept` c
    ON value_source_concept_id = concept_id
  ),
  sex_at_birth AS (
  SELECT
    p.person_id,
    COALESCE(o.value_as_concept_id, 0) AS sex_at_birth_concept_id,
    COALESCE(o.value_source_concept_id, 0) AS sex_at_birth_source_concept_id,
    COALESCE(c.concept_code, "No matching concept") AS sex_at_birth_source_value
  FROM `{{project}}.{{dataset}}.person` p
  LEFT JOIN `{{project}}.{{dataset}}.observation` o
    ON p.person_id = o.person_id AND observation_source_concept_id = {{sex_at_birth_concept_id}}
  LEFT JOIN `{{project}}.{{dataset}}.concept` c
    ON value_source_concept_id = concept_id
  ),
  repopulate_person_from_observation AS (
  select DISTINCT * 
  from 
    (SELECT
    per.person_id,
    gender.gender_concept_id,
    sex_at_birth.sex_at_birth_concept_id,
    EXTRACT(YEAR from birth_datetime) as year_of_birth,
    EXTRACT(MONTH from birth_datetime) as month_of_birth,
    EXTRACT(DAY from birth_datetime) as day_of_birth,
    birth_datetime,
    --Case statement to get proper race domain matches
    case race_ob.value_source_concept_id
      when 1586142 then 8515 --asian
      when 1586143 then 8516 --black/aa
      when 1586146 then 8527 --white
      --otherwise, just use the standard mapped answer (or 0)
      else coalesce(race_ob.value_as_concept_id, 0) 
      end AS race_concept_id,
    --Hardcode to the standard non-hispanic or hispanic code as applicable.
    if(ethnicity_ob.value_as_concept_id is null, 
    --Case this out based on the race_ob (race) values, ie if it's a skip/pna respect that.
    case race_ob.value_source_concept_id
      when 0 then 0 --missing answer
      when null then 0 --missing answer
      when 903079 then 903079 --PNA
      when 903096 then 903096 --Skip
      when 1586148 then 1586148 --None of these
      --otherwise, it's non-hispanic
      else 38003564
      end
    --Assign HLS if it's present
    , 38003563) AS ethnicity_concept_id,
    location_id,
    per.provider_id,
    care_site_id,
    cast(per.person_id as STRING) as person_source_value,
    gender.gender_source_value,
    gender.gender_source_concept_id,
    sex_at_birth.sex_at_birth_source_value,
    sex_at_birth.sex_at_birth_source_concept_id,
    coalesce(race_ob.value_source_value, "No matching concept") AS race_source_value,
    coalesce(race_ob.value_source_concept_id, 0) AS race_source_concept_id,
    coalesce(ethnicity_ob.value_source_value, 
    --fill in the skip/pna/none of these if needed
    if(race_ob.value_source_concept_id in (903079,903096,1586148),race_ob.value_source_value,null)
    --otherwise it is no matching
    ,"No matching concept") AS ethnicity_source_value,
    coalesce(ethnicity_ob.value_source_concept_id, 0) AS ethnicity_source_concept_id
  FROM
    `{{project}}.{{dataset}}.person` AS per
  LEFT JOIN gender
  ON
    per.person_id = gender.person_id
  LEFT JOIN sex_at_birth
  ON
    per.person_id = sex_at_birth.person_id
  LEFT JOIN
    `{{project}}.{{dataset}}.observation` race_ob
  ON
    per.person_id = race_ob.person_id
    AND race_ob.observation_concept_id = 1586140
    AND race_ob.value_source_concept_id != 1586147
  LEFT JOIN
    `{{project}}.{{dataset}}.observation` ethnicity_ob
  ON
    per.person_id = ethnicity_ob.person_id
    AND ethnicity_ob.observation_concept_id=1586140
    AND ethnicity_ob.value_source_concept_id = 1586147)
    )
SELECT
  person_id,
  gender_concept_id,
  year_of_birth,
  month_of_birth,
  day_of_birth,
  birth_datetime,
  CASE
    WHEN (ethnicity_concept_id = 38003563 AND race_concept_id = 0) THEN {{aou_custom_concept}}
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
    WHEN (ethnicity_concept_id = 38003563 AND race_concept_id = 0) THEN "None Indicated"
  ELSE
  race_source_value
END
  AS race_source_value,
  race_source_concept_id,
  ethnicity_source_value,
  ethnicity_source_concept_id,
  sex_at_birth_concept_id,
  sex_at_birth_source_concept_id,
  sex_at_birth_source_value
FROM
  repopulate_person_from_observation
""")


def get_repopulate_person_post_deid_queries(project_id, dataset_id):
    """
    This Function returns a parsed query to repopulate the person table using observation.

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return: A list of query dictionaries.
    """
    queries_list = []
    query = dict()
    query[cdr_consts.QUERY] = REPOPULATE_PERSON_QUERY.render(
        project=project_id,
        dataset=dataset_id,
        gender_concept_id=GENDER_CONCEPT_ID,
        sex_at_birth_concept_id=SEX_AT_BIRTH_CONCEPT_ID,
        aou_custom_concept=AOU_NONE_INDICATED_CONCEPT_ID)
    query[cdr_consts.DESTINATION_TABLE] = PERSON_TABLE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    queries_list.append(query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id,
            [(get_repopulate_person_post_deid_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(get_repopulate_person_post_deid_queries,)])
