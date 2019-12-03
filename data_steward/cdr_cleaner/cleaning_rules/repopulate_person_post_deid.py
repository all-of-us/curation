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
"""

import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts


PERSON_TABLE = 'person'

REPOPULATE_PERSON_QUERY = """
select DISTINCT * from (SELECT
  per.person_id,
  case ob.value_source_concept_id
    when 1585846 then 8507 --male
    when 1585847 then 8532 --female
    else 2000000009 --generalized
    end  AS gender_concept_id,
  EXTRACT(YEAR from birth_datetime) as year_of_birth,
  EXTRACT(MONTH from birth_datetime) as month_of_birth,
  EXTRACT(DAY from birth_datetime) as day_of_birth,
  birth_datetime,
  --Case statement to get proper race domain matches
  case ob2.value_source_concept_id
    when 1586142 then 8515 --asian
    when 1586143 then 8516 --black/aa
    when 1586146 then 8527 --white
    --otherwise, just use the standard mapped answer (or 0)
    else coalesce(ob2.value_as_concept_id, 0) 
    end AS race_concept_id,
  --Hardcode to the standard non-hispanic or hispanic code as applicable.
  if(ob3.value_as_concept_id is null, 
  --Case this out based on the ob2 (race) values, ie if it's a skip/pna respect that.
  case ob2.value_source_concept_id
    when 0 then 0 --missing answer
    when null then 0 --missing answer
    when 903079 then 1177221 --PNA
    when 1177221 then 1177221 --PNA
    when 903096 then 903096 --Skip
    when 1586148 then 45882607 --None of these
    when 45882607 then 45882607 --None of these
    --otherwise, it's non-hispanic
    else 38003564
    end
  --Assign HLS if it's present
  , 38003563) AS ethnicity_concept_id,
  location_id,
  per.provider_id,
  care_site_id,
  cast(per.person_id as STRING) as person_source_value,
  coalesce(ob.value_source_value, "No matching concept") AS gender_source_value,
  coalesce(ob.value_source_concept_id, 0) AS gender_source_concept_id,
  coalesce(ob2.value_source_value, "No matching concept") AS race_source_value,
  coalesce(ob2.value_source_concept_id, 0) AS race_source_concept_id,
  coalesce(ob3.value_source_value, 
  --fill in the skip/pna/none of these if needed
  if(ob2.value_source_concept_id in (903079,903096,1586148),ob2.value_source_value,null)
  --otherwise it is no matching
  ,"No matching concept") AS ethnicity_source_value,
  coalesce(ob3.value_source_concept_id, 0) AS ethnicity_source_concept_id
FROM
  `{project}.{dataset}.person` AS per
LEFT JOIN
  `{project}.{dataset}.observation` ob
ON
  per.person_id = ob.person_id
  --Updated to sex at birth, not gender
  AND ob.observation_source_concept_id =1585845
LEFT JOIN
  `{project}.{dataset}.observation` ob2
ON
  per.person_id = ob2.person_id
  AND ob2.observation_concept_id = 1586140
  AND ob2.value_source_concept_id != 1586147
LEFT JOIN
  `{project}.{dataset}.observation` ob3
ON
  per.person_id = ob3.person_id
  AND ob3.observation_concept_id=1586140
  AND ob3.value_source_concept_id = 1586147)
"""


def get_repopulate_person_post_deid_queries(project_id, dataset_id):
    """
    This Function returns a parsed query to repopulate the person table using observation.

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :return: A list of query dictionaries.
    """
    queries_list = []
    query = dict()
    query[cdr_consts.QUERY] = REPOPULATE_PERSON_QUERY.format(dataset=dataset_id,
                                                             project=project_id)
    query[cdr_consts.DESTINATION_TABLE] = PERSON_TABLE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    queries_list.append(query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_repopulate_person_post_deid_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, query_list)
