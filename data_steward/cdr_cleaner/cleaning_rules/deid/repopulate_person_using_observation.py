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
from abc import abstractmethod

import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from common import JINJA_ENV, PERSON

LOGGER = logging.getLogger(__name__)

AOU_NONE_INDICATED_CONCEPT_ID = 2100000001
AOU_NONE_INDICATED_SOURCE_VALUE = 'AoUDRC_NoneIndicated'

REPOPULATE_PERSON_QUERY_TEMPLATE = JINJA_ENV.from_string("""
WITH race AS 
(
    -- Below PPI race concepts are translated to the standard OMOP race concepts --
    -- The manual mapping is necessary because the PPI race concepts are in the Answer class --
    -- where as the OMOP race concepts are in the race class --
    -- such mappings do not exist in concept_relationship --  
    SELECT
        * REPLACE (
            CASE race_source_concept_id 
                WHEN 1586142 THEN 8515 /*asian*/
                WHEN 1586143 THEN 8516 /*black/aa*/
                WHEN 1586146 THEN 8527 /*white*/
                WHEN 1586144 THEN 38003615 /*MENA*/
                WHEN 1586145 THEN 8557 /*NHPI*/
                WHEN 1586141 THEN 8657 /*AIAN*/
                ELSE race_concept_id
            END AS race_concept_id
        )
    FROM {{project}}.{{sandbox_dataset}}.{{race_sandbox_table}}
),
ethnicity AS
(
    -- This manual mapping of the concept id 1586147 (hispanic or latino) to the standard OMOP -- 
    -- concept id 38003563 is necessary because the mapping is missing in concept_relationship -- 
    -- we want to use the Ethnicity vocabulary in ethnicity_concept_id in the person table -- 
    SELECT  
        * REPLACE (
            IF(ethnicity_source_concept_id = 1586147, 38003563, ethnicity_concept_id) AS ethnicity_concept_id
        )
    FROM {{project}}.{{sandbox_dataset}}.{{ethnicity_sandbox_table}}
) 

SELECT DISTINCT 
    p.person_id,
    gs.gender_concept_id,
    bs.year_of_birth,
    bs.month_of_birth,
    bs.day_of_birth,
    bs.birth_datetime,
    CASE 
        WHEN (es.ethnicity_concept_id = 38003563 AND rs.race_concept_id = 0)  THEN {{aou_none_indicated_concept_id}}
        ELSE rs.race_concept_id 
    END AS race_concept_id,
    es.ethnicity_concept_id,
    CAST(p.location_id AS INT64) AS location_id,
    CAST(p.provider_id AS INT64) AS provider_id,
    CAST(p.care_site_id AS INT64) AS care_site_id,
    p.person_source_value,
    gs.gender_source_value,
    gs.gender_source_concept_id,
    CASE 
        WHEN (es.ethnicity_concept_id = 38003563 AND rs.race_concept_id = 0) THEN '{{aou_none_indicated_source_value}}'
        ELSE rs.race_source_value 
    END AS race_source_value,
    rs.race_source_concept_id,
    es.ethnicity_source_value,
    es.ethnicity_source_concept_id,
    ss.sex_at_birth_concept_id,
    ss.sex_at_birth_source_concept_id,
    ss.sex_at_birth_source_value
FROM {{project}}.{{dataset}}.person AS p
LEFT JOIN {{project}}.{{sandbox_dataset}}.{{gender_sandbox_table}} AS gs
    ON p.person_id = gs.person_id
LEFT JOIN {{project}}.{{sandbox_dataset}}.{{sex_at_birth_sandbox_table}} AS ss
    ON p.person_id = ss.person_id
LEFT JOIN race AS rs
    ON p.person_id = rs.person_id
LEFT JOIN ethnicity AS es 
    ON p.person_id = es.person_id
LEFT JOIN {{project}}.{{sandbox_dataset}}.{{birth_info_sandbox_table}} AS bs 
    ON p.person_id = bs.person_id
""")


class AbstractRepopulatePerson(BaseCleaningRule):
    BIRTH = 'birth'
    GENDER = 'gender'
    SEX_AT_BIRTH = 'sex_at_birth'
    RACE = 'race'
    ETHNICITY = 'ethnicity'

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 issue_numbers, description, affected_datasets,
                 affected_tables):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """

        super().__init__(issue_numbers=issue_numbers,
                         description=description,
                         affected_datasets=affected_datasets,
                         affected_tables=affected_tables,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    @abstractmethod
    def get_gender_query(self, gender_sandbox_table) -> dict:
        """
        This method creates a query for generating a sandbox table for storing the cleaned up 
        version of the gender information 
        
        :param gender_sandbox_table: 
        :return: 
        """
        pass

    @abstractmethod
    def get_sex_at_birth_query(self, sex_at_birth_sandbox_table) -> dict:
        """
        This method creates a query for generating a sandbox table for storing the cleaned up 
        version of the sex at birth information 
        
        :param sex_at_birth_sandbox_table: 
        :return: 
        """
        pass

    @abstractmethod
    def get_race_query(self, race_sandbox_table) -> dict:
        """
        This method creates a query for generating a sandbox table for storing the cleaned up 
        version of the race information including race_concept_id, race_source_concept_id, 
        race_source_value 
        
        :param race_sandbox_table: 
        :return: 
        """
        pass

    @abstractmethod
    def get_ethnicity_query(self, ethnicity_sandbox_table) -> dict:
        """
        This method creates a query for generating a sandbox table for storing the cleaned up 
        version of the ethnicity information including ethnicity_concept_id, 
        ethnicity_source_concept_id, ethnicity_source_value 
        
        :param ethnicity_sandbox_table: 
        :return: 
        """
        pass

    @abstractmethod
    def get_birth_info_query(self, birth_info_sandbox_table) -> dict:
        """
        This method creates a query for generating a sandbox table for storing the cleaned up 
        version of the birth information including birth_datetime, year_of_birth, month_of_birth, 
        day_of_birth 

        
        :return: 
        """
        pass

    def get_gender_sandbox_table(self):
        return self.sandbox_table_for(self.GENDER)

    def get_sex_at_birth_sandbox_table(self):
        return self.sandbox_table_for(self.SEX_AT_BIRTH)

    def get_race_sandbox_table(self):
        return self.sandbox_table_for(self.RACE)

    def get_ethnicity_sandbox_table(self):
        return self.sandbox_table_for(self.ETHNICITY)

    def get_birth_info_sandbox_table(self):
        return self.sandbox_table_for(self.BIRTH)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        sandbox_queries = [
            self.get_gender_query(self.get_gender_sandbox_table()),
            self.get_sex_at_birth_query(self.get_sex_at_birth_sandbox_table()),
            self.get_race_query(self.get_race_sandbox_table()),
            self.get_ethnicity_query(self.get_ethnicity_sandbox_table()),
            self.get_birth_info_query(self.get_birth_info_sandbox_table())
        ]

        repopulate_person_query = REPOPULATE_PERSON_QUERY_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            gender_sandbox_table=self.get_gender_sandbox_table(),
            sex_at_birth_sandbox_table=self.get_sex_at_birth_sandbox_table(),
            race_sandbox_table=self.get_race_sandbox_table(),
            ethnicity_sandbox_table=self.get_ethnicity_sandbox_table(),
            birth_info_sandbox_table=self.get_birth_info_sandbox_table(),
            aou_none_indicated_concept_id=AOU_NONE_INDICATED_CONCEPT_ID,
            aou_none_indicated_source_value=AOU_NONE_INDICATED_SOURCE_VALUE)

        queries = [{
            cdr_consts.QUERY: repopulate_person_query,
            cdr_consts.DESTINATION_TABLE: PERSON,
            cdr_consts.DESTINATION_DATASET: self.dataset_id,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE
        }]

        return sandbox_queries + queries

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [
            self.get_race_sandbox_table(),
            self.get_ethnicity_sandbox_table(),
            self.get_gender_sandbox_table(),
            self.get_sex_at_birth_sandbox_table(),
            self.get_birth_info_sandbox_table()
        ]
