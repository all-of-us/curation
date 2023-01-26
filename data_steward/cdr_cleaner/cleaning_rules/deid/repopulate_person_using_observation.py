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
"""
import logging
from abc import abstractmethod
from typing import NamedTuple, List

import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from common import JINJA_ENV, PERSON

LOGGER = logging.getLogger(__name__)

# Aou Non Indicated concept_id and the corresponding source value
AOU_NONE_INDICATED_CONCEPT_ID = 2100000001
AOU_NONE_INDICATED_SOURCE_VALUE = 'AoUDRC_NoneIndicated'

REPOPULATE_PERSON_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT DISTINCT
    p.person_id,
    gs.gender_concept_id,
    bs.year_of_birth,
    bs.month_of_birth,
    bs.day_of_birth,
    bs.birth_datetime,
    CASE -- a special case to handle that requires inputs from both race ethnicity --
        WHEN rs.race_concept_id = {{aou_none_indicated_concept_id}} THEN race_source_concept_id = {{aou_none_indicated_concept_id}}
        WHEN rs.race_concept_id = 0 THEN {{aou_none_indicated_concept_id}}
        ELSE rs.race_concept_id
    END AS race_concept_id,
    es.ethnicity_concept_id,
    CAST(p.location_id AS INT64) AS location_id,
    CAST(p.provider_id AS INT64) AS provider_id,
    CAST(p.care_site_id AS INT64) AS care_site_id,
    p.person_source_value,
    gs.gender_source_value,
    gs.gender_source_concept_id,
    CASE -- a special case to handle that requires inputs from both race ethnicity
        WHEN rs.race_concept_id = {{aou_none_indicated_concept_id}} THEN rs.race_source_concept_id = {{aou_none_indicated_concept_id}}
        WHEN rs.race_concept_id = 0 THEN '{{aou_none_indicated_source_value}}'
        ELSE rs.race_source_value
    END AS race_source_value,
    rs.race_source_concept_id,
    es.ethnicity_source_value,
    es.ethnicity_source_concept_id
FROM {{project}}.{{dataset}}.person AS p
LEFT JOIN {{project}}.{{sandbox_dataset}}.{{gender_sandbox_table}} AS gs
    ON p.person_id = gs.person_id
LEFT JOIN {{project}}.{{sandbox_dataset}}.{{race_sandbox_table}} AS rs
    ON p.person_id = rs.person_id
LEFT JOIN {{project}}.{{sandbox_dataset}}.{{ethnicity_sandbox_table}} AS es
    ON p.person_id = es.person_id
LEFT JOIN {{project}}.{{sandbox_dataset}}.{{birth_info_sandbox_table}} AS bs
    ON p.person_id = bs.person_id
""")


class ConceptTranslation(NamedTuple):
    """
    A NamedTuple for storing the manual translations of the demographics concepts. The reason we
    need to do this manually is that PPI demographics concepts are in the Answer class  whereas
    the OMOP demographics concepts are in the demographics class such mappings do not exist in
    concept_relationship. e.g. 1586142 -> 8515
    """
    concept_id: int
    translated_concept_id: int
    comment: str = None


class AbstractRepopulatePerson(BaseCleaningRule):
    BIRTH = 'birth'
    GENDER = 'gender'
    RACE = 'race'
    ETHNICITY = 'ethnicity'

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 issue_numbers, description, affected_datasets, affected_tables,
                 table_namer):
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
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

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

    @abstractmethod
    def get_gender_manual_translation(self) -> List[ConceptTranslation]:
        """
        Define manual mappings to translate PPI gender concepts to the standard OMOP gender concepts.
        The reason we need to do this manually is that PPI gender concepts are in the Answer class
        whereas the OMOP gender concepts are in the gender class such mappings do not exist in
        concept_relationship

        :return:
        """
        pass

    @abstractmethod
    def get_race_manual_translation(self) -> List[ConceptTranslation]:
        """
        Define manual mappings to translate PPI race concepts to the standard OMOP race concepts.
        The reason we need to do this manually is that PPI race concepts are in the Answer class
        whereas the OMOP race concepts are in the race class such mappings do not exist in
        concept_relationship

        :return:
        """
        pass

    @abstractmethod
    def get_ethnicity_manual_translation(self) -> List[ConceptTranslation]:
        """
        Define manual mappings to translate PPI ethnicity concepts to the standard OMOP ethnicity
        concepts. The reason we need to do this manually is that PPI ethnicity concepts are in
        the Answer class whereas the OMOP ethnicity concepts are in the ethnicity class such
        mappings do not exist in concept_relationship

        :return:
        """
        pass

    def get_gender_sandbox_table(self):
        """
        Sandbox table for storing the gender information for repopulating person

        :return:
        """
        return self.sandbox_table_for(self.GENDER)

    def get_race_sandbox_table(self):
        """
        Sandbox table for storing the race information for repopulating person

        :return:
        """
        return self.sandbox_table_for(self.RACE)

    def get_ethnicity_sandbox_table(self):
        """
        Sandbox table for storing the ethnicity information for repopulating person

        :return:
        """
        return self.sandbox_table_for(self.ETHNICITY)

    def get_birth_info_sandbox_table(self):
        """
        Sandbox table for storing the birth information for repopulating person

        :return:
        """
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
            self.get_race_query(self.get_race_sandbox_table()),
            self.get_ethnicity_query(self.get_ethnicity_sandbox_table()),
            self.get_birth_info_query(self.get_birth_info_sandbox_table())
        ]

        repopulate_person_query = REPOPULATE_PERSON_QUERY_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            gender_sandbox_table=self.get_gender_sandbox_table(),
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

    def get_sandbox_tablenames(self):
        return [
            self.get_race_sandbox_table(),
            self.get_ethnicity_sandbox_table(),
            self.get_gender_sandbox_table(),
            self.get_birth_info_sandbox_table()
        ]
