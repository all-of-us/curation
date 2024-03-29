"""
This script updates OBSERVATION, SURVEY_CONDUCT, and PERSON tables with the
given new set of the basics survey responses.

This is a one-off cleaning rule for DC-3016 hot-fix. It is not expected to
run for every CDR release.

Original Issues: DC-3016 and its subtasks.
"""

#Python imports
import logging

# Project imports
from common import (JINJA_ENV, OBSERVATION, PERSON, SURVEY_CONDUCT)
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner.clean_cdr import (COMBINED, CONTROLLED_TIER_DEID,
                                             CONTROLLED_TIER_DEID_BASE,
                                             CONTROLLED_TIER_DEID_CLEAN, QUERY,
                                             RDR, REGISTERED_TIER_DEID,
                                             REGISTERED_TIER_DEID_BASE,
                                             REGISTERED_TIER_DEID_CLEAN)
from resources import ext_table_for, fields_for, mapping_table_for
from retraction.retract_utils import (is_combined_release_dataset,
                                      is_deid_dataset, is_deid_release_dataset,
                                      is_rdr_dataset)

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC3016']

CREATE_NEW_OBS_ID_LOOKUP = JINJA_ENV.from_string("""
CREATE TABLE `{{project}}.{{obs_id_lookup_dataset}}.{{new_obs_id_lookup}}`
(
    source_observation_id INT64 NOT NULL OPTIONS(description="observation_id from the source table."),
    observation_id INT64 NOT NULL OPTIONS(description="Newly assigned observation_id for the destination dataset.")
)
OPTIONS
(
    description="Lookup table for observation_ids. New observation_ids are assigned when loaded so no observation_id will be a duplicate."
)
""")

INSERT_NEW_OBS_ID_LOOKUP = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{obs_id_lookup_dataset}}.{{new_obs_id_lookup}}`
(source_observation_id, observation_id)
SELECT
    inc_o.observation_id,
    ROW_NUMBER() OVER(ORDER BY inc_o.observation_id
        ) + (
        SELECT MAX(observation_id) 
        FROM `{{project}}.{{dataset_with_largest_observation_id}}.{{table}}`
        )
FROM `{{project}}.{{incremental_dataset}}.{{table}}` inc_o
""")

SANDBOX_OBS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
    SELECT *
    FROM `{{project}}.{{dataset}}.{{table}}` o
    WHERE EXISTS (
        SELECT 1 FROM `{{project}}.{{incremental_dataset}}.{{table}}` inc_o
        WHERE inc_o.person_id = o.person_id
        AND inc_o.observation_source_concept_id = o.observation_source_concept_id
    )
)
""")

SANDBOX_OBS_MAPPING_EXT = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
    SELECT *
    FROM `{{project}}.{{dataset}}.{{table}}`
    WHERE observation_id IN (
        SELECT observation_id FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table_obs}}`
    )
)
""")

GENERIC_SANDBOX = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
    SELECT *
    FROM `{{project}}.{{dataset}}.{{table}}`
    WHERE {{domain}}_id IN (
        SELECT {{domain}}_id FROM `{{project}}.{{incremental_dataset}}.{{table}}` 
    )
)
""")

GENERIC_DELETE = JINJA_ENV.from_string("""
DELETE FROM `{{project}}.{{dataset}}.{{table}}`
WHERE {{domain}}_id IN (
    SELECT {{domain}}_id 
    FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` 
)
""")

INSERT_OBS = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{table}}`
    (observation_id, person_id, observation_concept_id,
     observation_date, observation_datetime, observation_type_concept_id,
     value_as_number, value_as_string, value_as_concept_id,
     qualifier_concept_id, unit_concept_id, provider_id,
     visit_occurrence_id, visit_detail_id,
     observation_source_value, observation_source_concept_id,
     unit_source_value, qualifier_source_value,
     value_source_concept_id, value_source_value,
     questionnaire_response_id)
SELECT
    new_id.observation_id,
    inc_o.person_id,
    inc_o.observation_concept_id,
    inc_o.observation_date,
    inc_o.observation_datetime,
    inc_o.observation_type_concept_id,
    inc_o.value_as_number,
    inc_o.value_as_string,
    inc_o.value_as_concept_id,
    inc_o.qualifier_concept_id,
    inc_o.unit_concept_id,
    inc_o.provider_id,
    inc_o.visit_occurrence_id,
    inc_o.visit_detail_id,
    inc_o.observation_source_value,
    inc_o.observation_source_concept_id,
    inc_o.unit_source_value,
    inc_o.qualifier_source_value,
    inc_o.value_source_concept_id,
    inc_o.value_source_value,
    inc_o.questionnaire_response_id
FROM `{{project}}.{{incremental_dataset}}.{{table}}` inc_o
JOIN `{{project}}.{{obs_id_lookup_dataset}}.{{new_obs_id_lookup}}` new_id
ON inc_o.observation_id = new_id.source_observation_id
WHERE inc_o.person_id IN ( -- Include only existing participants --
    SELECT DISTINCT person_id
    FROM `{{project}}.{{dataset}}.person`
)
{% if exclude_lookup_dataset and exclude_lookup_table %}
AND inc_o.person_id NOT IN (
    SELECT 
        {% if is_deid %} research_id {% else %} person_id {% endif %}
    FROM `{{project}}.{{exclude_lookup_dataset}}.{{exclude_lookup_table}}` 
)
{% endif %}
""")

INSERT_OBS_MAPPING = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{table}}`
    (observation_id, src_dataset_id, src_observation_id, src_hpo_id, src_table_id)
SELECT
    oim.observation_id,
    i.src_dataset_id,
    i.src_observation_id,
    i.src_hpo_id,
    i.src_table_id
FROM `{{project}}.{{incremental_dataset}}.{{table}}` i
JOIN `{{project}}.{{obs_id_lookup_dataset}}.{{new_obs_id_lookup}}` oim
ON i.observation_id = oim.source_observation_id
WHERE i.observation_id IN (
    SELECT observation_id FROM `{{project}}.{{incremental_dataset}}.{{domain}}`
    WHERE person_id IN ( -- Include only existing participants --
        SELECT DISTINCT person_id
        FROM `{{project}}.{{dataset}}.person`
    )
)
{% if exclude_lookup_dataset and exclude_lookup_table %}
AND i.observation_id IN (
    SELECT observation_id FROM `{{project}}.{{incremental_dataset}}.{{domain}}`
    WHERE person_id NOT IN (
        SELECT 
            {% if is_deid %} research_id {% else %} person_id {% endif %}
        FROM `{{project}}.{{exclude_lookup_dataset}}.{{exclude_lookup_table}}` 
    )
)
{% endif %}
""")

INSERT_OBS_EXT = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{table}}`
    (observation_id, src_id, survey_version_concept_id)
SELECT
    oim.observation_id,
    i.src_id,
    i.survey_version_concept_id
FROM `{{project}}.{{incremental_dataset}}.{{table}}` i
JOIN `{{project}}.{{obs_id_lookup_dataset}}.{{new_obs_id_lookup}}` oim
ON i.observation_id = oim.source_observation_id
WHERE i.observation_id IN (
    SELECT observation_id FROM `{{project}}.{{incremental_dataset}}.{{domain}}`
    WHERE person_id IN ( -- Include only existing participants --
        SELECT DISTINCT person_id
        FROM `{{project}}.{{dataset}}.person`
    )
)
{% if exclude_lookup_dataset and exclude_lookup_table %}
AND i.observation_id IN (
    SELECT observation_id FROM `{{project}}.{{incremental_dataset}}.{{domain}}`
    WHERE person_id NOT IN (
        SELECT 
            {% if is_deid %} research_id {% else %} person_id {% endif %}
        FROM `{{project}}.{{exclude_lookup_dataset}}.{{exclude_lookup_table}}` 
    )
)
{% endif %}
""")

# NOTE Due to the incremental dataset's setup, state_of_residence_concept_id and
# state_of_residence_source_value MUST come from the source dataset, not from the
# incremental dataset.
INSERT_PERS_EXT = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{table}}`
    (person_id, src_id, state_of_residence_concept_id, state_of_residence_source_value,
     sex_at_birth_concept_id, sex_at_birth_source_concept_id, sex_at_birth_source_value)
SELECT
    i.person_id,
    i.src_id,
    sb.state_of_residence_concept_id,
    sb.state_of_residence_source_value,
    i.sex_at_birth_concept_id,
    i.sex_at_birth_source_concept_id,
    i.sex_at_birth_source_value
FROM `{{project}}.{{incremental_dataset}}.{{table}}` i
LEFT JOIN `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` sb
ON i.person_id = sb.person_id
WHERE i.person_id IN ( -- Include only existing participants --
    SELECT DISTINCT person_id
    FROM `{{project}}.{{dataset}}.person`
)
{% if exclude_lookup_dataset and exclude_lookup_table %}
AND i.person_id NOT IN (
    SELECT 
        {% if is_deid %} research_id {% else %} person_id {% endif %}
    FROM `{{project}}.{{exclude_lookup_dataset}}.{{exclude_lookup_table}}` 
)
{% endif %}
""")

GENERIC_INSERT = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{table}}` ({{columns}})
SELECT {{columns}} FROM `{{project}}.{{incremental_dataset}}.{{table}}` 
WHERE person_id IN ( -- Include only existing participants --
    SELECT DISTINCT person_id
    FROM 
        {% if table=='person' %} `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`
        {% else %} `{{project}}.{{dataset}}.person` {% endif %}
)
{% if exclude_lookup_dataset and exclude_lookup_table %}
AND person_id NOT IN (
    SELECT 
        {% if is_deid %} research_id {% else %} person_id {% endif %}
    FROM `{{project}}.{{exclude_lookup_dataset}}.{{exclude_lookup_table}}` 
)
{% endif %}
""")

GENERIC_INSERT_MAP_EXT = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{table}}`
SELECT * FROM `{{project}}.{{incremental_dataset}}.{{table}}`
WHERE {{domain}}_id IN (
    SELECT {{domain}}_id FROM `{{project}}.{{incremental_dataset}}.{{domain}}`
    WHERE person_id IN ( -- Include only existing participants --
        SELECT DISTINCT person_id
        FROM `{{project}}.{{dataset}}.person`
    )
)
{% if exclude_lookup_dataset and exclude_lookup_table %}
AND {{domain}}_id IN (
    SELECT {{domain}}_id FROM `{{project}}.{{incremental_dataset}}.{{domain}}`
    WHERE person_id NOT IN (
        SELECT 
            {% if is_deid %} research_id {% else %} person_id {% endif %}
        FROM `{{project}}.{{exclude_lookup_dataset}}.{{exclude_lookup_table}}` 
    )
)
{% endif %}
""")

OBS_MAPPING = mapping_table_for(OBSERVATION)
OBS_EXT = ext_table_for(OBSERVATION)
NEW_OBS_ID_LOOKUP = '_observation_id_map'

SC_MAPPING = mapping_table_for(SURVEY_CONDUCT)
SC_EXT = ext_table_for(SURVEY_CONDUCT)

PERS_EXT = ext_table_for(PERSON)


class RemediateBasics(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 incremental_dataset_id=None,
                 obs_id_lookup_dataset=None,
                 dataset_with_largest_observation_id=None,
                 exclude_lookup_dataset=None,
                 exclude_lookup_table=None,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Replace missing/meaningless records of the basics with the correct ones.'
        )
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             COMBINED, CONTROLLED_TIER_DEID,
                             CONTROLLED_TIER_DEID_BASE,
                             CONTROLLED_TIER_DEID_CLEAN, RDR,
                             REGISTERED_TIER_DEID, REGISTERED_TIER_DEID_BASE,
                             REGISTERED_TIER_DEID_CLEAN
                         ],
                         affected_tables=[
                             OBSERVATION, OBS_MAPPING, OBS_EXT, SURVEY_CONDUCT,
                             SC_EXT, SC_MAPPING, PERSON, PERS_EXT
                         ],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

        self.incremental_dataset_id = incremental_dataset_id
        self.dataset_with_largest_observation_id = dataset_with_largest_observation_id
        self.obs_id_lookup_dataset = obs_id_lookup_dataset
        self.exclude_lookup_dataset = exclude_lookup_dataset
        self.exclude_lookup_table = exclude_lookup_table

    def _get_query(self, template, domain, table) -> dict:
        """
        This function bundles the common parameters for the JINJA templates,
        and return rendered string in a dict, so we do not have to assign the
        same parameters over and over.
        Args:
            template: name of the JINJA template
            domain: name of the domain
            table: table name
        Returns: rendered DDL/DML in a dict
        """
        return {
            QUERY:
                template.render(
                    domain=domain,
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(table),
                    sandbox_table_obs=self.sandbox_table_for(OBSERVATION),
                    incremental_dataset=self.incremental_dataset_id,
                    table=table,
                    columns=', '.join([
                        field['name'] for field in fields_for(table)
                    ]) if template == GENERIC_INSERT else None,
                    new_obs_id_lookup=NEW_OBS_ID_LOOKUP,
                    dataset_with_largest_observation_id=self.
                    dataset_with_largest_observation_id,
                    obs_id_lookup_dataset=self.obs_id_lookup_dataset,
                    exclude_lookup_dataset=self.exclude_lookup_dataset,
                    exclude_lookup_table=self.exclude_lookup_table,
                    is_deid=is_deid_dataset(self.dataset_id))
        }

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """

        # Operations for person table must run seperately because it is used
        # for filtering existing participants in the JINJA teplates
        person_queries = [
            self._get_query(GENERIC_SANDBOX, PERSON, PERSON),
            self._get_query(GENERIC_DELETE, PERSON, PERSON),
            self._get_query(GENERIC_INSERT, PERSON, PERSON)
        ]

        sandbox_queries = [
            self._get_query(SANDBOX_OBS, OBSERVATION, OBSERVATION),
            self._get_query(GENERIC_SANDBOX, SURVEY_CONDUCT, SURVEY_CONDUCT)
        ]

        delete_queries = [
            self._get_query(GENERIC_DELETE, domain, table)
            for (domain, table) in [
                (OBSERVATION, OBSERVATION),
                (SURVEY_CONDUCT, SURVEY_CONDUCT),
            ]
        ]

        insert_queries = [
            self._get_query(INSERT_OBS, OBSERVATION, OBSERVATION),
            self._get_query(GENERIC_INSERT, SURVEY_CONDUCT, SURVEY_CONDUCT),
        ]

        # obs_ext and sc_ext exist in combined_release and deid datasets
        if is_combined_release_dataset(self.dataset_id) or is_deid_dataset(
                self.dataset_id):
            sandbox_queries.extend([
                self._get_query(SANDBOX_OBS_MAPPING_EXT, OBSERVATION, OBS_EXT),
                self._get_query(GENERIC_SANDBOX, SURVEY_CONDUCT, SC_EXT)
            ])
            delete_queries.extend([
                self._get_query(GENERIC_DELETE, OBSERVATION, OBS_EXT),
                self._get_query(GENERIC_DELETE, SURVEY_CONDUCT, SC_EXT)
            ])
            insert_queries.extend([
                self._get_query(INSERT_OBS_EXT, OBSERVATION, OBS_EXT),
                self._get_query(GENERIC_INSERT_MAP_EXT, SURVEY_CONDUCT, SC_EXT)
            ])

        # person_ext table exists only in deid base/clean datasets
        if is_deid_release_dataset(self.dataset_id):
            sandbox_queries.extend(
                [self._get_query(GENERIC_SANDBOX, PERSON, PERS_EXT)])
            delete_queries.extend(
                [self._get_query(GENERIC_DELETE, PERSON, PERS_EXT)])
            insert_queries.extend(
                [self._get_query(INSERT_PERS_EXT, PERSON, PERS_EXT)])

        # mapping tables exist only in non-deid and non-rdr datasets
        if not is_deid_dataset(self.dataset_id) and not is_rdr_dataset(
                self.dataset_id):
            sandbox_queries.extend([
                self._get_query(SANDBOX_OBS_MAPPING_EXT, OBSERVATION,
                                OBS_MAPPING),
                self._get_query(GENERIC_SANDBOX, SURVEY_CONDUCT, SC_MAPPING)
            ])
            delete_queries.extend([
                self._get_query(GENERIC_DELETE, OBSERVATION, OBS_MAPPING),
                self._get_query(GENERIC_DELETE, SURVEY_CONDUCT, SC_MAPPING)
            ])
            insert_queries.extend([
                self._get_query(INSERT_OBS_MAPPING, OBSERVATION, OBS_MAPPING),
                self._get_query(GENERIC_INSERT_MAP_EXT, SURVEY_CONDUCT,
                                SC_MAPPING),
            ])

        return sandbox_queries + delete_queries + insert_queries + person_queries

    def setup_rule(self, client):
        """Create a lookup table for observation_id. New observation_ids need
        to be assigned for the destination table to ensure observation_ids
        will have NO duplicate. Each destination dataset can have different new
        observation_ids so this lookup table has `dataset_id` column.
        """
        if not client.table_exists(NEW_OBS_ID_LOOKUP,
                                   self.obs_id_lookup_dataset):
            create_lookup = self._get_query(CREATE_NEW_OBS_ID_LOOKUP, '',
                                            '')[QUERY]
            job = client.query(create_lookup)
            job.result()

            insert_lookup = self._get_query(INSERT_NEW_OBS_ID_LOOKUP,
                                            OBSERVATION, OBSERVATION)[QUERY]
            job = client.query(insert_lookup)
            job.result()

    def setup_validation(self, client):
        raise NotImplementedError("Please fix me.")

    def validate_rule(self):
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as ap
    import cdr_cleaner.clean_cdr_engine as clean_engine

    parser = ap.get_argument_parser()
    parser.add_argument(
        '--incremental_dataset_id',
        action='store',
        dest='incremental_dataset_id',
        help=('Dataset that needs to be loaded together with source_dataset.'),
        required=True)
    parser.add_argument(
        '--dataset_with_largest_observation_id',
        action='store',
        dest='dataset_with_largest_observation_id',
        help=(
            'Dataset that has the largest observation_id among all the datasets.'
        ),
        required=True)
    parser.add_argument('--obs_id_lookup_dataset',
                        action='store',
                        dest='obs_id_lookup_dataset',
                        help=(f'Dataset for NEW_OBS_ID_LOOKUP table.'),
                        required=True)
    parser.add_argument(
        '--exclude_lookup_dataset',
        action='store',
        dest='exclude_lookup_dataset',
        help=
        (f'Dataset that has the PID/RID table for exclusion (e.g. AIAN participants for W/O AIAN dataset).'
        ),
        required=False)
    parser.add_argument(
        '--exclude_lookup_table',
        action='store',
        dest='exclude_lookup_table',
        help=
        (f'PID/RID table for exclusion (e.g. AIAN participants for W/O AIAN dataset).'
        ),
        required=False)

    ARGS = parser.parse_args()

    clean_engine.add_console_logging(ARGS.console_log)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(RemediateBasics,)],
            incremental_dataset_id=ARGS.incremental_dataset_id,
            dataset_with_largest_observation_id=ARGS.
            dataset_with_largest_observation_id,
            obs_id_lookup_dataset=ARGS.obs_id_lookup_dataset,
            exclude_lookup_dataset=ARGS.exclude_lookup_dataset,
            exclude_lookup_table=ARGS.exclude_lookup_table)
        for query_dict in query_list:
            LOGGER.info(query_dict.get(QUERY))
    else:
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(RemediateBasics,)],
            incremental_dataset_id=ARGS.incremental_dataset_id,
            dataset_with_largest_observation_id=ARGS.
            dataset_with_largest_observation_id,
            obs_id_lookup_dataset=ARGS.obs_id_lookup_dataset,
            exclude_lookup_dataset=ARGS.exclude_lookup_dataset,
            exclude_lookup_table=ARGS.exclude_lookup_table)
