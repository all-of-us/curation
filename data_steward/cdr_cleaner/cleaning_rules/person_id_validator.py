"""
Run the person_id validation clean rule.

1.  The person_id in each of the defined tables exists in the person table.
    If not valid, remove the record.  This is accomplished by the
    extended class, DropMissingParticipants.
2.  The person_id is consenting.  If not consenting, remove EHR records.
    Keep PPI records.
"""
import logging

from cdr_cleaner.cleaning_rules.drop_rows_for_missing_persons import DropMissingParticipants
from constants.cdr_cleaner import clean_cdr as clean_consts
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC391', 'DC584','DC3266']

NON_EHR_CONSENT_LOOKUP = f'non_ehr_consented'

SET_LOOKUP_NON_EHR_CONSENTED_PERSONS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{lookup_table}}` AS (
SELECT person_id
FROM (
  SELECT person_id, value_source_concept_id, observation_date, observation_datetime,
  ROW_NUMBER() OVER(
    PARTITION BY person_id ORDER BY observation_date DESC, observation_datetime DESC,
    value_source_concept_id ASC) AS rn
  FROM `{{project}}.{{dataset}}.observation`
  WHERE observation_source_value = 'EHRConsentPII_ConsentPermission')
WHERE rn=1 AND value_source_concept_id != 1586100
)""")

SANDBOX_EHR_RECORDS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
    SELECT *
    FROM `{{project}}.{{dataset}}.{{table}}` t
    {% if dataset[0].isalpha() %}
    -- expect extension tables --
    JOIN `{{project}}.{{dataset}}.{{table}}_ext` moe
    {% else %}
    -- expect mapping tables --
    JOIN `{{project}}.{{dataset}}._mapping_{{table}}` moe
    {% endif %}
    USING ({{table}}_id)
    WHERE t.person_id in (
        SELECT distinct person_id
        FROM `{{project}}.{{sandbox_dataset}}.{{person_lookup}}`
    )
    -- Need to use Jinja templates to make this decision --
    {% if dataset[0].isalpha() %}
    -- should expect extension tables --
     and lower(moe.src_id) <> 'ppi/pm'
    {% else %}
    -- should expect mapping tables --
     and lower(moe.src_hpo_id) <> 'rdr'
    {% endif %}
)
""")

DELETE_EHR_RECORDS = JINJA_ENV.from_string("""
DELETE 
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE {{table}}_id IN (
    SELECT {{table}}_id FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`
)
""")

PERSON_TABLE_QUERY = JINJA_ENV.from_string("""
SELECT table_name
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
WHERE lower(COLUMN_NAME) = 'person_id'
-- exclude mapping and extension tables from the result set --
AND NOT REGEXP_CONTAINS(table_name, r'(?i)(^_mapping)|(_ext$)')
AND lower(table_name) <> 'person'
""")


class PersonIdValidation(DropMissingParticipants):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = (
            f'Sandbox and remove PIDs that cannot be found in the person table.'
            f'Drop EHR data for unconsented participants.')

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[],
                         affected_tables=[],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer,
                         depends_on=[])

    def setup_rule(self, client, *args, **keyword_args):
        person_table_query = PERSON_TABLE_QUERY.render(project=self.project_id,
                                                       dataset=self.dataset_id)
        person_tables = client.query(person_table_query).result()
        self.affected_tables = [
            table.get('table_name') for table in person_tables
        ]

    def get_query_specs(self):
        """
        Return query list of queries to ensure valid people are in the tables.

        The non-consenting queries rely on the mapping tables.  When using the
        combined and unidentified dataset, the last portion of the dataset name is
        removed to access these tables.  Any other dataset is expected to have
        these tables and uses the mapping tables from within the same dataset.

        :param sandbox_dataset_id: Identifies the sandbox dataset to store rows

        :return:  A list of string queries that can be executed to delete invalid
            records for invalid persons
        """
        queries = []

        # sandbox any person table record where the person does not have EHR consent
        queries.append({
            clean_consts.QUERY:
                SET_LOOKUP_NON_EHR_CONSENTED_PERSONS.render(
                    project=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    lookup_table=NON_EHR_CONSENT_LOOKUP,
                    dataset=self.dataset_id)
        })

        # generate queries to remove EHR records of non-ehr consented persons
        for table in self.affected_tables:
            # sandbox query
            queries.append({
                clean_consts.QUERY:
                    SANDBOX_EHR_RECORDS.render(
                        project=self.project_id,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(table),
                        dataset=self.dataset_id,
                        table=table,
                        person_lookup=NON_EHR_CONSENT_LOOKUP)
            })

            # delete ehr query
            queries.append({
                clean_consts.QUERY:
                    DELETE_EHR_RECORDS.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table=table,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(table))
            })

            # generate queries to remove person_ids of people not in the person table
        queries.extend(super().get_query_specs())

        return queries

    def get_sandbox_tablenames(self):
        sandbox_names = [
            self.sandbox_table_for(table) for table in self.affected_tables
        ] + [NON_EHR_CONSENT_LOOKUP]
        return sandbox_names


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(PersonIdValidation,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(PersonIdValidation,)])
