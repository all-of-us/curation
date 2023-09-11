"""
AOU_DEATH has a column primary_death_record. 
This is a boolean flag to determine whether this is the primary death record for the person. 
We must calculate this column at the end of each data tier generation because other cleaning 
rules may update or delete the death records and may affect which ones are primary.

Original Issue: DC-3223
"""
# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import AOU_DEATH, JINJA_ENV
from constants.cdr_cleaner.clean_cdr import (
    COMBINED, CONTROLLED_TIER_DEID, CONTROLLED_TIER_DEID_BASE,
    CONTROLLED_TIER_DEID_CLEAN, QUERY, REGISTERED_TIER_DEID,
    REGISTERED_TIER_DEID_BASE, REGISTERED_TIER_DEID_CLEAN, UNIONED)
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

AOU_DEATH_QUERY = JINJA_ENV.from_string("""
{% if sandbox %}
CREATE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
SELECT ad.* FROM `{{project}}.{{dataset}}.{{table}}` ad
JOIN (
{% else %}
UPDATE `{{project}}.{{dataset}}.{{table}}` ad
SET ad.primary_death_record = new_primary.primary_death_record
FROM (
{% endif %}
    SELECT 
        aou_death_id, 
        CASE WHEN aou_death_id IN (
            SELECT aou_death_id FROM `{{project}}.{{dataset}}.{{table}}`
            WHERE death_date IS NOT NULL -- NULL death_date records must not become primary --
            QUALIFY RANK() OVER (
                PARTITION BY person_id 
                ORDER BY
                    LOWER(src_id) NOT LIKE '%healthpro%' DESC, -- EHR records are chosen over HealthPro ones --
                    death_date ASC, -- Earliest death_date records are chosen over later ones --
                    death_datetime ASC NULLS LAST, -- Earliest non-NULL death_datetime records are chosen over later or NULL ones --
                    src_id ASC -- EHR site that alphabetically comes first is chosen --
            ) = 1
        ) THEN TRUE ELSE FALSE END AS primary_death_record
    FROM `{{project}}.{{dataset}}.{{table}}`    
) new_primary
{% if sandbox %} 
ON ad.aou_death_id = new_primary.aou_death_id
WHERE ad.primary_death_record != new_primary.primary_death_record
)
{% else %}
WHERE ad.aou_death_id = new_primary.aou_death_id
AND ad.primary_death_record != new_primary.primary_death_record
{% endif %}
""")


class CalculatePrimaryDeathRecord(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets
        may affect this SQL, append them to the list of Jira Issues.

        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ("Calculate primary_death_record for AOU_DEATH.")

        super().__init__(issue_numbers=['dc3223'],
                         description=desc,
                         affected_datasets=[
                             UNIONED, COMBINED, REGISTERED_TIER_DEID,
                             REGISTERED_TIER_DEID_BASE,
                             REGISTERED_TIER_DEID_CLEAN, CONTROLLED_TIER_DEID,
                             CONTROLLED_TIER_DEID_BASE,
                             CONTROLLED_TIER_DEID_CLEAN
                         ],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[AOU_DEATH],
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        action_list = ['sandbox', 'delete']
        queries = []

        for action in action_list:
            queries.append({
                QUERY:
                    AOU_DEATH_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table=AOU_DEATH,
                        sandbox_dataset=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(AOU_DEATH),
                        sandbox=action == 'sandbox')
            })

        return queries

    def setup_rule(self, client):
        pass

    def get_sandbox_tablenames(self):
        """
        Return a list table names created to backup deleted data.
        """
        return [self.sandbox_table_for(table) for table in self.affected_tables]

    def setup_validation(self, client):
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        raise NotImplementedError("Please fix me.")


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ARGS = parser.default_parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(CalculatePrimaryDeathRecord,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CalculatePrimaryDeathRecord,)])
