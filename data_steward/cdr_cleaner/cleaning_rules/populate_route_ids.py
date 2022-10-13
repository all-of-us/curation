"""
Using the drug_concept_id, one can infer the values to populate the route concept ID field.
For example, pseudoephedrine hydrochloride 7.5 MG Chewable Tablet (OMOP: 43012486) would have route as oral.
This cleaning rule populates the correct route_concept_ids based on the drug_concept_id.

Original Issues: DC-405, DC-817
"""

# Python imports
import logging
import os

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import DRUG_EXPOSURE, JINJA_ENV
import resources
from constants.bq_utils import WRITE_EMPTY
from constants.cdr_cleaner import clean_cdr as cdr_consts
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

DOSE_FORM_ROUTE_MAPPING_FILE = "dose_form_route_mappings.csv"
DOSE_FORM_ROUTE_MAPPING_TABLE = "_dose_form_route_mapping"
DRUG_ROUTE_MAPPING_TABLE = "_drug_route_mapping"

# If a drug maps to multiple dose forms, this can potentially create duplicate records in drug_exposure table
# We include only those drugs that map to different dose forms which in turn map to the same route
# We exclude drugs that map to different dose forms which in turn map to the different routes
# However, even with the following checks it is to be noted that there is potential
# for spurious duplicate records with different route_concept_ids to be created at this step and they must be removed
CREATE_DRUG_ROUTE_MAPPING = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{drug_route_mapping_table}}` AS (
    WITH drug_concept AS
    (
        SELECT concept_id FROM `{{project_id}}.{{dataset_id}}.concept` WHERE domain_id = 'Drug'
    ),
    drug_dose_form AS
    (
        SELECT 
            cr.concept_id_1, 
            cr.concept_id_2
        FROM drug_concept dc 
        JOIN `{{project_id}}.{{dataset_id}}.concept_relationship` cr 
        ON dc.concept_id = cr.concept_id_1
        WHERE cr.relationship_id = 'RxNorm has dose form'
    ),
    drug_route AS
    (
        SELECT DISTINCT 
            ddf.concept_id_1 AS drug_concept_id, 
            rm.route_concept_id
        FROM drug_dose_form ddf
        LEFT JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{dose_form_route_mapping_table}}` rm
        ON ddf.concept_id_2 = rm.dose_form_concept_id
        WHERE rm.route_concept_id IS NOT NULL
    ),
    drug_route_single AS
    (
        SELECT 
            drug_concept_id, 
            COUNT(1) n 
        FROM drug_route 
        GROUP BY drug_concept_id
        HAVING n = 1
    )
    SELECT 
        drug_concept_id, 
        route_concept_id 
    FROM drug_route dr
    WHERE EXISTS
    (
        SELECT 1 FROM drug_route_single drs WHERE dr.drug_concept_id = drs.drug_concept_id
    )
)
""")

SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS (
    SELECT de.* 
    FROM `{{project_id}}.{{dataset_id}}.{{table_id}}` de
    LEFT JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{drug_route_mapping_table}}` rm
    ON de.drug_concept_id = rm.drug_concept_id
    WHERE de.route_concept_id != rm.route_concept_id
    OR de.route_concept_id IS NULL
)
""")

UPDATE_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{table_id}}` de
SET route_concept_id = rm.route_concept_id
FROM `{{project_id}}.{{sandbox_dataset_id}}.{{drug_route_mapping_table}}` rm
WHERE de.drug_concept_id = rm.drug_concept_id
AND de.drug_exposure_id IN (
    SELECT drug_exposure_id FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}`
)
""")


class PopulateRouteIds(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.
        """
        desc = ('Update route_concept_id to correct values.')
        super().__init__(issue_numbers=['DC405', 'DC817'],
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         affected_tables=[DRUG_EXPOSURE],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def setup_rule(self, client):
        """
        Create mapping tables for dose_form - route and drug - route.
        """

        # Create the mapping table for dose_form & route
        client.upload_csv_data_to_bq_table(
            self.sandbox_dataset_id, DOSE_FORM_ROUTE_MAPPING_TABLE,
            os.path.join(resources.resource_files_path,
                         DOSE_FORM_ROUTE_MAPPING_FILE), WRITE_EMPTY)

        # Create the mapping table for drug & route
        create_mapping_table = CREATE_DRUG_ROUTE_MAPPING.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            drug_route_mapping_table=DRUG_ROUTE_MAPPING_TABLE,
            dose_form_route_mapping_table=DOSE_FORM_ROUTE_MAPPING_TABLE)
        job = client.query(create_mapping_table)
        job.result()

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self.affected_tables
        ]

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.
        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        sandbox_query = {
            cdr_consts.QUERY:
                SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    sandbox_table_id=self.sandbox_table_for(DRUG_EXPOSURE),
                    dataset_id=self.dataset_id,
                    table_id=DRUG_EXPOSURE,
                    drug_route_mapping_table=DRUG_ROUTE_MAPPING_TABLE)
        }

        update_query = {
            cdr_consts.QUERY:
                UPDATE_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    sandbox_table_id=self.sandbox_table_for(DRUG_EXPOSURE),
                    dataset_id=self.dataset_id,
                    table_id=DRUG_EXPOSURE,
                    drug_route_mapping_table=DRUG_ROUTE_MAPPING_TABLE)
        }

        return [sandbox_query, update_query]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(PopulateRouteIds,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(PopulateRouteIds,)])
