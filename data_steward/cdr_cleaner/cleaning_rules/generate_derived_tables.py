"""
Create Derived Tables

Until now, 'observation_period', 'drug_era, 'condition_era' were populated Pre-deid stages.
But re-creating these tables post-deid will allow OHDSI tools to run on workbench more accurately.
"""
# Python imports
import logging

# Third party imports

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import CONDITION_ERA, DRUG_ERA, OBSERVATION_PERIOD, JINJA_ENV
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

POPULATE_OBS_PRD = JINJA_ENV.from_string("""
INSERT INTO observation_period
SELECT person_id,
observation_period_start_date,
    CASE WHEN observation_period_end_date > DATE('2023-10-01') THEN '2023-10-01'
    ELSE observation_period_end_date
END AS observation_period_end_date,
period_type_concept_id
FROM (SELECT person_id,
    MIN(observation_period_start_date) AS observation_period_start_date,
    MAX(observation_period_end_date) AS observation_period_end_date,
    44814725 AS period_type_concept_id
    FROM (SELECT pt.person_id AS person_id,
            MIN(vt.visit_start_date) AS observation_period_start_date,
            MAX(vt.visit_end_date) AS observation_period_end_date
        FROM `{{project_id}}.{{dataset_id}}.person` AS pt
        JOIN `{{project_id}}.{{dataset_id}}.visit_occurrence` AS vt ON pt.person_id = vt.person_id
        JOIN `{{project_id}}.{{dataset_id}}.visit_occurrence_ext` AS e ON vt.visit_occurrence_id = e.visit_occurrence_id
            WHERE src_id LIKE '%EHR%'
            AND EXTRACT(YEAR FROM vt.visit_start_date) >= 1985
            AND vt.visit_start_date <= '2023-10-01'
        GROUP BY pt.person_id
        
        UNION ALL

        SELECT pt.person_id AS person_id,
            MIN(co.condition_start_date) AS observation_period_start_date,
            MAX(co.condition_start_date) AS observation_period_end_date
        FROM `{{project_id}}.{{dataset_id}}.person` AS pt 
        JOIN `{{project_id}}.{{dataset_id}}.condition_occurrence` AS co ON pt.person_id = co.person_id
        JOIN `{{project_id}}.{{dataset_id}}.condition_occurrence_ext` AS e ON co.condition_occurrence_id = e.condition_occurrence_id
            WHERE src_id LIKE '%EHR%'
            AND EXTRACT(YEAR FROM co.condition_start_date) >= 1985
            AND co.condition_start_date <= '2023-10-01'
        GROUP BY pt.person_id

        UNION ALL

        SELECT pt.person_id AS person_id,
            MIN(po.procedure_date) AS observation_period_start_date,
            MAX(po.procedure_date) AS observation_period_end_date
        FROM `{{project_id}}.{{dataset_id}}.person` AS pt 
        JOIN `{{project_id}}.{{dataset_id}}.procedure_occurrence` AS po ON pt.person_id = po.person_id
        JOIN `{{project_id}}.{{dataset_id}}.procedure_occurrence_ext` AS e ON po.procedure_occurrence_id = e.procedure_occurrence_id
            WHERE src_id LIKE '%EHR%'
            AND EXTRACT(YEAR FROM po.procedure_date) >= 1985
            AND po.procedure_date <= '2023-10-01'
        GROUP BY pt.person_id

        UNION ALL

        SELECT pt.person_id AS person_id,
            MIN(de.drug_exposure_start_date) AS observation_period_start_date,
            MAX(de.drug_exposure_start_date) AS observation_period_end_date
        FROM `{{project_id}}.{{dataset_id}}.person` AS pt 
        JOIN `{{project_id}}.{{dataset_id}}.drug_exposure` AS de ON pt.person_id = de.person_id
        JOIN `{{project_id}}.{{dataset_id}}.drug_exposure_ext` AS e ON de.drug_exposure_id = e.drug_exposure_id
            WHERE src_id LIKE '%EHR%'
            AND EXTRACT(YEAR FROM de.drug_exposure_start_date) >= 1985
            AND de.drug_exposure_start_date <= '2023-10-01'
        GROUP BY pt.person_id

        UNION ALL

        SELECT pt.person_id AS person_id,
            MIN(de.device_exposure_start_date) AS observation_period_start_date,
            MAX(de.device_exposure_start_date) AS observation_period_end_date
        FROM `{{project_id}}.{{dataset_id}}.person` AS pt 
        JOIN `{{project_id}}.{{dataset_id}}.device_exposure` AS de ON pt.person_id = de.person_id
        JOIN `{{project_id}}.{{dataset_id}}.device_exposure_ext` AS e ON de.device_exposure_id = e.device_exposure_id
            WHERE src_id LIKE '%EHR%'
            AND EXTRACT(YEAR FROM de.device_exposure_start_date) >= 1985
            AND de.device_exposure_start_date <= '2023-10-01'
        GROUP BY pt.person_id

        UNION ALL

        SELECT pt.person_id AS person_id,
            MIN(o.observation_date) AS observation_period_start_date,
            MAX(o.observation_date) AS observation_period_end_date
        FROM `{{project_id}}.{{dataset_id}}.person` AS pt 
        JOIN `{{project_id}}.{{dataset_id}}.observation` AS o ON pt.person_id = o.person_id
        JOIN `{{project_id}}.{{dataset_id}}.observation_ext` AS e ON o.observation_id = e.observation_id
            WHERE src_id LIKE '%EHR%'
            AND EXTRACT(YEAR FROM o.observation_date) >= 1985
            AND o.observation_date <= '2023-10-01'
        GROUP BY pt.person_id

        UNION ALL

        SELECT pt.person_id AS person_id,
            MIN(m.measurement_date) AS observation_period_start_date,
            MAX(m.measurement_date) AS observation_period_end_date
        FROM `{{project_id}}.{{dataset_id}}.person` AS pt 
        JOIN `{{project_id}}.{{dataset_id}}.measurement` AS m ON pt.person_id = m.person_id
        JOIN `{{project_id}}.{{dataset_id}}.measurement_ext` AS e ON m.measurement_id = e.measurement_id
            WHERE src_id LIKE '%EHR%'
            AND EXTRACT(YEAR FROM m.measurement_date) >= 1985
            AND m.measurement_date <= '2023-10-01'
        GROUP BY pt.person_id
    ) AS min_max_op
WHERE min_max_op.observation_period_end_date IS NOT NULL
GROUP BY person_id)
""")

POPULATE_DRG_ERA = JINJA_ENV.from_string("""
WITH ctePreDrugTarget AS (
  SELECT
    d.drug_exposure_id,d.person_id,c.concept_id AS drug_concept_id,d.drug_exposure_start_date AS drug_exposure_start_date,d.days_supply AS days_supply
    ,COALESCE(
      NULLIF(drug_exposure_end_date,NULL),
      NULLIF(DATE_ADD(drug_exposure_start_date,INTERVAL days_supply DAY),drug_exposure_start_date),
      DATE_ADD(drug_exposure_start_date, INTERVAL 1 DAY)
    ) AS drug_exposure_end_date
  FROM `{{project_id}}.{{dataset_id}}.drug_exposure` d
  JOIN `{{project_id}}.{{dataset_id}}.concept_ancestor` ca ON ca.descendant_concept_id = d.drug_concept_id
  JOIN `{{project_id}}.{{dataset_id}}.concept` c ON ca.ancestor_concept_id = c.concept_id
  WHERE c.vocabulary_id = 'RxNorm'
  AND c.concept_class_id = 'Ingredient'
  AND d.drug_concept_id != 0
  AND coalesce(d.days_supply,0) >= 0)
,cteSubExposureEndDates AS (
  SELECT person_id,drug_concept_id,event_date AS end_date
  FROM (
    SELECT person_id,drug_concept_id,event_date,event_type,
    MAX(start_ordinal) OVER (PARTITION BY person_id,drug_concept_id
      ORDER BY event_date,event_type ROWS unbounded preceding) AS start_ordinal,
      ROW_NUMBER() OVER (PARTITION BY person_id,drug_concept_id
        ORDER BY event_date,event_type) AS overall_ord
    FROM (
      SELECT person_id,drug_concept_id,drug_exposure_start_date AS event_date,
      -1 AS event_type,
      ROW_NUMBER() OVER (PARTITION BY person_id,drug_concept_id
        ORDER BY drug_exposure_start_date) AS start_ordinal
      FROM ctePreDrugTarget
      UNION ALL
      SELECT person_id,drug_concept_id,drug_exposure_end_date,1 AS event_type,NULL
      FROM ctePreDrugTarget
    ) r
  ) e
  WHERE (2 * e.start_ordinal) - e.overall_ord = 0)
,cteDrugExposureEnds AS (
  SELECT 
    dt.person_id,dt.drug_concept_id,dt.drug_exposure_start_date,MIN(e.end_date) AS drug_sub_exposure_end_date
  FROM ctePreDrugTarget dt
  JOIN cteSubExposureEndDates e ON dt.person_id = e.person_id AND dt.drug_concept_id = e.drug_concept_id AND e.end_date >= dt.drug_exposure_start_date
  GROUP BY dt.drug_exposure_id,dt.person_id,dt.drug_concept_id,dt.drug_exposure_start_date)
,cteSubExposures AS (
  SELECT ROW_NUMBER() OVER (PARTITION BY person_id,drug_concept_id,drug_sub_exposure_end_date ORDER BY person_id) row_num
    ,person_id,drug_concept_id,MIN(drug_exposure_start_date) AS drug_sub_exposure_start_date,drug_sub_exposure_end_date,COUNT(*) AS drug_exposure_count
  FROM cteDrugExposureEnds
  GROUP BY person_id,drug_concept_id,drug_sub_exposure_end_date)
,cteFinalTarget AS (
  SELECT row_num,person_id,drug_concept_id,drug_sub_exposure_start_date,drug_sub_exposure_end_date,drug_exposure_count
    ,DATE_DIFF(drug_sub_exposure_end_date,drug_sub_exposure_start_date, DAY) AS days_exposed
  FROM cteSubExposures)
,cteEndDates AS (
  SELECT person_id,drug_concept_id,DATE_ADD(event_date, INTERVAL -30 DAY) AS end_date
  FROM (
    SELECT person_id,drug_concept_id,event_date,event_type,
    MAX(start_ordinal) OVER (PARTITION BY person_id,drug_concept_id
      ORDER BY event_date,event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal,
      ROW_NUMBER() OVER (PARTITION BY person_id,drug_concept_id
        ORDER BY event_date,event_type) AS overall_ord
    FROM (
      SELECT person_id,drug_concept_id,drug_sub_exposure_start_date AS event_date,-1 AS event_type,
        ROW_NUMBER() OVER (PARTITION BY person_id,drug_concept_id ORDER BY drug_sub_exposure_start_date) AS start_ordinal
      FROM cteFinalTarget
      UNION ALL
      SELECT person_id,drug_concept_id,DATE_ADD(drug_sub_exposure_end_date, INTERVAL 30 DAY),1 AS event_type,NULL
      FROM cteFinalTarget
    ) r
  ) e
  WHERE (2 * e.start_ordinal) - e.overall_ord = 0)
,cteDrugEraEnds AS (
  SELECT ft.person_id,ft.drug_concept_id,ft.drug_sub_exposure_start_date,MIN(e.end_date) AS drug_era_end_date,drug_exposure_count,days_exposed
  FROM cteFinalTarget ft
  JOIN cteEndDates e ON ft.person_id = e.person_id AND ft.drug_concept_id = e.drug_concept_id AND e.end_date >= ft.drug_sub_exposure_start_date
  GROUP BY ft.person_id,ft.drug_concept_id,ft.drug_sub_exposure_start_date,drug_exposure_count,days_exposed)
SELECT
  ROW_NUMBER() OVER(ORDER BY person_id) drug_era_id
  ,person_id
  ,drug_concept_id
  ,TIMESTAMP(MIN(drug_sub_exposure_start_date)) AS drug_era_start_date
  ,TIMESTAMP(drug_era_end_date) as drug_era_end_date
  ,SUM(drug_exposure_count) AS drug_exposure_count
  ,DATE_DIFF(drug_era_end_date,MIN(drug_sub_exposure_start_date), DAY) - SUM(days_exposed) as gap_days
FROM cteDrugEraEnds dee
GROUP BY person_id,drug_concept_id,drug_era_end_date
""")

POPULATE_COND_ERA = JINJA_ENV.from_string("""
WITH cteConditionTarget AS (
  SELECT co.person_id,co.condition_concept_id,co.condition_start_date
    ,COALESCE(co.condition_end_date, DATE_ADD(condition_start_date, INTERVAL 1 DAY)) AS condition_end_date
  FROM `{{project_id}}.{{dataset_id}}.condition_occurrence` co)
,cteCondEndDates AS (
  SELECT person_id
    ,condition_concept_id
    ,DATE_ADD(event_date, INTERVAL -30 DAY) AS end_date
  FROM (
    SELECT e1.person_id,e1.condition_concept_id,e1.event_date
      ,COALESCE(e1.start_ordinal, MAX(e2.start_ordinal)) start_ordinal
      ,e1.overall_ord
    FROM (
      SELECT person_id,condition_concept_id,event_date,event_type,start_ordinal
        ,ROW_NUMBER() OVER (PARTITION BY person_id,condition_concept_id ORDER BY event_date,event_type) AS overall_ord
      FROM (
        SELECT person_id,condition_concept_id,condition_start_date AS event_date,- 1 AS event_type
          ,ROW_NUMBER() OVER (PARTITION BY person_id,condition_concept_id ORDER BY condition_start_date) AS start_ordinal
        FROM cteConditionTarget
        UNION ALL
        SELECT person_id,condition_concept_id,DATE_ADD(condition_end_date, INTERVAL 30 DAY),1 AS event_type,NULL
        FROM cteConditionTarget) r
    ) e1
    JOIN (
      SELECT person_id,condition_concept_id,condition_start_date AS event_date
        ,ROW_NUMBER() OVER (PARTITION BY person_id,condition_concept_id ORDER BY condition_start_date) AS start_ordinal
      FROM cteConditionTarget
    ) e2 ON e1.person_id = e2.person_id
    AND e1.condition_concept_id = e2.condition_concept_id
    AND e2.event_date <= e1.event_date
    GROUP BY e1.person_id,e1.condition_concept_id,e1.event_date,e1.start_ordinal,e1.overall_ord
  ) e
  WHERE (2 * e.start_ordinal) - e.overall_ord = 0)
,cteConditionEnds AS (
  SELECT c.person_id,c.condition_concept_id,c.condition_start_date,MIN(e.end_date) AS era_end_date
  FROM cteConditionTarget c
  JOIN cteCondEndDates e ON c.person_id = e.person_id
    AND c.condition_concept_id = e.condition_concept_id
    AND e.end_date >= c.condition_start_date
  GROUP BY c.person_id,c.condition_concept_id,c.condition_start_date)
SELECT ROW_NUMBER() OVER (ORDER BY person_id) AS condition_era_id
  ,person_id
  ,condition_concept_id
  ,TIMESTAMP(MIN(condition_start_date)) AS condition_era_start_date
  ,TIMESTAMP(era_end_date) AS condition_era_end_date
  ,COUNT(*) AS condition_occurrence_count
FROM cteConditionEnds
GROUP BY person_id,condition_concept_id,era_end_date
""")


class CreateDerivedTables(BaseCleaningRule):
    """
    Create derived tables.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = 'Create derived tables post-deid in both CT/RT and both base/clean.'

        super().__init__(issue_numbers=['DC3729'],
                         description=desc,
                         affected_datasets=[cdr_consts.REGISTERED_TIER_DEID_BASE,
                                            cdr_consts.REGISTERED_TIER_DEID_CLEAN,
                                            cdr_consts.CONTROLLED_TIER_DEID_BASE,
                                            cdr_consts.CONTROLLED_TIER_DEID_CLEAN],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[],
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        :return: a list of SQL strings to run
        """
        create_sandbox_table_list = []
        create_observation_period_table = POPULATE_OBS_PRD.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            storage_table_name=OBSERVATION_PERIOD)

        create_drug_era_table = POPULATE_DRG_ERA.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            storage_table_name=DRUG_ERA)

        create_condition_era_table = POPULATE_COND_ERA.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            storage_table_name=CONDITION_ERA)

        create_sandbox_table_list.append({cdr_consts.QUERY: create_observation_period_table})
        create_sandbox_table_list.append({cdr_consts.QUERY: create_drug_era_table})
        create_sandbox_table_list.append({cdr_consts.QUERY: create_condition_era_table})

        return create_sandbox_table_list

    def get_sandbox_tablenames(self):
        return [OBSERVATION_PERIOD, DRUG_ERA, CONDITION_ERA]

    def setup_rule(self, client):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


if __name__ == '__main__':
    from utils import pipeline_logging

    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(CreateDerivedTables,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CreateDerivedTables,)])
