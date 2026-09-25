"""
DEID rule to change PIDs to RIDs for OMOP tables and aou_death in the controlled tier plus.

RtCtPIDtoRID copies pipeline_tables.primary_pid_rid_mapping into the sandbox as _deid_map.
That table predates the pediatric feed, and PIDtoRID deletes every participant the map
omits rather than failing, so a CT+ run built from it silently drops the pediatric cohort.

This rule builds _deid_map from rdr_participant_research_ids_view instead, which does carry
them. For participants in both sources the IDs and shifts are equal, so the switch changes
who survives, not what ID anyone receives.

The RID is controlled_tier_id, as in CT, because regenerate_ct_plus_ids.py later converts
CT IDs to CT+ IDs by joining on that column.

A NULL shift is carried, not filtered. The view takes the shift from
primary_pid_rid_mapping, so pediatric participants arrive with a NULL one, and filtering
it would delete them. No CT or CT+ rule reads _deid_map.shift; the three that do are
registered tier only. Adding a date shifting rule to a CT+ stage has to resolve this first.
"""
# Python Imports
import logging

# Project Imports
from cdr_cleaner.cleaning_rules.deid.rt_ct_pid_rid_map import RtCtPIDtoRID
from common import (JINJA_ENV, PIPELINE_TABLES,
                    RDR_PARTICIPANT_RESEARCH_IDS_VIEW)

LOGGER = logging.getLogger(__name__)

# Participants with two controlled_tier_ids or two non-NULL shifts. The map's GROUP BY
# cannot collapse those to one row. COUNT(DISTINCT ...) ignores NULLs, so a NULL shift
# beside a valued one does not count.
CONFLICTING_PARTICIPANTS_TMPL = JINJA_ENV.from_string("""
WITH conflicting AS (
    SELECT participant_id
    FROM `{{project_id}}.{{ids_dataset_id}}.{{ids_view_id}}`
    WHERE participant_id IS NOT NULL
      AND controlled_tier_id IS NOT NULL
    GROUP BY participant_id
    HAVING COUNT(DISTINCT controlled_tier_id) > 1
        OR COUNT(DISTINCT registered_tier_date_shift) > 1
)
SELECT
    COUNT(*) AS conflicting_count,
    ARRAY_AGG(participant_id ORDER BY participant_id LIMIT 5) AS examples
FROM conflicting
""")

BUILD_DEID_MAP_TMPL = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{deid_map.project}}.{{deid_map.dataset_id}}.{{deid_map.table_id}}` AS
SELECT
    participant_id AS person_id,
    controlled_tier_id AS research_id,
    MAX(registered_tier_date_shift) AS shift
FROM `{{project_id}}.{{ids_dataset_id}}.{{ids_view_id}}`
WHERE participant_id IS NOT NULL
  AND controlled_tier_id IS NOT NULL
GROUP BY participant_id, controlled_tier_id
""")


class CtPlusPIDtoRID(RtCtPIDtoRID):
    """
    Use RID instead of PID for OMOP tables in the controlled tier plus.

    Identical to RtCtPIDtoRID except for where _deid_map comes from. See the module
    docstring for why the controlled tier's source cannot be reused.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None,
                 ids_dataset_id=PIPELINE_TABLES,
                 ids_view_id=RDR_PARTICIPANT_RESEARCH_IDS_VIEW):
        """
        :param ids_dataset_id: dataset holding the research IDs view
        :param ids_view_id: view mapping participant_id to the per-tier research IDs
        """
        super().__init__(project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)
        self.ids_dataset_id = ids_dataset_id
        self.ids_view_id = ids_view_id

    def assert_one_row_per_participant(self, client):
        """
        Stop the run if the research IDs view disagrees with itself about a participant.

        Such a participant would reach the map twice, and PIDtoRID's UPDATE ... FROM
        would then match two source rows for one target row, which BigQuery rejects
        after earlier tables are already rewritten. Failing here writes nothing.
        Byte identical duplicates and a NULL shift beside a valued one are not
        disagreements; build_deid_map() collapses them.

        :param client: BigQueryClient
        :raises RuntimeError: if any participant has rows that disagree
        """
        query = CONFLICTING_PARTICIPANTS_TMPL.render(
            project_id=self.project_id,
            ids_dataset_id=self.ids_dataset_id,
            ids_view_id=self.ids_view_id)
        row = list(client.query(query).result())[0]

        if row['conflicting_count']:
            raise RuntimeError(
                f'{self.ids_dataset_id}.{self.ids_view_id} carries disagreeing rows '
                f'for {row["conflicting_count"]} participants, for example '
                f'{sorted(row["examples"])}. Each has more than one controlled_tier_id '
                f'or more than one non-NULL registered_tier_date_shift, so there is no '
                f'single map row to give it. Resolve the view before rerunning.'
            )

    def build_deid_map(self, client):
        """
        Build the sandbox _deid_map from the research IDs view, one row per participant.

        A NULL controlled_tier_id is excluded: PIDtoRID would write it into person_id,
        and its delete path only removes participants the map omits. A NULL shift is
        kept (see the module docstring), and a valued shift wins over a NULL one.

        :param client: BigQueryClient
        """
        self.assert_one_row_per_participant(client)

        query = BUILD_DEID_MAP_TMPL.render(deid_map=self.deid_map,
                                           project_id=self.project_id,
                                           ids_dataset_id=self.ids_dataset_id,
                                           ids_view_id=self.ids_view_id)
        client.query(query).result()
        LOGGER.info(
            f'Built {self.deid_map.dataset_id}.{self.deid_map.table_id} from '
            f'{self.ids_dataset_id}.{self.ids_view_id}')

    def setup_rule(self, client):
        """
        Build the CT+ map, then run the inherited setup.

        The inherited setup discovers the person_id tables and copies
        primary_pid_rid_mapping only when _deid_map is absent, so building first keeps
        the discovery and skips the stale copy. Building every run also replaces a map
        another tier left in the sandbox, which the inherited setup would reuse.

        :param client: BigQueryClient
        """
        self.build_deid_map(client)
        super().setup_rule(client)


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(CtPlusPIDtoRID,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(CtPlusPIDtoRID,)])
