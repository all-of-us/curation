"""
DEID rule to change PIDs to RIDs for OMOP tables and aou_death in the controlled tier plus.

CT+ needs the same PID to RID substitution the controlled tier performs, but it cannot
take the map from the same place. RtCtPIDtoRID inherits PIDtoRID.setup_rule, which copies
pipeline_tables.primary_pid_rid_mapping into the sandbox as _deid_map when that table is
absent. That mapping table predates the pediatric feed and holds none of the pediatric
participants, and PIDtoRID deletes every row whose person_id is missing from the map
rather than failing on it. A CT+ run built from that map therefore drops the pediatric
cohort entirely, logs a warning from inspect_rule, and exits successfully.

This rule builds _deid_map from pipeline_tables.rdr_participant_research_ids_view instead.
The view is what RDR issues research IDs into, so it carries pediatric participants, and
across the participants the two sources share its controlled_tier_id equals the mapping
table's research_id and its registered_tier_date_shift equals the mapping table's shift.
Substituting the source therefore changes which participants survive, not what ID any
already-covered participant receives.

The RID written here is controlled_tier_id, matching the controlled tier, because
regenerate_ct_plus_ids.py converts CT IDs to CT+ IDs after the pipeline finishes and joins
its input person_id against that same column. Keying to controlled_tier_plus_id here would
leave the script nothing to convert from.

The shift column is carried through unchanged but is not read anywhere in CT or CT+: the
three rules that read _deid_map.shift are all registered in registered tier lists only
(DateUnShiftCopeResponses in REGISTERED_TIER_DEID_BASE_CLEANING_CLASSES,
FitbitDateShiftRule in REGISTERED_TIER_FITBIT_CLEANING_CLASSES, and
SurveyConductDateShiftRule commented out). This matters because the view sources the shift
from primary_pid_rid_mapping, so once the view stops inner joining that table, pediatric
participants will appear here with a NULL shift. That is inert today and is why a NULL
shift is not filtered out; filtering on it would delete the very cohort this rule exists
to keep. Anyone adding a date shifting rule to a CT+ stage has to resolve that first.
"""
# Python Imports
import logging

# Project Imports
from cdr_cleaner.cleaning_rules.deid.rt_ct_pid_rid_map import RtCtPIDtoRID
from common import (JINJA_ENV, PIPELINE_TABLES,
                    RDR_PARTICIPANT_RESEARCH_IDS_VIEW)

LOGGER = logging.getLogger(__name__)

# A participant carrying two rows that disagree on the ID or the shift. SELECT DISTINCT
# below collapses only byte identical duplicates, which is the shape the view has been
# observed carrying; a disagreeing pair survives it and would leave PIDtoRID's
# UPDATE ... FROM matching two source rows for one target row, which BigQuery rejects
# mid-run, after output tables have already been written.
CONFLICTING_PARTICIPANTS_TMPL = JINJA_ENV.from_string("""
WITH conflicting AS (
    SELECT participant_id
    FROM `{{project_id}}.{{ids_dataset_id}}.{{ids_view_id}}`
    WHERE participant_id IS NOT NULL
      AND controlled_tier_id IS NOT NULL
    GROUP BY participant_id
    HAVING COUNT(DISTINCT FORMAT('%T', STRUCT(controlled_tier_id,
                                              registered_tier_date_shift))) > 1
)
SELECT
    COUNT(*) AS conflicting_count,
    ARRAY_AGG(participant_id ORDER BY participant_id LIMIT 5) AS examples
FROM conflicting
""")

BUILD_DEID_MAP_TMPL = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{deid_map.project}}.{{deid_map.dataset_id}}.{{deid_map.table_id}}` AS
SELECT DISTINCT
    participant_id AS person_id,
    controlled_tier_id AS research_id,
    registered_tier_date_shift AS shift
FROM `{{project_id}}.{{ids_dataset_id}}.{{ids_view_id}}`
WHERE participant_id IS NOT NULL
  AND controlled_tier_id IS NOT NULL
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
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!

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

        The view is maintained outside this repository and has been observed carrying
        duplicate participant rows. Byte identical duplicates are harmless and the
        SELECT DISTINCT in build_deid_map() removes them. A pair that disagrees on
        controlled_tier_id or on the shift is not harmless and does not survive as a
        choice this rule is entitled to make silently: the map would carry the
        participant twice, and PIDtoRID's UPDATE ... FROM would then match two source
        rows for one target row, which BigQuery rejects. Failing here costs a run that
        has written nothing; failing there costs one that has already rewritten tables.

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
                f'{sorted(row["examples"])}. Each has more than one combination of '
                f'controlled_tier_id and registered_tier_date_shift, so there is no '
                f'single research_id to map it to. Resolve the view before rerunning.'
            )

    def build_deid_map(self, client):
        """
        Build the sandbox _deid_map from the research IDs view.

        A NULL controlled_tier_id is excluded rather than carried. PIDtoRID sets
        person_id from the map unconditionally, so a NULL RID would be written into
        person_id and would not be caught by the delete path, which only removes
        participants the map does not mention at all. A NULL shift is deliberately not
        excluded; see the module docstring.

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

        RtCtPIDtoRID.setup_rule() discovers the person_id tables and then defers to
        PIDtoRID.setup_rule(), which copies primary_pid_rid_mapping into the sandbox only
        when _deid_map is absent. Building the map first makes that copy a no-op, so the
        table discovery is inherited and the stale source is never read. Building
        unconditionally also means a map left in the sandbox by an earlier run of another
        tier is replaced rather than silently reused, which is the case the inherited
        early return allows.

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
