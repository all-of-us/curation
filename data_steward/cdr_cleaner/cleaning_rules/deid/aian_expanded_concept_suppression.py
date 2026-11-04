"""
Remove AIAN participants' expanded concept rows from the CT+ run.

The CT+ rules leave expanded rows inline for split_ct_plus_expanded_concepts.py to move
into the per-element add-ons. AIAN participants are in no add-on, and the mainline
assembly removes only rows an add-on holds, so their rows would otherwise be published
in the mainline, which must equal CT. Each row removed here would have been removed by
CT anyway.

ct_plus_expanded_concepts.py describes the whole sequence.

Original Issues: DL-2538
"""
# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.deid.concept_suppression import \
    AbstractConceptSuppression
from cdr_cleaner.cleaning_rules.deid.ct_plus_expanded_concepts import (
    build_expanded_lookup, expanded_row, get_expanded_tables, get_row_owner)
from common import AIAN_LIST, DEID_MAP, JINJA_ENV, NOTE_NLP
from constants import bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from resources import has_person_id

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DL2538']

# aian_list is keyed by participant id, so it goes through _deid_map, the map
# CtPlusPIDtoRID re-keyed person_id with.
AIAN_EXPANDED_ROWS = JINJA_ENV.from_string("""
SELECT
  d.*
FROM `{{project}}.{{dataset}}.{{domain_table}}` AS d
{{owner_join}}
WHERE {{expanded_row}}
AND {{person_id}} IN (
    SELECT m.research_id
    FROM `{{project}}.{{rdr_sandbox}}.{{aian_list}}` x
    JOIN `{{project}}.{{sandbox_dataset}}.{{deid_map}}` m
      ON m.person_id = x.person_id
)
""")


class AianExpandedConceptSuppressionCtPlus(AbstractConceptSuppression):
    """
    Suppress the expanded concept rows of AIAN participants.

    Must run after CtPlusPIDtoRID, which writes _deid_map. An empty aian_list removes
    nothing here, and split_ct_plus_expanded_concepts.py refuses on that and on any
    AIAN row this rule did not reach.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 rdr_sandbox_id,
                 table_namer=None):
        """
        :param rdr_sandbox_id: dataset holding aian_list, the RDR stage sandbox
        """
        desc = ('Sandbox and suppress the expanded concept rows of AIAN '
                'participants, who are in no CT+ add-on.')
        # A table with no way to reach a participant, such as provider, cannot hold a
        # participant's row. The split refuses on any expanded row left in one.
        affected_tables = [
            table for table in get_expanded_tables()
            if has_person_id(table) or table == NOTE_NLP
        ]
        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_PLUS_DEID],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            affected_tables=affected_tables,
            table_namer=table_namer)

        self.rdr_sandbox_id = rdr_sandbox_id

    def setup_rule(self, client, *args, **keyword_args):
        super().setup_rule(client, *args, **keyword_args)
        build_expanded_lookup(client, self.project_id, self.dataset_id,
                              self.sandbox_dataset_id)

    def get_sandbox_query(self, table_name):
        """
        Sandbox the table's expanded rows that belong to AIAN participants.

        :param table_name: domain table to sandbox from
        :return: query dictionary
        """
        owner_join, person_id = get_row_owner(self.project_id, self.dataset_id,
                                              table_name, 'd')
        query = AIAN_EXPANDED_ROWS.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            domain_table=table_name,
            owner_join=owner_join,
            person_id=person_id,
            expanded_row=expanded_row(self.project_id, self.sandbox_dataset_id,
                                      table_name,
                                      get_expanded_tables()[table_name], 'd'),
            rdr_sandbox=self.rdr_sandbox_id,
            aian_list=AIAN_LIST,
            deid_map=DEID_MAP)

        return {
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: self.sandbox_table_for(table_name),
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id
        }

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass
