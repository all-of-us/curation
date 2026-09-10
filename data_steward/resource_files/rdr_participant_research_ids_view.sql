-- Reference copy of pipeline_tables.rdr_participant_research_ids_view.
--
-- The deployed view is maintained by hand and is not created by any code in this
-- repository, so this file is a reference copy rather than a deployment artifact.
-- Changing it does not change the deployed view. Apply a change by running this
-- file against the target project, then confirm the deployed definition matches:
--
--   bq query --use_legacy_sql=false --project_id=aou-res-curation-prod \
--     < data_steward/resource_files/rdr_participant_research_ids_view.sql
--
--   bq show --format=prettyjson \
--     aou-res-curation-prod:pipeline_tables.rdr_participant_research_ids_view
--
-- The comments below the CREATE statement are stored as part of the deployed view
-- definition; these header comments are not.

CREATE OR REPLACE VIEW `aou-res-curation-prod.pipeline_tables.rdr_participant_research_ids_view` AS
SELECT rpri.participant_id, rpri.created, rpri.modified, rpri.research_id, rpri.registered_tier_id,
rpri.research_id AS controlled_tier_id, rpri.controlled_tier_plus_id AS controlled_tier_plus_id, prm.shift as registered_tier_date_shift,
ai.aian
FROM `all-of-us-rdr-prod.rdr_operational_datastream_multi_region.rdr_participant_research_ids` rpri
-- LEFT so a participant RDR has already issued IDs for is not dropped for want of a
-- date shift. shift is the only column taken from primary_pid_rid_mapping, and that
-- table is populated a stage later than the IDs it is joined to, so an inner join
-- caps this view at whatever population that stage last wrote.
left join `aou-res-curation-prod.pipeline_tables.primary_pid_rid_mapping`  prm
on prm.research_id = rpri.research_id
left join `all-of-us-rdr-prod.rdr_operational_datastream_multi_region.ppsc_awardee_insite` ai
on ai.participant_id = rpri.participant_id
