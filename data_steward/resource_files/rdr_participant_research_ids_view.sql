-- Reference copy of rdr_participant_research_ids_view.
--
-- The deployed view is maintained by hand and is not created by any code in this
-- repository, so this file is a reference copy rather than a deployment artifact.
-- Changing it does not change the deployed view. Apply a change by substituting the
-- placeholders below, running the file against the curation project, then confirming
-- the deployed definition matches:
--
--   bq query --use_legacy_sql=false --project_id=<curation project id> \
--     < data_steward/resource_files/rdr_participant_research_ids_view.sql
--
--   bq show --format=prettyjson \
--     <curation project id>:<pipeline dataset id>.rdr_participant_research_ids_view
--
-- !!IMPORTANT!!
--   Project names and dataset names are sensitive info. Keep them masked in this file.
--
-- The comments below the CREATE statement are stored as part of the deployed view
-- definition; these header comments are not.

CREATE OR REPLACE VIEW `<curation project id>.<pipeline dataset id>.rdr_participant_research_ids_view` AS
SELECT rpri.participant_id, rpri.created, rpri.modified, rpri.research_id, rpri.registered_tier_id,
rpri.research_id AS controlled_tier_id, rpri.controlled_tier_plus_id AS controlled_tier_plus_id, prm.shift as registered_tier_date_shift,
ai.aian
FROM `<rdr project id>.<rdr operational dataset id>.rdr_participant_research_ids` rpri
-- LEFT so a participant RDR has already issued IDs for is not dropped for want of a
-- date shift. shift is the only column taken from primary_pid_rid_mapping, and that
-- table is populated a stage later than the IDs it is joined to, so an inner join
-- caps this view at whatever population that stage last wrote.
left join `<curation project id>.<pipeline dataset id>.primary_pid_rid_mapping`  prm
on prm.research_id = rpri.research_id
left join `<rdr project id>.<rdr operational dataset id>.ppsc_awardee_insite` ai
on ai.participant_id = rpri.participant_id
