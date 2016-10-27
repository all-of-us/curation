# Sprint Reporter

Validate submissions for the All of Us data sprints

## Running
 * Update `_settings.py` and rename it to `settings.py`
 * Execute the following at the command line:
 
        python reporter.py

## Validation logic
 * File names must follow naming convention `{hpo_id}_{table}_datasprint_{sprint_num}.csv` 
     * `hpo_id` an hpo_id listed in [resources/hpo.csv](resources/hpo.csv)
     * `table` an OMOP CDM table listed in [resources/cdm.csv](resources/cdm.csv)
     * `sprint_num` sprint number of the submission
 * Files must be in CSV format (comma-delimited) as specified by [rfc4180](https://tools.ietf.org/html/rfc4180)
 * Column names and types must follow the conventions in [resources/cdm.csv](resources/cdm.csv)
