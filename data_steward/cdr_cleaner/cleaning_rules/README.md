# Cleaning Rules
All cleaning rules should be implemented as classes derived from `BaseCleaningRule`. The main objective of a cleaning rule is to generate queries returned by `get_query_specs()`. To do this a cleaning rule should use parameters `project_id`, `dataset_id` provided at runtime (e.g. when running `clean_cdr.py`).

Some initial setup such as loading lookup tables may be needed for a cleaning rule to generate working queries; such setup should be done in `setup_rule()`. Cleaning rules should store lookups, intermediate data, and backup data in the location indicated by the `sandbox_dataset_id` parameter.

## Implementation guidance
 
- Cleaning rules should ideally be [idempotent](https://en.wikipedia.org/wiki/Idempotence)
- Cleaning rules execute independently from one another, but may have implicit data interdependencies- be mindful of the order in which they run in the cleaning process.
- If you are authoring a cleaning rule which requires some parameter other than the [universal parameters](#universal-parameters), check for a suitable [variable parameter](#variable-parameters) before creating a custom one specific for your cleaning rule and ensure that is documented in the same way as other parameters.

# Cleaning rule configuration
Cleaning rules depend on configuration parameters which must be supplied at runtime when cleaning a dataset. Some parameters are needed by all cleaning rules whereas others may only pertain to a few. These dependencies become difficult to manage without some overarching design. This section describes the design objective and summarizes existing configuration dependencies.

## Parameters correctly supplied
> Note: for these examples assume that cleaning fitbit only requires the additional parameters `mapping_dataset_id` and `mapping_table_id`

The primary use case is clean_cdr is supplied all required parameters for all cleaning rules associated with the data stage. These parameters are successfully passed along to any cleaning rules that reference the parameter.
```
clean_cdr.py --data_stage FITBIT
             --project_id my_project
             --dataset_id fitbit
             --sandbox_dataset_id fitbit_sandbox
             --mapping_dataset_id mapping_dataset
             --mapping_table_id mapping_table
```

## Parameters missing
Another scenario is when `clean_cdr` is **NOT** supplied all the parameters needed by the cleaning rules associated with a run. The missing configuration is detected by `clean_cdr` prior to executing any of the cleaning rules. 
```
clean_cdr.py --data_stage FITBIT
             --project_id my_project
             --dataset_id fitbit
             --sandbox_dataset_id fitbit_sandbox

MissingParameterError: the parameter 'mapping_dataset_id' required by GenerateSiteMappingsAndExtTables, RemoveFitbitDataIfMaxAgeExceeded, PIDtoRID FitbitDateShift was not supplied. No cleaning rules have been applied.
```

## Configuration dependencies
### Universal parameters
All cleaning rules depend on the following configuration parameters. Note that these describe parameters associated with cleaning a single dataset (a single invocation of `clean_engine.clean_dataset()`).

| parameter | description |
| --- | --- |
| project_id | Identifies the project containing the dataset being cleaned |
| dataset_id | Identifies the dataset being cleaned |
| sandbox_dataset_id [^1] | Identifies the dataset where intermediary and backup data are stored by the cleaning rule |
 [^1]: At the time of writing not all cleaning rules make use of a sandbox dataset. This is likely because many rules were authored prior to sandboxing practices. `sandbox_dataset_id` is a universal dependency of class based cleaning rules, whether any data from that particular cleaning rule is being sandboxed or not.  
 
### Variable parameters
Depending on the rule(s) being applied, additional configuration parameters may be needed when cleaning a dataset. This section describes these variable parameters and the cleaning rules that reference them. Currently, a cleaning rule may specify that a variable parameter is optional by providing a default value.
> Note: We should plan for this section to be automatically generated in the future

| parameter | description |
| --- | --- |
| [mapping_dataset_id](#mapping_dataset_id) | Identifies the dataset containing mapping tables needed for deid |
| [mapping_table_id](#mapping_table_id) | The name of the table in `mapping_dataset_id` which maps `participant_id` --> `research_id` |
| [combined_dataset_id](#combined_dataset_id) | Identifies the dataset containing EHR + RDR data |
| [ehr_dataset_id](#ehr_dataset_id) | Identifies the dataset containing a snapshot of unioned EHR data |
| [validation_dataset_id](#validation_dataset_id) | Identifies the dataset containing participant match tables |
| [route_mapping_dataset_id](#route_mapping_dataset_id) | Identifies the dataset containing the route mapping lookup table |
| [year_threshold](#year_threshold) | |
| [cutoff_date](#cutoff_date) | Identifies the EHR/RDR cutoff date in 'YYYY-MM-DD' format |
| [observation_year_threshold](#observation_year_threshold) | |
| [ticket_number](#)[^2] | |
| [pids_project_id](#)[^2] | |
| [pids_dataset_id](#)[^2] | |
| [tablename](#)[^2] | |
[^2]: these parameters defined in remove_ehr_data_past_deactivation_date will probably be refactored
### mapping_dataset_id
 - RemoveFitbitDataIfMaxAgeExceeded
 - PIDtoRID
 - FitbitDateShift
### mapping_table_id
 - PIDtoRID
 - FitbitDateShift
### ehr_dataset_id
 - remove_non_matching_participant (optional)
### validation_dataset_id
 - remove_non_matching_participant (optional)
### route_mapping_dataset_id
 - populate_route_ids (optional)
### year_threshold
- remove_records_with_wrong_date (optional)
### cutoff_date
- remove_records_with_wrong_date
### observation_year_threshold
- remove_records_with_wrong_date (optional)
