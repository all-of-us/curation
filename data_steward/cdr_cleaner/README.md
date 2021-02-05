# CDR Cleaner
This module defines the stages of data cleaning (i.e. `RDR`, `EHR`, `UNIONED`, `COMBINED`, `DEID_BASE`, `DEID_CLEAN`, `FITBIT`) and discrete steps (cleaning rules) associated with each stage. 

- `cleaning_rules/` cleaning rules that run automatically with `clean_cdr`
- `manual_cleaning_rules/` cleaning rules that must be run manually (i.e. not yet integrated)
- `clean_cdr` runner for all stages of data cleaning
- `clean_cdr_engine` bigquery job execution logic

## Adding a cleaning rule
1. Create a subclass of `cleaning_rules.BaseCleaningRule` within `cleaning_rules/`
1. Create the associated unit and integration tests
1. Add the cleaning rule class to the list associated with the stage(s) the cleaning rule will be run. Be mindful of the ordering. 

## Universal cleaning rule representation with `infer_rule()`
At the time of writing not all cleaning rules have been refactored so that they are subclasses of `BaseCleaningRule`. As a result, they may appear in the codebase in either of two styles, **class-based** or **legacy**. This can increase the complexity of code in the `clean_cdr_engine` and `clean_cdr` modules which must reconcile both cleaning rule styles to support several use cases. A **temporary** solution is currently in place which converts cleaning rules to the 3-tuple structure described below via  `clean_cdr_engine.infer_rule()`.

### Use of this structure is temporary. It will be obsoleted after all cleaning rules are class-based.

### (query_function, setup_function, rule_info)

This structure is based directly on the use cases described below.
1. **List associated SQL queries.** This corresponds to the `get_query_specs()` method in class-based rules and specific functions defined in legacy cleaning rules. Either are called in `clean_cdr_engine` in order to execute the queries needed to apply a cleaning rule. Listing queries can also be useful in scenarios where side effects are not intended, such as troubleshooting/debugging issues and generating documentation.
1. **Perform prerequisite setup.** This corresponds to the `setup_rule()` method in class-based rules. This is called by `clean_cdr_engine` to perform side effecting operations needed prior to applying a cleaning rule, such as loading lookup tables. There is no analog for this with legacy cleaning rules.
1. **Get associated metadata.** This corresponds to [the decorator](https://github.com/all-of-us/curation/commit/076f0e96ab75cf10b372e3690db0be77bc68dce7#diff-3b89aa0193204be72138398be0c4995eR55) used by `clean_cdr` to retrieve module and function information from cleaning rules -both class-based and legacy- to support more detailed logging. This will also be useful for auto-generating documentation.

While creating this structure from class-based rules is straightforward, there are some points to note about how legacy rules are fit.

|  | class-based | legacy |
| --- | --- | --- |
| query_function | a reference to method `get_query_specs` | A function which wraps the legacy cleaning function. The wrapper encloses parameters that instances of class-based rules would encapsulate (e.g. `project_id`, `dataset_id`). |
| setup_function | a reference to method `setup_rule` | A function matching the signature of method `setup_rule()` that does nothing. |
