# Tests for Curation

These tests can be run with:

```Shell
$ ./tests/run_tests.sh -s [unit|integration|all] -r [file name pattern]
```
or with:
```Shell
$ PYTHONPATH=.:./data_steward:$PYTHONPATH python tests/runner.py --test-path <optional path>  --test_pattern <optional pattern> --coverage-file <required coverage file path> 
```

## Directory Structure

 * `unit_tests` is for white box tests. These run on their own (do not require standing up the services).
 * `integration_tests` require additional set up.  These tests often load data into cloud storage or big query.  They test integration of the project code with project assets.
 * `test_data` example payloads for testing
   * `five_persons` contains all records for a sample of five persons from the synpuf dataset
   * `nyc_five_person` contains all records for person_id 1-5 from the synpuf dataset
   * `pitt_five_person` contains all records for person_id 6-10 from the synpuf dataset
   * `rdr` contains all records for a sample of ten persons from a synthetic RDR dataset
   * `export` test payloads associated with json required by reports
     * `five_persons` sparse datasets (good for error handling checks)
     * `synpuf` empty by default, large (git ignored) files are downloaded here for some tests

## Conventions

 * New tests should be added to `unit_tests` or `integration_tests` as appropriate.
 * Test modules should be named as `<module_name_being_tested>_test.py`.  If the suffix is not `_test.py`, the test runner will not execute those tests.
 * If you need to add a new test module:
   * Add it appropriately in the directory structure.  The directory structure should mimic the `data_steward` directory structure inside both test directories.
   * Add a class method to echo the class name that is being tested.
