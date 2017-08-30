# Tests for Curation

These tests can be run with:

```Shell
./run_tests.sh -g ${sdk_dir}
```

## Directory Structure

 * `unit_test` is for white box tests. These run on their own (do not require standing up the services).
 * `test_data` example payloads for testing
   * `five_persons` contains all records for a sample of five persons from the synpuf dataset 
