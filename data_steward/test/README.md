# Tests for Curation

## Running Tests

The following environment variables must be available
 
| name | description |
| ---- | ----------- |
| `GOOGLE_APPLICATION_CREDENTIALS` | Location of service account credentials in JSON format (see [Google Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials#howtheywork)) |
| `GH_USERNAME` | Your github username |

Create cloud resources needed by tests:

```Shell
./../ci/setup.sh
```

Run tests:

```Shell
./run_tests.sh [-g /path/to/gae/sdk] [-r test-file-pattern] 
```

## Directory Structure

 * `unit_test` is for white box tests. These run on their own (do not require standing up the services).
 * `test_data` example payloads for testing
   * `five_persons` contains all records for a sample of five persons from the synpuf dataset
   * `nyc_five_person` contains all records for person_id 1-5 from the synpuf dataset
   * `pitt_five_person` contains all records for person_id 6-10 from the synpuf dataset
   * `rdr` contains all records for a sample of ten persons from a synthetic RDR dataset
   * `export` test payloads associated with json required by reports
     * `five_persons` sparse datasets (good for error handling checks)
     * `synpuf` empty by default, large (git ignored) files are downloaded here for some tests
