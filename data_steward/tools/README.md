# Tools for Curation
### set_path.sh

Sets PYTHONPATH to include everything in libs and the AppEngine SDK. For use when running python
scripts that rely on these libraries.

### load_vocab.sh

Given a path to vocabulary csv files downloaded from Athena:
 1. Adds the local vocabulary AoU_General (i.e. concepts in `resource_files/aou_general/concept.csv`)
 1. Fixes the format of date fields and remove any Windows line endings so BigQuery can load it
 1. Uploads the transformed files to a specified GCS bucket
 1. Loads the vocabulary in a specified BigQuery dataset
