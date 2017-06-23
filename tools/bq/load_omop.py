import argparse
import os

def main(args):
  for file in os.listdir(args.dir):
    if file.endswith('.csv'):
      table_name = file[0:len(file) - 4]
      if table_name[len(table_name) - 1].isdigit():
        last_underscore_index = table_name.rfind('_')
        table_name = table_name[0:last_underscore_index]
      schema_file = args.schema_dir + '/' + table_name + '.json'
      if os.path.exists(schema_file):
        print 'Loading %s/%s into %s:%s.%s...' % (args.dir, file, args.project, args.dataset, table_name)
        os.system('bq --project_id=%s load --skip_leading_rows 1 %s.%s %s/%s %s'
                  % (args.project, args.dataset, table_name, args.dir,
                     file, schema_file))
      else:
        print 'No schema for %s, skipping...' % file

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--dir', required=True,
                    help='Path to the directory containing OMOP files.')
parser.add_argument('--project', default='pmi-drc-api-test',
                    help='Project to load data into.')
parser.add_argument('--dataset', default='synpuf',
                    help='Dataset to load data into.')
parser.add_argument('--schema_dir', default='schemas',
                    help='Path to the directory containing BigQuery schema files.')
main(parser.parse_args())
