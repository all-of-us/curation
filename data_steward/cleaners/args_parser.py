import argparse
import clean_cdr_engine as clean_engine

parser = argparse.ArgumentParser(description='Parse project_id and dataset_id',
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('-p', '--project_id',
                    action='store', dest='project_id',
                    help='Project associated with the input and output datasets',
                    required=True)
parser.add_argument('-d', '--dataset_id',
                    action='store', dest='dataset_id',
                    help='Dataset where cleaning rules are to be applied',
                    required=True)
parser.add_argument('-s', action='store_true', help='Send logs to console')
args = parser.parse_args()
clean_engine.add_console_logging(args.s)
