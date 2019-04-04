"""
flash config file and check requirements for each environment

 * takes config name or custom config file and uses that to populate the required variables for our scripts

"""
import argparse
import json
import os
import time

import bq_utils
from resources import fields_path

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--config',
                        default='dev',
                        help='config file type')
    parser.add_argument('--config_file',
                        default=None,
                        help='custom config file')
    main(parser.parse_args())
