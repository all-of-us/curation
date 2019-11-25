#!/usr/bin/env python2

# Copyright 2015 Google Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Local test runner

Example invocation:
  $ python runner.py ~/google-cloud-sdk
"""
# Python imports
import argparse
import os
import sys
import time
import unittest

# Third party imports
try:
    import coverage
except ImportError:
    print('coverage package not found.  Run `pip install -r dev_requirements.txt`')

try:
    import xmlrunner
except ImportError:
    print('xmlrunner package not found.  Run `pip install -r dev_requirements.txt`')


def print_unsuccessful(function, trace, msg_type):
    print('\n======================================================================')
    print('{}:  {}'.format(msg_type, function))
    print('----------------------------------------------------------------------')
    print(trace)
    print('----------------------------------------------------------------------')


def main(test_path, test_pattern):
    # Discover and run tests.
    suite = unittest.TestLoader().discover(test_path, test_pattern)
    all_results = []

    coverage_filepath = os.getcwd()
    coverage_filepath = os.path.dirname(coverage_filepath)
    coverage_filepath = os.path.join(coverage_filepath, '.coveragerc')
    cov = coverage.Coverage(config_file=coverage_filepath)
    cov.start()

    start_time = time.time()
    for mod_tests in suite:
        result = xmlrunner.XMLTestRunner(output='test_results/junit').run(mod_tests)
#        result = unittest.TextTestRunner(verbosity=2).run(mod_tests)
        all_results.append(result)

    end_time = time.time()
    cov.stop()
    cov.save()

    cov.html_report()
    cov.xml_report()

    run = 0
    errors = []
    failures = []
    for item in all_results:
        run += item.testsRun
        errors.extend(item.errors)
        failures.extend(item.failures)

    print('\n\n\n**********************************************************************')
    print('ALL TEST RESULTS')
    print('**********************************************************************')
    message = "Ran {} tests in {} seconds.".format(run, end_time - start_time)

    if errors:
        for err in errors:
            print_unsuccessful(err[0], err[1], 'ERROR')
        message += "\n{} error(s).  ".format(len(errors))

    if failures:
        for fail in failures:
            print_unsuccessful(fail[0], fail[1], 'FAIL')
        message += "\n{} failure(s).".format(len(failures))

    print(message)
    return not errors and not failures


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--test-path',
        help='The path to look for tests, defaults to the current directory.',
        default=os.getcwd())
    parser.add_argument(
        '--test-pattern',
        help='The file pattern for test modules, defaults to *_test.py.',
        default='*_test.py')

    args = parser.parse_args()

    result_success = main(args.test_path, args.test_pattern)

    if not result_success:
        sys.exit(1)
