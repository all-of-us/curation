"""
    Health Information Privacy Lab
    Brad. Malin, Weiyi Xia, Steve L. Nyemba

    This file is designed to put rules into a canonical form.
    The framework distinguishes rules from their applications so as to allow simulation
     and have visibility into how the rules are applied given a variety of contexts

"""
from argparse import ArgumentParser, ArgumentTypeError

from constants.deid.deid import MAX_AGE


class Parse(object):
    """
    Parse rules into a canonical form.

    This utility class implements the various ways in which rules and their
    applications are put into a canonical form.
    Having rules in a canonical form makes it easy for an engine to apply them in batch
    """

    @staticmethod
    def init(rule_id, row, cache, tablename):
        try:
            row_rules = row.get('rules', '')
            _id, _key = row_rules.replace('@', '').split('.')
        except ValueError:
            _id, _key = None, None

        label = ".".join([_id, _key]) if _key is not None else rule_id

        if _id and _key and _id in cache and _key in cache[_id]:
            p = {'label': label, 'rules': cache[_id][_key]}

        else:
            p = {'label': label}
        for key in row:
            p[key] = row[key] if key not in p else p[key]

        p['tablename'] = tablename
        return p

    @staticmethod
    def shift(row, cache, tablename):
        return Parse.init('shift', row, cache, tablename)

    @staticmethod
    def generalize(row, cache, tablename):
        """
        parsing generalization and translating the features into a canonical form of stors
        """
        p = Parse.init('generalize', row, cache, tablename)

        return p

    @staticmethod
    def suppress(row, cache, tablename):
        """
        setup suppression rules to be applied to the 'row' i.e entry
        """
        return Parse.init('suppress', row, cache, tablename)

    @staticmethod
    def compute(row, cache, tablename):
        """
        Compute fields.

        This includes shifting dates and mapping values.
        """
        return Parse.init('compute', row, cache, tablename)

    @staticmethod
    def dml_statements(row, cache, tablename):
        """
        When indicated, it will create dml statements to execute after all other de-identifications.
        """
        return Parse.init('dml_statements', row, cache, tablename)


# This is the pythonic way to parse system arguments
def pipeline_list(pipeline):
    """
    Ensure the pipeline arguments are valid

    Return a list of pipeline steps.
    """
    valid = ['generalize', 'suppress', 'shift', 'compute']
    if isinstance(pipeline, str):
        plist = [term.strip() for term in pipeline.split(',')]
    elif isinstance(pipeline, list):
        plist = pipeline

    for item in plist:
        if not isinstance(item, str) or item not in valid:
            message = '%s:  Unknown pipeline operation.' % item
            raise ArgumentTypeError(message)
    return plist


def query_priority(priority):
    if priority.lower() == 'interactive':
        return 'INTERACTIVE'
    elif priority.lower() == 'batch':
        return 'BATCH'
    else:
        message = '%s:  Unknown priority type' % priority
        raise ArgumentTypeError(message)


def odataset_name_verification(odataset_name):
    """
    Ensures the output dataset name ends with _deid

    :param odataset_name: output dataset name
    :return: output dataset name
    """
    if odataset_name.endswith('_deid'):
        return odataset_name
    else:
        message = f'{odataset_name} must end with _deid'
        raise ArgumentTypeError(message)


def parse_args(raw_args=None):
    """
    Parse command line arguments.

    Returns a dictionary of namespace arguments.
    """
    parser = ArgumentParser(description='Parse deid command line arguments')
    parser.add_argument('--rules',
                        action='store',
                        dest='rules',
                        help='Filepath to the JSON file containing rules',
                        required=True)
    parser.add_argument('--run_as',
                        action='store',
                        dest='run_as_email',
                        help='Service account email address to impersonate',
                        required=True)
    parser.add_argument('--idataset',
                        action='store',
                        dest='idataset',
                        help='Name of the input dataset',
                        required=True)
    parser.add_argument('--odataset',
                        action='store',
                        dest='odataset',
                        type=odataset_name_verification,
                        help='Name of the output dataset must end with _deid ',
                        required=True)
    parser.add_argument('--private_key',
                        dest='private_key',
                        action='store',
                        required=True,
                        help='Service account file location')
    parser.add_argument(
        '--table',
        dest='table',
        action='store',
        required=True,
        help='Path that specifies how rules are applied on a table')
    parser.add_argument(
        '--action',
        dest='action',
        action='store',
        required=True,
        choices=['submit', 'simulate', 'debug'],
        help=('simulate: generate simulation without creating an '
              'output table\nsubmit: create an output table\n'
              'debug: print output without simulation or submit '
              '(runs alone)'))
    parser.add_argument('--cluster',
                        dest='cluster',
                        action='store_true',
                        help='Enable clustering on person_id')
    parser.add_argument('--log',
                        dest='log',
                        action='store',
                        help='Filepath for the log file')
    parser.add_argument(
        '--pipeline',
        dest='pipeline',
        action='store',
        default='generalize,suppress,shift,compute',
        type=pipeline_list,
        help=('Specifies operations and their order.  '
              'Operations are comma separated.  Default pipeline is:  '
              'generalize, suppress, shift, compute'))
    parser.add_argument(
        '--age-limit',
        dest='age_limit',
        action='store',
        default=MAX_AGE,
        type=int,
        help=(f'Optional parameter to set the maximum age limit.  '
              f'Defaults to {MAX_AGE}.'))
    parser.add_argument(
        '--interactive',
        dest='interactive',
        action='store',
        nargs='?',
        default='BATCH',
        type=query_priority,
        const='INTERACTIVE',
        help='Run the query in interactive mode.  Default is batch mode.')
    parser.add_argument('--version', action='version', version='deid-02')
    # normally, the parsed arguments are returned as a namespace object.  To avoid
    # rewriting a lot of existing code, the namespace elements will be turned into
    # a dictionary object and returned.
    return vars(parser.parse_args(raw_args))
