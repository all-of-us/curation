import resources
import os
import bq_utils

ACHILLES_ANALYSIS = 'achilles_analysis'
ACHILLES_RESULTS = 'achilles_results'
ACHILLES_RESULTS_DIST = 'achilles_results_dist'
ACHILLES_TABLES = [ACHILLES_ANALYSIS, ACHILLES_RESULTS, ACHILLES_RESULTS_DIST]
ACHILLES_DML_SQL_PATH = os.path.join(resources.resource_path, 'achilles_dml.sql')
COMMAND_SEP = ';'
PREFIX_PLACEHOLDER = 'synpuf_100.'
END_OF_IMPORTING_LOOKUP_MARKER = 'end of importing values into analysis lookup'


def _is_commented_line(s):
    s = s.strip()
    return s == '' or s.startswith('--')


def is_commented_block(s):
    ls = s.split('\n')
    return all(map(_is_commented_line, ls))


def is_active_command(s):
    return not is_commented_block(s)


def get_commands(sql_path):
    """

    """
    with open(sql_path, 'r') as f:
        text = f.read()
        commands = text.split(COMMAND_SEP)
        return filter(is_active_command, commands)


def qualify_tables(command, hpo_id):
    # TODO ensure this remains consistent with bq_utils.get_table_id
    table_prefix = hpo_id + '_'
    return command.replace(PREFIX_PLACEHOLDER, table_prefix)


def _get_load_analysis_commands(hpo_id):
    raw_commands = get_commands(ACHILLES_DML_SQL_PATH)
    commands = map(lambda cmd: qualify_tables(cmd, hpo_id), raw_commands)
    for command in commands:
        if END_OF_IMPORTING_LOOKUP_MARKER in command.lower():
            break
        yield command


def load_analyses(hpo_id):
    commands = _get_load_analysis_commands(hpo_id)
    for command in commands:
        bq_utils.query(command)


def create_tables(hpo_id, drop_existing=False):
    """
    Create the achilles related tables
    :param hpo_id: associated hpo id
    :param drop_existing: if True, drop existing tables
    :return:
    """
    for table_name in ACHILLES_TABLES:
        table_id = bq_utils.get_table_id(hpo_id, table_name)
        bq_utils.create_standard_table(table_name, table_id, drop_existing)
