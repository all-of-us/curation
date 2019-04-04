import re

import bq_utils

COMMAND_SEP = ';'
PREFIX_PLACEHOLDER = 'synpuf_100.'
TEMP_PREFIX = 'temp.'
TEMP_TABLE_PATTERN = re.compile('\s*INTO\s+([^\s]+)')
TRUNCATE_TABLE_PATTERN = re.compile('\s*truncate\s+table\s+([^\s]+)')
DROP_TABLE_PATTERN = re.compile('\s*drop\s+table\s+([^\s]+)')


def _is_commented_line(s):
    s = s.strip()
    return s == '' or s.startswith('--')


def is_commented_block(s):
    """
    True if the statement is commented or whitespace
    :param s:
    :return:
    """
    ls = s.split('\n')
    return all(map(_is_commented_line, ls))


def is_active_command(s):
    """
    True if the statement is uncommented
    :param s:
    :return:
    """
    return not is_commented_block(s)


def get_commands(sql_path):
    """
    Get the list of active (not commented) statements in the file at specified path
    :param sql_path:
    :return:
    """
    with open(sql_path, 'r') as f:
        text = f.read()
        commands = text.split(COMMAND_SEP)
        return filter(is_active_command, commands)


def qualify_tables(command, hpo_id=None):
    """
    Replaces placeholder text with proper qualifiers and renames temp tables
    :param command:
    :param hpo_id:
    :return:
    """

    def temp_repl(m):
        table_name = m.group(1).replace('.', '_')
        return bq_utils.get_table_id(hpo_id, table_name)

    # TODO ensure this remains consistent with bq_utils.get_table_id
    if hpo_id is None:
        table_prefix = ""
    else:
        table_prefix = hpo_id + '_'
    command = command.replace(PREFIX_PLACEHOLDER, table_prefix)
    command = re.sub('(temp.[^\s])', temp_repl, command)
    return command


def is_to_temp_table(q):
    """
    True if `q` is a DML statement that outputs to a temp table
    :param q:
    :return:
    """
    return 'INTO' in q


def get_temp_table_name(q):
    """
    Given a DML statement that outputs to a temp table, get the name of the temp table
    :param q:
    :return:
    """
    match = TEMP_TABLE_PATTERN.search(q)
    return match.group(1)


def get_temp_table_query(q):
    """
    Given a DML statement that outputs to a temp table, get the query
    :param q:
    :return:
    """
    table_name = get_temp_table_name(q)
    i = q.index(table_name)
    return q[i + len(table_name):].strip()


def is_truncate(q):
    """
    True if `q` is a TRUNCATE table statement
    :param q:
    :return:
    """
    return 'truncate' in q.lower()


def get_truncate_table_name(q):
    """
    Given a truncate DML statement, get the table being truncated
    :param q:
    :return:
    """
    match = TRUNCATE_TABLE_PATTERN.search(q)
    return match.group(1)


def is_drop(q):
    """
    True if `q` is a DROP table statement
    :param q:
    :return:
    """
    return 'drop' in q.lower()


def get_drop_table_name(q):
    """
    Given a drop DML statement, get the table being dropped
    :param q:
    :return:
    """
    match = DROP_TABLE_PATTERN.search(q)
    return match.group(1)
