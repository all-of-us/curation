# Python imports
import re
from io import open

# Project imports
import resources

COMMAND_SEP = ';'
PREFIX_PLACEHOLDER = 'synpuf_100.'
TEMP_PREFIX = 'temp.'
TEMP_TABLE_PATTERN = re.compile('\s*INTO\s+([^\s]+)')
TRUNCATE_TABLE_PATTERN = re.compile('\s*truncate\s+table\s+([^\s]+)')
DROP_TABLE_PATTERN = re.compile('\s*drop\s+table\s+([^\s]+)')
COMMENTED_BLOCK_REGEX = re.compile(
    '(?P<before_comment>(^)(.)*)(?P<comment>(\/\*)(.)*(\*\/))(?P<after_comment>(.)*$)',
    re.DOTALL)


def _is_commented_line(s):
    s = s.strip()
    return s == '' or s.startswith('--')


def is_commented_block(statement):
    """
    True if the statement is commented or whitespace
    :param statement:
    :return:
    """
    statement = statement.strip()
    ls = statement.split('\n')
    line_comments = all(map(_is_commented_line, ls))

    if line_comments:
        return line_comments

    block_comment = False
    match = COMMENTED_BLOCK_REGEX.search(statement)
    if match and not (match.group('before_comment') or
                      match.group('after_comment')):
        block_comment = True

    return block_comment


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
    commands = []
    with open(sql_path, 'r') as f:
        text = f.read()
        commands = text.split(COMMAND_SEP)
    commands = [command for command in commands if is_active_command(command)]
    return commands


def qualify_tables(command, hpo_id=None):
    """
    Replaces placeholder text with proper qualifiers and renames temp tables.
    
    For the following special cases it renames death to aou_death:
        - For unioned_ehr in EHR dataset, death table does not exist. Use aou_death instead.
        - For non-EHR, death table exists but is empty. Use aou_death instead.
        NOTE: RDR dataset does not have aou_death. But we do not run Achilles on them so it is not a problem.

    :param command:
    :param hpo_id: 
        HPO ID used as a prefix for table names.
        'unioned_ehr' for unioned EHR tables in EHR dataset.
        None if the dataset is not EHR, as the table names do not have prefixes.
    :return:
    """

    def temp_repl(m):
        table_name = m.group(1).replace('.', '_')
        return resources.get_table_id(table_name, hpo_id=hpo_id)

    table_prefix = resources.get_table_id(table_name="", hpo_id=hpo_id)

    if hpo_id == None:
        command = command.replace(f'{PREFIX_PLACEHOLDER}death', 'aou_death')
    elif hpo_id == 'unioned_ehr':
        command = command.replace(f'{PREFIX_PLACEHOLDER}death',
                                  f'unioned_ehr_aou_death')

    command = command.replace(PREFIX_PLACEHOLDER, table_prefix)
    command = re.sub('(temp\.[^\s])', temp_repl, command)
    return command


def is_to_temp_table(query):
    """
    Determine if the query is a DML statement that outputs to a temp table

    :param query: The query string to parse
    :return:  True if the query string is saving results to a temporary table.
        False if not a DML statement outputting to a temporary table.
    """
    # remove all line comments
    query_without_line_comments = []
    for line in query.split('\n'):
        if not _is_commented_line(line):
            query_without_line_comments.append(line)

    # remove all block comments
    query_string = ' '.join(query_without_line_comments)
    match = COMMENTED_BLOCK_REGEX.search(query_string)
    while match:
        query_string = match.group('before_comment') + match.group(
            'after_comment')
        match = COMMENTED_BLOCK_REGEX.search(query_string)

    query_list = query_string.split()
    insert_query = False
    if query_list[0].lower() == 'insert':
        insert_query = True

    stores_output = False
    for qualifier in [' INTO ', ' into ']:
        if qualifier in query or qualifier.lstrip() in query:
            stores_output = True

    return stores_output and not insert_query


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
