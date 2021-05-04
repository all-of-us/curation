"""
    Health Information Privacy Lab
    Brad. Malin, Weiyi Xia, Steve L. Nyemba

    The de-identification code applies to both the projection and filters (relational algebra terminology).
    The code will apply to both relational tables or meta-tables (alternative to relational modeling).

    Rules are expressed in JSON format with limited vocabulary and largely based on
    templates for every persistent data-store.  We have rules stored in one-place and the application
    of rules in another. This will allow for rules to be able to be shared

    Shifting:
        We shift dates given a random function or given a another table that has the number of days for the shift
    Generalization
        Rules for generalization are applied on a projected set of tuples and can have an conditions
    Suppression:
        Suppression rules apply to tuples provided a relational table and/or rows
    Compute:
        Computed fields stored
"""
# Python imports
import logging

# Third party imports
import numpy as np

# Project imports
from deid.parser import Parse
from resources import fields_for

LOGGER = logging.getLogger(__name__)


def create_on_string(item):
    """
    Given a dictionary of 'on' keys and values, create a string.

    A helper method to create a string from a list of values.

    :param item: a dictionary with 'field', 'qualifier', and 'values' keys.

    :return:  a tuple.  The first tuple member is a a string with the values
        for each key substituted such that it equals, 'field qualifier (values)'.
        the second value is the field name, if provided.  if a string is passed,
        the string is returned and None is sent as the specific field name
    """
    if isinstance(item, dict):
        field = item.get('field')
        qualifier = item.get('qualifier')
        values = item.get('values')

        # turn a list into a single string
        values = [str(value) for value in values]
        values = ' '.join(values)

        string = ' '.join([field, qualifier, '(', values, ')'])
        return string, field
    else:
        return item, None


def _get_boolean(value):
    """
    Return a boolean for a given string value.

    :param value:  The value to interpret as a boolean

    :return:  either True or False
    """
    true_bools = ['yes', 'y', 'true', 't']

    if isinstance(value, bool):
        return value
    elif isinstance(value, (str)):
        if value.lower() in true_bools:
            return True

    return False


def _get_case_condition_syntax(cond, regex, gen_value, rule, rules, syntax):
    """
    Build case statement syntax.

    :param cond:  The current conditional list
    :param regex:  The built up regular expression string
    :param gen_value:  The value to generalize values meeting the syntax into
    :param rule:  The current dictionary rule that is being processed
    :param rules:  The list of rules for processing a generalization

    :return:  A populated condition list, that when joined will form
        part of a proper CASE statement
    """
    if rules[0] == rule:
        cond.append(syntax['IF'])

    cond += [" ".join([syntax['OPEN'], regex, syntax['THEN'], gen_value])]

    if rule == rules[-1]:
        cond += [syntax['ELSE']]

    return cond


class Rules(object):

    def __init__(self, **args):
        self.cache = {}
        self.store_syntax = {
            "sqlite": {
                "apply": {
                    "REGEXP":
                        "LOWER(:FIELD) REGEXP LOWER(':VAR')",
                    "COUNT":
                        "SELECT COUNT(:FIELD) FROM :TABLE WHERE :KEY=:VALUE"
                },
                "cond_syntax": {
                    "IF": "CASE WHEN",
                    "OPEN": "",
                    "THEN": "THEN",
                    "ELSE": "ELSE",
                    "CLOSE": "END"
                },
                "random": "random() % 365 "
            },
            "bigquery": {
                "apply": {
                    "REGEXP":
                        "REGEXP_CONTAINS (LOWER(:FIELD), LOWER(':VAR'))",
                    "COUNT":
                        "SELECT COUNT (:KEY) FROM :DATASET.:TABLE AS :ALIAS WHERE :KEY=:VALUE",
                    "COUNT-DISTINCT":
                        ("SELECT COUNT (DISTINCT :ALIAS.:DISTINCT_FIELD) "
                         "FROM :DATASET.:TABLE AS :ALIAS WHERE :KEY=:VALUE"),
                    "SQL":
                        ":SQL_STATEMENT"
                },
                "cond_syntax": {
                    "IF": "CASE",
                    "OPEN": "WHEN",
                    "THEN": "THEN",
                    "ELSE": "ELSE",
                    "CLOSE": "END"
                },
                "random": "CAST( (RAND() * 364) + 1 AS INT64)"
            },
            "postgresql": {
                "cond_syntax": {
                    "IF": "CASE WHEN",
                    "OPEN": "",
                    "THEN": "THEN",
                    "ELSE": "ELSE",
                    "CLOSE": "END"
                },
                "shift": {
                    "date": "FIELD INTERVAL 'SHIFT DAY' ",
                    "datetime": "FIELD INTERVAL 'SHIFT DAY'"
                },
                "random": "(random() * 364) + 1 :: int"
            }
        }
        self.pipeline = args.get('pipeline',
                                 ['generalize', 'compute', 'suppress', 'shift'])
        self.cache = args.get('rules', [])
        self.parent = args.get('parent')

    def set(self, key, rule_id, **args):
        if key not in self.pipeline:
            raise (
                key +
                " is Unknown, [generalize, compute, suppress, shift] are allowed"
            )
        if key not in self.cache:
            self.cache[key] = {}
        if id not in self.cache[key]:
            self.cache[key][rule_id] = []

        self.cache[key][rule_id].append(args)

    def get(self, key, rule_id):
        return self.cache[key][rule_id]

    def validate(self, rule_id, entry, tablename):
        """
        Validating if a the application of a rule relative to a table is valid
        """
        p = rule_id in self.cache

        q = []

        for row in entry:

            if 'rules' in row:
                if not isinstance(row['rules'],
                                  list) and row['rules'].startswith(
                                      '@') and "into" in row:
                    #
                    # Making sure the expression is {apply,into} suggesting a rule applied relative to an attribute
                    # finding the rules that need to be applied to a given attribute
                    _id, _key = row['rules'].replace('@', '').split('.')
                    q.append((_id == rule_id) and (_key in self.cache[rule_id]))
                else:
                    q.append(1)
            elif 'rules' not in row and isinstance(row, list) or not p:
                #
                # assuming we are dealing with a list of strings applied
                # and dealign with self contained rules
                q.append(1)
            else:
                if 'on' in row and 'values' in row and 'qualifier' in row:
                    q.append(1)

        q = sum(q) == len(q)

        return (p and q) or (not p and q)


class Deid(Rules):
    """
    This class is designed to apply rules to structured data.

    For this to work we consider the following:
        - a rule can be applied to many fields and tables (broadest sense)
        - a field can have several rules applied to it:

    """

    def __init__(self, **args):
        Rules.__init__(self, **args)

    def validate(self, rule_id, entry, tablename):
        payload = None

        if Rules.validate(self, rule_id, entry, tablename):

            payload = {}
            payload = {"args": []}
            payload["pointer"] = getattr(self, rule_id)
            for row in entry:

                #
                # @TODO: Ensure that an error is thrown if the rule associated is not found
                p = getattr(Parse, rule_id)(row, self.cache, tablename)
                payload['args'] += [p]

        return payload

    def dml_statements(self, **args):
        """
        Drop duplicate generalized values.

        Arguments are accessed as key, value pairs of a dictionary.
        The following keys are defined for this function.

        :param generalized_values: a list of generalized integers that may produce
            duplicate records.  the list is joined and placed into the query.
        :param rules: a list of rules for the given table.  This is a list of the
            SQL lines that when joined, will produce a DML statement.
        :param tablename: a qualified table name of dataset.tablename
        :param label: a string denoting this is droping duplicates for the table
            with name, 'drop_duplicates.<tablename>'
        :param key_values:  a list of integer values to use to filter the records.
            the list is joined and placed into the query.
        :param store: a string name for the datastore

        :return: a dictionary defining the delete rule, a rule name, label, and
            specifying the query is a DML statement
        """
        dml_statements = []

        for rule_dict in args.get('rules', []):
            # get parameters passed to the function
            rule_list = rule_dict.get('statement', [])
            generalized_values = args.get('generalized_values', [])
            generalized_values = [str(value) for value in generalized_values]
            key_values = args.get('key_values', [])
            key_values = [str(value) for value in key_values]
            tablename = args.get('tablename')

            # create the SQL rule
            rule = ' '.join(rule_list)
            key_values_string = ', '.join(key_values)
            generalized_values_string = ', '.join(generalized_values)

            # replace values as required
            rule = rule.replace(':generalized_values',
                                generalized_values_string)
            rule = rule.replace(':key_values', key_values_string)
            rule = rule.replace(':odataset', self.parent.odataset)
            rule = rule.replace(':idataset', self.parent.idataset)

            # log values inserted into the query
            LOGGER.info(
                f"generalized_values causing duplicates: {generalized_values_string}    in table:  %s",
                tablename)
            LOGGER.info(
                f"key_values causing duplicates: {key_values_string}    in table:  %s",
                tablename)
            dml_statements.append({
                'apply': rule,
                'name': rule_dict.get('name', 'NAME_UNSET'),
                'label': rule_dict.get('label', 'LABEL_UNSET'),
                'dml_statement': True
            })

        return dml_statements

    def generalize(self, **args):
        """
        Apply generalizations given a set of rules.

        The rules apply to both meta tables and relational tables.
        The rules are defined in the config.json file.  The arguments, args,
        are defined in the <table_name>.json file.

        :fields list of target fields
        :rules  list of rules to be applied
        :label  context of what is being generalized
        """
        fields = args.get('fields', [args.get('value_field', '')])
        label = args.get('label', '')
        rules = args.get('rules', [])

        store_id = args.get('store', 'sqlite')
        syntax = self.store_syntax[store_id]['cond_syntax']
        out = []
        for name in fields:
            cond = []
            for rule in rules:
                qualifier = rule.get('qualifier', '')

                if 'apply' in rule:
                    #
                    # This will call a built-in SQL function (non-aggregate)'
                    fillter = args.get('filter', name)
                    LOGGER.info(
                        f"generalizing with SQL aggregates label:\t{label.split('.')[1]}\t\t"
                        f"on:\t{name}\t\ttype:\t{rule['apply']}\t\t")

                    if 'apply' not in self.store_syntax[store_id]:
                        regex = [
                            rule['apply'], "(", fillter, " , '",
                            "|".join(rule['values']), "') ", qualifier
                        ]
                    else:
                        template = self.store_syntax[store_id]['apply'][
                            rule['apply']]
                        regex = template.replace(':FIELD', fillter).replace(
                            ':FN', rule['apply'])

                        if ':VAR' in template:
                            regex = regex.replace(":VAR",
                                                  "|".join(rule['values']))

                        if rule['apply'] in [
                                'COUNT', 'COUNT-DISTINCT', 'AVG', 'SUM'
                        ]:
                            #
                            # Dealing with an aggregate expression. It is important to know what we are counting
                            # count(:field) from :table [where filter]
                            #
                            regex = regex.replace(
                                ':TABLE', args.get('table', 'table NOT SET'))
                            regex = regex.replace(
                                ':KEY',
                                args.get('key_field', 'key_field NOT SET'))
                            regex = regex.replace(
                                ':VALUE',
                                args.get('value_field', 'value_field NOT SET'))
                            regex = regex.replace(
                                ':DATASET',
                                args.get('dataset', 'dataset NOT SET'))
                            regex = regex.replace(
                                ':ALIAS', args.get('alias', 'alias NOT SET'))
                            regex = regex.replace(
                                ':DISTINCT_FIELD',
                                rule.get('distinct', 'distinct NOT SET'))

                            if 'on' in rule:
                                key_row = args[
                                    'key_row'] if 'key_row' in args else name
                                key_row = key_row.replace(':name', name)
                                conjunction = ' AND ' if 'qualifier' in rule else ' WHERE '

                                if isinstance(rule.get('on'), list):
                                    try:
                                        val_list = " IN ('" + "','".join(
                                            rule['on']) + "')"
                                    except TypeError:
                                        val_list = [
                                            str(val_item)
                                            for val_item in rule['on']
                                        ]
                                        val_list = " IN (" + ",".join(
                                            val_list) + ")"
                                    regex += conjunction + key_row + val_list
                                # the following conditions added to help with nullable columns
                                elif 'exists' in rule.get('on', ''):
                                    val_list = rule.get('on', '')
                                    regex += conjunction + val_list
                                else:
                                    val_list = "(" + rule.get('on', '') + ")"
                                    regex += conjunction + val_list

                            if 'on' in args:
                                conditional, _ = create_on_string(
                                    args.get('on'))

                                alias = args.get('alias', 'alias NOT SET')
                                conditional = conditional.replace(
                                    ':join_tablename', alias)
                                regex += ' AND ' + conditional

                            regex = ' '.join(['(', regex, ')', qualifier])
                        elif rule['apply'] in ['SQL']:
                            statement = rule.get('statement',
                                                 ['statement NOT SET'])
                            statement = ' '.join(statement)
                            statement = statement.replace(
                                ':table', args.get('table', 'table_NOT_SET'))
                            statement = statement.replace(':fields', fillter)
                            regex = regex.replace(':SQL_STATEMENT', statement)

                            regex = ' '.join([regex, qualifier])

                        else:
                            regex = ' '.join([regex, qualifier])
                        #
                        # Is there a filter associated with the aggregate function or not
                        #
                    if 'into' in rule or 'into' in args:
                        gen_value = args.get('into', rule.get('into', ''))

                        if not isinstance(gen_value, int):
                            gen_value = "'" + gen_value + "'"
                        else:
                            gen_value = str(gen_value)

                        regex = "".join(regex)
                        cond = _get_case_condition_syntax(
                            cond, regex, gen_value, rule, rules, syntax)

                else:
                    #
                    # Process a generalization given a list of values with no overhead of an aggregate function
                    # @TODO: Document what is going on here
                    #   - An if or else type of generalization given a list of values or function
                    #   - IF <filter> IN <values>, THEN <generalized-value> else <attribute>
                    LOGGER.info(
                        f"generalizing inline arguments label:\t{{label.split('.')[1]}}\t\t"
                        f"on:\t{name}\t\ttype:\tinline")
                    fillter = args.get('filter', name)
                    qualifier = rule.get('qualifier', '')
                    gen_value = args.get('into', rule.get('into', ''))

                    if isinstance(gen_value, int):
                        gen_value = str(gen_value)
                        values = [str(value) for value in rule['values']]
                        values = '(' + ','.join(values) + ')'
                    else:
                        gen_value = "'" + gen_value + "'"
                        values = "('" + "','".join(rule['values']) + "')"

                    regex_list = [fillter, qualifier, values]

                    regex = " ".join(regex_list)
                    cond = _get_case_condition_syntax(cond, regex, gen_value,
                                                      rule, rules, syntax)

            #
            # Let's build the syntax here to make it sound for any persistence storage
            cond += [name]
            cond_counts = sum([1 for xchar in cond if syntax['IF'] in xchar])
            cond += np.repeat(syntax['CLOSE'], cond_counts).tolist()

            cond += ['AS', name]
            result = {"name": name, "apply": " ".join(cond), "label": label}
            if 'on' in args:
                result['on'] = args['on']

            out.append(result)

            # allows to generalize one field off another field's values
            # essentially, it copies generalized values of one field to another
            # if the source field is not generalized, the field retains its original value
            if 'copy_to' in args:
                copy_to = args.get('copy_to')
                if not isinstance(copy_to, list):
                    copy_to = [copy_to]

                for copy_field in copy_to:
                    # save as copy_field
                    cond[-1] = copy_field
                    # fall back to original field value
                    cond[-4] = copy_field
                    copy_result = {
                        "name": copy_field,
                        "apply": " ".join(cond),
                        "label": label
                    }
                    if 'on' in args:
                        copy_result['on'] = args['on']
                    out.append(copy_result)
        #
        # This will return the fields that need generalization as specified.
        #
        return out

    def suppress(self, **args):
        """
        We should be able to suppress the columns and/or rows provided specification
        """

        rules = args.get('rules', {})
        label = args.get('label')
        fields = args.get('fields', [])
        store_id = args.get('store')
        apply_fn = self.store_syntax[store_id][
            'apply'] if 'apply' in self.store_syntax[store_id] else {}
        out = []

        tablename = args.get('tablename').split('.')[1]
        field_definitions = {}
        for field_def in fields_for(tablename):
            field_definitions[field_def.get('name')] = field_def

        if fields and 'on' not in args:
            #
            # This applies on a relational table's columns, it is simple we just nullify the fields
            #
            for name in fields:
                field_definition = field_definitions.get(name)
                field_type = field_definition.get('type').lower()
                field_mode = field_definition.get('mode').lower()

                if not rules:
                    #
                    # This scenario, we know the fields upfront and don't have a rule for them
                    # We just need them removed (simple/basic case)
                    #
                    #-- This will prevent accidental type changes
                    if field_type == 'string':
                        if field_mode == 'nullable':
                            value = "FORMAT('%i', NULL) AS " + name
                        else:
                            value = "'' AS " + name
                    elif field_type == 'integer':
                        if field_mode == 'nullable':
                            value = 'NULL AS ' + name
                        else:
                            value = "0 AS " + name

                    out.append({"name": name, "apply": value, "label": label})
                    LOGGER.info(
                        f"suppress fields(columns) for:\t{label.split('.')[1]}")
                else:
                    #
                    # If we have alist of fields to be removed, The following code will figure out which ones apply
                    # This will apply to all tables that are passed through this engine
                    #

                    for rule in rules:
                        if 'apply' not in rules:
                            if name in rule['values']:
                                #-- This will prevent accidental type changes
                                if field_type == 'string':
                                    if field_mode == 'nullable':
                                        value = "FORMAT('%i', NULL) AS " + name
                                    else:
                                        value = "'' AS " + name
                                elif field_type == 'integer':
                                    if field_mode == 'nullable':
                                        value = 'NULL AS ' + name
                                    else:
                                        value = "0 AS " + name
                                out.append({
                                    "name": name,
                                    "apply": (value),
                                    "label": label
                                })
            LOGGER.info(
                f"suppress fields(columns):\t{label.split('.')[1]}\t\tfor:\t{fields}"
            )

        else:
            #
            # In this case we are just removing the entire row we will be expecting :
            #   - filter    as the key field to match the filter
            #   - The values of the filter are provided by the rule
            apply_qualifier = {
                'IN': 'NOT IN',
                '=': '<>',
                'NOT IN': 'IN',
                '<>': '=',
                '': 'IS FALSE',
                'TRUE': 'IS FALSE'
            }

            if not rules:
                #
                # A row suppression rule has been provided in the form of an SQL filter
                # The qualifier needs to be flipped ...
                on = args['on']

                if isinstance(on, dict):
                    try:
                        # using string values
                        fillter_values = "'" + "','".join(
                            on.get('values')) + "'"
                    except TypeError:
                        # using non-string values, could be integers, floats, etc
                        int_strings = [str(value) for value in on.get('values')]
                        fillter_values = ','.join(int_strings)

                    fillter = ' '.join([
                        args.get('qualifier'), '(',
                        on.get('condition'), "(", fillter_values, "))"
                    ])
                    on = fillter

                # don't lower the actual 'on' argument.  it may have undesirable
                # side effects.
                on_lower = on.lower()

                # order is important between 'not <operator>' and '<operator>' comparisons.
                # 'not <operator>' must come first
                if 'not exists ' in on_lower:
                    fillter = on.replace('NOT EXISTS ', 'EXISTS ')
                    fillter = fillter.replace('not exists ', 'EXISTS ')
                elif 'exists ' in on_lower:
                    fillter = on.replace('EXISTS ', 'NOT EXISTS ')
                    fillter = fillter.replace('exists ', 'NOT EXISTS ')
                elif ' not in ' in on_lower:
                    fillter = on.replace(' NOT IN ', ' IN ')
                    fillter = fillter.replace(' not in ', ' IN ')
                elif ' in ' in on_lower:
                    fillter = on.replace(' IN ', ' NOT IN ')
                    fillter = fillter.replace(' in ', ' NOT IN ')
                elif '<>' in on_lower or '!=' in on_lower:
                    fillter = on.replace('<>', '=')
                    fillter = fillter.replace('!=', '=')
                elif '=' in on_lower:
                    fillter = on.replace('=', '<>')
                elif ' not like ' in on_lower:
                    fillter = on.replace(' NOT LIKE ', ' LIKE ')
                    fillter = fillter.replace(' not like ', ' LIKE ')
                elif ' like ' in on_lower:
                    fillter = on.replace(' LIKE ', ' NOT LIKE ')
                    fillter = fillter.replace(' like ', ' NOT LIKE ')

                fillter = {"filter": fillter, "label": "suppress.ROWS"}
                found = [
                    1 * (fillter == row)
                    for row in self.cache['suppress']['FILTERS']
                ]

                if np.sum(found) == 0:
                    self.cache['suppress']['FILTERS'] += [fillter]
                    self.parent.deid_rules['suppress']['FILTERS'] = self.cache[
                        'suppress']['FILTERS']
                return []

            for rule in rules:
                qualifier = args['qualifier'] if 'qualifier' in args else ''

                if 'apply' in rule and rule['apply'] in apply_fn:
                    template = self.store_syntax[store_id]['apply'][
                        rule['apply']]
                    key_field = args['filter'] if 'filter' in args else args[
                        'on']
                    expression = template.replace(':VAR',
                                                  "|".join(rule['values']))
                    expression = expression.replace(':FN', rule['apply'])
                    expression = expression.replace(':FIELD', key_field)
                    self.cache['suppress']['FILTERS'].append({
                        "filter": expression + ' ' + qualifier,
                        "label": label
                    })
                elif 'on' in args:
                    # If we have no application of a function, we will
                    # assume an expression of type <attribute> IN <list>
                    # we have a basic SQL statement here
                    qualifier = 'IN' if qualifier == '' else qualifier
                    qualifier = apply_qualifier[qualifier]
                    if 'values' in rule:
                        expression = " ".join([
                            args['on'], qualifier,
                            "('" + "','".join(rule['values']) + "')"
                        ])
                    else:
                        expression = args['on']

                    self.cache['suppress']['FILTERS'].append({
                        "filter": expression,
                        "label": label
                    })
        return out

    def shift(self, **args):
        """
        Shifting application.

        Shifting will always occur on a column, either the column is specified
        or is given a field with conditional values
           - simply secified are physical date/datetime columns (nothing special here)
           - But if we are dealing with a meta table, a condition must be provided. {key,value}
        """
        label = args['label']
        #
        # we can not shift dates on records where filters don't apply
        #
        if not self.cache['suppress']['FILTERS']:
            return []

        out = []

        if 'fields' in args:
            fields = args['fields']

            for name in fields:
                rules = args['rules']
                result = {
                    "apply": rules.replace(':FIELD', name),
                    "label": label,
                    "name": name
                }
                if 'on' in args:
                    result['on'] = args['on']
                    xchar = ' AS ' if ' AS ' in result['apply'] else ' as '
                    suffix = xchar + result['apply'].split(xchar)[-1]

                    result['apply'] = ' '.join([
                        'CAST(', result['apply'].replace(suffix, ''),
                        'AS STRING ) ', suffix
                    ])
                out.append(result)
        else:
            pass

        return out

    def compute(self, **args):
        """
        Simple applications of aggregate functions on a field (SQL Standard)

        Additionally we add a value from another table thus an embedded query
        with a table{key_field, key_value, table, field} as follows:

            field       projection of the embedded query
            key_field   field used in the join/filter
            key_value   external field that will hold the value of key_field
            table       external table
        """
        fields = args['fields'] if 'fields' in args else [args['key_field']]
        value_field = args['value_field']
        label = args['label']
        out = []

        rule_str = ' '.join(args.get('rules', []))
        statement = rule_str.replace(':FIELD',
                                     fields[0]).replace(':value_field',
                                                        value_field)

        if 'key_field' in args:
            statement = statement.replace(':key_field', args['key_field'])

        if 'table' in args:
            statement = statement.replace(':table', args['table'])

        out.append({"apply": statement, "name": fields[0], "label": label})
        return out

    def apply(self, info, store_id='sqlite', tablename=None):
        """
        :info    is a specification of a table and the rules associated
        """
        out = []
        r = {}
        ismeta = info['info'][
            'type'] if 'info' in info and 'type' in info['info'] else False

        for rule_id in self.pipeline:
            if rule_id in info:
                r = self.validate(rule_id, info[rule_id], tablename)

                if r:
                    r = dict(r, **{'ismeta': ismeta})
                    pointer = r['pointer']
                    tmp = [
                        pointer(**dict(args, **{"store": store_id}))
                        for args in r['args']
                    ]
                    if tmp:
                        for _item in tmp:
                            if _item:
                                if isinstance(_item, dict):
                                    out.append(_item)
                                else:
                                    out += _item
            else:
                LOGGER.info('rule_id:  {} not in info'.format(rule_id))

        return out
