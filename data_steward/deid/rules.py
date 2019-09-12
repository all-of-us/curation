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
import json
import logging

import numpy as np

from parser import Parse

class Rules(object):
    def __init__(self, **args):
        self.cache = {}
        self.store_syntax = {

            "sqlite": {
                "apply": {"REGEXP": "LOWER(:FIELD) REGEXP LOWER(':VAR')",
                          "COUNT": "SELECT COUNT(:FIELD) FROM :TABLE WHERE :KEY=:VALUE"},
                "cond_syntax": {"IF": "CASE WHEN", "OPEN": "", "THEN": "THEN", "ELSE": "ELSE", "CLOSE": "END"},
                "random": "random() % 365 "
            },
            "bigquery": {
                "apply": {"REGEXP": "REGEXP_CONTAINS (LOWER(:FIELD), LOWER(':VAR'))",
                          "COUNT": "SELECT COUNT (:KEY) FROM :TABLE WHERE :KEY=:VALUE"},
                "cond_syntax": {"IF": "IF", "OPEN": "(", "THEN": ",", "ELSE": ",", "CLOSE": ")"},
                "random": "CAST( (RAND() * 364) + 1 AS INT64)"
            },
            "postgresql": {
                "cond_syntax": {"IF": "CASE WHEN", "OPEN": "", "THEN": "THEN", "ELSE": "ELSE", "CLOSE": "END"},
                "shift": {"date": "FIELD INTERVAL 'SHIFT DAY' ", "datetime": "FIELD INTERVAL 'SHIFT DAY'"},
                "random": "(random() * 364) + 1 :: int"
            }
        }
        self.pipeline = args['pipeline']
        self.cache = args['rules']
        self.parent = args['parent']
        #--

    def set(self, key, rule_id, **args):
        if key not in self.pipeline: #['generalize','suppress','compute','shift'] :
            raise (key + " is Unknown, [suppress, generalize, compute, shift] are allowed")
        if key not in self.cache:
            self.cache[key] = {}
        if id not in self.cache[key]:
            self.cache[key][rule_id] = []

        self.cache[key][rule_id].append(args)

    def get(self, key, rule_id):
        return self.cache[key][rule_id]

    def validate(self, rule_id, entry):
        """
        Validating if a the application of a rule relative to a table is valid
        """
        p = rule_id in self.cache

        q = []

        for row in entry:

            if 'rules' in row:
                if not isinstance(row['rules'], list) and row['rules'].startswith('@')  and "into" in row:
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

    def validate(self, rule_id, entry):
        payload = None

        if Rules.validate(self, rule_id, entry):

            payload = {}
            payload = {"args": []}
            payload["pointer"] = getattr(self, rule_id)
            for row in entry:

                #
                # @TODO: Ensure that an error is thrown if the rule associated is not found

                p = getattr(Parse, rule_id)(row, self.cache)
                payload['args'] += [p]


        return payload

    def aggregate(self, sql, **args):
        pass

    def log(self, **args):
        logging.info(json.dumps(args))

    def generalize(self, **args):
        """
        Apply generalizations given a set of rules.

        The rules apply to both meta tables and relational tables

        :fields list of target fields
        :rules  list of rules to be applied
        :label  context of what is being generalized
        """
        fields = args['fields'] if 'fields' in args else [args['value_field']]
        label = args['label']
        rules = args['rules']


        store_id = args['store'] if 'store' in args else 'sqlite'
        syntax = self.store_syntax[store_id]['cond_syntax']
        out = []
        label = args['label']
        for name in fields:
            cond = []
            for rule in rules:
                qualifier = rule['qualifier'] if 'qualifier' in rule else ''

                if 'apply' in rule:
                    #
                    # This will call a built-in SQL function (non-aggregate)'
                    # qualifier = rule['qualifier'] if 'qualifier' in rule else ''
                    fillter = args['filter'] if 'filter' in args else name
                    self.log(module='generalize', label=label.split('.')[1], on=name, type=rule['apply'])

                    if 'apply' not in self.store_syntax[store_id]:
                        regex = [rule['apply'], "(", fillter, " , '", "|".join(rule['values']), "') ", qualifier]
                    else:
                        template = self.store_syntax[store_id]['apply'][rule['apply']]
                        regex = template.replace(':FIELD', fillter).replace(':FN', rule['apply'])

                        if ':VAR' in template:
                            regex = regex.replace(":VAR", "|".join(rule['values']))

                        if rule['apply'] in ['COUNT', 'AVG', 'SUM']:
                            #
                            # Dealing with an aggregate expression. It is important to know what we are counting
                            # count(:field) from :table [where filter]
                            #
                            regex = regex.replace(':TABLE', args['table'])
                            regex = regex.replace(':KEY', args['key_field'])
                            regex = regex.replace(':VALUE', args['value_field'])

                            if 'on' in rule:
                                key_row = args['key_row'] if 'key_row' in args else name
                                key_row = key_row.replace(':name', name)
                                conjunction = ' AND ' if 'qualifier' in rule else ' WHERE '

                                if isinstance(rule['on'], list):
                                    try:
                                        val_list = " IN ('" + "','".join(rule['on']) + "')"
                                    except TypeError:
                                        val_list = [str(val_item) for val_item in rule['on']]
                                        val_list = " IN (" + ",".join(val_list) + ")"
                                    regex += conjunction + key_row + val_list
                                # the following conditions added to help with nullable columns
                                elif 'exists' in rule.get('on', ''):
                                    val_list = rule.get('on', '')
                                    regex += conjunction + val_list
                                else:
                                    val_list = "(" + rule.get('on', '') + ")"
                                    regex += conjunction + val_list

                            if 'on' in args:
                                regex += ' AND ' + args['on']

                            regex = ' '.join(['(', regex, ')', qualifier])
                        else:
                            regex = ' '.join([regex, qualifier])
                        #
                        # Is there a filter associated with the aggregate function or not
                        #
                    _into = rule['into'] if 'into' not in args else args['into']

                    if not isinstance(_into, (int, long)):
                        _into = "'" + _into + "'"
                    else:
                        _into = str(_into)

                    regex = "".join(regex)
                    cond += [" ".join([syntax['IF'], syntax['OPEN'], regex, syntax['THEN'], _into])]

                    if rules.index(rule) % 2 == 0 or rules.index(rule) % 3:
                        cond += [syntax['ELSE']]

                else:
                    #
                    # Process a generalization given a list of values with no overhead of an aggregate function
                    # @TODO: Document what is going on here
                    #   - An if or else type of generalization given a list of values or function
                    #   - IF <filter> IN <values>, THEN <generalized-value> else <attribute>
                    self.log(module='generalize', label=label.split('.')[1], on=name, type='inline')
                    key_field = args['key_field'] if 'key_field' in args else name

                    if 'on' in args and 'key_field' in args:
                        key_field += ' AND '+args['on']

                    fillter = args['filter'] if 'filter' in args else name
                    qualifier = rule['qualifier']
                    _into = rule['into'] if 'into' not in args else args['into']

                    if isinstance(_into, (int, long)):
                        _into = str(_into)
                        values = [str(value) for value in rule['values']]
                        values = '(' + ','.join(values) + ')'
                    else:
                        _into = "'" + _into + "'"
                        values = "('" + "','".join(rule['values']) + "')"

                    regex = " ".join([fillter, qualifier, values])
                    cond += [" ".join([syntax['IF'], syntax['OPEN'], regex, syntax['THEN'], _into])]

                    if rules.index(rule) % 2 == 0 or rules.index(rule) % 3:
                        cond += [syntax['ELSE']]

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
        #
        # This will return the fields that need generalization as specified.
        #
        return out

    def suppress(self, **args):
        """
        We should be able to suppress the columns and/or rows provided specification
        """

        rules = args['rules'] if 'rules' in args else {}
        label = args['label']
        fields = args['fields'] if 'fields' in args else []
        store_id = args['store']
        apply_fn = self.store_syntax[store_id]['apply'] if 'apply' in self.store_syntax[store_id] else {}
        out = []

        if fields and 'on' not in args:
            #
            # This applies on a relational table's columns, it is simple we just nullify the fields
            #
            for name in fields:
                if not rules:
                    #
                    # This scenario, we know the fields upfront and don't have a rule for them
                    # We just need them removed (simple/basic case)
                    #
                    #-- This will prevent accidental type changes
                    if name.endswith('_value') or name.endswith('_string'):
                        value = "FORMAT('%i', NULL) AS " + name
                    else:
                        value = 'NULL AS ' + name

                    out.append({"name": name, "apply": value, "label": label})
                    self.log(module='suppression', label=label.split('.')[1], type='columns')
                else:
                    #
                    # If we have alist of fields to be removed, The following code will figure out which ones apply
                    # This will apply to all tables that are passed through this engine
                    #

                    for rule in rules:
                        if 'apply' not in rules:
                            if name in rule['values']:
                                #-- This will prevent accidental type changes
                                if name.endswith('_value') or name.endswith('_string'):
                                    value = "FORMAT('%i', NULL) AS " + name
                                else:
                                    value = 'NULL AS ' + name
                                out.append({"name": name, "apply": (value), "label": label})
            self.log(module='suppress', label=label.split('.')[1], on=fields, type='columns')

        else:
            #
            # In this case we are just removing the entire row we will be expecting :
            #   - filter    as the key field to match the filter
            #   - The values of the filter are provided by the rule
            #
            apply_qualifier = {'IN': 'NOT IN', '=': '<>', 'NOT IN': 'IN', '<>': '=', '': 'IS FALSE', 'TRUE': 'IS FALSE'}
            if not rules:
                #
                # A row suppression rule has been provided in the form of an SQL filter
                # For now we assume a simple scenario on="field qualifier ..."
                # The qualifier needs to be flipped ...
                #
                on = args['on']
                if 'not exists ' in on:
                    fillter = on.replace('not exists ', 'exists ')
                elif 'exists ' in on:
                    fillter = on.replace('exists ', 'not exists ')
                elif ' in ' in on or ' IN ' in args:
                    fillter = on.replace(' IN ', ' NOT IN ').replace(' in ', ' NOT IN ')
                elif ' not in ' in on or ' NOT IN ' in args:
                    fillter = on.replace(' NOT IN ', ' IN ').replace(' not in ', ' IN ')
                elif ' = ' in on:
                    fillter = on.replace(' = ', ' <> ')
                elif ' <> ' in on:
                    fillter = on.replace(' <> ', ' = ')
                elif ' not like ' in on:
                    # order is important between 'not like' and 'like' comparisons.
                    # 'not like' must come first
                    fillter = on.replace(' not like ', ' like ')
                elif ' like ' in on:
                    fillter = on.replace(' like ', ' not like ')

                fillter = {"filter": fillter, "label": "suppress.ROWS"}
                found = [1 * (fillter == row) for row in self.cache['suppress']['FILTERS']]

                if np.sum(found) == 0:
                    self.cache['suppress']['FILTERS'] += [fillter]
                    self.parent.deid_rules['suppress']['FILTERS'] = self.cache['suppress']['FILTERS']
                return []

            for rule in rules:
                qualifier = args['qualifier'] if 'qualifier' in args else ''

                if 'apply' in rule and rule['apply'] in apply_fn:
                    template = self.store_syntax[store_id]['apply'][rule['apply']]
                    key_field = args['filter'] if 'filter' in args else args['on']
                    expression = template.replace(':VAR', "|".join(rule['values']))
                    expression = expression.replace(':FN', rule['apply'])
                    expression = expression.replace(':FIELD', key_field)
                    self.cache['suppress']['FILTERS'].append({"filter": expression +' '+ qualifier, "label": label})
                elif 'on' in args:
                    # If we have no application of a function, we will
                    # assume an expression of type <attribute> IN <list>
                    # we have a basic SQL statement here
                    qualifier = 'IN' if qualifier == '' else qualifier
                    qualifier = apply_qualifier[qualifier]
                    if 'values' in rule:
                        expression = " ".join([args['on'], qualifier, "('"+ "','".join(rule['values'])+"')"])
                    else:
                        expression = args['on']

                    self.cache['suppress']['FILTERS'].append({"filter": expression, "label": label})
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
                result = {"apply": rules.replace(':FIELD', name), "label": label, "name": name}
                if 'on' in args:
                    result['on'] = args['on']
                    xchar = ' AS ' if ' AS ' in result['apply'] else ' as '
                    suffix = xchar +result['apply'].split(xchar)[-1]

                    result['apply'] = ' '.join(['CAST(', result['apply'].replace(suffix, ''), 'AS STRING ) ', suffix])
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
        # r = {'label':args['label']}
        label = args['label']
        out = []

        statement = args['rules'].replace(':FIELD', fields[0]).replace(':value_field', value_field)
        if 'key_field' in args:
            statement = statement.replace(':key_field', args['key_field'])
        if 'table' in args:
            statement = statement.replace(':table', args['table'])
        out .append({"apply": statement, "name": fields[0], "label": label})
        return out

    def apply(self, info, store_id='sqlite'):
        """
        :info    is a specification of a table and the rules associated
        """
        out = []
        r = {}
        ismeta = info['info']['type'] if 'info' in info and 'type' in info['info'] else False

        for rule_id in self.pipeline: #['generalize', 'compute', 'suppress', 'shift'] :

            if rule_id in info:
                r = self.validate(rule_id, info[rule_id])

                if r:
                    r = dict(r, **{'ismeta': ismeta})
                    pointer = r['pointer']
                    tmp = [pointer(** dict(args, **{"store": store_id})) for args in r['args']]
                    if tmp:
                        for _item in tmp:
                            if _item:
                                if isinstance(_item, dict):
                                    out.append(_item)
                                else:
                                    out += _item

        return out
