import json
import sys
import os

sql_commands = open("omop.sql").readlines()

table_name = None
table_columns = []
for line in sql_commands:
    words = line.split()
    if table_name:
        if words[0] == ");" or words[0] == ")" or words[0] ==";":
            with open("schemas/%s.json" % table_name, "w") as schema:
              json.dump(table_columns, schema, indent=4)
            print "Created %s.json" % table_name
            table_name = None
            table_columns = []
            continue
        if words[0] == "(": continue
        column_type = words[1].lower()
        if column_type[-1] == ',': column_type = column_type[:-1]
        if column_type.find('(') != -1: column_type = column_type.split('(')[0]
        if (column_type == "bigint"): t = "integer"
        elif (column_type == "integer"): t = "integer"
        elif (column_type == "timestamp"): t = "timestamp"
        elif (column_type == "character"): t = "string"
        elif (column_type == "varchar"): t = "string"
        elif (column_type == "text"): t = "string"
        elif (column_type == "character"): t = "string"
        elif (column_type == "boolean"): t = "string"  # todo - translate "t" and "f" to something else
        elif (column_type == "double"): t = "float"
        elif (column_type == "numeric"): t = "float"
        elif (column_type == "date"): t = "date"
        elif (column_type == "datetime"): t = "datetime"
        else: assert False, "Unknown type: %s" % column_type
        mode = "nullable"
        if words[2].lower() == "not":
          mode = "required"
        table_columns.append({ "name": words[0], "type": t, "mode": mode })
    elif len(words) >= 2 and words[0] == 'CREATE' and words[1] == 'TABLE':
        table_name = words[2]
