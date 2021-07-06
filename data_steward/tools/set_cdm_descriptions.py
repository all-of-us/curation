import csv
import json
from collections import OrderedDict
from io import open
from os import listdir
from urllib import request

CDM_CSV_URL = "https://raw.githubusercontent.com/OHDSI/CommonDataModel/master/OMOP_CDM_v5_3.csv"

response = request.urlopen(CDM_CSV_URL)
reader = csv.DictReader(response)
table_map = {}
for row in reader:
    table_name = row['table']
    del row['table']
    table = table_map.get(table_name)
    if not table:
        table = {}
        table_map[table_name] = table
    table[row['field']] = row['description']

for f in listdir('../resource_files/schemas'):
    table_name = f[0:f.index('.')]
    description_map = table_map.get(table_name)
    if not description_map:
        print("No descriptions found for %s; skipping." % table_name)
        continue
    print("Updating descriptions for %s..." % table_name)
    with open('../resource_files/schemas/%s' % f) as json_file:
        schema_json = json.load(json_file, object_pairs_hook=OrderedDict)
        for column in schema_json:
            description = description_map.get(column['name'])
            if not description:
                print("No description found for column %s, skipping." %
                      column['name'])
            else:
                column['description'] = description
    with open('../resource_files/schemas/%s' % f, 'w') as json_file:
        json.dump(schema_json, json_file, indent=4, ensure_ascii=False)
