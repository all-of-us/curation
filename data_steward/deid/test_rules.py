from pymongo import MongoClient

from rules import Deid

table = [{"name":"id"},
         {"name":"dob"},
         {"name":"race"},
         {"name":"yob"},
         {"name":"ethnicity"},
         {"name":"gender"}]
fields = [field['name'] for field in table]
db = MongoClient()['deid']
r = list(db.rules.find())
cache = {}

for row in r:
    row_id = row['_id']
    del row['_id']
    cache[row_id] = row

drules = Deid()
drules.cache = cache

info = {
    "compute":[{"rules": "@compute.year", "fields": ["dob"]},
               {"rules": "@compute.id",
                "fields":["id"],
                "from": {"table": "seed", "field": "alt_id", "key_field": "id", "key_value": "sample.id"}}],
}
print drules.apply(info)
