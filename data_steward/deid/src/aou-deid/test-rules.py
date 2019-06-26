from rules import deid
from pymongo import MongoClient
import json
table = [{"name":"id"},{"name":"dob"},{"name":"race"},{"name":"yob"},{"name":"ethnicity"},{"name":"gender"}]
fields = [field['name'] for field in table]
db = MongoClient()['deid']
r = list(db.rules.find())
cache = {}
for row in r :
    id = row['_id']
    del row['_id']
    cache[id] = row
    
drules = deid()
drules.cache = cache 

info = {
    # "generalize" :[{"rules":"@generalize.race","table":"sample as lookup","fields":["race"], "key_field":"lookup.id","value_field":"sample.id"}],
    # "suppress":[{"rules":"@suppress.demographics-columns","fields":fields+['zip']},{"rules":"@suppress.demographics-rows","filter":"key_field","apply":"REGEXP"}]
    "compute":[{"rules":"@compute.year","fields":["dob"]},{"rules":"@compute.id","fields":["id"],"from":{"table":"seed","field":"alt_id","key_field":"id","key_value":"sample.id"}}],
}
print drules.apply(info)
# drules = deid()
# drules.set('generalize','race',values=['native','middle-eastern','pacific','islander'],into='Other',apply='REGEXP')
# drules.set('generalize','race',values=['histpanic','latino'],into='Non Hispanic-Latino',apply='REGEXP',qualifier='IS FALSE')
# drules.set('generalize','race',apply="COUNT",into="Multi-Racial",qualifier="> 1")
# drules.set('suppress','PII',values=['city','zip','workaddress','organtransplant','outsidetravel','personalmedicalhistory','circulatiory','generalconsent','piifeedback','diagnosishistory','othercancer','postpmbfeedback','_howoldwereyou','_areyoupatientallofus','piiname','socialsecurity','address','phone','email','signature','sandiegobloodbank','arizonaspecific'],apply='REGEXP')
# drules.set('suppress','ICD-9',values=["^E8[0-4].*","^E91[0,3].*","^E9([9,7,6,5]|28.0).*","^79[8,9].*","^V3.*","^(76[4-9]|77[0-9])\\\\.([0-9]){0,2}.*","^P[0-9]{2}\\\\.[0-9]{1}.*","^Z38.*","^R99.*","^Y3[5,6,7,8].*","^x52.*","^(W6[5-9]|W7[0-4][0-9]).*","^(X92[0-9]|Y0[1-9]).*","^V[0-9]{2}.*"],apply='REGEXP')
# drules.set('suppress','demographics-columns',values=["year_of_birth","month_of_birth","day_of_birth","location_id","provider_id","care_site_id","person_source_value","race_source_concept_id","race_source_value","gender_source_concept_id","gender_source_value","ethnicity_source_concept_id","cause_concept_id","cause_source_value","cause_source_concept_id","cause_concept_id","cause_source_value","cause_source_concept_id","provider_id","visit_occurrence_id","address_1","address_2","city","zip","county","location_source_value","care_site_name","care_site_source_value","care_site_source_value"
# ])
# drules.set('suppress','demographics-rows',values=['sexualorientation','spokenwritten','sexatbirth','_employmentstatus','_highestgrade','_gender'],apply="REGEXP")
# drules.set('shift','date',values=['_Date'],apply='REGEXP',**{"from":{"table":"people_seed","shift_field":"shift","key_field":"id","value_field":":FIELD"}})



# # info = {
# #     # "suppress":[{"rules":"@suppress.race","fields"}]
# # # "generalize":[{"rules":"@generalize.race","into":"race","filter":"key_field"}],
# # # "suppress":[{"rules":"@suppress.noise","fields":["key_field"]},{"fields":['zip','gender']}],
# # # "compute":[{"rules":"@compute.year","fields":["dob"]},{"rules":"@compute.id","fields":["id"],"from":{"table":"seed","field":"alt_id","key_field":"id","key_value":"sample.id"}}],
# # # "shift":[{"fields":['dob'],'into':'date', "from":{"table":"seed","shift_field":"shift","key_field":"seed.id","value_field":"sample.id"},{"rules":"@shift.expiration" ,"into":"date","fields":['value'],"filter":"observation_source_value"}]
# # }
# client = MongoClient()
# db = client['deid']

# if 'rules' not in db.collection_names() :
#     db.create_collection('rules')
# for id in drules.cache :
#     row  = drules.cache[id]
#     row['_id'] = id
#     db['rules'].insert(row)
