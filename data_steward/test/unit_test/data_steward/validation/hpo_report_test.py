import unittest
from data_steward.validation import hpo_report

hpo_name = 'HPO'
folder = '2019-06-01/'
timestamp = 'Report timestamp: 2019-06-01 05:00:00'
duplicate_counts = []
drug_checks = [(u'12.77%', u'OTHER ANALGESICS AND ANTIPYRETICS', 6, u'Pain NSAIDS', 21604303),
               (u'8.51%', u'OPIOIDS', 4, u'Opioids', 21604254),
               (u'4.26%', u'HMG CoA reductase inhibitors', 2, u'Statins', 21601855),
               (u'4.26%', u'ANTIBACTERIALS FOR SYSTEMIC USE', 2, u'Antibiotics', 21602796),
               (u'4.26%', u'ANTIINFLAMMATORY AND ANTIRHEUMATIC PRODUCTS, NON-STEROIDS', 2, u'MSK NSAIDS', 21603933),
               (u'2.13%', u'SELECTIVE CALCIUM CHANNEL BLOCKERS WITH MAINLY VASCULAR EFFECTS', 1, u'CCB', 21601745),
               (u'2.13%', u'ACE INHIBITORS, PLAIN', 1, u'ACE Inhibitor', 21601783)]
results = [('care_site.csv', 0, 0, 0),
           ('condition_occurrence.csv', 1, 1, 1),
           ('death.csv', 0, 0, 0),
           ('device_exposure.csv', 0, 0, 0),
           ('drug_exposure.csv', 1, 1, 1),
           ('fact_relationship.csv', 0, 0, 0),
           ('location.csv', 0, 0, 0),
           ('measurement.csv', 1, 1, 1),
           ('note.csv', 0, 0, 0),
           ('observation.csv', 0, 0, 0),
           ('person.csv', 1, 1, 1),
           ('procedure_occurrence.csv', 1, 1, 1),
           ('provider.csv', 0, 0, 0),
           ('specimen.csv', 0, 0, 0),
           ('visit_occurrence.csv', 1, 1, 1),
           ('participant_match.csv', 0, 0, 0),
           ('pii_address.csv', 0, 0, 0),
           ('pii_email.csv', 0, 0, 0),
           ('pii_mrn.csv', 0, 0, 0),
           ('pii_name.csv', 0, 0, 0),
           ('pii_phone_number.csv', 0, 0, 0)]
errors = []
heel_errors = [(38, u'ERROR: 606 - Distribution of age by procedure_concept_id (count = 38); min value should not be negative', 606, 2),
               (36, u'ERROR: 406 - Distribution of age by condition_concept_id (count = 36); min value should not be negative', 406, 2),
               (32, u'ERROR: 706 - Distribution of age by drug_concept_id (count = 32); min value should not be negative', 706, 2),
               (32, u'ERROR: 713-Number of drug exposure records with invalid visit_id; count (n=32) should not be > 0', 713, 1),
               (28, u'ERROR: 209-Number of visit records with end date < start date; count (n=28) should not be > 0', 209, 1),
               (27, u'ERROR: 613-Number of procedure occurrence records with invalid visit_id; count (n=27) should not be > 0', 613, 1),
               (27, u'ERROR: 705-Number of drug exposure records, by drug_concept_id by drug_type_concept_id; 27 concepts in data are not in vocabulary', 705, 5),
               (24, u'ERROR: 411-Number of condition occurrence records with end date < start date; count (n=24) should not be > 0', 411, 1),
               (22, u'ERROR: 413-Number of condition occurrence records with invalid visit_id; count (n=22) should not be > 0', 413, 1),
               (18, u'ERROR: 711-Number of drug exposure records with end date < start date; count (n=18) should not be > 0', 711, 1),
               (13, u'ERROR: 206 - Distribution of age by visit_concept_id (count = 13); min value should not be negative', 206, 2),
               (8, u'ERROR: 600-Number of persons with at least one procedure occurrence, by procedure_concept_id; 8 concepts in data are not in correct vocabulary', 600, 14),
               (7, u'ERROR: 202-Number of persons by visit occurrence start month, by visit_concept_id; 7 concepts in data are not in correct vocabulary', 202, 10),
               (7, u'ERROR: 400-Number of persons with at least one condition occurrence, by condition_concept_id; 7 concepts in data are not in correct vocabulary', 400, 12),
               (5, u'ERROR: 4-Number of persons by race; 5 concepts in data are not in correct vocabulary', 4, 8),
               (5, u'ERROR: 5-Number of persons by ethnicity; 5 concepts in data are not in correct vocabulary (CMS Ethnicity)', 5, 9),
               (2, u'ERROR: 2-Number of persons by gender; 2 concepts in data are not in correct vocabulary', 2, 7)]


class HpoReportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')
        # validate_submission
        # { 'results': [('person.csv', found, parsed, loaded), ],
        #   'errors': [('visit_occurrence.csv', 'error message'), ]
        #   'warnings': [('procedure.csv', 'Unknown file'


    def test_render(self):
        r = hpo_report.render(hpo_name='Columbia',
                              folder='2019-06-01-v2',
                              results=[],
                              errors=[],
                              warnings=[],
                              heel_errors=[],
                              drug_checks=[])
        print r
