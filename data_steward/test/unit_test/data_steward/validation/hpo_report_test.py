import unittest
import os
from data_steward.validation import hpo_report
import constants.validation.hpo_report as consts


class HpoReportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.render_output_path = 'test_render.html'
        self.error_occurred_text = 'incomplete'
        self.submission_error_text = 'Validation failed'

        self.hpo_name = 'Fake HPO'
        self.folder = '2019-06-01/'
        self.timestamp = '2019-06-01 05:00:00'
        self.heel_errors = [
            (38, u'ERROR: 606 - Distribution of age by procedure_concept_id (count = 38); min value should not be negative', 606, 2),
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
        self.results = [
           ('care_site.csv', 0, 0, 0),
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
        self.drug_class_metrics = [
            (u'12.77%', u'OTHER ANALGESICS AND ANTIPYRETICS', 6, u'Pain NSAIDS', 21604303),
            (u'8.51%', u'OPIOIDS', 4, u'Opioids', 21604254),
            (u'4.26%', u'HMG CoA reductase inhibitors', 2, u'Statins', 21601855),
            (u'4.26%', u'ANTIBACTERIALS FOR SYSTEMIC USE', 2, u'Antibiotics', 21602796),
            (u'4.26%', u'ANTIINFLAMMATORY AND ANTIRHEUMATIC PRODUCTS, NON-STEROIDS', 2, u'MSK NSAIDS', 21603933),
            (u'2.13%', u'SELECTIVE CALCIUM CHANNEL BLOCKERS WITH MAINLY VASCULAR EFFECTS', 1, u'CCB', 21601745),
            (u'2.13%', u'ACE INHIBITORS, PLAIN', 1, u'ACE Inhibitor', 21601783)]
        if os.path.exists(self.render_output_path):
            os.remove(self.render_output_path)

    def assert_report_data_in_output(self, report_data, render_output):
        """
        Helper to test if all values in specified report data appear in rendered output

        :param report_data: data being rendered
        :param render_output: result of rendering
        :return: True if all report data appear in the render output
        """
        if report_data.get(consts.ERROR_OCCURRED_REPORT_KEY):
            self.assertTrue(self.error_occurred_text in render_output)
        else:
            self.assertFalse(self.error_occurred_text in render_output)
        if report_data.get(consts.SUBMISSION_ERROR_REPORT_KEY):
            self.assertTrue(self.submission_error_text in render_output)
        else:
            self.assertFalse(self.submission_error_text in render_output)
        self.assertTrue(report_data[consts.HPO_NAME_REPORT_KEY] in render_output)
        self.assertTrue(report_data[consts.FOLDER_REPORT_KEY]in render_output)
        self.assertTrue(report_data[consts.TIMESTAMP_REPORT_KEY] in render_output)
        for (file_name, found, parsed, loaded) in report_data.get(consts.RESULTS_REPORT_KEY, []):
            self.assertTrue(file_name in render_output)
        for (file_name, message) in report_data.get(consts.ERRORS_REPORT_KEY, []):
            self.assertTrue(file_name in render_output)
            self.assertTrue(message in render_output)
        for (file_name, message) in report_data.get(consts.WARNINGS_REPORT_KEY, []):
            self.assertTrue(file_name in render_output)
            self.assertTrue(message in render_output)
        for (table_name, count) in report_data.get(consts.NONUNIQUE_KEY_METRICS_REPORT_KEY, []):
            self.assertTrue(table_name in render_output)
        for (record_count, heel_error, analysis_id, rule_id) in report_data.get(consts.HEEL_ERRORS_REPORT_KEY, []):
            self.assertTrue(heel_error in render_output)
        for (_, concept_name, _, drug_class, _) in report_data.get(consts.DRUG_CLASS_METRICS_REPORT_KEY, []):
            self.assertTrue(concept_name in render_output)
            self.assertTrue(drug_class in render_output)

    def save_render_output(self, render_output):
        with open(self.render_output_path, 'wb') as out_fp:
            out_fp.write(render_output)

    def test_render(self):
        report_data = {
            consts.HPO_NAME_REPORT_KEY: self.hpo_name,
            consts.TIMESTAMP_REPORT_KEY: self.timestamp,
            consts.FOLDER_REPORT_KEY: self.folder,
            consts.RESULTS_REPORT_KEY: self.results,
            consts.ERRORS_REPORT_KEY: [],
            consts.WARNINGS_REPORT_KEY: [],
            consts.NONUNIQUE_KEY_METRICS_REPORT_KEY: [],
            consts.DRUG_CLASS_METRICS_REPORT_KEY: self.drug_class_metrics,
            consts.HEEL_ERRORS_REPORT_KEY: self.heel_errors
        }
        # report ok
        render_output = hpo_report.render(report_data)
        self.save_render_output(render_output)
        self.assert_report_data_in_output(report_data, render_output)

        # submission error
        report_data[consts.SUBMISSION_ERROR_REPORT_KEY] = 'Required files are missing'
        render_output = hpo_report.render(report_data)
        self.save_render_output(render_output)
        self.assert_report_data_in_output(report_data, render_output)

        # error occurred
        report_data[consts.ERROR_OCCURRED_REPORT_KEY] = True
        render_output = hpo_report.render(report_data)
        self.save_render_output(render_output)
        self.assert_report_data_in_output(report_data, render_output)

        report_data[consts.ERRORS_REPORT_KEY] = [('visit_occurrence.csv', 'Fake error')]
        render_output = hpo_report.render(report_data)
        self.save_render_output(render_output)


    def tearDown(self):
        if os.path.exists(self.render_output_path):
            os.remove(self.render_output_path)
