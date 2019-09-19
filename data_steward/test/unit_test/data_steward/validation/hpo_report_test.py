from __future__ import print_function
import unittest
import os
from validation import hpo_report
import constants.validation.hpo_report as consts
from io import open


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
            {'analysis_id': 606,
             'rule_id': 2,
             'heel_error': 'ERROR: 606 - Distribution of age by procedure_concept_id (count = 38); min value should not be negative',
             'record_count': 38},
            {'analysis_id': 406,
             'rule_id': 2,
             'heel_error': 'ERROR: 406 - Distribution of age by condition_concept_id (count = 36); min value should not be negative',
             'record_count': 36},
            {'analysis_id': 706,
             'rule_id': 2,
             'heel_error': 'ERROR: 706 - Distribution of age by drug_concept_id (count = 32); min value should not be negative',
             'record_count': 32},
            {'analysis_id': 713,
             'rule_id': 1,
             'heel_error': 'ERROR: 713-Number of drug exposure records with invalid visit_id; count (n=32) should not be > 0',
             'record_count': 32},
            {'analysis_id': 209,
             'rule_id': 1,
             'heel_error': 'ERROR: 209-Number of visit records with end date < start date; count (n=28) should not be > 0',
             'record_count': 28},
            {'analysis_id': 613,
             'rule_id': 1,
             'heel_error': 'ERROR: 613-Number of procedure occurrence records with invalid visit_id; count (n=27) should not be > 0',
             'record_count': 27},
            {'analysis_id': 705,
             'rule_id': 5,
             'heel_error': 'ERROR: 705-Number of drug exposure records, by drug_concept_id by drug_type_concept_id; 27 concepts in data are not in vocabulary',
             'record_count': 27},
            {'analysis_id': 411,
             'rule_id': 1,
             'heel_error': 'ERROR: 411-Number of condition occurrence records with end date < start date; count (n=24) should not be > 0',
             'record_count': 24},
            {'analysis_id': 413,
             'rule_id': 1,
             'heel_error': 'ERROR: 413-Number of condition occurrence records with invalid visit_id; count (n=22) should not be > 0',
             'record_count': 22},
            {'analysis_id': 711,
             'rule_id': 1,
             'heel_error': 'ERROR: 711-Number of drug exposure records with end date < start date; count (n=18) should not be > 0',
             'record_count': 18},
            {'analysis_id': 206,
             'rule_id': 2,
             'heel_error': 'ERROR: 206 - Distribution of age by visit_concept_id (count = 13); min value should not be negative',
             'record_count': 13},
            {'analysis_id': 600,
             'rule_id': 14,
             'heel_error': 'ERROR: 600-Number of persons with at least one procedure occurrence, by procedure_concept_id; 8 concepts in data are not in correct vocabulary',
             'record_count': 8},
            {'analysis_id': 202,
             'rule_id': 10,
             'heel_error': 'ERROR: 202-Number of persons by visit occurrence start month, by visit_concept_id; 7 concepts in data are not in correct vocabulary',
             'record_count': 7},
            {'analysis_id': 400,
             'rule_id': 12,
             'heel_error': 'ERROR: 400-Number of persons with at least one condition occurrence, by condition_concept_id; 7 concepts in data are not in correct vocabulary',
             'record_count': 7},
            {'analysis_id': 4,
             'rule_id': 8,
             'heel_error': 'ERROR: 4-Number of persons by race; 5 concepts in data are not in correct vocabulary',
             'record_count': 5},
            {'analysis_id': 5,
             'rule_id': 9,
             'heel_error': 'ERROR: 5-Number of persons by ethnicity; 5 concepts in data are not in correct vocabulary (CMS Ethnicity)',
             'record_count': 5},
            {'analysis_id': 2,
             'rule_id': 7,
             'heel_error': 'ERROR: 2-Number of persons by gender; 2 concepts in data are not in correct vocabulary',
             'record_count': 2}]
        self.results = [
            {'file_name': 'care_site.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'condition_occurrence.csv',
             'found': 1,
             'parsed': 1,
             'loaded': 1},
            {'file_name': 'death.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'device_exposure.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'drug_exposure.csv',
             'found': 1,
             'parsed': 1,
             'loaded': 1},
            {'file_name': 'fact_relationship.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'location.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'measurement.csv',
             'found': 1,
             'parsed': 1,
             'loaded': 1},
            {'file_name': 'note.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'observation.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'person.csv',
             'found': 1,
             'parsed': 1,
             'loaded': 1},
            {'file_name': 'procedure_occurrence.csv',
             'found': 1,
             'parsed': 1,
             'loaded': 1},
            {'file_name': 'provider.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'specimen.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'visit_occurrence.csv',
             'found': 1,
             'parsed': 1,
             'loaded': 1},
            {'file_name': 'participant_match.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'pii_address.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'pii_email.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'pii_mrn.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'pii_name.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0},
            {'file_name': 'pii_phone_number.csv',
             'found': 0,
             'parsed': 0,
             'loaded': 0}]
        self.completeness = [
            {
                'column_name': 'condition_concept_id',
                'concept_zero_count': 369213,
                'null_count': 0,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 0.8246766937430018,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'condition_end_date',
                'concept_zero_count': 0,
                'null_count': 2105898,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 0.0,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'condition_start_date',
                'concept_zero_count': 0,
                'null_count': 0,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 1.0,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'condition_source_value',
                'concept_zero_count': 0,
                'null_count': 0,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 1.0,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'condition_occurrence_id',
                'concept_zero_count': 0,
                'null_count': 0,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 1.0,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'provider_id',
                'concept_zero_count': 0,
                'null_count': 2105898,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 0.0,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'condition_end_datetime',
                'concept_zero_count': 0,
                'null_count': 2105898,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 0.0,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'condition_status_concept_id',
                'concept_zero_count': 0,
                'null_count': 939416,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 0.5539119178611689,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'person_id',
                'concept_zero_count': 0,
                'null_count': 0,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 1.0,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'condition_source_concept_id',
                'concept_zero_count': 137161,
                'null_count': 231576,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 0.8249027255831004,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'condition_status_source_value',
                'concept_zero_count': 0,
                'null_count': 0,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 1.0,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'condition_type_concept_id',
                'concept_zero_count': 0,
                'null_count': 0,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 1.0,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'condition_start_datetime',
                'concept_zero_count': 0,
                'null_count': 0,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 1.0,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'visit_occurrence_id',
                'concept_zero_count': 0,
                'null_count': 668024,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 0.6827842564074803,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'stop_reason',
                'concept_zero_count': 0,
                'null_count': 0,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 1.0,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            }
        ]
        self.drug_class_metrics = [
            {'percentage': '12.77%',
             'concept_name': 'OTHER ANALGESICS AND ANTIPYRETICS',
             'count': 6,
             'drug_class': 'Pain NSAIDS',
             'concept_id': 21604303},
            {'percentage': '8.51%',
             'concept_name': 'OPIOIDS',
             'count': 4,
             'drug_class': 'Opioids',
             'concept_id': 21604254},
            {'percentage': '4.26%',
             'concept_name': 'HMG CoA reductase inhibitors',
             'count': 2,
             'drug_class': 'Statins',
             'concept_id': 21601855},
            {'percentage': '4.26%',
             'concept_name': 'ANTIBACTERIALS FOR SYSTEMIC USE',
             'count': 2,
             'drug_class': 'Antibiotics',
             'concept_id': 21602796},
            {'percentage': '4.26%',
             'concept_name': 'ANTIINFLAMMATORY AND ANTIRHEUMATIC PRODUCTS, NON-STEROIDS',
             'count': 2,
             'drug_class': 'MSK NSAIDS',
             'concept_id': 21603933},
            {'percentage': '2.13%',
             'concept_name': 'SELECTIVE CALCIUM CHANNEL BLOCKERS WITH MAINLY VASCULAR EFFECTS',
             'count': 1,
             'drug_class': 'CCB',
             'concept_id': 21601745},
            {'percentage': '2.13%',
             'concept_name': 'ACE INHIBITORS, PLAIN',
             'count': 1,
             'drug_class': 'ACE Inhibitor',
             'concept_id': 21601783}]
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
            self.assertIn(file_name, render_output)
        for (file_name, message) in report_data.get(consts.ERRORS_REPORT_KEY, []):
            self.assertIn(file_name, render_output)
            self.assertIn(message, render_output)
        for (file_name, message) in report_data.get(consts.WARNINGS_REPORT_KEY, []):
            self.assertIn(file_name, render_output)
            self.assertIn(message, render_output)
        for d in report_data.get(consts.NONUNIQUE_KEY_METRICS_REPORT_KEY, []):
            self.assertIn(d['table_name'], render_output)
        for d in report_data.get(consts.HEEL_ERRORS_REPORT_KEY, []):
            self.assertIn(d['heel_error'], render_output)
        for d in report_data.get(consts.DRUG_CLASS_METRICS_REPORT_KEY, []):
            self.assertIn(d['concept_name'], render_output)
            self.assertIn(d['drug_class'], render_output)

    def save_render_output(self, render_output):
        with open(self.render_output_path, 'w') as out_fp:
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
            consts.HEEL_ERRORS_REPORT_KEY: self.heel_errors,
            consts.COMPLETENESS_REPORT_KEY: self.completeness
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

        report_data[consts.ERRORS_REPORT_KEY] = [{'file_name': 'visit_occurrence.csv', 'message': 'Fake error'}]
        render_output = hpo_report.render(report_data)
        self.save_render_output(render_output)
        self.assert_report_data_in_output(report_data, render_output)

    def tearDown(self):
        if os.path.exists(self.render_output_path):
            os.remove(self.render_output_path)
