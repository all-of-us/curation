import settings
import glob
import os
import re
import csv
from csvkit import table
from csvkit import DictReader
from csv_info import CsvInfo

RESULT_SUCCESS = 'success'
SPRINT_RE = re.compile('(\w+)_(person|visit_occurrence|condition_occurrence|procedure_occurrence|drug_exposure|measurement)_datasprint_(\d+)\.csv')
FILENAME_FORMAT = '<hpo_id>_<table>_DataSprint_<sprint_number>.csv'
MSG_CANNOT_PARSE_FILENAME = 'Cannot parse filename'
MSG_INVALID_SPRINT_NUM = 'Invalid sprint num'
MSG_INVALID_TABLE_NAME = 'Invalid table name'
MSG_INVALID_HPO_ID = 'Invalid HPO ID'
MSG_INVALID_TYPE = 'Type mismatch'

HEADER_KEYS = ['filename', 'hpo_id', 'sprint_num', 'table_name']
ERROR_KEYS = ['message', 'column_name', 'actual', 'expected']


def get_cdm_table_columns():
    with open(settings.cdm_metadata_path) as f:
        return table.Table.from_csv(f)


def get_hpo_info():
    with open(settings.hpo_csv_path) as f:
        return list(DictReader(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL))


def parse_filename(filename):
    m = SPRINT_RE.match(filename.lower())
    if m and len(m.groups()) == 3:
        return dict(sprint_num=int(m.group(3)), hpo_id=m.group(1), table_name=m.group(2))
    return None


def type_eq(cdm_column_type, submission_column_type):
    """
    Compare column type in spec with column type in submission
    :param cdm_column_type:
    :param submission_column_type:
    :return:
    """
    if submission_column_type == 'time':
        return cdm_column_type == 'character varying'
    if cdm_column_type == 'integer':
        return submission_column_type == 'int'
    if cdm_column_type in ('character varying', 'text'):
        return submission_column_type in ('str', 'unicode')
    if cdm_column_type == 'date':
        return submission_column_type in ('str', 'unicode', 'date')
    if cdm_column_type == 'numeric':
        return submission_column_type == 'float'
    raise Exception('Unsupported CDM column type ' + cdm_column_type)


def evaluate_submission(file_path):
    """
    Evaluates submission structure and content
    :param file_path: path to csv file
    :return:
    """
    result = {'passed': False, 'errors': []}

    file_path_parts = file_path.split(os.sep)
    filename = file_path_parts[-1]
    result['filename'] = filename
    parts = parse_filename(filename)

    if parts is None:
        result['errors'].append(dict(message=MSG_CANNOT_PARSE_FILENAME, actual=filename, expected=FILENAME_FORMAT))
        return result

    in_sprint_num, in_hpo_id, in_table_name = parts['sprint_num'], parts['hpo_id'], parts['table_name']

    result['sprint_num'] = parts['sprint_num']
    result['hpo_id'] = parts['hpo_id']
    result['table_name'] = parts['table_name']

    hpos = get_hpo_info()
    hpo_ids = set(map(lambda h: h['hpo_id'].lower(), hpos))

    if in_hpo_id not in hpo_ids:
        result['errors'].append(dict(message=MSG_INVALID_HPO_ID, actual=in_hpo_id, expected=';'.join(hpo_ids)))
        return result

    cdm_table_columns = get_cdm_table_columns()
    all_meta_items = cdm_table_columns.to_rows()

    if in_sprint_num != settings.sprint_num:
        result['errors'].append(dict(message=MSG_INVALID_SPRINT_NUM, actual=in_sprint_num, expected=settings.sprint_num))
        return result

    # CSV parser is flexible/lenient, but we can only support proper comma-delimited files
    with open(file_path) as input_file:
        sprint_info = CsvInfo(input_file, in_sprint_num, in_hpo_id, in_table_name)

        # get table metadata
        meta_items = filter(lambda r: r[0] == in_table_name, all_meta_items)

        # Check each column exists with correct type and required
        for meta_item in meta_items:
            meta_column_name = meta_item[1]
            meta_column_required = not meta_item[2]
            meta_column_type = meta_item[3]
            submission_has_column = False

            for submission_column in sprint_info.columns:
                submission_column_name = submission_column['name'].lower()
                if submission_column_name == meta_column_name:
                    submission_has_column = True
                    submission_column_type = submission_column['type'].lower()

                    # If all empty don't do type check
                    if submission_column_type != 'nonetype':
                        if not type_eq(meta_column_type, submission_column_type):
                            e = dict(message=MSG_INVALID_TYPE,
                                     column_name=submission_column_name,
                                     actual=submission_column_type,
                                     expected=meta_column_type)
                            result['errors'].append(e)

                    # Invalid if any nulls present in a required field
                    if meta_column_required and submission_column['stats']['nulls']:
                        result['errors'].append(dict(message='NULL values are not allowed for column',
                                                     column_name=submission_column_name))

            if not submission_has_column and meta_column_required:
                result['errors'].append(dict(message='Missing required column', column_name=meta_column_name))
    return result


def process_dir(d):
    out_dir = os.path.join(d, 'errors')
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    for f in glob.glob(os.path.join(d, '*.csv')):
        file_path_parts = f.split(os.sep)
        filename = file_path_parts[-1]
        output_filename = os.path.join(out_dir, filename)

        result = evaluate_submission(f)
        rows = []
        for error in result['errors']:
            row = dict()
            for header_key in HEADER_KEYS:
                row[header_key] = result.get(header_key)
            for error_key in ERROR_KEYS:
                row[error_key] = error.get(error_key)
            rows.append(row)

        with open(output_filename, 'w') as out:
            field_names = HEADER_KEYS + ERROR_KEYS
            writer = csv.DictWriter(out, fieldnames=field_names, lineterminator='\n', quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(rows)


if __name__ == '__main__':
    process_dir(settings.csv_dir)
