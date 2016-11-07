import settings
import glob
import os
import re
from csvkit import table
from csvkit import DictReader
from csv_info import CsvInfo

RESULT_SUCCESS = 'success'
SPRINT_RE = re.compile('(\w+)_(person|visit_occurrence|condition_occurrence|procedure_occurrence)_datasprint_(\d+)\.csv')
PERSON_COLUMNS = []

MSG_INVALID_SPRINT_NUM = 'Invalid sprint num: {sprint_num}. Expected {settings.sprint_num}.'
MSG_INVALID_TABLE_NAME = 'Invalid table name: {table_name}.'
MSG_INVALID_HPO_ID = 'Invalid HPO ID: {hpo_id}.'
MSG_INVALID_TYPE = 'Column {submission_column_name} was type {submission_column_type}. Expected {meta_column_type}.'
MSG_CANNOT_PARSE_FILENAME = 'Cannot parse filename {filename}'


def get_cdm_table_columns():
    with open(settings.cdm_metadata_path) as f:
        return table.Table.from_csv(f)


def get_hpo_info():
    with open(settings.hpo_csv_path) as f:
        return list(DictReader(f, delimiter='\t'))


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
    lhs = cdm_column_type.lower()
    if submission_column_type == 'time':
        return lhs == 'character varying'
    if lhs == 'integer':
        return submission_column_type == 'int'
    if lhs in ('character varying', 'text'):
        return submission_column_type in ('str', 'unicode')
    if lhs == 'date':
        return submission_column_type in ('str', 'unicode', 'date')
    if lhs == 'numeric':
        return submission_column_type == 'float'
    raise Exception('Unsupported CDM column type ' + cdm_column_type)


def evaluate_submission(file_path):
    """
    Evaluates submission structure and content
    :param file_path: path to csv file
    :return:
    """
    result = {'passed': False, 'messages': []}

    file_path_parts = file_path.split(os.sep)
    filename = file_path_parts[-1]
    parts = parse_filename(filename)

    if parts is None:
        result['messages'].append(MSG_CANNOT_PARSE_FILENAME.format(filename=filename))
        return result

    in_sprint_num, in_hpo_id, in_table_name = parts['sprint_num'], parts['hpo_id'], parts['table_name']

    cdm_table_columns = get_cdm_table_columns()
    table_name_column = cdm_table_columns[0]
    table_names = set(map(lambda s: s.lower(), table_name_column))
    all_meta_items = cdm_table_columns.to_rows()

    if in_table_name not in table_names:
        result['messages'].append(MSG_INVALID_TABLE_NAME.format(table_name=in_table_name))
        return result

    hpos = get_hpo_info()
    hpo_ids = set(map(lambda h: h['hpo_id'].lower(), hpos))

    if in_hpo_id not in hpo_ids:
        result['messages'].append(MSG_INVALID_HPO_ID.format(hpo_id=in_hpo_id))
        return result

    if in_sprint_num != settings.sprint_num:
        result['messages'].append(MSG_INVALID_SPRINT_NUM.format(sprint_num=in_sprint_num, settings=settings))
        return result

    # CSV parser is flexible/lenient, but we can only support proper comma-delimited files
    with open(file_path) as input_file:
        sprint_info = CsvInfo(input_file, in_sprint_num, in_hpo_id, in_table_name)

        # get table metadata
        meta_items = filter(lambda r: r[0].lower() == in_table_name, all_meta_items)

        # Check each column exists with correct type and required
        for meta_item in meta_items:
            meta_column_name = meta_item[1].lower()
            meta_column_required = not meta_item[2]
            meta_column_type = meta_item[3].lower()
            submission_has_column = False

            for submission_column in sprint_info.columns:
                submission_column_name = submission_column['name'].lower()
                if submission_column_name == meta_column_name:
                    submission_has_column = True
                    submission_column_type = submission_column['type'].lower()

                    # If all empty don't do type check
                    if submission_column_type != 'nonetype':
                        if not type_eq(meta_column_type, submission_column_type):
                            msg = MSG_INVALID_TYPE.format(submission_column_name=submission_column_name,
                                                          submission_column_type=submission_column_type,
                                                          meta_column_type=meta_column_type)
                            result['messages'].append(msg)

                    # Invalid if any nulls present in a required field
                    if meta_column_required and submission_column['stats']['nulls']:
                        result['messages'].append('Column ' + submission_column_name + ' does not allow NULL values')

            if not submission_has_column:
                result['messages'].append('Missing column ' + meta_column_name)

    # If it gets this far, success
    passed = len(result['messages']) == 0
    if passed:
        result['passed'] = True
        result['messages'].append('Passed sprint ' + str(in_sprint_num) + ' validation')
    return result


def process_dir(d):
    for f in glob.glob(os.path.join(d, '*.csv')):
        result = evaluate_submission(f)
        if result['passed']:
            output_filename = f + '.pass'
        else:
            output_filename = f + '.fail'
        with open(output_filename, 'w') as out:
            for message in result['messages']:
                out.write(message + '\n')


if __name__ == '__main__':
    process_dir(settings.csv_dir)
