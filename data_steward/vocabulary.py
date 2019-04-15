import os
import csv
import re
import sys

csv.field_size_limit(sys.maxsize)

CONCEPT = 'concept'
CONCEPT_ANCESTOR = 'concept_ancestor'
CONCEPT_CLASS = 'concept_class'
CONCEPT_RELATIONSHIP = 'concept_relationship'
CONCEPT_SYNONYM = 'concept_synonym'
DOMAIN = 'domain'
DRUG_STRENGTH = 'drug_strength'
RELATIONSHIP = 'relationship'
VOCABULARY = 'vocabulary'
VOCABULARY_TABLES = [CONCEPT, CONCEPT_ANCESTOR, CONCEPT_CLASS, CONCEPT_RELATIONSHIP, CONCEPT_SYNONYM, DOMAIN,
                     DRUG_STRENGTH, RELATIONSHIP, VOCABULARY]
DELIMITER = '\t'
LINE_TERMINATOR = '\n'
RAW_DATE_REGEX = r'\d{8}$'  # yyyymmdd
BQ_DATE_REGEX = r'\d{4}-\d{2}-\d{2}$'  # yyyy-mm-dd

TRANSFORM_CSV = 'transform_csv'
ERRORS = 'errors'


def format_date_str(s):
    """
    Format a date string to yyyymmdd if it is not already
    :param s: the date string
    :return: the formatted date string
    """
    if re.match(BQ_DATE_REGEX, s):
        return s
    elif re.match(RAW_DATE_REGEX, s):
        parts = s[0:4], s[4:6], s[6:8]
        return '-'.join(parts)
    else:
        raise ValueError('Cannot parse value {v} as date'.format(v=s))


def _transform_csv(in_fp, out_fp, err_fp=None):
    if not err_fp:
        err_fp = sys.stderr
    csv_reader = csv.reader(in_fp, delimiter=DELIMITER)
    header = next(csv_reader)
    date_indexes = []
    for i in range(0, len(header)):
        if header[i].endswith('_date'):
            date_indexes.append(i)
    csv_writer = csv.writer(out_fp, delimiter=DELIMITER, lineterminator=LINE_TERMINATOR)
    csv_writer.writerow(header)
    for row_index, row in enumerate(csv_reader):
        try:
            for i in date_indexes:
                row[i] = format_date_str(row[i])
            csv_writer.writerow(row)
        except Exception, e:
            message = 'Error %s transforming row:\n%s' % (e.message, row)
            err_fp.write(message)


def transform_csv(file_path, out_dir):
    """
    Transform a local csv file (for BQ load) and save result in specified directory

    :param file_path: Path to the csv file
    :param out_dir: Directory to save the transformed file
    """
    file_name = os.path.basename(file_path)
    table_name, _ = os.path.splitext(file_name.lower())
    out_file_name = os.path.join(out_dir, file_name)
    err_dir = os.path.join(out_dir, ERRORS)
    err_file_name = os.path.join(err_dir, file_name)
    if not os.path.exists(err_dir):
        os.makedirs(err_dir)
    with open(file_path, 'rb') as in_fp, open(out_file_name, 'wb') as out_fp, open(err_file_name, 'wb') as err_fp:
        _transform_csv(in_fp, out_fp, err_fp)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('command', choices=[TRANSFORM_CSV])
    parser.add_argument('--file', required=True)
    parser.add_argument('--out_dir', required=True)
    args = parser.parse_args()
    if args.command == TRANSFORM_CSV:
        transform_csv(args.file, args.out_dir)
