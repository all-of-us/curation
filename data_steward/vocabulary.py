import csv
import os
import re
import sys
import warnings

from resources import AOU_GENERAL_PATH, AOU_GENERAL_CONCEPT_CSV_PATH, hash_dir
from common import CONCEPT, VOCABULARY, DELIMITER, LINE_TERMINATOR, RAW_DATE_REGEX, BQ_DATE_REGEX, TRANSFORM_FILES, \
    APPEND_VOCABULARY, APPEND_CONCEPTS, ADD_AOU_GENERAL, ERRORS, AOU_GEN, AOU_GEN_VOCABULARY_CONCEPT_ID, \
    AOU_GEN_VOCABULARY_REFERENCE, ERROR_APPENDING

csv.field_size_limit(sys.maxsize)


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
    for index, item in enumerate(header):
        if item.endswith('_date'):
            date_indexes.append(index)
    csv_writer = csv.writer(out_fp, delimiter=DELIMITER, lineterminator=LINE_TERMINATOR)
    csv_writer.writerow(header)
    for row in csv_reader:
        try:
            for i in date_indexes:
                row[i] = format_date_str(row[i])
            csv_writer.writerow(row)
        except (ValueError, IndexError), e:
            message = 'Error %s transforming row:\n%s' % (e.message, row)
            err_fp.write(message)


def transform_file(file_path, out_dir):
    """
    Format file date fields and standardize line endings a local csv file and save result in specified directory

    :param file_path: Path to the csv file
    :param out_dir: Directory to save the transformed file
    """
    file_name = os.path.basename(file_path)
    out_file_name = os.path.join(out_dir, file_name)
    err_dir = os.path.join(out_dir, ERRORS)
    err_file_name = os.path.join(err_dir, file_name)
    if not os.path.exists(err_dir):
        os.makedirs(err_dir)
    with open(file_path, 'rb') as in_fp, open(out_file_name, 'wb') as out_fp, open(err_file_name, 'wb') as err_fp:
        _transform_csv(in_fp, out_fp, err_fp)


def transform_files(in_dir, out_dir):
    """
    Transform vocabulary files in a directory and save result in another directory

    :param in_dir: Directory containing vocabulary csv files
    :param out_dir: Directory to save the transformed file
    """
    fs = os.listdir(in_dir)
    for f in fs:
        in_path = os.path.join(in_dir, f)
        transform_file(in_path, out_dir)


def get_aou_general_version():
    return hash_dir(AOU_GENERAL_PATH)


def get_aou_general_vocabulary_row():
    aou_gen_version = get_aou_general_version()
    # vocabulary_id vocabulary_name vocabulary_reference vocabulary_version vocabulary_concept_id
    return DELIMITER.join([AOU_GEN, AOU_GEN, AOU_GEN_VOCABULARY_REFERENCE, aou_gen_version,
                           AOU_GEN_VOCABULARY_CONCEPT_ID])


def append_concepts(in_path, out_path):
    with open(out_path, 'wb') as out_fp:
        # copy original rows line by line for memory efficiency
        with open(in_path, 'rb') as in_fp:
            for row in in_fp:
                if AOU_GEN in row:
                    # skip it so it is appended below
                    warnings.warn(ERROR_APPENDING.format(in_path=in_path))
                else:
                    out_fp.write(row)

        # append new rows
        with open(AOU_GENERAL_CONCEPT_CSV_PATH, 'rb') as aou_gen_fp:
            _ = next(aou_gen_fp)  # skip header
            for row in aou_gen_fp:
                out_fp.write(row)


def append_vocabulary(in_path, out_path):
    new_row = get_aou_general_vocabulary_row()
    with open(out_path, 'wb') as out_fp:
        # copy original rows line by line for memory efficiency
        with open(in_path, 'rb') as in_fp:
            for row in in_fp:
                if AOU_GEN in row:
                    # skip it so it is appended below
                    warnings.warn(ERROR_APPENDING.format(in_path=in_path))
                else:
                    out_fp.write(row)
        # append new row
        out_fp.write(new_row)


def add_aou_general(in_dir, out_dir):
    fs = os.listdir(in_dir)
    concept_in_path = None
    vocabulary_in_path = None
    # Case-insensitive search for concept and vocabulary files
    for f in fs:
        t, _ = os.path.splitext(f.lower())
        in_path = os.path.join(in_dir, f)
        if t == CONCEPT:
            concept_in_path = in_path
        elif t == VOCABULARY:
            vocabulary_in_path = in_path
    if concept_in_path is None:
        raise IOError('CONCEPT.csv was not found in %s' % in_dir)
    if vocabulary_in_path is None:
        raise IOError('VOCABULARY.csv was not found in %s' % in_dir)

    concept_out_path = os.path.join(out_dir, os.path.basename(concept_in_path))
    append_concepts(concept_in_path, concept_out_path)

    vocabulary_out_path = os.path.join(out_dir, os.path.basename(vocabulary_in_path))
    append_vocabulary(vocabulary_in_path, vocabulary_out_path)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('command', choices=[TRANSFORM_FILES, ADD_AOU_GENERAL, APPEND_VOCABULARY, APPEND_CONCEPTS])
    parser.add_argument('--in_dir', required=True)
    parser.add_argument('--out_dir', required=True)
    args = parser.parse_args()
    if args.command == TRANSFORM_FILES:
        transform_files(args.in_dir, args.out_dir)
    elif args.command == ADD_AOU_GENERAL:
        add_aou_general(args.in_dir, args.out_dir)
    elif args.command == APPEND_VOCABULARY:
        append_vocabulary(args.file, args.out_dir)
    elif args.command == APPEND_CONCEPTS:
        append_concepts(args.file, args.out_dir)
