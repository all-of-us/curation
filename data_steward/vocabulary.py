import csv
import logging
import os
import sys
import warnings
import re

from common import CONCEPT, VOCABULARY, DELIMITER, LINE_TERMINATOR, TRANSFORM_FILES, \
    APPEND_VOCABULARY, APPEND_CONCEPTS, ADD_AOU_GENERAL, ERRORS, AOU_GEN_ID, AOU_GEN_VOCABULARY_CONCEPT_ID, \
    AOU_GEN_VOCABULARY_REFERENCE, ERROR_APPENDING, AOU_GEN_NAME
from resources import AOU_GENERAL_PATH, AOU_GENERAL_CONCEPT_CSV_PATH, hash_dir
from io import open

RAW_DATE_PATTERN = re.compile(r'\d{8}$')
BQ_DATE_PATTERN = re.compile(r'\d{4}-\d{2}-\d{2}$')

csv.field_size_limit(sys.maxsize)


def format_date_str(date_str):
    """
    Format a date string to yyyymmdd if it is not already
    :param date_str: the date string
    :return: the formatted date string
    :raises:  ValueError if a valid date object cannot be parsed from the string
    """
    if BQ_DATE_PATTERN.match(date_str):
        formatted_date_str = date_str
    elif RAW_DATE_PATTERN.match(date_str):
        parts = date_str[0:4], date_str[4:6], date_str[6:8]
        formatted_date_str = '-'.join(parts)
    else:
        raise ValueError('Cannot parse value {v} as date'.format(v=date_str))
    return formatted_date_str


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
        except (ValueError, IndexError) as e:
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

    try:
        os.makedirs(err_dir)
    except OSError:
        logging.info("Error directory:\t%s\t already exists", err_dir)

    with open(file_path, 'r') as in_fp, open(out_file_name, 'w') as out_fp, open(err_file_name, 'w') as err_fp:
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
    return DELIMITER.join([AOU_GEN_ID, AOU_GEN_NAME, AOU_GEN_VOCABULARY_REFERENCE, aou_gen_version,
                           AOU_GEN_VOCABULARY_CONCEPT_ID])


def append_concepts(in_path, out_path):
    with open(out_path, 'w') as out_fp:
        # copy original rows line by line for memory efficiency
        with open(in_path, 'r') as in_fp:
            for row in in_fp:
                if AOU_GEN_ID in row:
                    # skip it so it is appended below
                    warnings.warn(ERROR_APPENDING.format(in_path=in_path))
                else:
                    out_fp.write(row)

        # append new rows
        with open(AOU_GENERAL_CONCEPT_CSV_PATH, 'r') as aou_gen_fp:
            # Sending the first five lines of the file because tab delimiters
            # are causing trouble with the Sniffer and has_header method
            five_lines = ''
            for _ in range(0, 5):
                five_lines += aou_gen_fp.readline()

            has_header = csv.Sniffer().has_header(five_lines)
            aou_gen_fp.seek(0)
            # skip header if present
            if has_header:
                next(aou_gen_fp)
            for row in aou_gen_fp:
                out_fp.write(row)


def append_vocabulary(in_path, out_path):
    new_row = get_aou_general_vocabulary_row()
    with open(out_path, 'w') as out_fp:
        # copy original rows line by line for memory efficiency
        with open(in_path, 'r') as in_fp:
            for row in in_fp:
                if AOU_GEN_ID in row:
                    # skip it so it is appended below
                    warnings.warn(ERROR_APPENDING.format(in_path=in_path))
                else:
                    out_fp.write(row)
        # append new row
        out_fp.write(new_row)


def add_aou_general(in_dir, out_dir):
    file_names = os.listdir(in_dir)
    concept_in_path = None
    vocabulary_in_path = None
    # Case-insensitive search for concept and vocabulary files
    for file_name in file_names:
        table_name, _ = os.path.splitext(file_name.lower())
        in_path = os.path.join(in_dir, file_name)
        if table_name == CONCEPT:
            concept_in_path = in_path
        elif table_name == VOCABULARY:
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

    arg_parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    arg_parser.add_argument('command', choices=[TRANSFORM_FILES, ADD_AOU_GENERAL, APPEND_VOCABULARY, APPEND_CONCEPTS])
    arg_parser.add_argument('--in_dir', required=True)
    arg_parser.add_argument('--out_dir', required=True)
    args = arg_parser.parse_args()
    if args.command == TRANSFORM_FILES:
        transform_files(args.in_dir, args.out_dir)
    elif args.command == ADD_AOU_GENERAL:
        add_aou_general(args.in_dir, args.out_dir)
    elif args.command == APPEND_VOCABULARY:
        append_vocabulary(args.file, args.out_dir)
    elif args.command == APPEND_CONCEPTS:
        append_concepts(args.file, args.out_dir)
