import os
import csv

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
LINE_TERMINATOR = '\r\n'


def _transform_csv(in_fp, out_fp):
    csv_reader = csv.reader(in_fp, delimiter=DELIMITER)
    header = next(csv_reader)
    date_indexes = []
    for i in range(0, len(header)):
        if header[i].endswith('_date'):
            date_indexes.append(i)
    csv_writer = csv.writer(out_fp, delimiter=DELIMITER, lineterminator=LINE_TERMINATOR)
    csv_writer.writerow(header)
    for row in csv_reader:
        for i in date_indexes:
            orig = row[i]
            dateparts = orig[0:4], orig[4:6], orig[6:8]
            row[i] = '-'.join(dateparts)
        csv_writer.writerow(row)


def transform_csv(file_path, out_dir):
    """
    Transform a local csv file (for BQ load) and save result in specified directory

    :param file_path: Path to the csv file
    :param out_dir: Directory to save the transformed file
    """
    file_name = os.path.basename(file_path)
    table_name, _ = os.path.splitext(file_name.lower())
    out_file_name = os.path.join(out_dir, file_name)
    with open(file_path, 'rb') as in_fp, open(out_file_name, 'wb') as out_fp:
        _transform_csv(in_fp, out_fp)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(name='command', choices=['transform_csv'], required=True)
    parser.add_argument('--file', required=True)
    parser.add_argument('--out_dir', required=True)
    args = parser.parse_args()
    transform_csv(args.file, args.out_dir)
