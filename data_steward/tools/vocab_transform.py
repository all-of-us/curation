import csv
import os


def transform_csv_file(file_path, out_dir):
    """
    Transform a local csv file (for BQ consumption) and save result in specified directory

    :param file_path: Path to the csv file
    :param out_dir: Directory to save the transformed file
    :return:
    """
    file_name = os.path.basename(file_path)
    table_name, _ = os.path.splitext(file_name.lower())
    out_file_name = os.path.join(out_dir, file_name)
    with open(file_path, 'rb') as csv_in, open(out_file_name, 'wb') as csv_out:
        csv_reader = csv.reader(csv_in, delimiter='\t')
        header = next(csv_reader)
        date_indexes = []
        for i in range(0, len(header)):
            if header[i].endswith('_date'):
                date_indexes.append(i)
        csv_writer = csv.writer(csv_out, delimiter='\t')
        csv_writer.writerow(header)
        for row in csv_reader:
            for i in date_indexes:
                orig = row[i]
                dateparts = orig[0:4], orig[4:6], orig[6:8]
                row[i] = '-'.join(dateparts)
            csv_writer.writerow(row)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--file')
    parser.add_argument('--out_dir')
    args = parser.parse_args()
    transform_csv_file(args.file, args.out_dir)
