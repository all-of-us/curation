import argparse
import papermill as pm
import sys


def parse_notebook_params(parser, expected_params):
    # parser = argparse.ArgumentParser()

    for param in expected_params:
        parser.add_argument(f"--{param}", required=True)

    args = parser.parse_args()
    print(args)
    print(sys.argv)
    if all([vars(args).get(arg) for arg in expected_params]):
        print("contains all args")


def main():
    parser = argparse.ArgumentParser(
        description="Execute a jupyter notebook with parameters")
    parser.add_argument('notebook_path', help='path to a jupyter notebook')
    # parser.add_argument('--vocab_path',
    #                     help='a text filing containing a list of terminologies')

    args = parser.parse_known_args()
    notebook_path = args[0].notebook_path

    contents = pm.inspect_notebook(notebook_path)
    parameter_names = contents.keys()

    parse_notebook_params(parser, parameter_names)

    # if not (set(parameter_names).issubset(set(args))

    # vocab_path = args.vocab_path

    # parameter_names = contents.keys()
    # print(vocab_path)
    # pm.execute_notebook(notebook_path,
    #                     'output.ipynb',
    #                     parameters={'vocabulary_file': vocab_path})


if __name__ == '__main__':
    main()