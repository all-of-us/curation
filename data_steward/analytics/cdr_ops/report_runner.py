import argparse
import papermill as pm
import sys


def parse_notebook_params(notebook_path, expected_params):
    parser = argparse.ArgumentParser(
        prog=f"{sys.argv[0]} {notebook_path}",
        description=f"Execute notebook {notebook_path} with parameters")

    for param in expected_params:
        parser.add_argument(f"--{param}", required=True)

    args = parser.parse_args()
    # print(args)
    # print(sys.argv)
    # if all([vars(args).get(arg) for arg in expected_params]):
    #     print("contains all args")


def main():
    parser = argparse.ArgumentParser(
        description="Execute a jupyter notebook with parameters",
        add_help=False)
    parser.add_argument('notebook_path', help='path to a jupyter notebook')
    # parser.add_argument('--vocab_path',
    #                     help='a text filing containing a list of terminologies')

    args = parser.parse_known_args()
    notebook_path = args[0].notebook_path

    contents = pm.inspect_notebook(notebook_path)
    parameter_names = contents.keys()

    # parser2 = argparse.ArgumentParser(
    #     prog=f"{sys.argv[0]} {notebook_path}",
    #     description=f"Execute notebook {notebook_path} with parameters")

    # for param in parameter_names:
    #     parser2.add_argument(f"--{param}", required=True)

    # args2 = parser2.parse_known_args()

    # print(args2)
    parse_notebook_params(notebook_path, parameter_names)

    # if not (set(parameter_names).issubset(set(args))

    # vocab_path = args.vocab_path

    # parameter_names = contents.keys()
    # print(vocab_path)
    # pm.execute_notebook(notebook_path,
    #                     'output.ipynb',
    #                     parameters={'vocabulary_file': vocab_path})


if __name__ == '__main__':
    main()