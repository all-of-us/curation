import subprocess
import papermill as pm
import argparse
import logging


NOTEBOOK_TEMPLATE_NAME = "rdr_qc_template.ipynb"
EXECUTED_NOTEBOOK_NAME = "rdr_qc_export.ipynb"

logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

def generate_notebook(python_file):
    """Create a template notebook from a .py file using jupytext"""
    logger.info("Generating notebook from .py file...")
    subprocess.run(
        ["jupytext", python_file, f"-o={NOTEBOOK_TEMPLATE_NAME}", "--set-kernel", "-"]
    )
    return True


def run_notebook_qc(project_id, old_rdr, new_rdr):
    """Run the parameterized notebook using Papermill"""
    logger.info("Running the rdr export QC notebook...")
    pm.execute_notebook(
        f"{NOTEBOOK_TEMPLATE_NAME}",
        f"{EXECUTED_NOTEBOOK_NAME}",
        parameters=dict(project_id=project_id, old_rdr=old_rdr, new_rdr=new_rdr)
    )
    return True


def generate_report():
    """Convert the QC notebook into an HTML report"""
    logger.info("Generating HTML report for the rdr export QC...")
    subprocess.run(
        ["jupyter", "nbconvert", f"{EXECUTED_NOTEBOOK_NAME}", "--to=html", "--no-input"]
    )
    return True


if __name__ == "__main__":
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument('--project_id', type=str, required=True, help='Set the project id in BigQuery')
    my_parser.add_argument('--old_rdr', type=str, required=True, help='Set the dataset name for the old rdr export')
    my_parser.add_argument('--new_rdr', type=str, required=True, help='Set the dataset name for the most recent rdr export')

    args = my_parser.parse_args()
    PROJECT_ID = args.project_id
    OLD_RDR = args.old_rdr
    NEW_RDR = args.new_rdr

    generate_notebook("rdr_export_qc.py")
    run_notebook_qc(PROJECT_ID, OLD_RDR, NEW_RDR)
    generate_report()









