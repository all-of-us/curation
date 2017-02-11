# Sprint Reporter

Validate submissions for the All of Us data sprints. Visit the [website](https://cumc-dbmi.github.io/pmi_sprint_reporter/) for the sprint specifications.

## Requirements

 * Python 2.7.* (download from [here](https://www.python.org/downloads/) and install)
 * pip (download [get-pip.py](https://bootstrap.pypa.io/get-pip.py) and run `python get-pip.py`)

## Installation / Configuration

 * Install requirements by running
 
        pip install -r requirements.txt
 
 * Update `_settings.py` and rename it to `settings.py`
 
## reporter

    python reporter.py

This module checks files for conformance to All of Us Data Sprint specifications. It attempts to read CSV files in a configured directory and load them into a configured database, thereby creating an instance of the OMOP Common Data Model (CDM).

Errors and issues encountered along the way are logged to a table `pmi_spring_reporter_log`. An abbreviated log is written to `docs/_data/log.json` to be presented on the website [report page](https://cumc-dbmi.github.io/pmi_sprint_reporter/report.html).

## achilles

This runs an abbreviated version of Automated Characterization of Health Information at Large-scale Longitudinal Evidence Systems ([ACHILLES](http://www.ohdsi.org/analytic-tools/achilles-for-data-characterization/)). It creates and populates `ACHILLES_{analysis, results, results_derived, results_dist, HEEL_results}`, tables which help to characterize an OMOP CDM instance.

*Assumes `conn_str` in `settings` refers to a valid instance of OMOP CDM (e.g. `reporter` was run successfully).*

    python achilles.py

## webapi

This module helps to automate the backend configuration of [Atlas](http://www.ohdsi.org/web/wiki/doku.php?id=documentation:software:atlas), a tool which enables researchers to conduct scientific analyses on data sets conforming to the OMOP CDM standard. It populates `source` and `source_daimon` tables so Atlas can be used to analyze datasets created by [reporter](#reporter) and [achilles](#achilles).

*Assumes `webapi_conn_str_str` in `settings` refers to a valid instance of the application database (see [WebAPI](https://github.com/OHDSI/WebAPI)).*

    python webapi.py
