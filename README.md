# Sprint Reporter

Validate submissions for the All of Us data sprints. Visit the [website](https://cumc-dbmi.github.io/pmi_sprint_reporter/) for the sprint specifications.

This tool checks files for conformance to All of Us Data Sprint specifications. It attempts to read files in a configured directory, load them into a configured database and evaluate the content. Errors and issues encountered along the way are logged to a table `pmi_spring_reporter_log`.

## Requirements
 * Python 2.7.* (download from [here](https://www.python.org/downloads/) and install)
 * pip (download [get-pip.py](https://bootstrap.pypa.io/get-pip.py) and run `python get-pip.py`)

## Running
 * Install requirements by running
 
        pip install -r requirements.txt
 
 * Update `_settings.py` and rename it to `settings.py`
 * Run the reporter
 
        python reporter.py
