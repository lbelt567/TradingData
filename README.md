# Data Automation Framework

This repository automates the retrieval, parsing, and storage of data from multiple sources. The framework is designed to be extensible, supporting additional scripts for scraping and processing various data sources over time.

## Features
- **Automated Data Retrieval**: Fetches and processes data from specified URLs.
- **Data Parsing**: Uses custom logic to extract meaningful information and save it in structured formats.
- **CSV File Updates**: Automatically updates output files and commits changes to the repository.
- **Scheduled Execution**: Runs scripts on a predefined schedule using GitHub Actions.
- **Extensible Design**: Easily add new scripts to scrape and process additional data sources.

---

## Prerequisites
Before using this framework, ensure the following:
- **Python 3.10 or newer** installed on your local machine.
- Required dependencies installed.

---

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/<username>/<repository-name>.git
   cd <repository-name>

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt

## Usage
1. Run any script locally
   ```bash
   python <script_name>.py

---

## Automation with GitHub Actions
- The framework uses GitHub Actions to automate script execution. The current configuration:
- Executes scripts hourly using a cron schedule.
- Commits and pushes updated files back to the repository.

## Workflow Highlights

The workflow is defined in `.github/workflows/main.yaml` and includes:

- **Scheduled execution**:
  ```yaml
  on:
    schedule:
      - cron: '0 * * * *'  # Runs at the start of every hour
- Dependency management.
- Automatic file commits.

---

## Adding New Scripts
To add a new script for scraping or processing data:

- Create a new script file in the root directory (e.g., `new_source_script.py`).
- Define the logic for fetching and processing the data.
- Update the workflow file to include the new script.

## Additionally resourses
https://phoenixnap.com/kb/set-up-cron-job-linux
